#!/usr/bin/python2.5

import sys, os, errno, logging, random, bisect, math, time
from collections import defaultdict
from gensim import corpora, models, similarities
from utilities import DELIMITER, open_safely
from process_data import STORIES_FILENAME, READS_FILENAME, \
    EARLIEST_ACCEPTABLE_TIMESTAMP, LATEST_ACCEPTABLE_TIMESTAMP, \
    NEW_STORIES_FEED_URL_INDEX, NEW_STORIES_TITLE_INDEX, \
    STORIES_TIMESTAMP_INDEX, EVENTS_USER_ID_INDEX, EVENTS_STORY_ID_INDEX, \
    NEW_EVENTS_TIMESTAMP_INDEX
from stem_processed_stories import STEMMED_STORIES_EXTENSION
from liblinearutil import parameter, problem, train, predict
from svmutil import svm_parameter, svm_problem, svm_train, svm_predict


"""
Are we going to get dictionary data from just positives (probably not),
positives and selected negatives, or every story seen thus far (both
reasonable)?
"""
STOPLIST = frozenset(['a', 'able', 'about', 'across', 'after', 'all', 'almost',
                      'also', 'am', 'among', 'an', 'and', 'any', 'are', 'as',
                      'at', 'be', 'because', 'been', 'but', 'by', 'can',
                      'cannot', 'could', 'dear', 'did', 'do', 'does', 'either',
                      'else', 'ever', 'every', 'for', 'from', 'get', 'got',
                      'had', 'has', 'have', 'he', 'her', 'hers', 'him', 'his',
                      'how', 'however', 'i', 'if', 'in', 'into', 'is', 'it',
                      'its', 'just', 'least', 'let', 'like', 'likely', 'may',
                      'me', 'might', 'most', 'must', 'my', 'neither', 'no',
                      'nor', 'not', 'of', 'off', 'often', 'on', 'only', 'or',
                      'other', 'our', 'own', 'rather', 'said', 'say', 'says',
                      'she', 'should', 'since', 'so', 'some', 'than', 'that',
                      'the', 'their', 'them', 'then', 'there', 'these', 'they',
                      'this', 'tis', 'to', 'too', 'twas', 'us', 'wa', 'want',
                      'wants', 'was', 'we', 'were', 'what', 'when', 'where',
                      'which', 'while', 'who', 'whom', 'why', 'will', 'with',
                      'would', 'yet', 'you', 'your', 's', 've', 'd', 're', 'll',
                      't', 'nt'])

############################################

"""
Reads in the story data to a matrix for quick acess. 
"""
def _read_stories():
    stories = []
    story_stream = open_safely(STORIES_FILE_PATH)
    for story_as_str in story_stream:
        story_as_list = story_as_str[:-1].lower().split(DELIMITER)
        time_first_read = int(story_as_list[STORIES_TIMESTAMP_INDEX])
        story_as_list[STORIES_TIMESTAMP_INDEX] = time_first_read
        stories.append(tuple(story_as_list))
    story_stream.close()
    return stories

"""
Events is a matrix of story reads by user. Each user has a vector of stories they have read and when they read them. 
"""
def _read_events():
    event_stream = open_safely(EVENTS_FILE_PATH)
    events = [tuple(map(int, event[:-1].split(DELIMITER))) for event in \
              event_stream]
    event_stream.close()
    return events

def _binary_search(list, elem):
    index_of_leftmost_match = bisect.bisect_left(list, (elem, ))
    i = index_of_leftmost_match + 1
    list_length = len(list)
    while (i < list_length) and (list[i][0] == elem):
        i += 1
    return range(index_of_leftmost_match, i)

"""
Retreives all positive samples from a user (the stories they have read) that occur either before
the current date (training) or after i.
"""
def _get_pos_samples(stories, events, user_id, curr_day, corpus_dict, predict):
    corpus = []
    feedlist = set()
    for event in events:
        if (event[EVENTS_USER_ID_INDEX] == user_id) and \
                ((event[NEW_EVENTS_TIMESTAMP_INDEX] <= curr_day) != predict):
            story = stories[event[EVENTS_STORY_ID_INDEX]]
            tokenized_title = story[NEW_STORIES_TITLE_INDEX].split()
            corpus.append(corpus_dict.doc2bow(tokenized_title, True))
            feedlist.add(story[NEW_STORIES_FEED_URL_INDEX])
    return corpus, feedlist

"""
Retreives negative samples (stories not read by the user).
It gets as many negative samples as positive ones  (mostly to limit runtime).
For training, _get_neg_samples() reselects (up to the number of positive samples)
the samples that have been previously misclassified.
It uses binary search to find the stories in feeds that they are subscribed to but did not read.

"""
def _get_neg_samples(stories, events, user_id, curr_day, corpus_dict, predict,
                     feedlist, num_get, reselect):
    corpus_list = []
    indexes_of_users_events = _binary_search(events, user_id)
    ids_of_stories_user_read = \
        frozenset([events[event_index][EVENTS_STORY_ID_INDEX] for event_index \
                   in indexes_of_users_events])
    for feed_url in feedlist:
        ids_of_stories_in_feed = frozenset(_binary_search(stories, feed_url))
        ids_of_stories_user_ignored = \
            ids_of_stories_in_feed.difference(ids_of_stories_user_read)
        stories_user_ignored = [(stories[story_id], story_id) for story_id in \
                                ids_of_stories_user_ignored]
        for story in stories_user_ignored:
            if (story[0][STORIES_TIMESTAMP_INDEX] <= curr_day) != predict:
                corpus_list.append((story[0][NEW_STORIES_TITLE_INDEX], story[1],
                                    story[0][STORIES_TIMESTAMP_INDEX]))
    sampled_corpus_list=[]
    if not predict:
        if (len(reselect)) > num_get:
            sampled_corpus_list= random.sample(reselect, num_get)
        else:
            sampled_corpus_list= reselect
    coprus_list= list(frozenset(corpus_list).difference(frozenset(sampled_corpus_list)))
    if (len(corpus_list) +len(sampled_corpus_list)) <= num_get:
        sampled_corpus_list += corpus_list
    else:
        sampled_corpus_list += random.sample(corpus_list, (num_get-len(sampled_corpus_list)))
    corp= [corpus_dict.doc2bow(story_title[0].split(), True) for story_title in sampled_corpus_list]
    if predict:
        chosen_stories = sampled_corpus_list
    else:
        chosen_stories = []
    return (corp,  chosen_stories)

"""
Gets the corpus to be trained or tested on, runs tfidf on it and then
returns it.
"""
def _tfidf(tfidf, dictionary, stories, events, user_id, curr_time, reselect,
           predict):
    # positive
    corpus_pos, feedlist = \
        _get_pos_samples(stories, events, user_id, curr_time, dictionary,
                         predict)
    num_pos = len(corpus_pos)
    
    # negative
    corpus_neg, chosen_stories = \
        _get_neg_samples(stories, events, user_id, curr_time, dictionary,
                         predict, feedlist, num_pos, reselect)
    pos_corpus = tfidf[corpus_pos]
    to_trans = corpus_pos + corpus_neg
    trans_corpus = tfidf[to_trans]
    num_neg = len(corpus_neg)
    return(trans_corpus, pos_corpus, num_pos, num_neg, chosen_stories)

"""
Creates tfidf model
"""
def _build_tfidf_model(corpus_dict, stories, curr_day):
    my_corpus = [corpus_dict.doc2bow(story[NEW_STORIES_TITLE_INDEX].split()) \
                 for story in stories if \
                 story[STORIES_TIMESTAMP_INDEX] <= curr_day]
    return models.TfidfModel(my_corpus)

"""
Normalizes the tfidf vectors in preperation for input into the SVM.
"""
def normalize(tf_idf_scores_as_list):
    score_means = defaultdict(float)
    for document in tf_idf_scores_as_list:
        for word_index, word in enumerate(document):
            word_id = word[0]
            tf_idf_score = word[1]
            score_means[word_id] += tf_idf_score
            document[word_index] = list(word) 
    
    num_documents = len(tf_idf_scores_as_list)
    for word_id, sum_of_scores in score_means.iteritems():
        score_means[word_id] = sum_of_scores / num_documents
    
    score_standard_deviations = defaultdict(float)
    for document in tf_idf_scores_as_list:
        for word in document:
            word_id = word[0]
            word[1] -= score_means[word_id]
            mean_adjusted_score = word[1]
            score_standard_deviations[word_id] += mean_adjusted_score * \
                mean_adjusted_score
    
    for word_id, sum_of_squares in score_standard_deviations.iteritems():
        score_standard_deviations[word_id] = \
            math.sqrt(sum_of_squares / num_documents)
    
    for document in tf_idf_scores_as_list:
        for word_index, word in enumerate(document):
            word_id = word[0]
            standard_deviation = score_standard_deviations[word_id]
            if standard_deviation != 0.0:
                word[1] /= standard_deviation
            document[word_index] = tuple(word)
    
    return tf_idf_scores_as_list

"""
Converts the matrix into a libSVM readable form.
Calls normalize to normalize the tfidf scores. 
"""
def _convert_to_sparse_matrix(tf_idf_scores, num_pos, num_neg, option):
    if option:
        matrix = []
        num = 0
        for line in tf_idf_scores:
            if line == []:
                if num < num_pos:
                    num_pos -= 1
                else:
                    num_neg -= 1
                num -= 1
            else:
                matrix.append(line)
                num += 1
        normalized_tf_idf_scores = normalize(matrix)
        return (map(dict, normalized_tf_idf_scores), num_pos, num_neg)
    else:
        tf_idf_scores_as_list = list(tf_idf_scores)
        normalized_tf_idf_scores = normalize(tf_idf_scores_as_list)
        return (map(dict, normalized_tf_idf_scores), num_pos, num_neg)

def _convert_to_matrix(input):
    matrix=[]
    for line in input:
        line=list(line)
        num=0
        to_con=[]
        for elem in line:
            if elem != 0:
                to_con+=[(num, elem)]
            num+=1
        matrix+=[to_con]
    return matrix

################################################

"""
Calculates the precision, recall and f-1 score.
"""
def _p_r_f_one(actual, predicted):
    num = 0
    true_pos = 0
    false_pos = 0
    false_neg = 0
    for actual_label in actual:
        predicted_label = predicted[num]
        if actual_label == predicted_label:
            if predicted_label == 1:
                true_pos += 1
        else:
            if predicted_label == 1:
                false_pos += 1
            else:
                false_neg += 1
        num += 1
    
    num_predicted_pos = true_pos + false_pos
    if num_predicted_pos == 0:
        p = 0
    else:
        p = true_pos / float(num_predicted_pos)
    
    num_actual_pos = true_pos + false_neg
    if num_actual_pos == 0:
        r = 0
    else:
        r = true_pos / float(num_actual_pos)
    
    f1_denominator = p + r
    if f1_denominator == 0:
        f = 0
    else:
        f = (2 * p * r) / float(f1_denominator)
    
    return (p, r, f)

################################################


def _lib_train_liblinear(user_tfidf, num_pos, num_neg, ignore):
    param = parameter('-s 0')
    sparse_user_tfidf, num_pos, num_neg = \
        _convert_to_sparse_matrix(user_tfidf, num_pos, num_neg, ignore)
    labels = ([1] * num_pos) + ([-1] * num_neg)
    prob = problem(labels, sparse_user_tfidf)
    modellog = train(prob, param)
    return modellog

def _lib_predict_liblinear(to_predict, num_pos, num_neg, modellog):
    sparse_to_predict, num_pos, num_neg = \
        _convert_to_sparse_matrix(to_predict, num_pos, num_neg, False)
    labels_predict = ([1] * num_pos) + ([-1] * num_neg)
    p_labs, p_acc, p_vals = predict(labels_predict, sparse_to_predict, modellog)
    return (p_labs, p_acc, p_vals, labels_predict)

def _lib_train_libsvm(user_tfidf, num_pos, num_neg, ignore):
    sparse_user_tfidf, num_pos, num_neg = \
        _convert_to_sparse_matrix(user_tfidf, num_pos, num_neg, ignore)
    labels = ([1] * num_pos) + ([-1] * num_neg)

    param = svm_parameter("-t %d" % KERNEL_NUMBER)
    prob = svm_problem(labels, sparse_user_tfidf)
    modellog = svm_train(prob, param)
    return modellog

def _lib_predict_libsvm(to_predict, num_pos, num_neg, modellog):
    sparse_to_predict, num_pos, num_neg = \
        _convert_to_sparse_matrix(to_predict, num_pos, num_neg, False)
    labels_predict = ([1] * num_pos) + ([-1] * num_neg)
    p_labs, p_acc, p_vals = svm_predict(labels_predict, sparse_to_predict, modellog)
    return (p_labs, p_acc, p_vals, labels_predict)


"""
Switchboard for function call. Can use liblinear, libsvm or gensim cosine similarity. 
"""
def _train_and_predict(user_tfidf, pos_tfidf, to_predict, num_pos_train,
                       num_neg_train, num_pos_predict, num_neg_predict,
                       version, ignore):
    if version == "liblinear" or version=="Liblinear":
        p_labs, p_acc, p_vals, labels_predict = \
            _lib_predict_liblinear(to_predict, num_pos_predict, num_neg_predict,
                                   _lib_train_liblinear(user_tfidf,
                                                        num_pos_train,
                                                        num_neg_train, ignore))
        return (p_labs, p_vals, labels_predict)
    if version == "libsvm" or version == "Libsvm":
        p_labs, p_acc, p_vals, labels_predict = \
            _lib_predict_libsvm(to_predict, num_pos_predict, num_neg_predict,
                                _lib_train_libsvm(user_tfidf, num_pos_train,
                                                  num_neg_train, ignore))
        return (p_labs, p_vals, labels_predict)
    if version == "similarity" or version == "Similarity":
        index = similarities.SparseMatrixSimilarity(pos_tfidf) 
        train_sims = index[user_tfidf]
        sims = index[to_predict]
        p_labs, p_acc, p_vals, labels_predict = \
            _lib_predict_libsvm(_convert_to_matrix(sims), num_pos_predict,
                                   num_neg_predict, _lib_train_libsvm( \
                                   _convert_to_matrix(train_sims),
                                   num_pos_train, num_neg_train, ignore))
        return (p_labs, [], labels_predict)

################################################

"""
Start of the heart of the program.  Goes over all of the days for every user
"""
def classify(version):
    if not os.path.exists(OUTPUT_DIRECTORY):
        os.mkdir(OUTPUT_DIRECTORY)
    ignore = False
    logging.basicConfig(format = '%(asctime)s : %(levelname)s : %(message)s',
    		    level = logging.INFO)
    
    random.seed()
     
    ############################################
    

    stories = _read_stories()
    events = _read_events()
    #NUM_USERS_TO_ANALYZE = 500

    user_list = [[] for i in range(NUM_USERS_TO_ANALYZE)]
    day = 0
    max_day = 30 
    curr_day = EARLIEST_ACCEPTABLE_TIMESTAMP
    curr_day +=  SECONDS_IN_DAY
    reselect_by_user = [[] for i in range(NUM_USERS_TO_ANALYZE)]
    while (day < max_day):
        corpus_dict = corpora.Dictionary(story[NEW_STORIES_TITLE_INDEX].split() \
                                         for story in stories if \
                                         story[STORIES_TIMESTAMP_INDEX] <= curr_day)
        # remove stop words and words that appear only once
        stop_ids = [corpus_dict.token2id[stopword] for stopword in STOPLIST if \
                    stopword in corpus_dict.token2id]
        once_ids = [tokenid for tokenid, docfreq in corpus_dict.dfs.iteritems() if \
                    docfreq == 1]
        '''	for tokenid in once_ids:
            # replace token with UNK
    	    corpus_dict[tokenid] = "UNK"'''
        
        # remove stop words and words that appear only once
        corpus_dict.filter_tokens(stop_ids + once_ids)
    
        # remove gaps in id sequence after words that were removed
        corpus_dict.compactify()
    ####################
    #      tf-idf      #
    ####################
    
        tfidf = _build_tfidf_model(corpus_dict, stories, curr_day)
        for user_id in range(NUM_USERS_TO_ANALYZE):
            #user_id+=904
            user_tfidf, pos_tfidf, num_pos_train, num_neg_train, to_ignore = \
                _tfidf(tfidf, corpus_dict, stories, events, user_id, curr_day,
                       reselect_by_user[user_id], False)
            #reselect_by_user = [[] for i in range(NUM_USERS_TO_ANALYZE)]
            if user_tfidf != []:
                # modelsvm = train(labels, corpus_tfidf)
                to_predict, other_tfidf, num_pos_predict, num_neg_predict, \
                    chosen_stories = _tfidf(tfidf, corpus_dict, stories, events,
                                            user_id, curr_day, [] , True)
               
                if to_predict != []:
                    p_labs, p_vals, labels_predict = \
                        _train_and_predict(user_tfidf, pos_tfidf, to_predict,
                                           num_pos_train, num_neg_train,
                                           num_pos_predict, num_neg_predict,
                                           version, ignore)
                    reselect = []
                    num_bool = True
                    for i in range(len(p_labs)):
                        if  labels_predict[i] == -1:
                            if num_bool:
                                num_pos_predict = i
                                num_bool = False
                            if p_labs[i] == 1:
                                next_day = curr_day + SECONDS_IN_DAY
                                if chosen_stories[i-num_pos_predict][2] <= next_day:
                                    reselect+=[chosen_stories[i-num_pos_predict]]
                    reselect_by_user[user_id]+=reselect
                    p, r, f= _p_r_f_one(labels_predict, p_labs)
                    user_list[user_id].append((p,r,f, day))
        
        curr_day += SECONDS_IN_DAY
        day += 1
               
                
    user_a_p = 0
    user_a_r = 0
    user_a_f = 0
    skipped = 0
    print("Read stories from %s" % STORIES_FILE_PATH)
    print("Read events from %s" % EVENTS_FILE_PATH)
    print("%d users were analyzed" % NUM_USERS_TO_ANALYZE)
    output_file_name = "reselect.py %s %s %d %d output written at %d.txt" % \
        (version, sys.argv[2], KERNEL_NUMBER, NUM_USERS_TO_ANALYZE, time.time())
    output_file_path = OUTPUT_DIRECTORY + output_file_name
    print("Outputting precision, recall, and f_1 scores to %s" % \
          output_file_path)
    user_id =0
    output_stream = open_safely(output_file_path, "w")
    for user in user_list:
        av_p = 0
        av_r = 0
        for results in user:
            av_p += results[0]
            av_r += results[1]
            f_1 = results[2]
            day = results[3]
            output_stream.write("%.3f\t%.3f\t%.3f\t%d\t%d\n" % \
                                (results[0], results[1], f_1, day, user_id))
        if len(user) > 0:
            av_p = av_p / float(len(user))
            av_r = av_r / float(len(user))
            denominator = av_p + av_r
            if denominator == 0.0:
                av_f = 0.0
            else:
                av_f = (2 * av_p * av_r) / float(av_p + av_r)
            user_a_p += av_p
            user_a_r += av_r
        else:
            skipped += 1
        user_id += 1
    user_a_p = user_a_p / float(NUM_USERS_TO_ANALYZE - skipped)
    user_a_r = user_a_r / float(NUM_USERS_TO_ANALYZE - skipped)
    denominator = user_a_p + user_a_r
    if denominator == 0.0:
        user_a_f = 0.0
    else:
        user_a_f = (2 * user_a_p * user_a_r) / float(user_a_p + user_a_r)
    output_stream.write("%.3f\t%.3f\t%.3f\t-1\t-1\n" % \
                        (user_a_p, user_a_r, user_a_f))
    output_stream.close()

if __name__ == "__main__":
    
    if len(sys.argv) > 6:
        print >> sys.stderr, "Expected fewer arguments."
        sys.exit(errno.E2BIG)

    if len(sys.argv) < 6:
        print >> sys.stderr, "Expected more arguments."
        sys.exit(errno.EINVAL)
    
    STEMMING = (sys.argv[2].lower() == "y")
    KERNEL_NUMBER = int(sys.argv[3])
    NUM_USERS_TO_ANALYZE = int(sys.argv[4]) 
    LOG_FILE_PATH = sys.argv[5]
    SECONDS_IN_DAY = 86400
    STEMMED_STORIES_FILENAME = STORIES_FILENAME + STEMMED_STORIES_EXTENSION
    if STEMMING:
        STORIES_FILE_PATH = LOG_FILE_PATH + STEMMED_STORIES_FILENAME
    else:
        STORIES_FILE_PATH = LOG_FILE_PATH + STORIES_FILENAME
    EVENTS_FILE_PATH = LOG_FILE_PATH + READS_FILENAME
    OUTPUT_DIRECTORY = "../Final Results/"
    classify(sys.argv[1])
