#!/usr/bin/python2.5

import sys, re, time
from nltk.stem.porter import PorterStemmer
from nltk.tokenize.regexp import WordPunctTokenizer
from utilities import DELIMITER, check_num_arguments, report_time_elapsed, \
    open_safely, write_2d_iterable
from process_data import NEW_STORIES_TITLE_INDEX

NUM_ARGUMENTS = 2
"""
The expected number of arguments to this module when executed as a script.  The
path to this file is included in this count.
"""

PROGRAM_USAGE = "Usage: %s <stories_file_path>" % __file__
# A description of how to run execute this program from the command-line.

STEMMED_STORIES_EXTENSION = ".stemmed"

############################################

def stem_processed_stories(input_file_path):
    """
    """
    start_time = time.time()
    if not isinstance(input_file_path, str):
        raise TypeError("Expected input_file_path to be of type str.")
    
    stemmer = PorterStemmer()
    stories_list = []
    prog = re.compile('\W+')
    story_stream = open_safely(input_file_path)
    for story_as_str in story_stream:
        story_as_list = story_as_str[:-1].lower().split(DELIMITER)
        story_title = story_as_list[NEW_STORIES_TITLE_INDEX]
        tok_contents = WordPunctTokenizer().tokenize(story_title)
        stem_contents = [stemmer.stem(word) for word in tok_contents if \
                         prog.match(word) is None]
        story_as_list[NEW_STORIES_TITLE_INDEX] = " ".join(stem_contents)
        stories_list.append(story_as_list)
    
    story_stream.close()
    output_file_path = input_file_path + STEMMED_STORIES_EXTENSION
    write_2d_iterable(stories_list, output_file_path)
    print("Output stemmed stories to %s" % output_file_path)
    report_time_elapsed(start_time)

if __name__ == "__main__":
    check_num_arguments(NUM_ARGUMENTS, PROGRAM_USAGE)
    stem_processed_stories(sys.argv[1])
