#!/usr/bin/python2.5

import sys, os, functools, time, socket, html2text, pprint
from collections import defaultdict
from utilities import DELIMITER, check_num_arguments, report_time_elapsed, \
    open_safely

NUM_ARGUMENTS = 1
"""The expected number of arguments to this module when executed as a script.
The path to this file is included in this count.
"""

PROGRAM_USAGE = "Usage: %s" % __file__
# A description of how to run execute this program from the command-line.

"""
A boolean that determines whether to only use story titles or to fetch the full
story bodies.
"""
FETCH_FULL_STORIES = True

"""
The length of time in seconds to wait for a server containing a story before
giving up and skipping it.
"""
TIMEOUT_LENGTH = 10

# The shortest string that I will believe is a bonafide extracted story.
MIN_STORY_LENGTH = 256

# File names of the input and output data.
STORIES_FILENAME = "stories_v2.log"
READS_FILENAME = "user_story_reads_v2.log"
CLICKTHROUGHS_FILENAME = "user_story_clickthroughs_v2.log"
USER_IDS_FILENAME = "user_ids.log" # This file has no corresponding input file.

# File path to directory input data is read from.
RAW_DATA_DIRECTORY = "../Raw Data/"

# File paths to the input data, log files provided by Pulse.
RAW_STORIES_FILE_PATH = RAW_DATA_DIRECTORY + STORIES_FILENAME
RAW_READS_FILE_PATH = RAW_DATA_DIRECTORY + READS_FILENAME
RAW_CLICKTHROUGHS_FILE_PATH = RAW_DATA_DIRECTORY + CLICKTHROUGHS_FILENAME

# File path to the directory in which to write output data.
PROCESSED_DATA_DIRECTORY = "../Processed Data/"

# File paths to which to output processed data.
PROCESSED_STORIES_FILE_PATH = PROCESSED_DATA_DIRECTORY + STORIES_FILENAME
PROCESSED_READS_FILE_PATH = PROCESSED_DATA_DIRECTORY + READS_FILENAME
PROCESSED_CLICKTHROUGHS_FILE_PATH = PROCESSED_DATA_DIRECTORY + \
    CLICKTHROUGHS_FILENAME
USER_IDS_FILE_PATH = PROCESSED_DATA_DIRECTORY + USER_IDS_FILENAME

# Brief descriptions of the type of data in each input and/or output log file.
STORIES_DESCRIPTOR = "stories"
READS_DESCRIPTOR = "user story reads"
CLICKTHROUGHS_DESCRIPTOR = "user story clickthroughs"
USER_IDS_DESCRIPTOR = "user IDs"

"""
This timestamp corresponds to the beginning of August 2011.  Since Alphonso Labs
stated the data came from this month, data timestamped before August 2011 is
suspect.  The timestamp is represented in seconds since the Unix epoch.
"""
EARLIEST_ACCEPTABLE_TIMESTAMP = 1312156800

EARLIEST_ACCEPTABLE_YEAR = time.gmtime(EARLIEST_ACCEPTABLE_TIMESTAMP).tm_year

"""
This timestamp corresponds to the end of August 2011.  Since Alphonso Labs
stated the data came from this month, data timestamped after August 2011 is
suspect.  The timestamp is represented in seconds since the Unix epoch.
"""
LATEST_ACCEPTABLE_TIMESTAMP = 1314835199

LATEST_ACCEPTABLE_YEAR = time.gmtime(LATEST_ACCEPTABLE_TIMESTAMP).tm_year

"""
Zero-based field indices for raw story data.  The story log file format differs
from that of the other two input files.
"""
OLD_STORIES_URL_INDEX = 0
OLD_STORIES_TITLE_INDEX = 1
OLD_STORIES_FEED_URL_INDEX = 2
OLD_STORIES_FEED_TITLE_INDEX = 3
STORIES_TIMESTAMP_INDEX = 4
STORIES_NUM_FIELDS = 5

"""
Zero-based field indices for processed story data.  The output format differs
from the intput format, because the feed_url and feed_title fields are moved to
the left of the story_url and story_title fields so that a lexicographic sort
arranges the stories by feed.
"""
NEW_STORIES_FEED_URL_INDEX = 0
NEW_STORIES_FEED_TITLE_INDEX = 1
NEW_STORIES_URL_INDEX = 2
NEW_STORIES_TITLE_INDEX = 3

"""
Zero-based field indices for raw user read and clickthrough data.  Both the user
read and clickthrough log files have the same format.
"""
EVENTS_USER_ID_INDEX = 0
EVENTS_STORY_URL_INDEX = 1
EVENTS_STORY_TITLE_INDEX = 2
EVENTS_FEED_URL_INDEX = 3
EVENTS_FEED_TITLE_INDEX = 4
OLD_EVENTS_TIMESTAMP_INDEX = 5
OLD_EVENTS_NUM_FIELDS = 6

"""
Zero-based field indices for processed user read and clickthrough data.  The
output format differs from the input format because the story_url, story_title,
feed_url, and feed_title fields are replaced by a single story_id field.
"""
EVENTS_STORY_ID_INDEX = 1
NEW_EVENTS_TIMESTAMP_INDEX = 2

"""
Adds the given story to the given defaultdict if no stories already in the
defaultdict differ from the given story by at most the time at which they were
first read.  If a story has multiple times first read associated it with it,
then, somewhat arbitrarily, the earliest of these is retained and all others
discarded.  Reorders the fields to ensure that stories are eventually sorted by
feed.  story is a row of data, formatted as a list, from the stories log file.
time_first_read is one of the times at which the given story is purported to
have first been read.  time_first_read is represented as seconds since the Unix
epoch.  stories_dict is a defaultdict with a default value greater than or equal
to any valid time first read.  Intended for use as a callback function in
_clean_data.
"""
def _insert_story(story, time_first_read, stories_dict, to_ignore):
    key = (story[OLD_STORIES_FEED_URL_INDEX],
           story[OLD_STORIES_FEED_TITLE_INDEX], story[OLD_STORIES_URL_INDEX],
           story[OLD_STORIES_TITLE_INDEX])
    stories_dict[key] = min(stories_dict[key], time_first_read)

"""
Adds the given story to the given dict if no stories already in the dict differ
from the given story by at most the time at which they were first read.  If a
story has multiple times first read associated it with it, then, somewhat
arbitrarily, the earliest of these is retained and all others discarded.
Reorders the fields to ensure that stories are eventually sorted by feed.  Skips
stories for which story content could not be fetched, those for which the story
content was less than MIN_STORY_LENGTH characters long, and those with
story_urls for which a previous attempt to fetch story content failed.  story is
a row of data, formatted as a list, from the stories log file.  time_first_read
is one of the times at which the given story is purported to have first been
read.  time_first_read is represented as seconds since the Unix epoch.
story_contents_dict is a dict mapping from story_urls to a best guess at the
full contents of the story at said URL or None if the story contents could not
be extracted.  Intended for use as a callback function in _clean_data.

TODO: Handle the authorization problem with websites like
www.filmschoolrejects.com, which immediately follows
http://www.f-secure.com/weblog/archives/00002226.html
TODO: Check if feed_url and story_url are valid absolute URIs.
"""
def _insert_full_story(story, time_first_read, stories_dict,
                       story_contents_dict):
    story_url = story[OLD_STORIES_URL_INDEX]
    key = (story[OLD_STORIES_FEED_URL_INDEX],
           story[OLD_STORIES_FEED_TITLE_INDEX], story_url,
           story[OLD_STORIES_TITLE_INDEX])
    if key in stories_dict:
        value = stories_dict[key]
        value[0] = min(value[0], time_first_read)
    elif story_url in story_contents_dict:
        story_contents = story_contents_dict[story_url]
        if story_contents is not None:
            stories_dict[key] = [time_first_read, story_contents]
    else:
        pprint.pprint(key, sys.stderr)
        story_contents = html2text.extractFromURL(story_url)
        if (story_contents is not None) and \
                (len(story_contents) >= MIN_STORY_LENGTH):
            stories_dict[key] = [time_first_read, story_contents]
            story_contents_dict[story_url] = story_contents
        else:
            story_contents_dict[story_url] = None

"""
Adds the given user event to the given set to filter out duplicates.  Events are
represented as (user_id, story_id, time_occurred) tuples.  Skips events that
reference stories that don't appear in the given dict because the time at which
such stories were first read is unknown.  event is a row, formatted as a list,
from either the user reads or clickthroughs log file.  time_occurred is the time
at which the given event occurred.  time_occurred is represented as seconds
since the Unix epoch.  stories_dict is a mapping from (feed_url, feed_title,
story_url, story_title) tuples to the corresponding story_id.  Intended for use
as a callback function in _clean_data.
"""
def _insert_event(event, time_occurred, stories_dict, events_set):
    key = (event[EVENTS_FEED_URL_INDEX], event[EVENTS_FEED_TITLE_INDEX],
           event[EVENTS_STORY_URL_INDEX], event[EVENTS_STORY_TITLE_INDEX])
    if key in stories_dict:
        story_id = stories_dict[key]
        element = (event[EVENTS_USER_ID_INDEX], story_id, time_occurred)
        events_set.add(element)

"""
Calls the given callback function with a nicely formatted version of the given
row of data from one of the Pulse log files, or does nothing if the row is
corrupt.  The row is corrupt if it contains the wrong number of fields, empty
fields, an improperly formatted timestamp, or an unrealistic timestamp.  The
input row is formatted as a string with the final newline character removed.
num_fields is the number of fields of data per row.  timestamp_index is the
zero-based index of the field containing the timestamp in the input row.
insert_data_fn is a callback function that accepts four parameters: a row,
formatted as a list, from the log file; the row's timestamp represented as
seconds since the Unix epoch; stories_dict; and callback_data.  stories_dict is
a dict with keys corresponding to valid stories.  stories_dict is either built
up by insert_data_fn during the execution of _clean_data or has already been
constructed during a prior call to _clean_data.  The callee of _clean_data can
pass additional state to insert_data_fn using the callback_data parameter.
"""
def _clean_row(row, num_fields, timestamp_index, insert_data_fn, stories_dict,
               callback_data):
    split_row = row.split(DELIMITER)
    if len(split_row) == num_fields:
        try:
            timestamp_as_struct = time.strptime(split_row[timestamp_index],
                                                "%Y-%m-%d %H:%M:%S")
        except ValueError:
            return
        if (timestamp_as_struct.tm_year >= EARLIEST_ACCEPTABLE_YEAR) and \
               (timestamp_as_struct.tm_year <= LATEST_ACCEPTABLE_YEAR):
            timestamp_as_float = time.mktime(timestamp_as_struct)
            timestamp_as_int = int(timestamp_as_float)
            if (timestamp_as_int >= EARLIEST_ACCEPTABLE_TIMESTAMP) and \
                   (timestamp_as_int <= LATEST_ACCEPTABLE_TIMESTAMP):
                if all([field != "" for field in split_row]):
                    insert_data_fn(split_row, timestamp_as_int, stories_dict,
                                   callback_data)

"""
Reads the given Pulse log file, filtering out corrupted rows, and calling the
given callback function once for each well-formed row.  The raw log file that
still contains corrupted rows is located at input_file_path.  num_fields is the
number of fields of data per row.  timestamp_index is the zero-based index of
the field containing the timestamp in the log file.  data_descriptor is a string
briefly describing the data in the plural form that is used to notify the user
upon completion.  insert_data_fn is a callback function that accepts four
parameters: a row, formatted as a list, from the log file; the row's timestamp
represented as seconds since the Unix epoch; stories_dict; and callback_data.
stories_dict is a dict mapping with keys corresponding to valid stories.
stories_dict is either built up by insert_data_fn during the execution of
_clean_data or has already been constructed during a prior call to _clean_data.
stories_dict is assumed to already have been built if and only if it isn't empty
when passed in.  The callee can pass additional state to insert_data_fn using
the callback_data parameter.
"""
def _clean_data(input_file_path, num_fields, timestamp_index, data_descriptor,
                insert_data_fn, stories_dict, callback_data = None):
    start_time = time.time()
    stories_dict_already_built = (len(stories_dict) > 0)
    num_rows = 0
    input_stream = open_safely(input_file_path)
    for row in input_stream:
        num_rows += 1
        row_without_newline = row[:-1]
        _clean_row(row_without_newline, num_fields, timestamp_index,
                   insert_data_fn, stories_dict, callback_data)
    
    input_stream.close()
    
    if stories_dict_already_built:
        # We just cleaned user reads or clickthroughs.
        num_valid_rows = len(callback_data)
    else:
        # We just cleaned stories.
        num_valid_rows = len(stories_dict)
    
    num_invalid_rows = num_rows - num_valid_rows
    discard_rate = float(100 * num_invalid_rows) / float(num_rows)
    print("Read a total of %d %s, %d (%.2f%%) of which were discarded." %
          (num_rows, data_descriptor, num_invalid_rows, discard_rate))
    report_time_elapsed(start_time)

"""
Replaces the original 38-character hexadecimal user IDs found in the input log
files with 0, 1, 2, etc. to reduce output file size and memory consumption of
programs that read in the processed data.  user_ids_dict is a dict from the
original user IDs to their replacements.  events_list is input as a list of
(original_user_id, story_id, time_occurred) tuples and output as a list of
(new_user_id, story_id, time_occurred) tuples.
"""
def _reassign_user_ids(user_ids_dict, events_list):
    for event_num, event in enumerate(events_list):
        original_user_id = event[EVENTS_USER_ID_INDEX]
        new_user_id = user_ids_dict[original_user_id]
        events_list[event_num] = (new_user_id, event[EVENTS_STORY_ID_INDEX],
                                  event[NEW_EVENTS_TIMESTAMP_INDEX])

"""
Returns a list of the original user IDs found in the given lists of user events.
User IDs are sorted in ascending lexicographic order.  The original user IDs in
the given lists are replaced with the corresponding indices of these user IDs in
the returned list of user IDs.
"""
def get_user_ids(reads_list, clickthroughs_list):
    start_time = time.time()
    user_ids_set = set()
    for read in reads_list:
        user_ids_set.add(read[EVENTS_USER_ID_INDEX])
    for clickthrough in clickthroughs_list:
        user_ids_set.add(clickthrough[EVENTS_USER_ID_INDEX])
    user_ids_list = sorted(user_ids_set)
    user_ids_dict = dict([(original_user_id, new_user_id) for \
                          (new_user_id, original_user_id) in \
                          enumerate(user_ids_list)])
    _reassign_user_ids(user_ids_dict, reads_list)
    _reassign_user_ids(user_ids_dict, clickthroughs_list)
    print("Reassigned %s from original values to 0, 1, 2, etc." % \
          USER_IDS_DESCRIPTOR)
    report_time_elapsed(start_time)
    return user_ids_list

"""
Writes the stories in the given dict to PROCESSED_STORIES_FILE_PATH in ascending
lexicographic order.  Stories are output in newline-delimited raw text format.
Within a given row, fields are delimited by DELIMITER.  Replaces the value of
each story in the given dict with the zero-based row number of the story in
PROCESSED_STORIES_FILE_PATH.  Thus, when the function returns, stories_dict
contains a mapping from stories to their IDs.  The output fields are (feed_url,
feed_title, story_url, story_title) if FETCH_FULL_STORIES is False.  If
FETCH_FULL_STORIES is true, then feed_title is replaced by feed_title + " " +
story_contents.
"""
def _write_stories(stories_dict):
    start_time = time.time()
    sorted_stories = sorted(stories_dict.keys())
    row_num = 0
    output_stream = open_safely(PROCESSED_STORIES_FILE_PATH, "w")
    for story_key in sorted_stories:
        if FETCH_FULL_STORIES:
            story_timestamp, story_contents = stories_dict[story_key]
            story_title_with_contents = story_key[NEW_STORIES_TITLE_INDEX] + \
                " " + story_contents
            story_sans_timestamp_as_tuple = \
                (story_key[NEW_STORIES_FEED_URL_INDEX],
                 story_key[NEW_STORIES_FEED_TITLE_INDEX],
                    story_key[NEW_STORIES_URL_INDEX],
                    story_title_with_contents)
            story_sans_timestamp_as_str = \
                DELIMITER.join(story_sans_timestamp_as_tuple)
        else:
            story_timestamp = stories_dict[story_key]
            story_sans_timestamp_as_str = DELIMITER.join(story_key)
        story_timestamp_as_str = DELIMITER + str(story_timestamp)
        story_as_str = story_sans_timestamp_as_str + story_timestamp_as_str
        output_stream.write(story_as_str + "\n")
        stories_dict[story_key] = row_num
        row_num += 1
    output_stream.close()
    print("Wrote %d cleaned and sorted %s to %s" %
          (row_num, STORIES_DESCRIPTOR, PROCESSED_STORIES_FILE_PATH))
    report_time_elapsed(start_time)

"""
Writes the user events in the given list to the given output file.  Events are
output in newline-delimited raw text format.  Within a given row, fields are
delimited by DELIMITER.  event_descriptor is a string briefly describing the
events in the plural form that is used to notify the user upon completion.
"""
def _write_events(events_list, output_file_path, event_descriptor):
    start_time = time.time()
    output_stream = open_safely(output_file_path, "w")
    for event in events_list:
        output_stream.write(DELIMITER.join(map(str, event)) + "\n")
    output_stream.close()
    num_events = len(events_list)
    print("Wrote %d cleaned and sorted %s to %s" %
          (num_events, event_descriptor, output_file_path))
    report_time_elapsed(start_time)

"""
Writes the user IDs in the given list to USER_IDS_FILE_PATH.  User IDs are
output in newline-delimited raw text format.  The IDs written are the original
hexadecimal IDs from the raw Pulse log files, and the new IDs are the row
numbers of the old IDs in the output file, USER_IDS_FILE_PATH.
"""
def _write_user_ids(user_ids_list):
    start_time = time.time()
    output_stream = open_safely(USER_IDS_FILE_PATH, "w")
    for user_id in user_ids_list:
        output_stream.write(user_id + "\n")
    output_stream.close()
    num_users = len(user_ids_list)
    print(("Wrote %d cleaned and sorted original 38-character hexadecimal %s " +
           "to %s") % (num_users, USER_IDS_DESCRIPTOR, USER_IDS_FILE_PATH))
    report_time_elapsed(start_time)

"""
Creates a clean version of the raw log files for the Pulse project.  Corrupt and
duplicate data as well as data that references missing or corrupt data are
excluded from the output.  Conflicting data are resolved where possible.  Output
files are written to PROCESSED_DATA_DIRECTORY with same filenames as the inputs.
One additional file with name USER_IDS_FILENAME is written that contains all of
the user IDs present in the processed data sorted in ascending lexicographic
order.
"""
def process_data():
    socket.setdefaulttimeout(TIMEOUT_LENGTH)

    if FETCH_FULL_STORIES:
        stories_dict = {}
        insert_story_fn = _insert_full_story
        story_contents_dict = {}
    else:
        timestamp_factory = functools.partial(int, LATEST_ACCEPTABLE_TIMESTAMP)
        stories_dict = defaultdict(timestamp_factory)
        insert_story_fn = _insert_story
        story_contents_dict = None
    _clean_data(RAW_STORIES_FILE_PATH, STORIES_NUM_FIELDS,
                STORIES_TIMESTAMP_INDEX, STORIES_DESCRIPTOR, insert_story_fn,
                stories_dict, story_contents_dict)
    
    if not os.path.exists(PROCESSED_DATA_DIRECTORY):
        os.mkdir(PROCESSED_DATA_DIRECTORY)
    
    _write_stories(stories_dict)
    
    reads_set = set()
    _clean_data(RAW_READS_FILE_PATH, OLD_EVENTS_NUM_FIELDS,
                OLD_EVENTS_TIMESTAMP_INDEX, READS_DESCRIPTOR, _insert_event,
                stories_dict, reads_set)
    clickthroughs_set = set()
    _clean_data(RAW_CLICKTHROUGHS_FILE_PATH, OLD_EVENTS_NUM_FIELDS,
                OLD_EVENTS_TIMESTAMP_INDEX, CLICKTHROUGHS_DESCRIPTOR,
                _insert_event, stories_dict, clickthroughs_set)
    
    reads_list = sorted(reads_set)
    clickthroughs_list = sorted(clickthroughs_set)
    user_ids_list = get_user_ids(reads_list, clickthroughs_list)
    _write_events(reads_list, PROCESSED_READS_FILE_PATH, READS_DESCRIPTOR)
    _write_events(clickthroughs_list, PROCESSED_CLICKTHROUGHS_FILE_PATH,
                  CLICKTHROUGHS_DESCRIPTOR)
    _write_user_ids(user_ids_list)

if __name__ == "__main__":
    check_num_arguments(NUM_ARGUMENTS, PROGRAM_USAGE)
    process_data()
