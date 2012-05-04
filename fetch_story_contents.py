#!/usr/bin/python2.5
"""Create processed Pulse log files with story contents when run as a program.

The only public function is fetch_story_contents, which behaves like the
program.
"""

import sys, os, time, socket, html2text
from os import path
from utilities import DELIMITER, check_num_arguments, write_iterable, \
    write_2d_iterable
from process_data import TIMEOUT_LENGTH, MIN_STORY_LENGTH, STORIES_FILENAME, \
    READS_FILENAME, CLICKTHROUGHS_FILENAME, USER_IDS_FILENAME, \
    STORIES_DESCRIPTOR, READS_DESCRIPTOR, CLICKTHROUGHS_DESCRIPTOR, \
    NEW_STORIES_URL_INDEX, NEW_STORIES_TITLE_INDEX, EVENTS_STORY_ID_INDEX, \
    open_safely, report_time_elapsed, get_user_ids

NUM_ARGUMENTS = 2
"""The expected number of arguments to this module when executed as a script.
The path to this file is included in this count.
"""

PROGRAM_USAGE = "Usage: %s <input_directory>" % __file__
# A description of how to run execute this program from the command-line.

SUB_DIRECTORY_NAME = "Processed Data with Story Contents/"
# The name of the directory in which to place the output log files.

def _read_stories(input_file_path):
    """Return a list of stories with full contents and a dict with new IDs.

    Generate list elements in tuple form, where each element corresponds to a
    single line of the given processed stories log file.  Do not trim the
    newline off the end of the last element of the tuple.  Append a space
    followed by the full story contents to the title of each story.  Omit
    stories for which the full story contents could not be fetched.  Generate
    dict entries mapping from story IDs in the input file to story IDs in the
    output file.  Maintain the ordering of the input file in the list.  This
    ordering is equivalent to ascending order of both old story IDs and new
    story IDs.  Be warned that fetching full story contents is quite slow and
    consumes a great deal of bandwidth, so you or other users on your network
    may experience connectivity problems while executing this function.

    input_file_path, a str, is the file path to the processed Pulse stories log
    file that contains story URLs and titles but not the full contents of the
    stories themselves.
    """
    start_time = time.time()
    old_story_id = 0
    new_story_id = 0
    stories_list = []
    story_id_dict = {}
    story_contents_dict = {}
    socket.setdefaulttimeout(TIMEOUT_LENGTH)
    input_stream = open_safely(input_file_path)
    
    for story_as_str in input_stream:
        story_as_list = story_as_str.split(DELIMITER)
        story_url = story_as_list[NEW_STORIES_URL_INDEX]
        if story_url in story_contents_dict:
            story_contents = story_contents_dict[story_url]
        else:
            story_contents = html2text.extractFromURL(story_url)
            if (story_contents is not None) and \
                    (len(story_contents) <= MIN_STORY_LENGTH):
                story_contents = None
            story_contents_dict[story_url] = story_contents
        if story_contents is not None:
            story_as_list[NEW_STORIES_TITLE_INDEX] += " " + story_contents
            stories_list.append(tuple(story_as_list))
            story_id_dict[old_story_id] = new_story_id
            new_story_id += 1
        old_story_id += 1
        
    input_stream.close()
    num_stories_discarded = old_story_id - new_story_id
    discard_rate = float(100 * num_stories_discarded) / float(old_story_id)
    print(("Read a total of %d %s, %d (%.2f%%) of which were discarded " + \
           "because their full contents could not be fetched.") % \
           (old_story_id, STORIES_DESCRIPTOR, num_stories_discarded,
            discard_rate))
    report_time_elapsed(start_time)
    return (stories_list, story_id_dict)

def _read_events(story_id_dict, input_file_path, event_descriptor):
    """"Return a list of the events in the given file with the given story IDs.

    Generate list elements in [user_id, new_story_id, time_occurred] form, and
    maintain the ordering of the input file.
    
    inpurt_file_path, a str, is the file path to the Pulse event log file to
    read in.
    story_id_dict, a dict, maps from old story IDs to new story IDs.  Only
    events with story IDs that are keys in story_id_dict are retained in the
    output, but these old story IDs are replaced with the corresponding new
    story IDs.
    event_descriptor, a str, briefly describes the events in the plural form and
    is used to notify the user upon completion.
    """
    events_list = []
    num_events = 0
    num_events_kept = 0
    input_stream = open_safely(input_file_path)
    for event_as_str in input_stream:
        event_as_list = map(int, event_as_str[:-1].split(DELIMITER))
        old_user_id = event_as_list[EVENTS_STORY_ID_INDEX]
        if old_user_id in story_id_dict:
            event_as_list[EVENTS_STORY_ID_INDEX] = story_id_dict[old_user_id]
            events_list.append(event_as_list)
            num_events_kept += 1
        num_events += 1
    input_stream.close()
    num_events_discarded = num_events - num_events_kept
    discard_rate = float(100 * num_events_discarded) / float(num_events)
    print(("Read a total of %d %s, %d (%.2f%%) of which were discarded " + \
           "because the full contents of the associated story could not be " + \
           "fetched.") % (num_events, event_descriptor, num_events_discarded,
                          discard_rate))
    return events_list

def fetch_story_contents(input_directory):
    """Write processed Pulse log files with full story contents.
    
    Output files by the same name in a sub-directory of the given directory
    named SUB_DIRECTORY_NAME.  Append a space followed by the full story
    contents to the story titles.  Remove stories for which no content could not
    be fetched and events involving these stories.  Reassign story and user IDs
    to 0, 1, 2, etc. to fill the resulting gaps in the ID sequences.  Output a
    user IDs log file in which row numbers correspond to the new user IDs, and
    row values correspond to the old user IDs.
    
    input_directory, a str, is the file path to a directory containing processed
    Pulse log files lacking full story contents.
    """
    if not isinstance(input_directory, str):
        raise TypeError("Expected input_directory to be of type str.")
    
    if not path.isdir(input_directory):
        raise ValueError("Could not find given directory: %s" % input_directory)
    
    input_stories_path = path.join(input_directory, STORIES_FILENAME)
    stories_list, story_id_dict = _read_stories(input_stories_path)
    input_reads_path = path.join(input_directory, READS_FILENAME)
    reads_list = _read_events(story_id_dict, input_reads_path, READS_DESCRIPTOR)
    input_clickthroughs_path = path.join(input_directory,
                                         CLICKTHROUGHS_FILENAME)
    clickthroughs_list = _read_events(story_id_dict, input_clickthroughs_path,
                                      CLICKTHROUGHS_DESCRIPTOR)
    user_ids_list = get_user_ids(reads_list, clickthroughs_list) 
    reads_list = [map(str, read) for read in reads_list]
    clickthroughs_list = [map(str, clickthrough) for clickthrough in \
                          clickthroughs_list]
    user_ids_list = map(str, user_ids_list)
    
    output_directory = path.join(input_directory, SUB_DIRECTORY_NAME)
    if not path.exists(output_directory):
        os.mkdir(output_directory)
    
    output_stories_path = path.join(output_directory, STORIES_FILENAME)
    write_2d_iterable(stories_list, output_stories_path, "")
    output_reads_path = path.join(output_directory, READS_FILENAME)
    write_2d_iterable(reads_list, output_reads_path)
    output_clickthroughs_path = path.join(output_directory,
                                          CLICKTHROUGHS_FILENAME)
    write_2d_iterable(clickthroughs_list, output_clickthroughs_path)
    output_users_path = path.join(output_directory, USER_IDS_FILENAME)
    write_iterable(user_ids_list, output_users_path)

if __name__ == "__main__":
    check_num_arguments(NUM_ARGUMENTS, PROGRAM_USAGE)
    fetch_story_contents(sys.argv[1])
