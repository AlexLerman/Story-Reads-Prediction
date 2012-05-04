#!/usr/bin/python2.5
"""Create processed Pulse log files for a range of users when run as a program.

The only public function is create_fixtures, which behaves like the program.
"""

import sys, os, errno, time
from utilities import DELIMITER, check_num_arguments, report_time_elapsed, \
    write_iterable
from process_data import STORIES_FILENAME, READS_FILENAME, \
    CLICKTHROUGHS_FILENAME, PROCESSED_DATA_DIRECTORY, \
    PROCESSED_STORIES_FILE_PATH, PROCESSED_READS_FILE_PATH, \
    PROCESSED_CLICKTHROUGHS_FILE_PATH, EVENTS_USER_ID_INDEX, \
    EVENTS_STORY_ID_INDEX, NEW_EVENTS_TIMESTAMP_INDEX, open_safely

NUM_ARGUMENTS = 3
"""The expected number of arguments to this module when executed as a script.
The path to this file is included in this count.
"""

PROGRAM_USAGE = "Usage: %s <min_user_id> <max_user_id>" % __file__
# A description of how to run execute this program from the command-line.

def _convert_user_id_to_int(user_id):
    """Convert the given user ID from a str to an int.
    
    Print an error message to stderr if the given user ID could not be
    converted to an int.  Execution is immediately terminated in this case with
    exit signal errno.EINVAL.
    """
    try:
        return int(user_id)
    except ValueError:
        print >> sys.stderr, "User ID must be an integer but was %s." % user_id
        print >> sys.stderr, PROGRAM_USAGE
        sys.exit(errno.EINVAL)

def _get_largest_user_id(reads_list, clickthroughs_list):
    """Return the largest user ID in the given event lists.
    
    Return None if both lists are empty.  Assume that both lists are sorted in
    ascending lexicographic order and that each element of the lists is in the
    form (user_id, story_id, time_occurred).
    
    reads_list, a list, contains all read events for a range of users.
    clickthroughs_list, a list, contains all clickthrough events for a range of
    users.
    """
    if len(reads_list) > 0:
        if len(clickthroughs_list) > 0:
            return max(reads_list[-1][EVENTS_USER_ID_INDEX],
                       clickthroughs_list[-1][EVENTS_USER_ID_INDEX])
        return reads_list[-1][EVENTS_USER_ID_INDEX]
    if len(clickthroughs_list) > 0:
        return clickthroughs_list[-1][EVENTS_USER_ID_INDEX]
    return None

def _read_events(min_user_id, max_user_id, input_file_path):
    """Return a list of the events in the given file for the given users.

    Generate list elements of the form (user_id, story_id, time_occurred), and
    maintain the ordering of the input file.
    
    min_user_id, an int, is the smallest user ID that will be included, and is
    in processed form (i.e., 0, 1, 2) rather than the original 38-character
    hexadecimal format.
    max_user_id, an int, is the largest user ID that will be included, and is
    in processed form (i.e., 0, 1, 2) rather than the original 38-character
    hexadecimal format.
    inpurt_file_path, a str, is the file path to the Pulse event log file to
    read in.
    """
    events_list = []
    input_stream = open_safely(input_file_path)
    for event_as_str in input_stream:
        event_as_tuple = map(int, tuple(event_as_str[:-1].split(DELIMITER)))
        curr_user_id = event_as_tuple[EVENTS_USER_ID_INDEX]
        if (min_user_id <= curr_user_id) and (curr_user_id <= max_user_id):
            events_list.append(event_as_tuple)
    input_stream.close()
    return events_list

def _read_stories(story_ids):
    """Return a list of stories with the given IDs and a dict with new IDs.

    Generate list elements in newline-terminated str form, where each element is
    a single line of the processed stories log file.  Generate dict entries
    mapping from story IDs in the input file to story IDs in the output file.
    Maintain the ordering of the input file in the list.  This ordering is
    equivalent to ascending order of both old story IDs and new story IDs.

    story_ids, a set, contains the IDs of the stories to include in the output.
    """
    old_story_id = 0
    new_story_id = 0
    stories_list = []
    story_id_dict = {}
    input_stream = open_safely(PROCESSED_STORIES_FILE_PATH)
    for story in input_stream:
        if old_story_id in story_ids:
            stories_list.append(story)
            story_id_dict[old_story_id] = new_story_id
            new_story_id += 1
        old_story_id += 1
    input_stream.close()
    return (stories_list, story_id_dict)

def _write_events(events_list, output_file_path, story_id_dict, user_id_offset):
    """Write the given events to the given output file using new story IDs.
    
    Maintain the ordering of events_list in the output file.  Write events in
    newline-delimited raw text format.  Within each event, delimit fields by
    DELIMITER.  Write events with fields (new_user_id, new_story_id,
    time_occurred), where new user IDs start from 0.  Assume the the first
    element in events_list belongs to the user with the smallest ID of those in
    the list.
    
    events_list, a list, contains all the events of a given type (reads or
    clickthroughs) for a range of users.  Each element of events_list is in the
    form (old_user_id, old_story_id, time_occurred).
    output_file_path, a str, is the file path to which to output events.
    story_id_dict, a dict, maps from old story IDs to new story IDs.
    user_id_offset, an int, is the value that must be subtracted from an old
    user ID to produce the corresponding new user ID.
    """ 
    output_stream = open_safely(output_file_path, "w")
    
    for old_event in events_list:
        old_user_id = old_event[EVENTS_USER_ID_INDEX]
        new_user_id = old_user_id - user_id_offset
        old_story_id = old_event[EVENTS_STORY_ID_INDEX]
        new_story_id = story_id_dict[old_story_id]
        time_occurred = old_event[NEW_EVENTS_TIMESTAMP_INDEX]
        new_event = (new_user_id, new_story_id, time_occurred)
        output_stream.write(DELIMITER.join(map(str, new_event)) + "\n")
    
    output_stream.close()

def create_fixtures(min_user_id, max_user_id):
    """Create processed Pulse log files with data only for the given users.

    Assume processed data is available in PROCESSED_DATA_DIRECTORY.  Include
    only events performed by the given users and stories referenced in such
    events.  Reassign story IDs to account for the omission of other stories.
    Reassign the given user IDs to 0, 1, 2, etc. to account for the omission
    of other users.  Place output in a directory named Fixtures for Users
    min_user_id-max_user_id within PROCESSED_DATA_DIRECTORY, creating such a
    directory if it does not already exist.
    
    min_user_id, an int, is the smallest user ID to include in the output, and
    is in processed form (i.e., 0, 1, 2) rather than the original 38-character
    hexadecimal format.
    max_user_id, an int, is the largest user ID to include in the output, and
    is in processed form (i.e., 0, 1, 2) rather than the original 38-character
    hexadecimal format.
    """
    start_time = time.time()
    if not isinstance(min_user_id, int) or not isinstance(max_user_id, int):
        raise TypeError("min_user_id and max_user_id must both be of type int.")
    if min_user_id > max_user_id:
        raise ValueError(("min_user_id is %d but must be less than or " + \
                          "equal to max_user_id, which is %d.") % \
                          (min_user_id, max_user_id))
    if min_user_id < 0:
        raise ValueError(("min_user_id is %d, but user IDs must be " +
                          "non-negative.") % min_user_id)
    reads_list = _read_events(min_user_id, max_user_id,
                              PROCESSED_READS_FILE_PATH)
    clickthroughs_list = _read_events(min_user_id, max_user_id,
                                      PROCESSED_CLICKTHROUGHS_FILE_PATH)
    max_user_id_found = _get_largest_user_id(reads_list, clickthroughs_list)
    if max_user_id_found is None:
        raise LookupError(("No User IDs in the range [%d, %d] were found in" + \
                           " the processed data.") % (min_user_id, max_user_id))
    if max_user_id_found < max_user_id:
        raise LookupError(("max_user_id is %d, but the largest user ID in " + \
                          "the processed data is %d.") % \
                          (max_user_id, max_user_id_found))
    story_ids = frozenset([event[EVENTS_STORY_ID_INDEX] for event in \
                           reads_list + clickthroughs_list])
    stories_list, story_id_dict = _read_stories(story_ids)
    output_directory = "%sFixtures for Users %d-%d/" % \
        (PROCESSED_DATA_DIRECTORY, min_user_id, max_user_id)
    if not os.path.exists(output_directory):
        os.mkdir(output_directory)
    output_reads_path = output_directory + READS_FILENAME
    _write_events(reads_list, output_reads_path, story_id_dict, min_user_id)
    output_clickthroughs_path = output_directory + CLICKTHROUGHS_FILENAME
    _write_events(clickthroughs_list, output_clickthroughs_path, story_id_dict,
                  min_user_id)
    output_stories_path = output_directory + STORIES_FILENAME
    write_iterable(stories_list, output_stories_path, "")
    print("Output fixtures in directory: %s" % output_directory)
    report_time_elapsed(start_time)

if __name__ == "__main__":
    check_num_arguments(NUM_ARGUMENTS, PROGRAM_USAGE)
    _min_user_id = _convert_user_id_to_int(sys.argv[1])
    _max_user_id = _convert_user_id_to_int(sys.argv[2])
    create_fixtures(_min_user_id, _max_user_id)
