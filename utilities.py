#!/usr/bin/python2.5
"""Contains various utility functions useful for processing Pulse data.

check_num_arguments halts a program if the wrong number of arguments were
supplied.
report_time_elapsed prints a message notifying the user of the time elapsed.
open_safely opens a file or halts the program if the file could not be opened.
write_iterable writes the contents of an iterable to a text file.
write_2d_iterable writes the contents of an iterable of iterables to a text
file.
"""

import sys, errno, time
from datetime import timedelta

DELIMITER = "\t"
"""The string used to separate each field in the input and output log files.
This string should not appear in the fields themselves.
"""


def check_num_arguments(num_arguments_expected, program_usage):
    """Terminate execution if the wrong number of arguments were supplied.
    
    Print an error message to sys.stderr notifying the user that an incorrect
    number of command-line arguments were passed.
    
    num_arguments_expected, an int, is the number of arguments the command-line
    program expects to receive, including the name of the program itself.
    program_usage, a str, explains how to use a program via the command-line.
    It is used to inform the user how to correct their attempt to invoke the
    program.
    """
    if len(sys.argv) > num_arguments_expected:
        print >> sys.stderr, "Expected fewer arguments."
        print >> sys.stderr, program_usage
        sys.exit(errno.E2BIG)

    if len(sys.argv) < num_arguments_expected:
        print >> sys.stderr, "Expected more arguments."
        print >> sys.stderr, program_usage
        sys.exit(errno.EINVAL)

def report_time_elapsed(start_time):
    """Print a message notifying the user of the time elapsed since start_time.
    
    Round the elapsed time to the nearest second.
    
    start_time, an int, is represented in seconds since the Unix epoch.
    """
    stop_time = time.time()
    time_elapsed_as_float = stop_time - start_time
    time_elapsed_as_int = int(round(time_elapsed_as_float))
    time_elapsed_as_str = str(timedelta(seconds=time_elapsed_as_int))
    print("Time required: " + time_elapsed_as_str + " (HH:MM:SS).")

def open_safely(file_path, mode = "r"):
    """Open the file with the given file path in the given mode.
    
    Return the corresponding stream if successful.  If the file could not be
    opened, then print an error message to sys.stderr and exit the program.  The
    caller is responsible for closing the stream when finished using the file.
    
    Refer to the documentation of the built-in Python function, open for an
    explanation of the parameters.
    """
    try:
        return open(file_path, mode)
    except IOError:
        print >> sys.stderr, "Could not open file: %s in mode %s" % \
            (file_path, mode)
        sys.exit(errno.EIO)


def write_iterable(iterable, output_file_path, delimiter = "\n"):
    """Write the contents of the given iterable to the given output file.
    
    Output elements of the iterable in the order in which they are supplied.
    Add the given delimiter after each element.
    
    iterable, a iterable with str elements, is assumed to have contents small
    enough to fit on the supplied hardware.
    output_file_path, a str, is the file path to which to output the iterable.
    delimiter, a str, is added after each element of the outer iterable in the
    output.
    """
    output_stream = open_safely(output_file_path, "w")
    for element in iterable:
        output_stream.write(element + delimiter)
    output_stream.close()

def write_2d_iterable(iterable, output_file_path, delimiter = "\n"): 
    """Write the contents of the given 2-dimensional iterable to the given file.
    
    Delimit elements of the inner iterables with DELIMITER.
    
    iterable is an iterable whose elements are themselves iterables.  These
    inner iterables' elements are of type str.
    delimiter, a str, is added after each element of the outer iterable in the
    output.
    """
    output_stream = open_safely(output_file_path, "w")
    for outer_element in iterable:
        output_stream.write(DELIMITER.join(outer_element) + delimiter)
    output_stream.close()
