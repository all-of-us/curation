"""
Enforces common logging conventions and code reuse.

Original Issue = DC-637

The intent of this module is to allow other modules to setup logging easily without
duplicating code.
"""

# Python imports
import logging
import os
from datetime import datetime


def generate_paths(log_filepath_list):
    """
    Generates filepaths from the list of passed filepaths

    :param log_filepath_list:  desired string path and or name of the log file
                               example: ['path/', 'faked.log', 'path/fake.log']
    """
    default_file_name = datetime.now().strftime('curation%Y%m%d_%H%M%S.log')

    # iterates through log_filepath_list and sets path to
    # provided path and default_file_name
    default_output_log_path = [
        os.path.join(filepath, default_file_name)
        for filepath in log_filepath_list
        if 'log' not in os.path.basename(filepath)
    ]

    # iterates through log_filepath_list and sets path to default path
    # if just filename was item passed in log_filepath_list
    default_path = 'logs/'
    default_log_path = [
        os.path.join(default_path, filename)
        for filename in log_filepath_list
        if not os.path.dirname(filename)
    ]

    # iterates through log_filepath_list and sets path to log files
    output_log_path = [
        os.path.join(filepath)
        for filepath in log_filepath_list
        if os.path.dirname(filepath) and 'log' in os.path.basename(filepath)
    ]

    # appends all generated filepaths to list of log_paths
    log_path = default_output_log_path + default_log_path + output_log_path

    # if any path in log_path list doesn't exist, it will be created
    for path in log_path:
        if not os.path.isdir(path):
            try:
                os.makedirs(os.path.dirname(path))
            except OSError:
                # directory already exists
                pass

    return log_path


def create_logger(filename, console_logging=False):
    # gets new logger, will be created if doesn't exist
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)

    # formatters for both FileHandler and StreamHandler
    file_formatter = logging.Formatter(
        fmt='%(asctime)s - %(levelname)s - %(name)s - %(message)s',
        datefmt='%(asctime)s')
    stream_formatter = logging.Formatter(
        '%(levelname)s - %(name)s - %(message)s')

    file_handler = logging.FileHandler(filename)
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)

    if console_logging is True:
        stream_handler = logging.StreamHandler()
        stream_handler.setLevel(logging.INFO)
        stream_handler.setFormatter(stream_formatter)
        logger.addHandler(stream_handler)

    return logging.getLogger(filename)


def setup_logger(log_filepath_list, console_logging=True):
    """
    Sets up python logging to file and console for use in other modules.

    :param log_filepath_list:  desired string path and or name of the log file
                               example: ['path/', 'faked.log', 'path/fake.log']
    :param console_logging:  determines if log is output to desire and/or default file
                             and console, or just the desired and/or default file
    """

    log_path = generate_paths(log_filepath_list)
    print(f'log_path: {log_path}')

    log_list = []

    for filename in log_path:
        log_list.append(create_logger(filename, console_logging))
    return log_list
