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

# Project imports
import resources

DEFAULT_LOG_DIR = os.path.join(resources.base_path, 'logs')
"""Default location for log files"""

_LOG_FMT = '%(asctime)s - %(levelname)s - %(name)s - %(message)s'
_LOG_DATEFMT = '%Y-%m-%d %H:%M:%S'


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
    """
    Sets up python logging to file

    :param filename:  name of the log file
    :param console_logging: if False will only create FileHandler, if True
                            will create both FileHandler and StreamHandler
    """
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
    :param console_logging:  determines if log is output to desired and/or default file
                             and console, or just the desired and/or default file
    """

    log_path = generate_paths(log_filepath_list)

    log_list = []

    for filename in log_path:
        log_list.append(create_logger(filename, console_logging))
    return log_list


def get_default_log_path(logger_name):
    """
    Get the log file path associated with a logger
    with the specified name

    :param logger_name: name of the logger
    :return: absolute path to the log file
    """
    return os.path.join(DEFAULT_LOG_DIR, f'{logger_name}.log')


def get_logger(logger_name):
    """
    Get a logger with the specified name, creating it if necessary.
    The logger writes >= INFO logs to stderr and >=DEBUG logs to 
    a file at a standard location `DEFAULT_LOG_DIR`.

    :param logger_name: name of the logger (usually set to __name__)
    :return: the logger
    :example:
    >>> LOGGER = get_logger(__name__)
    >>> # Additional handlers can be added if needed
    >>> LOGGER.addHandler(logging.FileHandler('my_custom.log'))
    >>> def func(p1, p2):
    >>>     LOGGER.debug(f"func called with {p1}, {p2}")
    """
    logger = logging.getLogger(logger_name)
    # explicitly set logger level to DEBUG otherwise
    # the level would be that of the closest ancestor
    logger.setLevel(logging.DEBUG)

    # prevent adding handlers more than once
    if not logger.hasHandlers():
        formatter = logging.Formatter(fmt=_LOG_FMT, datefmt=_LOG_DATEFMT)

        # handler that emits >=INFO records to stderr
        stream_handler = logging.StreamHandler()
        stream_handler.setLevel(logging.INFO)
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)

        # handler that emits >= DEBUG records by
        # appending to new or existing file
        handler_filename = get_default_log_path(logger_name)
        file_handler = logging.FileHandler(handler_filename, mode='a')
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger
