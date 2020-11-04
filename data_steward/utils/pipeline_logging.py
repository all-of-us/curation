"""
Enforces common logging conventions and code reuse.

Original Issue = DC-637

The intent of this module is to allow other modules to setup logging easily without
duplicating code.
"""

# Python imports
import logging
import logging.config
import os
import sys
from datetime import datetime

# Project imports
import resources

DEFAULT_LOG_DIR = os.path.join(resources.base_path, 'logs')
"""Default location for log files"""
DEFAULT_LOG_LEVEL = logging.INFO

LOG_FORMAT = '%(asctime)s - %(levelname)s - %(name)s - %(message)s'
LOG_DATEFMT = '%Y-%m-%d %H:%M:%S'
FILENAME_FMT = '%Y%m%d'

_FILE_HANDLER = 'curation_file_handler'
"""Identifies the file log handler"""
_CONSOLE_HANDLER = 'curation_console_handler'
"""Identifies the console handler"""

_DEFAULT_CONFIG = {
    'version': 1,
    'formatters': {
        'default': {
            'class': 'logging.Formatter',
            'format': LOG_FORMAT,
            'datefmt': LOG_DATEFMT
        }
    },
    'handlers': {
        _CONSOLE_HANDLER: {
            'class': 'logging.StreamHandler',
            'formatter': 'default',
        },
        _FILE_HANDLER: {
            'class': 'logging.FileHandler',
            'mode': 'a',
            'formatter': 'default'
        }
    },
    'root': {
        'handlers': [_CONSOLE_HANDLER, _FILE_HANDLER]
    },
    # otherwise defaults to True which would disable
    # any loggers that exist at configuration time
    'disable_existing_loggers': False
}
"""A dict describing the configuration. Only to be used by `get_config`
which provides missing required attributes."""


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


def _get_date_str():
    """
    Get current date formatted using FILENAME_FMT
    :return: 
    """
    return datetime.today().strftime(FILENAME_FMT)


def _get_log_file_path():
    """
    Get the abs path of the log file to use. 
    
    The location is DEFAULT_LOG_DIR and the file name is the current
    date formatted using FILENAME_FMT.

    :return: absolute path to the log file
    """
    date_str = _get_date_str()
    return os.path.join(DEFAULT_LOG_DIR, f'{date_str}.log')


def _get_config(filename, level):
    """
    Get a dictionary which describes the logging configuration
    
    :param filename: Create the FileHandler using the specified filename.
    :param level: Set the root logger level to the specified level 
                  (i.e. logging.{DEBUG,INFO,WARNING,ERROR}).
    :return: the default configuration dict
    """
    # copy _DEFAULT_CONFIG to avoid modifying it
    default_config = dict(_DEFAULT_CONFIG)
    default_config['root']['level'] = level
    file_log_handler_dict = default_config['handlers'][_FILE_HANDLER]
    file_log_handler_dict['filename'] = filename
    return default_config


def _except_hook(exc_type, exc_value, exc_traceback):
    """
    Log exception info to root logger. Used as a hook for uncaught exceptions prior 
    to system exit.
    
    :param exc_type: type of the exception
    :param exc_value: the exception
    :param exc_traceback: the traceback associated with the exception
    """
    root_logger = logging.getLogger()
    root_logger.critical('Uncaught exception',
                         exc_info=(exc_type, exc_value, exc_traceback))


def configure(level=logging.INFO):
    """
    Configure the logging system for use by pipeline.
    
    Creates a handler which writes to sys.stderr and a handler which appends to a file 
    named according to the current date. Both handlers' formattters are set 
    using the LOG_FORMAT format string and are added to the root logger.
    
    :param level: Set the root logger level to the specified level (i.e. 
                  logging.{DEBUG,INFO,WARNING,ERROR}), defaults to INFO.
    
    :example:
    >>> from utils import pipeline_logging
    >>> 
    >>> LOGGER = logging.getLogger(__name__)
    >>>
    >>> def func(p1):
    >>>     LOGGER.debug(f"func called with p1={p1}")
    >>>     LOGGER.info("func called")
    >>>
    >>> if __name__ == '__main__':
    >>>     pipeline_logging.configure()
    >>>     func(1, 2)
    """
    filename = _get_log_file_path()
    default_config = _get_config(filename, level)
    os.makedirs(DEFAULT_LOG_DIR, exist_ok=True)
    logging.config.dictConfig(default_config)
    sys.excepthook = _except_hook
