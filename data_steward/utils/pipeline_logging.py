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
from app_identity import get_application_id

DEFAULT_LOG_DIR = os.path.join(resources.base_path, 'logs')
"""Default location for log files"""
DEFAULT_LOG_LEVEL = logging.INFO

LOG_FORMAT = '%(asctime)s - %(levelname)s - %(name)s - %(message)s'
LOG_DATEFMT = '%Y-%m-%d %H:%M:%S'
LOG_FILENAME_DATEFMT = '%Y%m%d'

_FILE_HANDLER = 'curation_file_handler'
"""Identifies the file log handler"""
_CONSOLE_HANDLER = 'curation_console_handler'
"""Identifies the console handler"""


def _get_log_date_str():
    """
    Get current date formatted using LOG_FILENAME_DATEFMT
    :return: 
    """
    return datetime.today().strftime(LOG_FILENAME_DATEFMT)


def get_log_filename() -> str:
    """
    Construct runtime-specific log filename
    """
    try:
        # attempt to add application id suffix (google project name)
        app_id = get_application_id()
        filename_suffix = f'-{app_id}'
    except RuntimeError:
        # if we cannot, add "-no-project" suffix as a visual hint that this runtime probably did not interact with a
        # google project
        # TODO: emit warning of some description?
        filename_suffix = '-no-project'
    finally:
        # compile and return final log filename
        return f'{_get_log_date_str()}{filename_suffix}.log'


def _get_log_file_path():
    """
    Get the abs path of the log file to use. 
    
    The location is DEFAULT_LOG_DIR and the file name is the current
    date formatted using FILENAME_FMT.

    :return: absolute path to the log file
    """
    return os.path.join(DEFAULT_LOG_DIR, get_log_filename())


def _get_config(level, add_console_handler):
    """
    Get a dictionary which describes the logging configuration
    
    :param level: Set the root logger level to the specified level 
                  (i.e. logging.{DEBUG,INFO,WARNING,ERROR}).
    :param add_console_handler: If set to True a console log handler is added
                                to the root logger.
    :return: the configuration dict
    """
    handlers = [_FILE_HANDLER]
    if add_console_handler:
        handlers.append(_CONSOLE_HANDLER)
    config = {
        'version': 1,
        'formatters': {
            'default': {
                'class': 'logging.Formatter',
                'format': LOG_FORMAT,
                'datefmt': LOG_DATEFMT
            }
        },
        'handlers': {
            _FILE_HANDLER: {
                'class': 'logging.FileHandler',
                'mode': 'a',
                'formatter': 'default',
                'filename': _get_log_file_path()
            },
            # console handler is only used if referenced
            # by root logger config below
            _CONSOLE_HANDLER: {
                'class': 'logging.StreamHandler',
                'formatter': 'default',
            }
        },
        'root': {
            'level': level,
            'handlers': handlers
        },
        # otherwise defaults to True which would disable
        # any loggers that exist at configuration time
        'disable_existing_loggers': False
    }
    return config


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


def configure(level=logging.INFO, add_console_handler=False):
    """
    Configure the logging system for use by pipeline.
    
    By default creates a handler which appends to a file named according to the 
    current date. A handler which writes to the console (sys.stderr) can optionally be added. 
    Both handlers' formattters are set using the LOG_FORMAT format string and are added to 
    the root logger.
    
    :param level: Set the root logger level to the specified level (i.e. 
                  logging.{DEBUG,INFO,WARNING,ERROR}), defaults to INFO.
    :param add_console_handler: If set to True a console log handler is 
                                added to the root logger otherwise it is not.
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
    config = _get_config(level, add_console_handler)
    os.makedirs(DEFAULT_LOG_DIR, exist_ok=True)
    logging.config.dictConfig(config)
    sys.excepthook = _except_hook
