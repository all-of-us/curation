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
FILENAME_LOGGERNAME_SLUG = '{loggername}'
FILENAME_FMT = '%Y%m%d-' + FILENAME_LOGGERNAME_SLUG

_FILE_HANDLER = 'curation_file_handler'
"""Identifies the file log handler"""
_CONSOLE_HANDLER = 'curation_console_handler'
"""Identifies the console handler"""


class PipelineLoggingFileHandler(logging.FileHandler):
    """
    PipelineLoggingFileHandler is a simple overload of the upstream logging.FileHandler class that allows for Logger
    name-specific filename formatting when used in conjunction with our PipelineLogger class

    TODO:
    """

    def __init__(self, filename, mode='a', encoding=None, delay=True):
        # define instance var
        self._logger_name = ""
        # this handler must not open its file stream prior to the owning logger being defined
        if delay is False:
            cname = self.__class__.__name__
            raise Exception(f'Cannot construct {cname} with delay = False!', delay)
        # call super
        super(PipelineLoggingFileHandler, self).__init__(filename, mode, encoding, True)

    """
    set_logger_name will only ever be called if this handler is utilized by our PipelineLogger class.  In all other
    cases, this method is pointless.
    """
    def set_logger_name(self, logger_name: str) -> None:
        # if a name has already been set for this file handle...
        if self._logger_name != "":
            # ...and the one provided is not equivalent to the original, throw maybe probably informative exception.
            if logger_name != self._logger_name:
                current_name = self._logger_name
                cname = self.__class__.__name__
                raise Exception(f'This {cname} instance already registered to Logger {current_name}', logger_name)

            # fast-exit this if block if this the result of a command reissue
            # hopefully won't ever happen _but who knows!_
            return

        # define logger name
        self._logger_name = logger_name

        # stream should _not_ be open at this point.
        if self.stream is not None:
            # TODO: should this be handled differently?
            cname = self.__class__.__name__
            raise Exception(f'Instance of {cname} already has stream open')

        # update base filename
        self.baseFilename = self.baseFilename.replace(FILENAME_LOGGERNAME_SLUG, logger_name)


class PipelineLogger(logging.Logger):
    """
    PipelineLogger provides a place to override certain things about the built-in logger
    """

    def __init__(self, name, level=logging.NOTSET):
        # store configured name of logger
        self._name = name
        # call super
        super(PipelineLogger, self).__init__(name, level)

    def get_name(self) -> str:
        """
        get_name returns the name of this specific logger instance
        """
        return self._name

    def addHandler(self, hdlr: logging.Handler) -> None:
        """
        addHandler is a simple override of the base logging.Logger.addHandler method to allow us to modify provided
        handlers as-needed.


        Current use case is simply to inject log filenames with the "name" of this logger instance
        """
        # if this handler is of our custom file handler type...
        if isinstance(hdlr, PipelineLoggingFileHandler):
            # ...set handler logger name attribute
            # note: if the handler has already been used with a different logger, it will raise an exception here.
            hdlr.set_logger_name(self._name)

        # execute upstream handler registration call
        super(PipelineLogger, self).addHandler(hdlr)


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
                'class': PipelineLoggingFileHandler.__qualname__,
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
    logging.setLoggerClass(PipelineLogger)
    config = _get_config(level, add_console_handler)
    os.makedirs(DEFAULT_LOG_DIR, exist_ok=True)
    logging.config.dictConfig(config)
    sys.excepthook = _except_hook
