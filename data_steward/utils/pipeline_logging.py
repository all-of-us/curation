"""
Enforces common logging conventions and code reuse.

Original Issue = DC-637

The intent of this module is to allow other modules to setup logging easily without
duplicating code.
"""

# Python imports
import logging
import os
from pathlib import Path
from datetime import datetime


def setup_logger(log_filepath_list, console_logging=False):
    """
    Sets up python logging to file and or console for use in other modules.

    :param log_filepath_list:  desired string path and name of the log file
    :param console_logging:  determines if log should be output to the console
                             is false by default, if true, will log output to console
    """

    # iterates through log_filepath_list and sets path to provided path and default_file_name
    default_file_name = datetime.now().strftime('curation%Y%m%d_%H%M%S.log')
    default_output_log_path = [
        os.path.join(filepath, default_file_name)
        for filepath in log_filepath_list
        if 'log' not in os.path.basename(filepath)
    ]

    # iterates through log_filepath_list and sets path to log files
    output_log_path = [
        Path(filepath)
        for filepath in log_filepath_list
        if 'log' in os.path.basename(filepath)
    ]

    log_path = output_log_path + default_output_log_path

    # if any path in output_log_path or default_output_log_path doesn't exist, will be created
    for path in log_path:
        try:
            os.makedirs(path)
        except OSError:
            # directory already exists
            pass

    # gets new logger, will be created if doesn't exist
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)

    # formatters for both FileHandler and StreamHandler
    file_formatter = logging.Formatter(
        fmt='%(asctime)s - %(levelname)s - %(name)s - %(message)s',
        datefmt='%(asctime)s')
    stream_formatter = logging.Formatter(
        '%(levelname)s - %(name)s - %(message)s')

    for filename in log_path:
        if console_logging:
            file_handler = logging.FileHandler(filename)
            file_handler.setLevel(logging.INFO)
            file_handler.setFormatter(file_formatter)
            logger.addHandler(file_handler)

            return logger

        if console_logging is True:
            file_handler = logging.FileHandler(filename)
            stream_handler = logging.StreamHandler()

            file_handler.setLevel(logging.INFO)
            stream_handler.setLevel(logging.INFO)

            file_handler.setFormatter(file_formatter)
            stream_handler.setFormatter(stream_formatter)

            logger.addHandler(file_handler)
            logger.addHandler(stream_handler)

            return logger
