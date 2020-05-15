"""
Enforces common logging conventions and code reuse.

Original Issue = DC-637

The intent of this module is to allow other modules to setup logging easily without
duplicating code.
"""

# Python imports
import logging
import os


def setup(log_file_list, console_logging=False):
    # gets new logger, will be created if doesn't exist
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    log_dir = os.path.join(os.path.normpath(os.getcwd() + os.sep + os.pardir),
                           'logs')

    # want output to go to file
    if console_logging:
        # Sets default log location if non is specified
        if not log_file_list:
            log_file_list = os.path.join(log_dir, 'curation%Y%m%d_%H%M%S.log')
        formatter = logging.Formatter(
            fmt='%(asctime)s - %(levelname)s - %(name)s - %(message)s',
            datefmt='%(asctime)s')
        file_handler = logging.FileHandler(log_file_list)
        file_handler.setLevel(logging.INFO)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    # want output to go to console
    if console_logging is True:
        stream_formatter = logging.Formatter(
            '%(levelname)s - %(name)s - %(message)s')
        file_formatter = logging.Formatter(
            fmt='%(asctime)s - %(levelname)s - %(name)s - %(message)s',
            datefmt='%(asctime)s')
        stream_handler = logging.StreamHandler()
        file_handler = logging.FileHandler(log_file_list)

        stream_handler.setLevel(logging.INFO)
        file_handler.setLevel(logging.INFO)

        stream_handler.setFormatter(stream_formatter)
        file_handler.setFormatter(file_formatter)

        logger.addHandler(stream_handler)
        logger.addHandler(file_handler)
