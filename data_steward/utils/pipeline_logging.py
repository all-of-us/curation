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
    output_log_path = [
        Path(filepath)
        for filepath in log_filepath_list
        if 'log' in os.path.basename(filepath)
    ]

    default_file_name = datetime.now().strftime('curation%Y%m%d_%H%M%S.log')
    default_output_log_path = [
        os.path.join(filepath, default_file_name)
        for filepath in log_filepath_list
        if 'log' not in os.path.basename(filepath)
    ]

    # if any path in output_log_path or default_output_log_path doesn't exist, will be created
    for output_log, default_output_log in [
            output_log_path, default_output_log_path
    ]:
        try:
            os.makedirs(output_log, default_output_log)
        except OSError:
            # directory already exists
            pass

    # gets new logger, will be created if doesn't exist
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)

    file_formatter = logging.Formatter(
        fmt='%(asctime)s - %(levelname)s - %(name)s - %(message)s',
        datefmt='%(asctime)s')
    stream_formatter = logging.Formatter(
        '%(levelname)s - %(name)s - %(message)s')

    # want output to go to file
    if console_logging:
        file_handler1 = logging.FileHandler(default_file_name)
        file_handler1.setLevel(logging.INFO)
        file_handler1.setFormatter(file_formatter)
        logger.addHandler(file_handler1)

        return logger

    if console_logging is True:
        file_handler1 = logging.FileHandler(default_file_name)
        stream_handler = logging.StreamHandler()

        file_handler1.setLevel(logging.INFO)
        stream_handler.setLevel(logging.INFO)

        stream_handler.setFormatter(stream_formatter)
        file_handler1.setFormatter(file_formatter)

        logger.addHandler(stream_handler)
        logger.addHandler(file_handler1)

        return logger

    # # want output to go to console
    # if console_logging is True:
    #
    #     file_formatter = logging.Formatter(
    #         fmt='%(asctime)s - %(levelname)s - %(name)s - %(message)s',
    #         datefmt='%(asctime)s')
    #     stream_handler = logging.StreamHandler()
    #     file_handler = logging.FileHandler(log_filepath_list)
    #
    #     stream_handler.setLevel(logging.INFO)
    #     file_handler.setLevel(logging.INFO)
    #
    #     return logger

    # # want output to go to file
    # if console_logging:
    #     # Sets default log location if non is specified
    #     if not log_filepath_list:
    #         log_filepath_list = os.path.join(log_dir, 'curation%Y%m%d_%H%M%S.log')
    #     formatter =
    #     file_handler = logging.FileHandler(log_filepath_list)
    #     file_handler.setLevel(logging.INFO)
    #     file_handler.setFormatter(formatter)
    #     logger.addHandler(file_handler)
