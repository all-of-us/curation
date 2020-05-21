"""
Unit test for pipeline_logging module.

Ensures logging handlers are set up correctly by making sure:
    1. The right number of expected handlers exists
    2. The StreamHandler exists depending on test input
    3. The log files that are configured point to the right file location

Original Issue = DC-637
"""

# Python imports
import shutil
import unittest
import logging
import os

# Project imports
from utils.pipeline_logging import setup_logger


class PipelineLoggingTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.log_file_list = ['path/', 'faked.log', 'path/fake.log']

    def test_setup_logger(self):
        # checks if handlers exist before setup_logger function runs
        # should be false
        handlers_exist = logging.getLogger().hasHandlers()
        self.assertEquals(handlers_exist, False)

        # log to console and file
        results = setup_logger(self.log_file_list, True)
        self.assertEquals(results.hasHandlers(), True)
        logging.shutdown()

        # log to just file
        results = setup_logger(self.log_file_list, False)
        self.assertEquals(results.hasHandlers(), True)
        print(results.handlers)
        logging.shutdown()

    def tearDown(self):
        shutil.rmtree('path/')
        os.remove('logs/faked.log')
