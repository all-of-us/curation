"""
Unit test for pipeline_logging module.

Ensures logging handlers are set up correctly by making sure:
    1. The right number of expected handlers exists
    2. The StreamHandler exists depending on test input
    3. The log files that are configured point to the right file location

Original Issue = DC-637
"""

# Python imports
import unittest
import logging

# Project imports
from utils.pipeline_logging import setup_logger


class PipelineLoggingTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.log_file_list = ['path/fake.log', 'path/']
        self.stream_handler = logging.StreamHandler()

    def test_setup(self):
        handlers_exist = logging.getLogger().hasHandlers()

        # log to specified log file location AND the console
        results = setup_logger(self.log_file_list, True)
        self.assertEquals(results.hasHandlers(), 2)
        logging.shutdown()

        # log to specified log file location NOT the console
        results = setup_logger(self.log_file_list, False)
        self.assertEquals(results.hasHandlers(), 1)
        self.assertTrue(results.matches(self.stream_handler))
        logging.shutdown()

        # log to default file location AND the console
        results = setup_logger(True)
        self.assertEquals(results.hasHandlers(), 2)
        logging.shutdown()

        # log to the default file location NOT the console
        results = setup_logger(False)
        self.assertEquals(results.hasHandlers(), 1)
        self.assertTrue(results.matches(self.stream_handler))

        logging.shutdown()
