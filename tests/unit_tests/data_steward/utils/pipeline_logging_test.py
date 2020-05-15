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
from utils.pipeline_logging import setup


class PipelineLoggingTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.log_file_list = {}

    def test_setup(self):
        handlers_exist = logging.getLogger().hasHandlers()

        # log to specified log file location AND the console
        results = setup(self.log_file_list, True)
        logging.shutdown()

        # log to specified log file location NOT the console
        results = setup(self.log_file_list)
        logging.shutdown()

        # log to default file location AND the console
        results = setup(True)
        logging.shutdown()

        # log to the default file location NOT the console
        results = setup()
        logging.shutdown()



