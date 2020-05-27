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
import mock
from datetime import datetime

# Project imports
from utils.pipeline_logging import setup_logger, generate_paths


class PipelineLoggingTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.log_file_list = ['path/', 'faked.log', 'path/fake.log']
        self.log_path = [datetime.now().strftime('path/curation%Y%m%d_%H%M%S.log'),
                         'logs/faked.log', 'path/fake.log']

    @mock.patch('utils.pipeline_logging.generate_paths')
    def test_generate_paths(self, mock_generate_paths):
        # checks that log_path is generated properly
        results = generate_paths(self.log_file_list)
        self.assertEquals(results, self.log_path)

    @mock.patch('utils.pipeline_logging.logging')
    def test_setup_logger(self, mock_logging):
        # checks that console_logger function is called
        results = setup_logger(self.log_file_list, True)
        print(f'results: {results}')
        mock_logging.assert_has_calls(logging.StreamHandler())


    def tearDown(self):
        shutil.rmtree('path/')
