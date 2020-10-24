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
import mock
from datetime import datetime

# Project imports
import utils.pipeline_logging as pl


class FakeEmit:
    """
    Callable used to capture records passed to log handlers
    for testing purposes
    """

    def __init__(self):
        self.records = []

    def __call__(self, record: logging.LogRecord):
        """
        Stand in for :meth:`logging.Handler.emit`

        :param record: the logging record to handle
        """
        self.records.append(record)

    @property
    def call_count(self):
        """Number of times emit was called"""
        return len(self.records)

    @property
    def messages(self):
        """Get the messages logged"""
        return [record.msg for record in self.records]


class PipelineLoggingTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.log_file_list = ['path/', 'faked.log', 'path/fake.log']
        self.log_path = [
            datetime.now().strftime('path/curation%Y%m%d_%H%M%S.log'),
            'logs/faked.log', 'path/fake.log'
        ]
        pl.generate_paths(self.log_file_list)

    @mock.patch('utils.pipeline_logging.generate_paths')
    def test_generate_paths(self, mock_generate_paths):
        # checks that log_path is generated properly
        results = pl.generate_paths(self.log_file_list)
        self.assertEquals(results, self.log_path)
        self.assertListEqual(results, self.log_path)

    @mock.patch('utils.pipeline_logging.logging.StreamHandler')
    @mock.patch('utils.pipeline_logging.logging.FileHandler')
    @mock.patch('utils.pipeline_logging.logging.getLogger')
    def test_create_logger(self, mock_get_logger, mock_file_handler,
                           mock_stream_handler):
        # Tests console_logger function creates only FileHandler when console logging is set to False
        pl.create_logger(self.log_file_list[1], False)
        mock_file_handler.return_value.setLevel.assert_called_with(logging.INFO)
        mock_get_logger.return_value.addHandler.assert_any_call(
            mock_file_handler.return_value)
        mock_stream_handler.assert_not_called()
        mock_get_logger.assert_called_with(self.log_file_list[1])

        # Tests console_logger function creates both FileHandler and StreamHandler when console logging is set to True
        pl.create_logger(self.log_file_list[1], True)
        mock_file_handler.return_value.setLevel.assert_called_with(logging.INFO)
        mock_get_logger.return_value.addHandler.assert_any_call(
            mock_file_handler.return_value)
        mock_stream_handler.assert_called()
        mock_stream_handler.return_value.setLevel.assert_called_with(
            logging.INFO)
        mock_get_logger.return_value.addHandler.assert_called_with(
            mock_stream_handler.return_value)
        mock_get_logger.assert_called_with(self.log_file_list[1])

    def test_get_logger(self):
        file_emit = FakeEmit()
        stderr_emit = FakeEmit()
        logger_name = 'pipeline_logging_test'
        mock_open = mock.patch('logging.FileHandler._open',
                               mock.mock_open(),
                               create=True)
        mock_open.start()
        with mock.patch('logging.FileHandler.emit', new=file_emit):
            with mock.patch('logging.StreamHandler.emit', new=stderr_emit):
                # calling multiple times results in the same logger instance
                self.assertEqual(id(pl.get_logger(logger_name)),
                                 id(pl.get_logger(logger_name)))
                logger = pl.get_logger(logger_name)
                self.assertEqual(logger_name, logger.name)
                logger.debug('debug message')
                # debug messages are logged to file
                self.assertEqual(file_emit.messages, ['debug message'])
                # debug messages are NOT logged to stderr
                self.assertEqual(0, stderr_emit.call_count)

                logger.info('info message')
                self.assertEqual(file_emit.messages,
                                 ['debug message', 'info message'])
                self.assertListEqual(stderr_emit.messages, ['info message'])
        mock_open.stop()

    def test_setup_logger(self):
        expected_list_true = []
        expected_list_false = []

        # Pre conditions
        for item in self.log_path:
            expected_list_true.append(pl.create_logger(item, True))

        for item in self.log_path:
            expected_list_false.append(pl.create_logger(item, False))

        # Post conditions
        self.assertEquals(pl.setup_logger(self.log_file_list, True),
                          expected_list_true)
        self.assertEquals(pl.setup_logger(self.log_file_list, False),
                          expected_list_false)

    def tearDown(self):
        shutil.rmtree('path/')
