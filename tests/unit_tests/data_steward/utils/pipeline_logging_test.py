"""
Unit test for pipeline_logging module.

Ensures logging handlers are set up correctly by making sure:
    1. The right number of expected handlers exists
    2. The StreamHandler exists depending on test input
    3. The log files that are configured point to the right file location

Original Issue = DC-637
"""

# Python imports
import os
import unittest
import logging
import mock

# Project imports
import utils.pipeline_logging as pl
from app_identity import PROJECT_ID


class PipelineLoggingTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        pass

    def assert_logs_handled(self, current_level, mock_file_emit,
                            mock_stream_emit):
        """
        Verify that logs at various levels are properly handled. 
        
        Multiple loggers are created, each of which emit info, warn, critical, debug messages.
        
        :param current_level: the root logging level to verify
        :param mock_file_emit: a mock of the file handler emit method
        :param mock_stream_emit: a mock of the stream handler emit method
        """
        log_items = [(logging.INFO, 'info from %s'),
                     (logging.WARN, 'warning from %s'),
                     (logging.CRITICAL, 'critical from %s'),
                     (logging.DEBUG, 'debug from %s')]
        expected = []
        for logger_name in ['a', 'a.b', 'c']:
            logger = logging.getLogger(logger_name)
            for (level, msg_fmt) in log_items:
                msg = msg_fmt % logger_name
                logger.log(level, msg_fmt % logger_name)
                if level >= current_level:
                    expected.append((level, msg))
        actual = [(log_record.levelno, log_record.msg)
                  for (log_record,), _ in mock_file_emit.call_args_list]
        self.assertListEqual(expected, actual)

        actual = [(log_record.levelno, log_record.msg)
                  for (log_record,), _ in mock_stream_emit.call_args_list]
        self.assertListEqual(expected, actual)

    def assert_correct_log_filename(self, file_handler: logging.FileHandler,
                                    expected_basename: str):
        """
        Verifies that the provided logging.FileHandler instance would be writing to a file whose name matches
        expected_basename
        :param file_handler: logging.FileHandler instance to test
        :param expected_basename: expected basename of log file
        """
        # extract target filename from logging filehandler
        log_filename = file_handler.baseFilename
        # assert value is of correct type and non-empty
        self.assertIsInstance(log_filename, str)
        self.assertNotEqual(log_filename, '')
        # normalize then split path based on local os path separator
        log_path_split = os.path.normpath(log_filename).split(os.path.sep)
        # assert last piece is expected log basename
        self.assertEqual(log_path_split[-1], expected_basename)

    def assert_sane_configure(self):
        """
        verifies that a utils.pipeline_logger.configure() call executed successfully

        TODO: pass in expected basename of log file? bit more flexible explicit, but may not worth the refactor.
        """
        # names are used to uniquely identify handlers both in standard logging module
        # and in this test case
        expected_hdlrs = [pl._FILE_HANDLER]

        # execute configuration
        pl.configure()
        # root level is set to default (i.e. INFO)
        self.assertEqual(logging.root.level, pl.DEFAULT_LOG_LEVEL)

        # handlers are added
        actual_hdlrs = [hdlr.name for hdlr in logging.root.handlers]
        self.assertEqual(expected_hdlrs, actual_hdlrs)

        # no duplicate handlers after additional calls to configure
        pl.configure()
        self.assertEqual(len(expected_hdlrs), len(logging.root.handlers))
        actual_hdlrs = [hdlr.name for hdlr in logging.root.handlers]
        self.assertEqual(expected_hdlrs, actual_hdlrs)

        for hdlr in logging.root.handlers:
            if isinstance(hdlr, logging.FileHandler):
                self.assert_correct_log_filename(hdlr, pl.get_log_filename())

        # add console log handler to configuration
        pl.configure(add_console_handler=True)
        actual_hdlrs = [hdlr.name for hdlr in logging.root.handlers]
        expected_hdlrs = [pl._FILE_HANDLER, pl._CONSOLE_HANDLER]
        self.assertEqual(expected_hdlrs, actual_hdlrs)

    @mock.patch('logging.FileHandler._open')
    @mock.patch.dict(os.environ, {PROJECT_ID: ''})
    def test_configure_no_app_id(self, mock_open):
        """
        Verify that root level and handlers are properly set after configure when GOOGLE_PROJECT_ID is not set / empty
        :param mock_open: mock to prevent side effect of opening file
        """
        self.assert_sane_configure()

    @mock.patch('logging.FileHandler._open')
    @mock.patch.dict(os.environ,
                     {PROJECT_ID: 'unit-test-project-this-is-not-real'})
    def test_configure_defined_app_id(self, mock_open):
        """
        Verify that root level and handlers are properly set after configure when GOOGLE_PROJECT_ID is set
        :param mock_open: mock to prevent side effect of opening file
        """
        self.assert_sane_configure()

    @mock.patch('logging.StreamHandler.emit')
    @mock.patch('logging.FileHandler.emit')
    @mock.patch('logging.FileHandler._open')
    def test_default_level(self, mock_open, mock_file_emit, mock_stream_emit):
        pl.configure(add_console_handler=True)
        self.assert_logs_handled(pl.DEFAULT_LOG_LEVEL, mock_file_emit,
                                 mock_stream_emit)

    @mock.patch('logging.StreamHandler.emit')
    @mock.patch('logging.FileHandler.emit')
    @mock.patch('logging.FileHandler._open')
    def test_specific_level(self, mock_open, mock_file_emit, mock_stream_emit):
        pl.configure(logging.CRITICAL, add_console_handler=True)
        self.assert_logs_handled(logging.CRITICAL, mock_file_emit,
                                 mock_stream_emit)

    def tearDown(self):
        pass
