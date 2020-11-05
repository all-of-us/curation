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

    def test_generate_paths(self):
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

    @mock.patch('logging.FileHandler._open')
    def test_configure(self, mock_open):
        """
        Verify that root level and handlers are properly set after configure
        :param mock_open: mock to prevent side effect of opening file
        """
        # names are used to uniquely identify handlers both in standard logging module
        # and in this test case
        expected_hdlrs = [pl._FILE_HANDLER]

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

        # add console log handler to configuration
        pl.configure(add_console_handler=True)
        actual_hdlrs = [hdlr.name for hdlr in logging.root.handlers]
        expected_hdlrs = [pl._FILE_HANDLER, pl._CONSOLE_HANDLER]
        self.assertEqual(expected_hdlrs, actual_hdlrs)

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
