"""
Unit test for slack_logging_handler module

Original Issue: DC-1159

Ensures that the slack logging messages are properly captured and sent to the curation slack alert channel
"""

# Python imports
import os
import logging
import mock
import unittest

# Project imports
from curation_logging.slack_logging_handler import initialize_slack_logging
from utils.slack_alerts import SLACK_TOKEN, SLACK_CHANNEL

TEST_SLACK_TOKEN = 'test_slack_token'
TEST_CHANNEL_NAME = 'channel_name'

INFO_MESSAGE = 'Do not send this'
WARNING_MESSAGE = 'logging.warning slack message sent by logging handler!'
CRITICAL_MESSAGE = 'logging.critical slack message sent by logging handler!'
ERROR_MESSAGE = 'logging.error slack message sent by logging handler!'


class SlackLoggingHandlerTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    @mock.patch.dict(os.environ, {
        SLACK_TOKEN: TEST_SLACK_TOKEN,
        SLACK_CHANNEL: TEST_CHANNEL_NAME
    })
    @mock.patch('curation_logging.slack_logging_handler.post_message')
    def test_initialize_slack_logging(self, mock_post_message):
        initialize_slack_logging()

        logging.info(INFO_MESSAGE)
        logging.debug(INFO_MESSAGE)

        logging.warning(WARNING_MESSAGE)
        logging.critical(CRITICAL_MESSAGE)
        logging.error(ERROR_MESSAGE)

        self.assertEqual(mock_post_message.call_count, 3)

        mock_post_message.assert_any_call(WARNING_MESSAGE)
        mock_post_message.assert_any_call(CRITICAL_MESSAGE)
        mock_post_message.assert_any_call(ERROR_MESSAGE)
Unit test for slack_logging_handler module

Original Issue: DC-1159

Ensures that the slack logging messages are properly captured and sent to the curation slack alert channel
"""

# Python imports
import logging
import unittest

# Project imports
from curation_logging.slack_logging_handler import initialize_slack_logging

root_logger = logging.getLogger('foo')


class SlackLoggingHandlerTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.slack_token = 'test_slack_token'
        self.channel_name = 'channel_name'

    def test_initialize_slack_logging(self):
        initialize_slack_logging()

        with self.assertLogs('foo', level=logging.WARNING) as cm:
            logging.getLogger('foo').info('Do not send this')
            logging.getLogger('foo').debug('Do not send this')
            logging.getLogger('foo').warning(
                'logging.warning slack message sent by logging handler!')
            logging.getLogger('foo').critical(
                'logging.critical slack message sent by logging handler!')
            logging.getLogger('foo').error(
                'logging.error slack message sent by logging handler!')
        self.assertEqual(cm.output, [
            'WARNING:foo:logging.warning slack message sent by logging handler!',
            'CRITICAL:foo:logging.critical slack message sent by logging handler!',
            'ERROR:foo:logging.error slack message sent by logging handler!'
        ])
