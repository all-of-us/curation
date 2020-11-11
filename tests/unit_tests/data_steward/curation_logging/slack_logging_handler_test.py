"""
Unit test for slack_logging_handler module

Original Issue: DC-1159

Ensures that the slack logging messages are properly captured and sent to the curation slack alert channel
Notes: -- if dev is using macOS and gets error:
            [SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed: unable to get local issuer certificate (_ssl.c:1108)
          will need to go to Macintosh HD > Applications > Python3.6 folder (or whatever version of python you're using) >
          double click on "Install Certificates.command" file.
          found: (https://stackoverflow.com/questions/50236117/scraping-ssl-certificate-verify-failed-error-for-http-en-wikipedia-org)
      -- dev will also need to add SLACK_TOKEN and SLACK_CHANNEL as environment variables
"""

# Python imports
import os
import logging
import mock
import unittest

# Project imports
from curation_logging.slack_logging_handler import initialize_slack_logging
from utils.slack_alerts import SLACK_TOKEN, SLACK_CHANNEL

GAE_ENV = 'GAE_ENV'
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

    def setUp(self):
        self.os_environ_patcher = mock.patch.dict(os.environ, {
            GAE_ENV: '',
            SLACK_CHANNEL: TEST_CHANNEL_NAME,
            SLACK_TOKEN: SLACK_TOKEN
        })
        self.os_environ_patcher.start()

    def tearDown(self):
        self.os_environ_patcher.stop()

    @mock.patch('curation_logging.slack_logging_handler.is_channel_available')
    @mock.patch('curation_logging.slack_logging_handler.post_message')
    def test_initialize_slack_logging(self, mock_post_message,
                                      mock_is_channel_available):
        mock_is_channel_available.return_value = True

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
