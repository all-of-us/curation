# Python imports
import os
import logging
import unittest
import mock
from mock import patch

# Project imports
from curation_logging.slack_logging_handler import initialize_slack_logging
from utils.slack_alerts import post_message, SLACK_CHANNEL, SLACK_TOKEN

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

    @mock.patch('utils.slack_alerts._get_slack_client')
    def test_initialize_slack_logging(self, mock_slack_client):
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
            'WARNING:root:logging.warning slack message sent by logging handler!',
            'CRITICAL:root:logging.critical slack message sent by logging handler!',
            'ERROR:root:logging.error slack message sent by logging handler!'
        ])
