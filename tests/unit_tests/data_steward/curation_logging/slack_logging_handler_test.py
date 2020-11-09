"""
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
