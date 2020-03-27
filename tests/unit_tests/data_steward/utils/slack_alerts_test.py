"""
A unit test class for the curation/data_steward/utils/slack_alerts module.
"""
import os
import mock
import unittest
from mock import patch

from slack.errors import SlackClientError

from utils import slack_alerts


class SlackAlertTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.slack_token = 'test_slack_token'
        self.channel_name = 'channel_name'

    def test_get_slack_token(self):
        with patch.dict(os.environ,
                        {slack_alerts.SLACK_TOKEN: self.slack_token}):
            self.assertEqual(self.slack_token, slack_alerts._get_slack_token())

        with self.assertRaises(slack_alerts.SlackConfigurationError) as c:
            slack_alerts._get_slack_token()

        self.assertEqual(c.exception.msg, slack_alerts.UNSET_SLACK_TOKEN_MSG)

    def test_get_slack_channel_name(self):
        with patch.dict(os.environ,
                        {slack_alerts.SLACK_CHANNEL: self.channel_name}):
            self.assertEqual(self.channel_name,
                             slack_alerts._get_slack_channel_name())

        with self.assertRaises(slack_alerts.SlackConfigurationError) as c:
            slack_alerts._get_slack_channel_name()

        self.assertEqual(c.exception.msg, slack_alerts.UNSET_SLACK_CHANNEL_MSG)

    @mock.patch('utils.slack_alerts._get_slack_client')
    def test_post_message(self, mock_slack_client):
        patch_environment = {
            slack_alerts.SLACK_CHANNEL: self.channel_name,
            slack_alerts.SLACK_TOKEN: self.slack_token
        }

        mock_chat_message = mock_slack_client.return_value.chat_postMessage
        text = 'fake slack alert message'

        with patch.dict(os.environ, patch_environment):
            slack_alerts.post_message(text)

            message_args = {
                'channel': self.channel_name,
                'verify': False,
                'text': text
            }

            mock_slack_client.assert_called_once()
            self.assertEqual(mock_chat_message.call_args_list,
                             [mock.call(**message_args)])
