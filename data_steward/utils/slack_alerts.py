"""
A utility module for sending messages to a slack channel.

The utility user must create the message body, this module is responsible
for sending the message.
"""
import os

import slack

# environment variable names
SLACK_TOKEN = 'SLACK_TOKEN'
SLACK_CHANNEL = 'SLACK_CHANNEL'

UNSET_SLACK_TOKEN_MSG = 'Slack token not set in environment variable %s' % SLACK_TOKEN
UNSET_SLACK_CHANNEL_MSG = 'Slack channel not set in environment variable %s' % SLACK_CHANNEL


class SlackConfigurationError(RuntimeError):
    """
    Raised when the required slack variables are not properly configured
    """

    def __init__(self, msg):
        super(SlackConfigurationError, self).__init__()
        self.msg = msg


def _get_slack_token():
    """
    Get the token used to interact with the Slack API

    :raises:
      SlackConfigurationError: token is not configured
    :return: configured Slack API token as str
    """
    if SLACK_TOKEN not in os.environ.keys():
        raise SlackConfigurationError(UNSET_SLACK_TOKEN_MSG)
    return os.environ[SLACK_TOKEN]


def _get_slack_channel_name():
    """
    Get name of the Slack channel to post notifications to

    :raises:
      SlackConfigurationError: channel name is not configured
    :return: the configured Slack channel name as str
    """
    if SLACK_CHANNEL not in os.environ.keys():
        raise SlackConfigurationError(UNSET_SLACK_CHANNEL_MSG)
    return os.environ[SLACK_CHANNEL]


def _get_slack_client():
    """
    Get web client for Slack

    :return: WebClient object to communicate with Slack
    """
    slack_token = _get_slack_token()
    return slack.WebClient(slack_token)


def post_message(text):
    """
    Post a system notification

    :param text: the message to post
    :return:
    """
    slack_client = _get_slack_client()
    slack_channel_name = _get_slack_channel_name()
    return slack_client.chat_postMessage(channel=slack_channel_name,
                                         text=text,
                                         verify=False)
