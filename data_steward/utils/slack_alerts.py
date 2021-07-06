"""
A utility module for sending messages to a slack channel.

The utility user must create the message body, this module is responsible
for sending the message.
"""

# Python imports
import sys
import os
import logging

# Third party imports
import slack
from slack.errors import SlackApiError

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
        super(SlackConfigurationError, self).__init__(msg)
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


def is_channel_available():
    """
    Test if the Slack channel is available
    :return:
    """
    try:
        client = _get_slack_client()
        channel_name = _get_slack_channel_name()
        response = client.conversations_list(limit=sys.maxsize)
        if response.status_code == 200:
            for channel in response.data['channels']:
                if channel['name'] == channel_name:
                    return True
    except (SlackConfigurationError, SlackApiError) as e:
        # if the environment variables are missing or the slack api failed to identify the channel
        logging.error(e)
    return False


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


def log_event_factory(job_name=None):
    # TODO: This is only a temporary solution. The problem is that slack_logging_handler is
    #  set to the WARNING level, calling logging.info would not send the message to the slack
    #  channel. The reason we set it to WARNING is that we don't want to flood the slack
    #  channel with trivial information and only want to get logs at the warning level and
    #  above. We need to replace the below code with the unified logging infrastructure in
    #  the future
    def log_event(func):

        display_job_name = job_name if job_name else func.__name__
        start_message = f'The {display_job_name} job has started.'
        end_message = f'The {display_job_name} job has completed successfully.'

        def wrapper(*args, **kwargs):
            try:
                post_message(start_message)
                logging.info(start_message)

                returned_val = func(*args, **kwargs)

                post_message(end_message)
                logging.info(end_message)

                return returned_val
            except (SlackConfigurationError, SlackApiError) as e:
                # if the environment variables are missing or the slack api failed to identify the
                # channel
                logging.exception(
                    f'Slack is not configured for posting messages, refer to playbook. {e}'
                )
                raise

        return wrapper

    return log_event
