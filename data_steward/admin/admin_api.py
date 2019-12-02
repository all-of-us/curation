import logging
import os

from flask import Flask
import app_identity

import slack
import api_util
from admin import key_rotation

SLACK_TOKEN = 'SLACK_TOKEN'
SLACK_CHANNEL = 'SLACK_CHANNEL'
UNSET_SLACK_TOKEN_MSG = 'Slack token not set in environment variable %s' % SLACK_TOKEN
UNSET_SLACK_CHANNEL_MSG = 'Slack channel not set in environment variable %s' % SLACK_CHANNEL

PREFIX = '/admin/v1/'
REMOVE_EXPIRED_KEYS_RULE = PREFIX + 'RemoveExpiredServiceAccountKeys'

BODY_HEADER_EXPIRED_KEY_TEMPLATE = '# Expired keys deleted\n'

BODY_HEADER_EXPIRING_KEY_TEMPLATE = '\n# Keys expiring soon\n'

BODY_TEMPLATE = ('service_account_email={service_account_email}\n'
                 'key_name={key_name}\n'
                 'created_at={created_at}\n')

app = Flask(__name__)


class AdminConfigurationError(RuntimeError):
    """
    Raised when the Admin API is not properly configured
    """

    def __init__(self, msg):
        super(AdminConfigurationError, self).__init__()
        self.msg = msg


def get_slack_token():
    """
    Get the token used to interact with the Slack API

    :raises:
      AdminConfigurationError: token is not configured
    :return: configured Slack API token as str
    """
    if SLACK_TOKEN not in os.environ.keys():
        raise AdminConfigurationError(UNSET_SLACK_TOKEN_MSG)
    return os.environ[SLACK_TOKEN]


def get_slack_channel_name():
    """
    Get name of the Slack channel to post notifications to

    :raises:
      AdminConfigurationError: channel name is not configured
    :return: the configured Slack channel name as str
    """
    if SLACK_CHANNEL not in os.environ.keys():
        raise AdminConfigurationError(UNSET_SLACK_CHANNEL_MSG)
    return os.environ[SLACK_CHANNEL]


def get_slack_client():
    """
    Get web client for Slack

    :return: WebClient object to communicate with Slack
    """
    slack_token = get_slack_token()
    return slack.WebClient(slack_token)


def post_message(text):
    """
    Post a system notification

    :param text: the message to post
    :return:
    """
    slack_client = get_slack_client()
    slack_channel_name = get_slack_channel_name()
    return slack_client.chat_postMessage(channel=slack_channel_name, text=text, verify=False)


# TODO Use jinja templates
def text_body(expired_keys, expiring_keys):
    """
    This creates a text body for _expired_keys and _expiring_keys
    :param expired_keys:
    :param expiring_keys:
    :return: the text body
    """
    result = ''

    if len(expired_keys) != 0:
        result += BODY_HEADER_EXPIRED_KEY_TEMPLATE
        for expired_key in expired_keys:
            result += BODY_TEMPLATE.format(service_account_email=expired_key['service_account_email'],
                                           key_name=expired_key['key_name'],
                                           created_at=expired_key['created_at'])

    if len(expiring_keys) != 0:
        result += BODY_HEADER_EXPIRING_KEY_TEMPLATE
        for expiring_key in expiring_keys:
            result += BODY_TEMPLATE.format(service_account_email=expiring_key['service_account_email'],
                                           key_name=expiring_key['key_name'],
                                           created_at=expiring_key['created_at'])
    return result


@api_util.auth_required_cron
def remove_expired_keys():
    project_id = app_identity.get_application_id()

    logging.info('Started removal of expired service account keys for %s' % project_id)

    expired_keys = key_rotation.delete_expired_keys(project_id)
    logging.info('Completed removal of expired service account keys for %s' % project_id)

    logging.info('Started listing expiring service account keys for %s' % project_id)
    expiring_keys = key_rotation.get_expiring_keys(project_id)
    logging.info('Completed listing expiring service account keys for %s' % project_id)

    if len(expiring_keys) != 0 or len(expired_keys) != 0:
        text = text_body(expired_keys, expiring_keys)
        post_message(text)
    return 'remove-expired-keys-complete'


app.add_url_rule(
    REMOVE_EXPIRED_KEYS_RULE,
    view_func=remove_expired_keys,
    methods=['GET'])
