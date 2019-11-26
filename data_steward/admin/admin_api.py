import logging
import os

from flask import Flask
import app_identity

import slack
import api_util
from admin import key_rotation

SLACK_TOKEN = 'SLACK_TOKEN'
SLACK_CHANNEL = 'SLACK_CHANNEL'
DEFAULT_SLACK_CHANNEL = 'test_channel'

LOGGER = logging.getLogger(__name__)

PREFIX = '/admin/v1/'
REMOVE_EXPIRED_KEYS_RULE = PREFIX + 'RemoveExpiredServiceAccountKeys'

BODY_HEADER_EXPIRED_KEY_TEMPLATE = '# Expired keys deleted\n'

BODY_HEADER_EXPIRING_KEY_TEMPLATE = '\n# Keys expiring soon\n'

BODY_TEMPLATE = ('service_account_email={service_account_email}\n'
                 'key_name={key_name}\n'
                 'created_at={created_at}\n')

app = Flask(__name__)


def get_slack_client():
    return slack.WebClient(os.environ[SLACK_TOKEN])


def get_slack_channel_name():
    channel_name = os.getenv(SLACK_CHANNEL, DEFAULT_SLACK_CHANNEL)
    return DEFAULT_SLACK_CHANNEL if channel_name == '' else channel_name


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
        get_slack_client().chat_postMessage(
            channel=get_slack_channel_name(),
            text=text_body(expired_keys, expiring_keys),
            verify=False
        )
    return 'remove-expired-keys-complete'


app.add_url_rule(
    REMOVE_EXPIRED_KEYS_RULE,
    view_func=remove_expired_keys,
    methods=['GET'])
