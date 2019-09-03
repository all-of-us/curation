import logging
import os

from flask import Flask
from google.appengine.api import app_identity
from google.appengine.api import mail
from googleapiclient.errors import HttpError

import api_util
import key_rotation

LOGGER = logging.getLogger(__name__)

SENDER_ADDRESS = 'curation-eng-alert@{project}.appspotmail.com'.format(project=app_identity.get_application_id())
NOTIFICATION_ADDRESS = os.environ.get('NOTIFICATION_ADDRESS')
SUBJECT = 'Project {project}: Service account key notices'.format(project=app_identity.get_application_id())

BODY_HEADER_EXPIRED_KEY_TEMPLATE = '''
# Expired keys deleted

'''

BODY_HEADER_EXPIRING_KEY_TEMPLATE = '''
# Keys expiring soon

'''

BODY_TEMPLATE = '''

service_account_email={service_account_email}
key_name={key_name}
created_at={create_at}

'''

PREFIX = '/admin/v1/'
REMOVE_EXPIRED_KEYS_RULE = PREFIX + 'RemoveExpiredServiceAccountKeys'
LIST_EXPIRING_KEYS_RULE = PREFIX + 'ListExpiringServiceAccountKeys'

app = Flask(__name__)


def assemble_email_body(keys, delete_action):
    email_body = BODY_HEADER_EXPIRED_KEY_TEMPLATE if delete_action else BODY_HEADER_EXPIRING_KEY_TEMPLATE

    for _key in keys:
        email_body += BODY_TEMPLATE.format(service_account_email=_key['service_account_email'],
                                           key_name=_key['key_name'],
                                           created_at=_key['created_at'])
    return email_body


@api_util.auth_required_cron
def remove_expired_keys():
    project_id = app_identity.get_application_id()
    logging.info('Started removal of expired service account keys for %s' % project_id)
    # suppressing so tests do not disrupt
    expired_keys = key_rotation.delete_expired_keys(project_id)
    logging.info('Completed removal of expired service account keys for %s' % project_id)

    if NOTIFICATION_ADDRESS is not None:
        try:
            mail.send_mail(sender=SENDER_ADDRESS,
                           to=NOTIFICATION_ADDRESS,
                           subject=SUBJECT,
                           body=assemble_email_body(expired_keys, True))
        except (
                HttpError):
            LOGGER.exception(
                "Failed to send to ${notification_address}"
            )
    else:
        LOGGER.exception(
            "The notification address is None"
        )

    return 'remove-expired-keys-complete'


@api_util.auth_required_cron
def list_expiring_keys():

    project_id = app_identity.get_application_id()
    expired_keys = key_rotation.get_expiring_keys(project_id)

    if NOTIFICATION_ADDRESS is not None:
        try:
            mail.send_mail(sender=SENDER_ADDRESS,
                           to=NOTIFICATION_ADDRESS,
                           subject=SUBJECT,
                           body=assemble_email_body(expired_keys, False))
        except (
                HttpError):
            LOGGER.exception(
                "Failed to send to ${notification_address}"
            )
    else:
        LOGGER.exception(
            "The notification address is None"
        )

    return 'list-expiring-keys-complete'


app.add_url_rule(
    REMOVE_EXPIRED_KEYS_RULE,
    view_func=remove_expired_keys,
    methods=['GET'])

app.add_url_rule(
    LIST_EXPIRING_KEYS_RULE,
    view_func=list_expiring_keys,
    methods=['GET'])

if __name__ == '__main__':
    remove_expired_keys()