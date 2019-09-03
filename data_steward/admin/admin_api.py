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

BODY_HEADER_EXPIRED_KEY_TEMPLATE = '# Expired keys deleted\n'

BODY_HEADER_EXPIRING_KEY_TEMPLATE = '\n# Keys expiring soon\n'

BODY_TEMPLATE = ('service_account_email={service_account_email}\n'
                 'key_name={key_name}\n'
                 'created_at={created_at}\n')

PREFIX = '/admin/v1/'
REMOVE_EXPIRED_KEYS_RULE = PREFIX + 'RemoveExpiredServiceAccountKeys'

app = Flask(__name__)


def email_body(_expired_keys, _expiring_keys):
    """
    This creates an email body for _expired_keys and _expiring_keys
    :param _expired_keys:
    :param _expiring_keys:
    :return: the email body
    """
    _email_body = ''

    if len(_expired_keys) != 0:
        _email_body += BODY_HEADER_EXPIRED_KEY_TEMPLATE
        for _key in _expired_keys:
            _email_body += BODY_TEMPLATE.format(service_account_email=_key['service_account_email'],
                                                key_name=_key['key_name'],
                                                created_at=_key['created_at'])

    if len(_expiring_keys) != 0:
        _email_body += BODY_HEADER_EXPIRING_KEY_TEMPLATE
        for _key in _expiring_keys:
            _email_body += BODY_TEMPLATE.format(service_account_email=_key['service_account_email'],
                                                key_name=_key['key_name'],
                                                created_at=_key['created_at'])
    return _email_body


@api_util.auth_required_cron
def remove_expired_keys():
    project_id = app_identity.get_application_id()
    logging.info('Started removal of expired service account keys for %s' % project_id)

    expired_keys = key_rotation.delete_expired_keys(project_id)
    logging.info('Completed removal of expired service account keys for %s' % project_id)

    logging.info('Started listing expiring service account keys for %s' % project_id)
    expiring_keys = key_rotation.get_expiring_keys(project_id)
    logging.info('Completed listing expiring service account keys for %s' % project_id)

    if NOTIFICATION_ADDRESS is not None:

        if len(expired_keys) != 0 and len(expired_keys) != 0:

            try:
                mail.send_mail(sender=SENDER_ADDRESS,
                               to=NOTIFICATION_ADDRESS,
                               subject=SUBJECT,
                               body=email_body(expired_keys, expiring_keys))
            except (
                    HttpError):
                LOGGER.exception(
                    "Failed to send to {notification_address}".format(notification_address=NOTIFICATION_ADDRESS)
                )
    else:
        LOGGER.exception(
            "The notification address is None"
        )

    return 'remove-expired-keys-complete'


app.add_url_rule(
    REMOVE_EXPIRED_KEYS_RULE,
    view_func=remove_expired_keys,
    methods=['GET'])
