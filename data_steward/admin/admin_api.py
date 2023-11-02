"""
A module for the basic `admin` endpoint.

It's solely responsible for:
(1) automating service key deletions and notifications, and
(2) monitoring PID/RID violation in non-prod envs
"""
import logging

# Third party imports
from flask import Flask

# Project imports
import api_util
import app_identity
from admin import key_rotation, prod_pid_detection
# from curation_logging.curation_gae_handler import (begin_request_logging,
#                                                    end_request_logging,
#                                                    initialize_logging)

PREFIX = '/admin/v1/'
REMOVE_EXPIRED_KEYS_RULE = f'{PREFIX}RemoveExpiredServiceAccountKeys'
"""Used as part of GCP monitoring and alerting.  String is part of the metric."""
BODY_HEADER_EXPIRED_KEY_TEMPLATE = '# Expired keys deleted\n'
"""Used as part of GCP monitoring and alerting.  String is part of the metric."""
BODY_HEADER_EXPIRING_KEY_TEMPLATE = '\n# Keys expiring soon\n'

BODY_TEMPLATE = ('service_account_email={service_account_email}\n'
                 'key_name={key_name}\n'
                 'created_at={created_at}\n')

DETECT_PID_VIOLATION_RULE = f'{PREFIX}DetectPersonIdViolation'

app = Flask(__name__)


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
            result += BODY_TEMPLATE.format(
                service_account_email=expired_key['service_account_email'],
                key_name=expired_key['key_name'],
                created_at=expired_key['created_at'])

    if len(expiring_keys) != 0:
        result += BODY_HEADER_EXPIRING_KEY_TEMPLATE
        for expiring_key in expiring_keys:
            result += BODY_TEMPLATE.format(
                service_account_email=expiring_key['service_account_email'],
                key_name=expiring_key['key_name'],
                created_at=expiring_key['created_at'])
    return result


@api_util.auth_required_cron
def remove_expired_keys():
    project_id = app_identity.get_application_id()

    logging.info(
        f'Started removal of expired service account keys for {project_id}')

    expired_keys = key_rotation.delete_expired_keys(project_id)
    logging.info(
        f'Completed removal of expired service account keys for {project_id}')

    logging.info(
        f'Started listing expiring service account keys for {project_id}')
    expiring_keys = key_rotation.get_expiring_keys(project_id)
    logging.info(
        f'Completed listing expiring service account keys for {project_id}')

    if len(expiring_keys) != 0 or len(expired_keys) != 0:
        text = text_body(expired_keys, expiring_keys)
        # message text will trigger a GCP metric and alert
        logging.info(text)
    return 'remove-expired-keys-complete'


@api_util.auth_required_cron
def detect_pid_violation():
    project_id = app_identity.get_application_id()
    prod_pid_detection.check_violation(project_id)
    return 'detect-pid-violation-complete'


# @app.before_first_request
# def set_up_logging():
#     initialize_logging()

app.add_url_rule(REMOVE_EXPIRED_KEYS_RULE,
                 endpoint='remove_expired_keys',
                 view_func=remove_expired_keys,
                 methods=['GET'])

app.add_url_rule(DETECT_PID_VIOLATION_RULE,
                 endpoint='detect_pid_violation',
                 view_func=detect_pid_violation,
                 methods=['GET'])

# app.before_request(
#     begin_request_logging)  # Must be first before_request() call.

# app.teardown_request(end_request_logging)
