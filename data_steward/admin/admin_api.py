import logging

from flask import Flask

import api_util
import bq_utils
import key_rotation

PREFIX = '/admin/v1/'
REMOVE_EXPIRED_KEYS_RULE = PREFIX + 'RemoveExpiredServiceAccountKeys'

app = Flask(__name__)


@api_util.auth_required_cron
def remove_expired_keys():
    project_id = bq_utils.app_identity.get_application_id()
    logging.info('Started removal of expired service account keys for %s' % project_id)
    key_rotation.delete_expired_keys(project_id)
    logging.info('Completed removal of expired service account keys for %s' % project_id)
    return 'remove-expired-keys-complete'


app.add_url_rule(
    REMOVE_EXPIRED_KEYS_RULE,
    view_func=remove_expired_keys,
    methods=['GET'])
