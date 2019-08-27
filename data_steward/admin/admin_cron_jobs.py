import api_util
import bq_utils
import key_rotation
import constants.validation.main as consts
from flask import Flask


app = Flask(__name__)


@api_util.auth_required_cron
def remove_expired_keys():
    project = bq_utils.app_identity.get_application_id()
    key_rotation.delete_keys_for_project(project)

    return 'remove-expired-keys-complete'


app.add_url_rule(
    consts.PREFIX + 'ServiceAccountKeyRotation',
    endpoint='remove_expired_keys',
    view_func=remove_expired_keys,
    methods=['GET'])
