from datetime import datetime
from google.oauth2 import service_account
import googleapiclient.discovery
import logging
import os

LOGGER = logging.getLogger(__name__)

KEY_VALID_LENGTH = 180

credentials = service_account.Credentials.from_service_account_file(
    filename=os.environ['GOOGLE_APPLICATION_CREDENTIALS'],
    scopes=['https://www.googleapis.com/auth/cloud-platform'])

service = googleapiclient.discovery.build(
    'iam', 'v1', credentials=credentials)


def list_service_accounts(project_id):
    """Lists all service accounts for the current project."""

    service_accounts_per_project_id = service.projects().serviceAccounts().list(
        name='projects/' + project_id).execute()

    return service_accounts_per_project_id['accounts']


def list_key_for_service_account(service_account_email):
    """Lists all service accounts for the current project."""

    service_keys_per_account = service.projects().serviceAccounts().keys().list(
        name='projects/-/serviceAccounts/' + service_account_email).execute()

    return [{'id': _key['name'], 'validAfterTime': _key['validAfterTime'], 'validBeforeTime': _key['validBeforeTime'],
             'email': service_account_email} for _key in service_keys_per_account['keys']]


def is_key_expired(_key):
    today_date = datetime.today().date()
    created_date = datetime.strptime(_key['validAfterTime'], '%Y-%m-%dT%H:%M:%SZ').date()
    delta = today_date - created_date
    return delta.days > KEY_VALID_LENGTH


def delete_key(full_key_name):
    """Deletes a service account key."""
    try:
        service.projects().serviceAccounts().keys().delete(name=full_key_name).execute()
        LOGGER.info('{key} is deleted'.format(full_key_name))
    except (
            googleapiclient.errors.HttpError):
        LOGGER.exception(
            "Unable to delete the key:\t%s",
            full_key_name
        )


def delete_keys_for_project(project_id):
    """
    Delete all the expired keys associated with the project
    :param project_id:
    :return:
    """
    for _service_account in list_service_accounts(project_id):
        for key in list_key_for_service_account(_service_account['email']):
            if is_key_expired(key):
                delete_key(key)


if __name__ == '__main__':
    pass
