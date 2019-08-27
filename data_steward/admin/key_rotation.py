from datetime import datetime
from oauth2client.client import GoogleCredentials
from googleapiclient.errors import HttpError
import googleapiclient.discovery as discovery
import logging

LOGGER = logging.getLogger(__name__)

KEY_EXPIRE_DAYS = 180


def get_iam_service():
    credentials = GoogleCredentials.get_application_default()
    return discovery.build('iam', 'v1', credentials=credentials)


def list_service_accounts(project_id):
    """
    List the service accounts associated with a project

    :param project_id: identifies the project
    :return: a list of service account objects
    """

    service_accounts_per_project_id = get_iam_service().projects().serviceAccounts().list(
        name='projects/' + project_id).execute()

    return service_accounts_per_project_id['accounts']


def list_keys_for_service_account(service_account_email):
    """
    List the keys associated with a service account

    :param service_account_email identifies the service account
    """

    service_keys_per_account = get_iam_service().projects().serviceAccounts().keys().list(
        name='projects/-/serviceAccounts/' + service_account_email).execute()

    return [{'id': _key['name'], 'validAfterTime': _key['validAfterTime'], 'validBeforeTime': _key['validBeforeTime'],
             'email': service_account_email} for _key in service_keys_per_account['keys']]


def is_key_expired(_key):
    """
    Determine if a key exceeds expiration period

    :param _key: service account key object
    :return: True if the key exceeds the expiration period, False otherwise
    """
    today_date = datetime.today().date()
    created_date = datetime.strptime(_key['validAfterTime'], '%Y-%m-%dT%H:%M:%SZ').date()
    delta = today_date - created_date
    return delta.days > KEY_EXPIRE_DAYS


def delete_key(_key):
    """
    Delete a service account key

    :param _key: service account key object
    """
    full_key_name = _key['id']
    try:
        get_iam_service().projects().serviceAccounts().keys().delete(name=full_key_name).execute()
        LOGGER.info('{full_key_name} is deleted'.format(full_key_name=full_key_name))
    except (
            HttpError):
        LOGGER.exception(
            "Unable to delete the key:\t%s",
            full_key_name
        )


def delete_expired_keys(project_id):
    """
    Delete all expired service account keys associated with a project

    :param project_id: identifies the project
    :return:
    """
    for _service_account in list_service_accounts(project_id):
        for key in list_keys_for_service_account(_service_account['email']):
            if is_key_expired(key):
                delete_key(key)


if __name__ == '__main__':
    import argparse

    PARSER = argparse.ArgumentParser(
        description='Delete all expired service account keys associated with a project',
        formatter_class=argparse.RawDescriptionHelpFormatter)
    PARSER.add_argument('project_id', help='Identifies the project')
    ARGS = PARSER.parse_args()
    delete_expired_keys(ARGS.project_id)
