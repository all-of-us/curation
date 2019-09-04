from datetime import datetime, timedelta
from oauth2client.client import GoogleCredentials
from googleapiclient.errors import HttpError
import googleapiclient.discovery as discovery
import logging

LOGGER = logging.getLogger(__name__)

KEY_EXPIRE_DAYS = 180
KEY_EXPIRE_ALERT_DAYS = 7
GCP_DTM_FMT = '%Y-%m-%dT%H:%M:%SZ'


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
    :return: a list of key objects
    """

    service_keys_per_account = get_iam_service().projects().serviceAccounts().keys().list(
        name='projects/-/serviceAccounts/' + service_account_email).execute()

    return service_keys_per_account['keys']


def is_key_expired_after_period(_key, days=KEY_EXPIRE_ALERT_DAYS):
    """
    Determine if a key will be expired after a specified number of days

    :param _key: service account key object
    :param days: number of days
    :return: True if the key is expiring soon based, False otherwise
    """
    now_dtm = datetime.now()
    create_dtm = datetime.strptime(_key['validAfterTime'], GCP_DTM_FMT)
    expire_dtm = create_dtm + timedelta(days=KEY_EXPIRE_DAYS)
    future_dtm = now_dtm + timedelta(days=days)
    return future_dtm > expire_dtm


def is_key_expired(_key):
    """
    Determine if a key exceeds expiration period

    :param _key: service account key object
    :return: True if the key exceeds the expiration period, False otherwise
    """
    return is_key_expired_after_period(_key, 0)


def delete_key(_key):
    """
    Delete a service account key

    :param _key: service account key object
    """
    full_key_name = _key['name']
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
    :return: a list of dicts that contain the info about the deleted keys
    """
    _deleted_keys = []
    for _service_account in list_service_accounts(project_id):
        for _key in list_keys_for_service_account(_service_account['email']):
            if is_key_expired(_key):
                delete_key(_key)

                _deleted_keys.append({'service_account_email': _service_account['email'],
                                      'key_name': _key['name'],
                                      'created_at': _key['validAfterTime']})

    return _deleted_keys


def get_expiring_keys(project_id):
    """
    List all expiring service account keys associated with a project

    :param project_id: identifies the project
    :return: a list of dicts that contain the info about the expiring keys
    """
    _expiring_keys = []
    for _service_account in list_service_accounts(project_id):
        for _key in list_keys_for_service_account(_service_account['email']):
            if is_key_expired_after_period(_key):
                _expiring_keys.append({'service_account_email': _service_account['email'],
                                       'key_name': _key['name'],
                                       'created_at': _key['validAfterTime']})

    return _expiring_keys


if __name__ == '__main__':
    import argparse

    PARSER = argparse.ArgumentParser(
        description='Delete all expired service account keys associated with a project',
        formatter_class=argparse.RawDescriptionHelpFormatter)
    PARSER.add_argument('-p', '--project_id',
                        help='Identifies the project',
                        dest='project_id',
                        required=True)
    PARSER.add_argument('-d', '--delete',
                        help='A flag to indicate whether or not to delete the keys',
                        action='store_true')
    ARGS = PARSER.parse_args()

    if ARGS.delete:
        deleted_keys = delete_expired_keys(ARGS.project_id)
    else:
        expiring_keys = get_expiring_keys(ARGS.project_id)
        for key in expiring_keys:
            print(key)