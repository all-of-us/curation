import json
import os

PROJECT_ID = 'project_id'
APPLICATION_ID = 'APPLICATION_ID'
GOOGLE_APPLICATION_CREDENTIALS = 'GOOGLE_APPLICATION_CREDENTIALS'
BIGQUERY_DATASET_ID = 'BIGQUERY_DATASET_ID'


def get_creds(creds_path):
    """
    loads a credentials as a json object
    :param creds_path: path to GCP credentials file
    :return:
    """
    with open(creds_path, 'rb') as creds_fp:
        return json.load(creds_fp)


def activate_creds(creds_path):
    """
    activates google cloud service account credentials
    :param creds_path: path to the service account key file
    :return:
    """
    creds = get_creds(creds_path)
    project_id = creds.get(PROJECT_ID)
    if not project_id:
        raise OSError('%s does not refer to a valid GCP key file' % creds_path)
    os.environ[APPLICATION_ID] = project_id
    os.environ[GOOGLE_APPLICATION_CREDENTIALS] = creds_path
    return creds


def set_default_dataset_id(dataset_id):
    """
    sets BIGQUERY_DATASET_ID environment variable to a name of dataset
    :param dataset_id: name of the dataset_id
    :return:
    """
    os.environ[BIGQUERY_DATASET_ID] = dataset_id
