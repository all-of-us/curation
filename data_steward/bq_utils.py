import os
from googleapiclient.discovery import build
from google.appengine.api import app_identity
import json

from googleapiclient.errors import HttpError

import resources
import common
import gcs_utils


def get_dataset_id():
    return os.environ.get('BIGQUERY_DATASET_ID')


def create_service():
    return build('bigquery', 'v2')


def get_table_id(hpo_id, table_name):
    """
    Get the bigquery table id associated with an HPOs CDM table
    :param hpo_id: ID of the HPO
    :param table_name: name of the CDM table
    :return: the table id
    """
    # TODO revisit this; currently prefixing table names with hpo_id
    return hpo_id + '_' + table_name


def load_table_from_bucket(hpo_id, cdm_table_name):
    """
    Load csv file from a bucket into a table in bigquery
    :param hpo_id: ID for the HPO site
    :param cdm_table_name: name of the CDM table
    :return: an object describing the associated bigquery job
    """
    if cdm_table_name not in common.CDM_TABLES:
        raise ValueError('{} is not a valid table to load'.format(cdm_table_name))

    app_id = app_identity.get_application_id()
    dataset_id = get_dataset_id()
    bq_service = build('bigquery', 'v2')

    bucket = gcs_utils.get_hpo_bucket(hpo_id)
    fields_filename = os.path.join(resources.fields_path, cdm_table_name + '.json')
    gcs_object_path = 'gs://%s/%s.csv' % (bucket, cdm_table_name)
    table_id = get_table_id(hpo_id, cdm_table_name)

    fields = json.load(open(fields_filename, 'r'))
    job_body = {
        'configuration':
            {
                'load':
                    {
                        'sourceUris': [gcs_object_path],
                        'schema': {'fields': fields},
                        'destinationTable': {
                            'projectId': app_id,
                            'datasetId': dataset_id,
                            'tableId': table_id
                        },
                        'skipLeadingRows': 1
                    }
            }
    }
    insert_result = bq_service.jobs().insert(projectId=app_id, body=job_body).execute()
    return insert_result


def delete_table(table_id):
    """
    Delete bigquery table by id

    Note: This will throw `HttpError` if the table doesn't exist. Use `table_exists` prior if necessary.
    :param table_id: id of the table
    :return:
    """
    app_id = app_identity.get_application_id()
    dataset_id = get_dataset_id()
    bq_service = create_service()
    return bq_service.tables().delete(projectId=app_id, datasetId=dataset_id, tableId=table_id).execute()


def table_exists(table_id):
    """
    Determine whether a bigquery table exists
    :param table_id: id of the table
    :return: `True` if the table exists, `False` otherwise
    """
    app_id = app_identity.get_application_id()
    dataset_id = get_dataset_id()
    bq_service = create_service()
    try:
        bq_service.tables().get(
            projectId=app_id,
            datasetId=dataset_id,
            tableId=table_id).execute()
        return True
    except HttpError, err:
        if err.resp.status != 404:
            raise
        return False


def get_job_details(job_id):
    """Get job resource corresponding to job_id
    :param job_id: id of the job to get (i.e. `jobReference.jobId` in response body of insert request)
    :returns: the job resource (for details see https://goo.gl/bUE49Z)
    """
    bq_service = create_service()
    app_id = app_identity.get_application_id()
    return bq_service.jobs().get(projectId=app_id, jobId=job_id).execute()
