import os
from googleapiclient.discovery import build
from google.appengine.api import app_identity
import json

from googleapiclient.errors import HttpError

import resources
import common
import gcs_utils

import time
import logging

BQ_DEFAULT_RETRY_COUNT = 10


class InvalidOperationError(RuntimeError):
    """Raised when an invalid Big Query operation attempted during the validation process"""

    def __init__(self, msg):
        super(InvalidOperationError, self).__init__(msg)


class BigQueryJobWaitError(RuntimeError):
    """Raised when jobs fail to complete after waiting"""

    BigQueryJobWaitErrorMsg = 'The following BigQuery jobs failed to complete: %s.'

    def __init__(self, job_ids, reason=''):
        msg = self.BigQueryJobWaitErrorMsg % job_ids
        if reason:
            msg += ' Reason: %s' % reason
        super(BigQueryJobWaitError, self).__init__(msg)


def get_dataset_id():
    return os.environ.get('BIGQUERY_DATASET_ID')


def get_rdr_dataset_id():
    return os.environ.get('RDR_DATASET_ID')


def get_ehr_rdr_dataset_id():
    return os.environ.get('EHR_RDR_DATASET_ID')


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


def get_table_info(table_id, dataset_id=None, project_id=None):
    """
    Get metadata describing a table

    :param table_id: ID of the table
    :param dataset_id: ID of the dataset containing the table (EHR dataset by default)
    :param project_id: associated project ID (default app ID by default)
    :return:
    """
    bq_service = create_service()
    if project_id is None:
        project_id = app_identity.get_application_id()
    if dataset_id is None:
        dataset_id = get_dataset_id()
    job = bq_service.tables().get(projectId=project_id, datasetId=dataset_id, tableId=table_id)
    return job.execute(num_retries=BQ_DEFAULT_RETRY_COUNT)


def load_csv(schema_path, gcs_object_path, project_id, dataset_id, table_id, write_disposition='WRITE_TRUNCATE',
             allow_jagged_rows=False):
    """
    Load csv file from a bucket into a table in bigquery
    :param schema_path: Path to the schema json file
    :param gcs_object_path: Path to the object (csv file) in GCS
    :param project_id:
    :param dataset_id:
    :param table_id:
    :return:
    """
    bq_service = create_service()

    fields = json.load(open(schema_path, 'r'))
    load = {'sourceUris': [gcs_object_path],
            'schema': {'fields': fields},
            'destinationTable': {
                'projectId': project_id,
                'datasetId': dataset_id,
                'tableId': table_id},
            'skipLeadingRows': 1,
            'allowQuotedNewlines': True,
            'writeDisposition': 'WRITE_TRUNCATE',
            'allowJaggedRows': allow_jagged_rows
            }
    job_body = {'configuration': {
            'load': load
        }
    }
    insert_job = bq_service.jobs().insert(projectId=project_id, body=job_body)
    insert_result = insert_job.execute(num_retries=BQ_DEFAULT_RETRY_COUNT)
    return insert_result


def load_cdm_csv(hpo_id, cdm_table_name, source_folder_prefix=""):
    """
    Load CDM file from a bucket into a table in bigquery
    :param hpo_id: ID for the HPO site
    :param cdm_table_name: name of the CDM table
    :return: an object describing the associated bigquery job
    """
    if cdm_table_name not in common.CDM_TABLES:
        raise ValueError('{} is not a valid table to load'.format(cdm_table_name))

    app_id = app_identity.get_application_id()
    dataset_id = get_dataset_id()
    bucket = gcs_utils.get_hpo_bucket(hpo_id)
    fields_filename = os.path.join(resources.fields_path, cdm_table_name + '.json')
    gcs_object_path = 'gs://%s/%s%s.csv' % (bucket, source_folder_prefix, cdm_table_name)
    table_id = get_table_id(hpo_id, cdm_table_name)
    allow_jagged_rows = cdm_table_name == 'observation'
    return load_csv(fields_filename, gcs_object_path, app_id, dataset_id, table_id, allow_jagged_rows=allow_jagged_rows)


def delete_table(table_id, dataset_id=None):
    """
    Delete bigquery table by id

    Note: This will throw `HttpError` if the table doesn't exist. Use `table_exists` prior if necessary.
    :param table_id: id of the table
    :param dataset_id: id of the dataset (EHR dataset by default)
    :return:
    """
    assert(table_id not in common.VOCABULARY_TABLES)
    app_id = app_identity.get_application_id()
    if dataset_id is None:
        dataset_id = get_dataset_id()
    bq_service = create_service()
    delete_job = bq_service.tables().delete(projectId=app_id, datasetId=dataset_id, tableId=table_id)
    logging.debug('Deleting {dataset_id}.{table_id}'.format(dataset_id=dataset_id, table_id=table_id))
    return delete_job.execute(num_retries=BQ_DEFAULT_RETRY_COUNT)


def table_exists(table_id, dataset_id=None):
    """
    Determine whether a bigquery table exists
    :param table_id: id of the table
    :return: `True` if the table exists, `False` otherwise
    """
    app_id = app_identity.get_application_id()
    if dataset_id is None:
        dataset_id = get_dataset_id()
    bq_service = create_service()
    try:
        bq_service.tables().get(
            projectId=app_id,
            datasetId=dataset_id,
            tableId=table_id).execute(num_retries=BQ_DEFAULT_RETRY_COUNT)
        return True
    except HttpError, err:
        if err.resp.status != 404:
            raise
        return False


def job_status_done(job_id):
    job_details = get_job_details(job_id)
    job_running_status = job_details['status']['state']
    return job_running_status == 'DONE'


def wait_on_jobs(job_ids, retry_count=BQ_DEFAULT_RETRY_COUNT, max_poll_interval=16):
    """
    Exponential backoff wait for jobs to complete
    :param job_ids:
    :param retry_count:
    :param max_poll_interval:
    :return: list of jobs that failed to complete or empty list if all completed
    """
    _job_ids = list(job_ids)
    poll_interval = 1
    for i in range(retry_count):
        logging.debug('Waiting %s seconds for completion of job(s): %s' % (poll_interval, _job_ids))
        time.sleep(poll_interval)
        _job_ids = filter(lambda s: not job_status_done(s), _job_ids)
        if len(_job_ids) == 0:
            return []
        if poll_interval < max_poll_interval:
            poll_interval = 2 ** i
    logging.debug('Job(s) failed to complete: %s' % _job_ids)
    return _job_ids


def get_job_details(job_id):
    """Get job resource corresponding to job_id
    :param job_id: id of the job to get (i.e. `jobReference.jobId` in response body of insert request)
    :returns: the job resource (for details see https://goo.gl/bUE49Z)
    """
    bq_service = create_service()
    app_id = app_identity.get_application_id()
    return bq_service.jobs().get(projectId=app_id, jobId=job_id).execute(num_retries=BQ_DEFAULT_RETRY_COUNT)


def merge_tables(source_dataset_id,
                 source_table_id_list,
                 destination_dataset_id,
                 destination_table_id):
    """Takes a list of table names and runs a copy job

    :source_table_name_list: list of tables to merge
    :source_dataset_name: dataset where all the source tables reside
    :destination_table_name: data goes into this table
    :destination_dataset_name: dataset where the destination table resides
    :returns: True if successfull. Or False if error or taking too long.

    """
    app_id = app_identity.get_application_id()
    source_tables = [{
        "projectId": app_id,
        "datasetId": source_dataset_id,
        "tableId": table_name
    } for table_name in source_table_id_list]
    job_body = {
        'configuration': {
            "copy": {
                "sourceTables": source_tables,
                "destinationTable": {
                          "projectId": app_id,
                          "datasetId": destination_dataset_id,
                          "tableId": destination_table_id
                        },
                "writeDisposition": "WRITE_TRUNCATE",
            }
        }
    }

    bq_service = create_service()
    insert_result = bq_service.jobs().insert(projectId=app_id,
                                             body=job_body).execute(num_retries=BQ_DEFAULT_RETRY_COUNT)
    job_id = insert_result['jobReference']['jobId']
    incomplete_jobs = wait_on_jobs([job_id])

    if len(incomplete_jobs) == 0:
        job_status = get_job_details(job_id)['status']
        if 'errorResult' in job_status:
            error_messages = ['{}'.format(item['message'])
                              for item in job_status['errors']]
            logging.info(' || '.join(error_messages))
            return False, ' || '.join(error_messages)
    else:
        logging.info("Wait timeout exceeded before load job with id '%s' was \
                     done" % job_id)
        return False, "Job timeout"
    return True, ""


def query(q, use_legacy_sql=False, destination_table_id=None, retry_count=BQ_DEFAULT_RETRY_COUNT,
          write_disposition='WRITE_EMPTY', destination_dataset_id=None):
    """
    Execute a SQL query on BigQuery dataset

    :param q: SQL statement
    :param use_legacy_sql: True if using legacy syntax, False by default
    :param destination_table_id: if set, output is saved in a table with the specified id
    :param retry_count: number of times to retry with randomized exponential backoff
    :param write_disposition: WRITE_TRUNCATE, WRITE_APPEND or WRITE_EMPTY (default)
    :param destination_dataset_id: dataset ID of destination table (EHR dataset by default)
    :return: if destination_table_id is supplied then job info, otherwise job query response
             (see https://goo.gl/AoGY6P and https://goo.gl/bQ7o2t)
    """
    bq_service = create_service()
    app_id = app_identity.get_application_id()

    if destination_table_id:
        if destination_dataset_id is None:
            destination_dataset_id = get_dataset_id()
        job_body = {
            'configuration':
                {
                    'query': {
                        'query': q,
                        'useLegacySql': use_legacy_sql,
                        'defaultDataset': {
                            'projectId': app_id,
                            'datasetId': get_dataset_id()
                        },
                        'destinationTable': {
                            'projectId': app_id,
                            'datasetId': destination_dataset_id,
                            'tableId': destination_table_id
                        },
                        'writeDisposition': write_disposition
                    }
                }
        }
        return bq_service.jobs().insert(projectId=app_id, body=job_body).execute(num_retries=retry_count)
    else:
        job_body = {
            'defaultDataset': {
                'projectId': app_id,
                'datasetId': get_dataset_id()
            },
            'query': q,
            'timeoutMs': 60000,
            'useLegacySql': use_legacy_sql
        }
        return bq_service.jobs().query(projectId=app_id, body=job_body).execute(num_retries=retry_count)


def create_table(table_id, fields, drop_existing=False, dataset_id=None):
    """
    Create a table with the given table id and schema
    :param table_id: id of the resulting table
    :param fields: a list of `dict` with the following keys: type, name, mode
    :param drop_existing: if True delete an existing table with the given table_id
    :param dataset_id: dataset to create the table in (defaults to EHR dataset)
    :return: table reference object
    """
    if dataset_id is None:
        dataset_id = get_dataset_id()
    if table_exists(table_id, dataset_id):
        if drop_existing:
            delete_table(table_id, dataset_id)
        else:
            raise InvalidOperationError('Attempt to create an existing table with id `%s`.' % table_id)
    bq_service = create_service()
    app_id = app_identity.get_application_id()
    insert_body = {
        "tableReference": {
            "projectId": app_id,
            "datasetId": dataset_id,
            "tableId": table_id
        },
        'schema': {'fields': fields}
    }
    field_names = [field['name'] for field in fields]
    if 'person_id' in field_names:
        insert_body['clustering'] = {
            'fields': ['person_id']
        }
        insert_body['timePartitioning'] = {
            'type': 'DAY'
        }
    insert_job = bq_service.tables().insert(projectId=app_id, datasetId=dataset_id, body=insert_body)
    return insert_job.execute(num_retries=BQ_DEFAULT_RETRY_COUNT)


def create_standard_table(table_name, table_id, drop_existing=False, dataset_id=None):
    """
    Create a supported OHDSI table
    :param table_name: the name of a table whose schema is specified
    :param table_id: name fo the table to create in the bigquery dataset
    :param drop_existing: if True delete an existing table with the given table_id
    :param dataset_id: dataset to create the table in
    :return: table reference object
    """
    fields_filename = os.path.join(resources.fields_path, table_name + '.json')
    fields = json.load(open(fields_filename, 'r'))
    return create_table(table_id, fields, drop_existing, dataset_id)


def list_tables(dataset_id=None):
    """
    List all the tables in the dataset

    :param dataset_id: dataset to list tables for (EHR dataset by default)
    :return: an object with the structure described at https://goo.gl/Z17MWs

    Example:
      result = list_tables()
      for table in result['tables']:
          print table['id']
    """
    bq_service = create_service()
    app_id = app_identity.get_application_id()
    if dataset_id is None:
        dataset_id = get_dataset_id()
    return bq_service.tables().list(projectId=app_id, datasetId=dataset_id).execute(num_retries=BQ_DEFAULT_RETRY_COUNT)


def list_dataset_contents(dataset_id):
    project_id = app_identity.get_application_id()
    service = create_service()
    req = service.tables().list(projectId=project_id, datasetId=dataset_id)
    all_tables = []
    while req:
        resp = req.execute()
        items = [item['id'].split('.')[-1] for item in resp.get('tables', [])]
        all_tables.extend(items or [])
        req = service.tables().list_next(req, resp)
    return all_tables
