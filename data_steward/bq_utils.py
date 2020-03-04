# Python imports
import json
import logging
import os
import socket
import time
from datetime import datetime

# Third party imports
import app_identity
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# Project imports
import common
import gcs_utils
import resources
from constants import bq_utils as bq_consts
from io import open

socket.setdefaulttimeout(bq_consts.SOCKET_TIMEOUT)


class InvalidOperationError(RuntimeError):
    """Raised when an invalid Big Query operation attempted during the validation process"""

    def __init__(self, msg):
        self.msg = msg
        super(InvalidOperationError, self).__init__(self.msg)


class BigQueryJobWaitError(RuntimeError):
    """Raised when jobs fail to complete after waiting"""

    BigQueryJobWaitErrorMsg = 'The following BigQuery jobs failed to complete: %s.'

    def __init__(self, job_ids, reason=''):
        msg = self.BigQueryJobWaitErrorMsg % job_ids
        if reason:
            msg += ' Reason: %s' % reason
        super(BigQueryJobWaitError, self).__init__(msg)


def get_output_project_id():
    return os.environ.get('OUTPUT_PROJECT_ID')


def get_dataset_id():
    return os.environ.get('BIGQUERY_DATASET_ID')


def get_unioned_dataset_id():
    return os.environ.get('UNIONED_DATASET_ID')


def get_rdr_dataset_id():
    return os.environ.get('RDR_DATASET_ID')


def get_combined_snapshot_dataset_id():
    return os.environ.get('COMBINED_SNAPSHOT')


def get_combined_dataset_id():
    return os.environ.get('COMBINED_DATASET_ID')


def get_combined_deid_clean_dataset_id():
    return os.environ.get('COMBINED_DEID_CLEAN_DATASET_ID')


def get_retraction_type():
    return os.environ.get('RETRACTION_TYPE')


def get_retraction_hpo_id():
    return os.environ.get('RETRACTION_HPO_ID')


def get_retraction_pid_table_id():
    return os.environ.get('RETRACTION_PID_TABLE_ID')


def get_retraction_sandbox_dataset_id():
    return os.environ.get('RETRACTION_SANDBOX_DATASET_ID')


def get_retraction_dataset_ids():
    """
    Dataset ids from which to retract, separated by spaces
    If retraction needs to be performed on all datasets in the project, set to 'all_datasets'
    :return: string 'all_datasets' or dataset_ids separated by spaces
    """
    return os.environ.get('RETRACTION_DATASET_IDS')


def get_retraction_submission_folder():
    """
    Submission folder from which to retract
    If retraction needs to be performed on all submissions by a site, set to 'all_folders'
    :return: string 'all_folders' or submission folder name
    """
    return os.environ.get('RETRACTION_SUBMISSION_FOLDER')


def get_combined_deid_dataset_id():
    return os.environ.get('COMBINED_DEID_DATASET_ID')


def get_validation_results_dataset_id():
    """
    Get the Validation dataset id.

    If the environment variable has not been set, default to the defined name,
    set the environment variable, and return the dataset name.

    :return:  A name for the validation dataset id.
    """
    dataset_id = os.environ.get(bq_consts.MATCH_DATASET, bq_consts.BLANK)
    if dataset_id == bq_consts.BLANK:
        date_string = datetime.now().strftime(bq_consts.DATE_FORMAT)
        dataset_id = bq_consts.VALIDATION_DATASET_FORMAT.format(date_string)
        os.environ[bq_consts.MATCH_DATASET] = dataset_id
    return dataset_id


def create_service():
    return build('bigquery', 'v2', cache={})


def get_table_id(hpo_id, table_name):
    """
    Get the bigquery table id associated with an HPOs CDM table
    :param hpo_id: ID of the HPO
    :param table_name: name of the CDM table
    :return: the table id
    """
    if hpo_id is None:
        return table_name
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
    job = bq_service.tables().get(projectId=project_id,
                                  datasetId=dataset_id,
                                  tableId=table_id)
    return job.execute(num_retries=bq_consts.BQ_DEFAULT_RETRY_COUNT)


def load_csv(schema_path,
             gcs_object_path,
             project_id,
             dataset_id,
             table_id,
             write_disposition=bq_consts.WRITE_TRUNCATE,
             allow_jagged_rows=False):
    """
    Load csv file from a bucket into a table in bigquery
    :param schema_path: Path to the schema json file
    :param gcs_object_path: Path to the object (csv file) in GCS
    :param project_id:
    :param dataset_id:
    :param table_id:
    :param write_disposition:  tell BQ how to handle existing tables.
        options are TRUNCATE, APPEND, and EMPTY.  default is TRUNCATE.
    :param allow_jagged_rows:
    :return:
    """
    bq_service = create_service()

    with open(schema_path, 'r') as schema_file:
        fields = json.load(schema_file)
    load = {
        'sourceUris': [gcs_object_path],
        bq_consts.SCHEMA: {
            bq_consts.FIELDS: fields
        },
        'destinationTable': {
            'projectId': project_id,
            'datasetId': dataset_id,
            'tableId': table_id
        },
        'skipLeadingRows': 1,
        'allowQuotedNewlines': True,
        'writeDisposition': write_disposition,
        'allowJaggedRows': allow_jagged_rows,
        'sourceFormat': 'CSV'
    }
    job_body = {'configuration': {'load': load}}
    insert_job = bq_service.jobs().insert(projectId=project_id, body=job_body)
    insert_result = insert_job.execute(
        num_retries=bq_consts.BQ_DEFAULT_RETRY_COUNT)
    return insert_result


def load_cdm_csv(hpo_id,
                 cdm_table_name,
                 source_folder_prefix="",
                 dataset_id=None):
    """
    Load CDM file from a bucket into a table in bigquery
    :param hpo_id: ID for the HPO site
    :param cdm_table_name: name of the CDM table
    :return: an object describing the associated bigquery job
    """
    if cdm_table_name not in resources.CDM_TABLES:
        raise ValueError(
            '{} is not a valid table to load'.format(cdm_table_name))

    app_id = app_identity.get_application_id()
    if dataset_id is None:
        dataset_id = get_dataset_id()
    bucket = gcs_utils.get_hpo_bucket(hpo_id)
    fields_filename = os.path.join(resources.fields_path,
                                   cdm_table_name + '.json')
    gcs_object_path = 'gs://%s/%s%s.csv' % (bucket, source_folder_prefix,
                                            cdm_table_name)
    table_id = get_table_id(hpo_id, cdm_table_name)
    allow_jagged_rows = cdm_table_name == 'observation'
    return load_csv(fields_filename,
                    gcs_object_path,
                    app_id,
                    dataset_id,
                    table_id,
                    allow_jagged_rows=allow_jagged_rows)


def load_pii_csv(hpo_id, pii_table_name, source_folder_prefix=""):
    """
    Load PII file from a bucket into a table in bigquery
    :param hpo_id: ID for the HPO site
    :param pii_table_name: name of the CDM table
    :return: an object describing the associated bigquery job
    """
    if pii_table_name not in common.PII_TABLES:
        raise ValueError(
            '{} is not a valid table to load'.format(pii_table_name))

    app_id = app_identity.get_application_id()
    dataset_id = get_dataset_id()
    bucket = gcs_utils.get_hpo_bucket(hpo_id)
    fields_filename = os.path.join(resources.fields_path,
                                   pii_table_name + '.json')
    gcs_object_path = 'gs://%s/%s%s.csv' % (bucket, source_folder_prefix,
                                            pii_table_name)
    table_id = get_table_id(hpo_id, pii_table_name)
    return load_csv(fields_filename, gcs_object_path, app_id, dataset_id,
                    table_id)


def load_from_csv(hpo_id, table_name, source_folder_prefix=""):
    """
    Load CDM or PII file from a bucket into a table in bigquery
    :param hpo_id: ID for the HPO site
    :param table_name: name of the CDM or PII table
    :return: an object describing the associated bigquery job
    """
    if resources.is_pii_table(table_name):
        return load_pii_csv(hpo_id, table_name, source_folder_prefix)
    else:
        return load_cdm_csv(hpo_id, table_name, source_folder_prefix)


def delete_table(table_id, dataset_id=None):
    """
    Delete bigquery table by id

    Note: This will throw `HttpError` if the table doesn't exist. Use `table_exists` prior if necessary.
    :param table_id: id of the table
    :param dataset_id: id of the dataset (EHR dataset by default)
    :return:
    """
    assert (table_id not in common.VOCABULARY_TABLES)
    app_id = app_identity.get_application_id()
    if dataset_id is None:
        dataset_id = get_dataset_id()
    bq_service = create_service()
    delete_job = bq_service.tables().delete(projectId=app_id,
                                            datasetId=dataset_id,
                                            tableId=table_id)
    logging.info('Deleting {dataset_id}.{table_id}'.format(
        dataset_id=dataset_id, table_id=table_id))
    return delete_job.execute(num_retries=bq_consts.BQ_DEFAULT_RETRY_COUNT)


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
            projectId=app_id, datasetId=dataset_id, tableId=table_id).execute(
                num_retries=bq_consts.BQ_DEFAULT_RETRY_COUNT)
        return True
    except HttpError as err:
        if err.resp.status != 404:
            raise
        return False


def job_status_done(job_id):
    """
    Check if the job is complete
    
    :param job_id: the job id
    :return: a bool indicating whether the job is done
    """
    job_details = get_job_details(job_id)
    job_running_status = job_details['status']['state']
    return job_running_status == 'DONE'


def job_status_errored(job_id):
    """
    Check if the job is complete with an error
    
    :param job_id: the job id
    :return: a tuple that contains a bool indicating whether the job is errored and its corresponding error message
    """
    job_details = get_job_details(job_id)
    job_status = job_details['status']
    job_running_state = job_status['state']
    is_errored = job_running_state == 'DONE' and 'errorResult' in job_status
    error_message = job_status['errorResult']['message'] if is_errored else None
    return is_errored, error_message


def wait_on_jobs(job_ids,
                 retry_count=bq_consts.BQ_DEFAULT_RETRY_COUNT,
                 max_poll_interval=300):
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
        logging.info('Waiting %s seconds for completion of job(s): %s' %
                     (poll_interval, _job_ids))
        time.sleep(poll_interval)
        _job_ids = [
            job_id for job_id in _job_ids if not job_status_done(job_id)
        ]
        if len(_job_ids) == 0:
            return []
        if poll_interval < max_poll_interval:
            poll_interval = 2**i
    logging.info('Job(s) failed to complete: %s' % _job_ids)
    return _job_ids


def get_job_details(job_id):
    """Get job resource corresponding to job_id
    :param job_id: id of the job to get (i.e. `jobReference.jobId` in response body of insert request)
    :returns: the job resource (for details see https://goo.gl/bUE49Z)
    """
    bq_service = create_service()
    app_id = app_identity.get_application_id()
    return bq_service.jobs().get(
        projectId=app_id,
        jobId=job_id).execute(num_retries=bq_consts.BQ_DEFAULT_RETRY_COUNT)


def merge_tables(source_dataset_id, source_table_id_list,
                 destination_dataset_id, destination_table_id):
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
    insert_result = bq_service.jobs().insert(
        projectId=app_id,
        body=job_body).execute(num_retries=bq_consts.BQ_DEFAULT_RETRY_COUNT)
    job_id = insert_result[bq_consts.JOB_REFERENCE][bq_consts.JOB_ID]
    incomplete_jobs = wait_on_jobs([job_id])

    if len(incomplete_jobs) == 0:
        job_status = get_job_details(job_id)['status']
        if 'errorResult' in job_status:
            error_messages = [
                '{}'.format(item['message']) for item in job_status['errors']
            ]
            logging.info(' || '.join(error_messages))
            return False, ' || '.join(error_messages)
    else:
        logging.info("Wait timeout exceeded before load job with id '%s' was \
                     done" % job_id)
        return False, "Job timeout"
    return True, ""


def query(q,
          use_legacy_sql=False,
          destination_table_id=None,
          retry_count=bq_consts.BQ_DEFAULT_RETRY_COUNT,
          write_disposition=bq_consts.WRITE_EMPTY,
          destination_dataset_id=None,
          batch=None,
          dry_run=False):
    """
    Execute a SQL query on BigQuery dataset

    :param q: SQL statement
    :param use_legacy_sql: True if using legacy syntax, False by default
    :param destination_table_id: if set, output is saved in a table with the specified id
    :param retry_count: number of times to retry with randomized exponential backoff
    :param write_disposition: WRITE_TRUNCATE, WRITE_APPEND or WRITE_EMPTY (default)
    :param destination_dataset_id: dataset ID of destination table (EHR dataset by default)
    :param batch: whether the query should be run in INTERACTIVE or BATCH mode.
        Defaults to INTERACTIVE.
    :param dry_run: Boolean. If true, validates query without running it. Helps with testing
    :return: if destination_table_id is supplied then job info, otherwise job query response
             (see https://goo.gl/AoGY6P and https://goo.gl/bQ7o2t)
    """
    bq_service = create_service()
    app_id = app_identity.get_application_id()

    priority_mode = bq_consts.INTERACTIVE if batch is None else bq_consts.BATCH

    if destination_table_id:
        if destination_dataset_id is None:
            destination_dataset_id = get_dataset_id()
        job_body = {
            'configuration': {
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
        return bq_service.jobs().insert(
            projectId=app_id, body=job_body).execute(num_retries=retry_count)
    else:
        job_body = {
            'defaultDataset': {
                'projectId': app_id,
                'datasetId': get_dataset_id()
            },
            'query': q,
            'timeoutMs': bq_consts.SOCKET_TIMEOUT,
            'useLegacySql': use_legacy_sql,
            'dryRun': dry_run,
            bq_consts.PRIORITY_TAG: priority_mode,
        }
        return bq_service.jobs().query(
            projectId=app_id, body=job_body).execute(num_retries=retry_count)


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
            raise InvalidOperationError(
                'Attempt to create an existing table with id `%s`.' % table_id)
    bq_service = create_service()
    app_id = app_identity.get_application_id()
    insert_body = {
        "tableReference": {
            "projectId": app_id,
            "datasetId": dataset_id,
            "tableId": table_id
        },
        bq_consts.SCHEMA: {
            bq_consts.FIELDS: fields
        }
    }
    field_names = [field['name'] for field in fields]
    if 'person_id' in field_names:
        insert_body['clustering'] = {bq_consts.FIELDS: ['person_id']}
        insert_body['timePartitioning'] = {'type': 'DAY'}
    insert_job = bq_service.tables().insert(projectId=app_id,
                                            datasetId=dataset_id,
                                            body=insert_body)
    return insert_job.execute(num_retries=bq_consts.BQ_DEFAULT_RETRY_COUNT)


def create_standard_table(table_name,
                          table_id,
                          drop_existing=False,
                          dataset_id=None,
                          force_all_nullable=False):
    """
    Create a supported OHDSI table
    :param table_name: the name of a table whose schema is specified
    :param table_id: name fo the table to create in the bigquery dataset
    :param drop_existing: if True delete an existing table with the given table_id
    :param dataset_id: dataset to create the table in
    :param force_all_nullable: if True, overrides all fields of the schema to be nullable, primarily for testing
    :return: table reference object
    """
    fields_filename = os.path.join(resources.fields_path, table_name + '.json')
    with open(fields_filename, 'r') as fields_file:
        fields = json.load(fields_file)
    if force_all_nullable:
        for f in fields:
            f["mode"] = "nullable"
    return create_table(table_id, fields, drop_existing, dataset_id)


def list_tables(dataset_id=None,
                max_results=bq_consts.LIST_TABLES_MAX_RESULTS,
                project_id=None):
    """
    List all the tables in the dataset

    :param dataset_id: dataset to list tables for (EHR dataset by default)
    :param max_results: maximum number of results to return
    :return: an object with the structure described at https://goo.gl/Z17MWs

    Example:
      result = list_tables()
      for table in result['tables']:
          print table['id']
    """
    bq_service = create_service()
    if project_id is None:
        app_id = app_identity.get_application_id()
    else:
        app_id = project_id
    if dataset_id is None:
        dataset_id = get_dataset_id()
    results = []
    request = bq_service.tables().list(projectId=app_id,
                                       datasetId=dataset_id,
                                       maxResults=max_results)
    while request is not None:
        response = request.execute(num_retries=bq_consts.BQ_DEFAULT_RETRY_COUNT)
        tables = response.get('tables', [])
        results.extend(tables or [])
        request = bq_service.tables().list_next(request, response)
    return results


def get_table_id_from_obj(table_obj):
    return table_obj['id'].split('.')[-1]


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


def list_datasets(project_id):
    """
    List the datasets in the specified project

    :param project_id: identifies the project
    :return:
    """
    service = create_service()
    req = service.datasets().list(projectId=project_id)
    all_datasets = []
    while req:
        resp = req.execute()
        items = [item for item in resp.get('datasets', [])]
        all_datasets.extend(items or [])
        req = service.datasets().list_next(req, resp)
    return all_datasets


def get_dataset_id_from_obj(dataset_obj):
    return dataset_obj['id'].split(':')[-1]


def large_response_to_rowlist(query_response):
    """
    Convert a query response to a list of dictionary objects

    This automatically uses the pageToken feature to iterate through a
    large result set.  Use cautiously.

    :param query_response: the query response object to iterate
    :return: list of dictionaries
    """
    bq_service = create_service()
    app_id = app_identity.get_application_id()

    page_token = query_response.get(bq_consts.PAGE_TOKEN)
    job_ref = query_response.get(bq_consts.JOB_REFERENCE)
    job_id = job_ref.get(bq_consts.JOB_ID)

    result_list = response2rows(query_response)
    while page_token:
        next_grouping = bq_service.jobs() \
            .getQueryResults(projectId=app_id, jobId=job_id, pageToken=page_token) \
            .execute(num_retries=bq_consts.BQ_DEFAULT_RETRY_COUNT)
        page_token = next_grouping.get(bq_consts.PAGE_TOKEN)
        intermediate_rows = response2rows(next_grouping)
        result_list.extend(intermediate_rows)

    return result_list


def response2rows(r):
    """
    Convert a query response to a list of dict

    :param r: a query response object
    :return: list of dict
    """
    rows = r.get(bq_consts.ROWS, [])
    schema = r.get(bq_consts.SCHEMA, {bq_consts.FIELDS: None})[bq_consts.FIELDS]
    return [_transform_row(row, schema) for row in rows]


def _transform_row(row, schema):
    """
    Apply the given schema to the given BigQuery data row. Adapted from https://goo.gl/dWszQJ.

    :param row: A single BigQuery row to transform
    :param schema: The BigQuery table schema to apply to the row, specifically
        the list of field dicts.
    :returns: Row as a dict
    """

    log = {}

    # Match each schema column with its associated row value
    for index, col_dict in enumerate(schema):
        col_name = col_dict['name']
        row_value = row['f'][index]['v']

        if row_value is None:
            log[col_name] = None
            continue

        # Recurse on nested records
        if col_dict['type'] == 'RECORD':
            row_value = self._recurse_on_row(col_dict, row_value)

        # Otherwise just cast the value
        elif col_dict['type'] == 'INTEGER':
            row_value = int(row_value)

        elif col_dict['type'] == 'FLOAT':
            row_value = float(row_value)

        elif col_dict['type'] == 'BOOLEAN':
            row_value = row_value in ('True', 'true', 'TRUE')

        elif col_dict['type'] == 'TIMESTAMP':
            row_value = float(row_value)

        log[col_name] = row_value

    return log


def list_all_table_ids(dataset_id=None):
    tables = list_tables(dataset_id)
    return [table['tableReference']['tableId'] for table in tables]


def create_dataset(project_id=None,
                   dataset_id=None,
                   description=None,
                   friendly_name=None,
                   overwrite_existing=None):
    """
    Creates a new dataset from the API.

    :param  project_id:  name of the project to create the dataset in.  defaults
        to the currently configured project if missing.
    :param  dataset_id:  name to give the new dataset.  it is required.
    :param  description:  dataset description.  it is required.
    :param  friendly_name:  an user friendly name for the dataset.  optional.
    :param  overwrite_existing:  Determine if the dataset should be overwritten
        if it already exists.  defaults to true / overwrite.

    """
    if dataset_id is None:
        raise RuntimeError("Cannot create a dataset without a name")

    if description is None:
        raise RuntimeError("Will not create a dataset without a description")

    if project_id is None:
        app_id = app_identity.get_application_id()
    else:
        app_id = project_id

    if overwrite_existing is None:
        overwrite_existing = bq_consts.TRUE
    elif not overwrite_existing:
        overwrite_existing = bq_consts.FALSE
    else:
        overwrite_existing = bq_consts.TRUE

    bq_service = create_service()

    job_body = {
        bq_consts.DATASET_REFERENCE: {
            bq_consts.PROJECT_ID: app_id,
            bq_consts.DATASET_ID: dataset_id,
        },
        bq_consts.DESCRIPTION: description,
    }

    if friendly_name:
        job_body.update({bq_consts.FRIENDLY_NAME: friendly_name})

    insert_dataset = bq_service.datasets().insert(projectId=app_id,
                                                  body=job_body)
    try:
        insert_result = insert_dataset.execute(
            num_retries=bq_consts.BQ_DEFAULT_RETRY_COUNT)
        logging.info('Created dataset:\t%s.%s', app_id, dataset_id)
    except HttpError as error:
        # dataset exists.  try deleting if deleteContents is set and try again.
        if error.resp.status == 409:
            if overwrite_existing == bq_consts.TRUE:
                rm_dataset = bq_service.datasets().delete(
                    projectId=app_id,
                    datasetId=dataset_id,
                    deleteContents=overwrite_existing)
                rm_dataset.execute(num_retries=bq_consts.BQ_DEFAULT_RETRY_COUNT)
                insert_result = insert_dataset.execute(
                    num_retries=bq_consts.BQ_DEFAULT_RETRY_COUNT)
                logging.info('Overwrote dataset %s.%s', app_id, dataset_id)
            else:
                logging.exception(
                    'Trying to create a duplicate dataset without overwriting '
                    'the existing dataset.  Cannot be done!\n\ncreate_dataset '
                    'called with values:\n'
                    'project_id:\t%s\n'
                    'dataset_id:\t%s\n'
                    'description:\t%s\n'
                    'friendly_name:\t%s\n'
                    'overwrite_existing:\t%s\n\n', project_id, dataset_id,
                    description, friendly_name, overwrite_existing)
                raise
        else:
            logging.exception(
                'Encountered an HttpError when trying to create '
                'dataset: %s.%s', app_id, dataset_id)
            raise

    return insert_result


def csv_line_to_sql_row_expr(row: dict, fields: list):
    """
    Translate a dict to a SQL row expression based on a fields spec

    :param row: dict whose values are all strings
    :param fields: bigquery fields spec with keys {name, type, mode, description}
    :return: SQL expression for row object
    :rtype: str
    :example:
    >>> fields = [{ 'name': 'int_col',  'type': 'integer', 'mode': 'required', 'description': ''},
    >>>           { 'name': 'date_col', 'type': 'date',    'mode': 'required', 'description': ''},
    >>>           { 'name': 'str_col',  'type': 'string',  'mode': 'nullable', 'description': ''}]
    >>> csv_line_to_sql_row_expr({'int_col': '1234', 'date_col': '2019-01-01', 'str_col': ''}, fields)
    "(1234, '2019-01-01', NULL)"
    """
    val_exprs = []
    # TODO refactor for all other types or use external library
    for field_name, val in row.items():
        field = next(filter(lambda f: f['name'] == field_name, fields), None)
        if field is None:
            raise InvalidOperationError(
                f'Unable to marshal {val}: field "{field_name}" was not found')
        if not val:
            if field['mode'] == 'nullable':
                val_expr = "NULL"
            elif field['type'] == 'string':
                val_expr = "''"
            else:
                raise InvalidOperationError(
                    f'Value not provided for required field {field_name}')
        elif field['type'] in ['string', 'date', 'timestamp']:
            val_expr = f"'{val}'"
        else:
            val_expr = f"{val}"
        val_exprs.append(val_expr)
    cols = ','.join(val_exprs)
    return f'({cols})'


def load_table_from_csv(project_id,
                        dataset_id,
                        table_name,
                        csv_path=None,
                        fields=None):
    """
    Loads BQ table from a csv file without making use of GCS buckets

    :param project_id: project containing the dataset
    :param dataset_id: dataset where the table needs to be created
    :param table_name: name of the table to be created
    :param csv_path: path to the csv file which needs to be loaded into BQ.
                     If None, assumes that the file exists in the resources folder with the name table_name.csv
    :param fields: fields in list of dicts format. If set to None, assumes that
                   the fields are stored in a json file in resources/fields named table_name.json
    :return: BQ response for the load query
    """
    if csv_path is None:
        csv_path = os.path.join(resources.resource_path, table_name + ".csv")
    table_list = resources.csv_to_list(csv_path)

    if fields is None:
        fields_filename = os.path.join(resources.fields_path,
                                       table_name + '.json')
        with open(fields_filename, 'r') as f:
            fields = json.load(f)
    field_names = ', '.join([field['name'] for field in fields])
    row_exprs = [csv_line_to_sql_row_expr(t, fields) for t in table_list]
    formatted_mapping_list = ', '.join(row_exprs)

    create_table(table_id=table_name,
                 fields=fields,
                 drop_existing=True,
                 dataset_id=dataset_id)

    table_populate_query = bq_consts.INSERT_QUERY.format(
        project_id=project_id,
        dataset_id=dataset_id,
        table_id=table_name,
        columns=field_names,
        mapping_list=formatted_mapping_list)
    result = query(table_populate_query)
    return result


def get_hpo_info():
    hpo_list = []
    project_id = app_identity.get_application_id()
    hpo_table_query = bq_consts.GET_HPO_CONTENTS_QUERY.format(
        project_id=project_id,
        LOOKUP_TABLES_DATASET_ID=bq_consts.LOOKUP_TABLES_DATASET_ID,
        HPO_SITE_ID_MAPPINGS_TABLE_ID=bq_consts.HPO_SITE_ID_MAPPINGS_TABLE_ID)
    hpo_response = query(hpo_table_query)
    hpo_table_contents = response2rows(hpo_response)
    for hpo_table_row in hpo_table_contents:
        hpo_id = hpo_table_row[bq_consts.HPO_ID].lower()
        hpo_name = hpo_table_row[bq_consts.SITE_NAME]
        if hpo_id and hpo_name:
            hpo_dict = {"hpo_id": hpo_id, "name": hpo_name}
            hpo_list.append(hpo_dict)
    return hpo_list


def has_primary_key(table):
    """
    Determines if a CDM table contains a numeric primary key field

    :param table: name of a CDM table
    :return: True if the CDM table contains a primary key field, False otherwise
    """
    if table not in resources.CDM_TABLES:
        raise AssertionError()
    fields = resources.fields_for(table)
    id_field = table + '_id'
    return any(field for field in fields
               if field['type'] == 'integer' and field['name'] == id_field)


def create_snapshot_dataset(project_id, dataset_id, snapshot_dataset_id):
    """

    :param dataset_id:
    :param project_id:
    :param snapshot_dataset_id:
    :return:
    """
    dataset_result = create_dataset(
        project_id=project_id,
        dataset_id=snapshot_dataset_id,
        description='Snapshot of {dataset_id}'.format(dataset_id=dataset_id),
        overwrite_existing=True)
    validation_dataset = dataset_result.get(bq_consts.DATASET_REF, {})
    snapshot_dataset_id = validation_dataset.get(bq_consts.DATASET_ID, '')
    # Create the empty tables in the new snapshot dataset
    for table_id in list_all_table_ids(dataset_id):
        metadata = get_table_info(table_id, dataset_id)
        fields = metadata['schema']['fields']
        create_table(table_id,
                     fields,
                     drop_existing=True,
                     dataset_id=snapshot_dataset_id)
    # Copy the table content from the current dataset to the snapshot dataset
    copy_table_job_ids = []
    for table_id in list_all_table_ids(dataset_id):
        select_all_query = (
            'SELECT * FROM `{project_id}.{dataset_id}.{table_id}` ')
        q = select_all_query.format(project_id=project_id,
                                    dataset_id=dataset_id,
                                    table_id=table_id)
        results = query(q,
                        use_legacy_sql=False,
                        destination_table_id=table_id,
                        destination_dataset_id=snapshot_dataset_id,
                        batch=True)
        copy_table_job_ids.append(results['jobReference']['jobId'])
    incomplete_jobs = wait_on_jobs(copy_table_job_ids)
    if len(incomplete_jobs) > 0:
        raise BigQueryJobWaitError(incomplete_jobs)
