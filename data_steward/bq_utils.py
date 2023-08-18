# Python imports
import logging
import os
import socket
import time
import warnings
from datetime import datetime

# Third party imports
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from deprecated import deprecated

# Project imports
import app_identity
import common
from gcloud.gcs import StorageClient
import resources
from constants import bq_utils as bq_consts

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


def get_rdr_project_id():
    return os.environ.get('RDR_PROJECT_ID')


@deprecated(reason='get_output_project_id is deprecated')
def get_output_project_id():
    return os.environ.get('OUTPUT_PROJECT_ID')


@deprecated(reason='Use resources.BIGQUERY_DATASET_ID instead')
def get_dataset_id():
    return os.environ.get('BIGQUERY_DATASET_ID')


def get_unioned_dataset_id():
    return os.environ.get('UNIONED_DATASET_ID')


def get_rdr_dataset_id():
    return os.environ.get('RDR_DATASET_ID')


@deprecated(reason='get_combined_snapshot_dataset_id is deprecated')
def get_combined_snapshot_dataset_id():
    return os.environ.get('COMBINED_SNAPSHOT')


def get_combined_dataset_id():
    return os.environ.get('COMBINED_DATASET_ID')


@deprecated(reason='get_combined_deid_clean_dataset_id is deprecated')
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


@deprecated(reason='get_fitbit_dataset_id is deprecated')
def get_fitbit_dataset_id():
    return os.environ.get('FITBIT_DATASET_ID')


def get_retraction_dataset_ids_table():
    """
    BigQuery table containing dataset ids from which to retract, on separate rows.
    If retraction needs to be performed on all datasets in the project, the table should contain  only "all_datasets"
    :return: string of table id 'all_datasets' or dataset_ids separated by spaces
    """
    return os.environ.get('RETRACTION_DATASET_IDS_TABLE')


def get_retraction_dataset_ids_dataset():
    """
    BigQuery dataset containing the table 'RETRACTION_DATASET_IDS_TABLE' defined above.
    :return: string of dataset containing the table 'RETRACTION_DATASET_IDS_TABLE'
    """
    return os.environ.get('RETRACTION_DATASET_IDS_DATASET')


def get_retraction_submission_folder():
    """
    Submission folder from which to retract
    If retraction needs to be performed on all submissions by a site, set to 'all_folders'
    :return: string 'all_folders' or submission folder name
    """
    return os.environ.get('RETRACTION_SUBMISSION_FOLDER')


@deprecated(reason='get_combined_deid_dataset_id is deprecated')
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


@deprecated(
    reason='Discovery client is being replaced by gcloud.gcs.BigQueryClient()')
def create_service():
    return build('bigquery', 'v2', cache={})


@deprecated(reason='Use resources.get_table_id(table_name, hpo_id=None) instead'
           )
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


@deprecated(reason='Use gcloud.bq.BigQueryClient.get_table(self, table) instead'
           )
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


def load_csv(table_name,
             gcs_object_path,
             project_id,
             dataset_id,
             table_id,
             write_disposition=bq_consts.WRITE_TRUNCATE,
             allow_jagged_rows=False):
    """
    Load csv file from a bucket into a table in bigquery
    :param table_name: table_name to load the fields from resource_files/schermas
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

    fields = resources.fields_for(table_name)
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
    app_id: str = app_identity.get_application_id()
    storage_client = StorageClient(app_id)
    hpo_bucket = storage_client.get_hpo_bucket(hpo_id)

    if cdm_table_name not in resources.CDM_TABLES:
        raise ValueError(
            '{} is not a valid table to load'.format(cdm_table_name))

    if not dataset_id:
        dataset_id: str = get_dataset_id()

    gcs_object_path: str = (f'gs://{hpo_bucket.name}/'
                            f'{source_folder_prefix}'
                            f'{cdm_table_name}.csv')
    table_id = get_table_id(hpo_id, cdm_table_name)
    allow_jagged_rows: bool = cdm_table_name == 'observation'
    return load_csv(cdm_table_name,
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

    storage_client = StorageClient(app_id)
    hpo_bucket = storage_client.get_hpo_bucket(hpo_id)
    gcs_object_path = (f'gs://{hpo_bucket.name}/'
                       f'{source_folder_prefix}'
                       f'{pii_table_name}.csv')
    table_id = get_table_id(hpo_id, pii_table_name)
    return load_csv(pii_table_name, gcs_object_path, app_id, dataset_id,
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
    return load_cdm_csv(hpo_id, table_name, source_folder_prefix)


@deprecated(
    reason=
    'see: https://cloud.google.com/python/docs/reference/bigquery/latest/google.cloud.bigquery.client.Client#google_cloud_bigquery_client_Client_delete_table'
)
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
    logging.info(f"Deleting {dataset_id}.{table_id}")
    return delete_job.execute(num_retries=bq_consts.BQ_DEFAULT_RETRY_COUNT)


@deprecated(
    reason=
    'Use gcloud.bq.BigQueryClient.table_exists(self, table_id: str, dataset_id: str = None) instead'
)
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


@deprecated(reason='job_status_errored is deprecated')
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


def sleeper(poll_interval):
    """
    Calls time.sleep, useful for testing purposes
    :param poll_interval:
    :return:
    """
    time.sleep(poll_interval)
    return


def wait_on_jobs(job_ids, retry_count=bq_consts.BQ_DEFAULT_RETRY_COUNT):
    """
    Implements exponential backoff to wait for jobs to complete
    :param job_ids: list of job_id strings
    :param retry_count: max number of iterations for exponent
    :return: list of jobs that failed to complete or empty list if all completed
    """
    job_ids = list(job_ids)
    poll_interval = 1
    for _ in range(retry_count):
        logging.info(
            f'Waiting {poll_interval} seconds for completion of job(s): {job_ids}'
        )
        sleeper(poll_interval)
        job_ids = [job_id for job_id in job_ids if not job_status_done(job_id)]
        if not job_ids:
            return job_ids
        if poll_interval < bq_consts.MAX_POLL_INTERVAL:
            poll_interval *= 2
    logging.info(f'Job(s) {job_ids} failed to complete')
    return job_ids


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
                f'Attempt to create an existing table with id `{table_id}`.')
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
    fields = resources.fields_for(table_name)
    if force_all_nullable:
        for f in fields:
            f["mode"] = "nullable"
    return create_table(table_id, fields, drop_existing, dataset_id)


@deprecated(
    reason=
    'Use gcloud.bq.BigQueryClient.list_tables(self, dataset: typing.Union[bigquery.DatasetReference, str]) instead'
)
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
    warnings.warn(
        "Function bq_utils.list_tables is deprecated and will be removed in a future version. "
        "Use `utils.bq.list_tables` if needed.",
        PendingDeprecationWarning,
        stacklevel=2,
    )
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


@deprecated(reason='get_table_id_from_obj is deprecated')
def get_table_id_from_obj(table_obj):
    return table_obj['id'].split('.')[-1]


@deprecated(
    reason=
    'Use gcloud.bq.BigQueryClient.list_tables(self, dataset: typing.Union[bigquery.DatasetReference, str]) instead'
)
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


@deprecated(reason=(
    'see: https://googleapis.dev/python/bigquery/latest/generated/google.cloud.bigquery.client.Client.html#google.cloud.bigquery.client.Client.list_datasets '
))
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


@deprecated(reason='get_dataset_id_from_obj is deprecated')
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


def _recurse_on_row(col_dict, nested_value):
    """
    Apply the schema specified by the given dict to the nested value by recursing on it.

        :param col_dict: the schema to apply to the nested value.
        :param nested_value: A value nested in a BigQuery row.
        :returns: Union[dict, list] objects from applied schema.
        """

    row_value = None

    # Multiple nested records
    if col_dict['mode'] == 'REPEATED' and isinstance(nested_value, list):
        row_value = [
            _transform_row(record['v'], col_dict['fields'])
            for record in nested_value
        ]

    # A single nested record
    else:
        row_value = _transform_row(nested_value, col_dict['fields'])

    return row_value


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
            row_value = _recurse_on_row(col_dict, row_value)

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


@deprecated(
    reason=
    'Use gcloud.bq.BigQueryClient.list_tables(self, dataset: typing.Union[bigquery.DatasetReference, str]) instead'
)
def list_all_table_ids(dataset_id=None):
    tables = list_tables(dataset_id)
    return [table['tableReference']['tableId'] for table in tables]


@deprecated(reason=(
    'see: https://googleapis.dev/python/bigquery/latest/generated/google.cloud.bigquery.client.Client.html#google.cloud.bigquery.client.Client.create_dataset'
))
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
        logging.info(f"Created dataset:\t{app_id}.{dataset_id}")
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
                logging.info(f"Overwrote dataset {app_id}.{dataset_id}")
            else:
                logging.exception(
                    f"Trying to create a duplicate dataset without overwriting "
                    f"the existing dataset.  Cannot be done!\n\ncreate_dataset "
                    f"called with values:\n"
                    f"project_id:\t{project_id}\n"
                    f"dataset_id:\t{dataset_id}\n"
                    f"description:\t{description}\n"
                    f"friendly_name:\t{friendly_name}\n"
                    f"overwrite_existing:\t{overwrite_existing}\n\n")
                raise
        else:
            logging.exception(f"Encountered an HttpError when trying to create "
                              f"dataset: {app_id}.{dataset_id}")
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
                     If None, assumes that the file exists in the resource_files folder with the name table_name.csv
    :param fields: fields in list of dicts format. If set to None, assumes that
                   the fields are stored in a json file in resource_files/fields named table_name.json
    :return: BQ response for the load query
    """
    if not csv_path:
        csv_path = os.path.join(resources.resource_files_path,
                                table_name + ".csv")
    table_list = resources.csv_to_list(csv_path)

    if not fields:
        fields = resources.fields_for(table_name)
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
        TABLES_DATASET_ID=bq_consts.LOOKUP_TABLES_DATASET_ID,
        HPO_SITE_TABLE=bq_consts.HPO_SITE_ID_MAPPINGS_TABLE_ID)
    hpo_response = query(hpo_table_query)
    hpo_table_contents = response2rows(hpo_response)
    for hpo_table_row in hpo_table_contents:
        hpo_id = hpo_table_row[bq_consts.HPO_ID].lower()
        hpo_name = hpo_table_row[bq_consts.SITE_NAME]
        if hpo_id and hpo_name:
            hpo_dict = {"hpo_id": hpo_id, "name": hpo_name}
            hpo_list.append(hpo_dict)
    return hpo_list


def get_hpo_bucket_info():
    hpo_list = []
    project_id = app_identity.get_application_id()
    hpo_table_query = bq_consts.GET_HPO_CONTENTS_QUERY.format(
        project_id=project_id,
        TABLES_DATASET_ID=bq_consts.LOOKUP_TABLES_DATASET_ID,
        HPO_SITE_TABLE=bq_consts.HPO_ID_BUCKET_NAME_TABLE_ID)
    hpo_response = query(hpo_table_query)
    hpo_table_contents = response2rows(hpo_response)
    for hpo_table_row in hpo_table_contents:
        hpo_id = hpo_table_row[bq_consts.HPO_ID.lower()].lower()
        hpo_bucket = hpo_table_row[bq_consts.BUCKET_NAME].lower()
        if hpo_id:
            hpo_dict = {"hpo_id": hpo_id, "bucket_name": hpo_bucket}
            hpo_list.append(hpo_dict)
    return hpo_list


def get_hpo_site_state_info():
    hpo_list = []
    project_id = app_identity.get_application_id()
    hpo_table_query = bq_consts.GET_HPO_CONTENTS_QUERY.format(
        project_id=project_id,
        TABLES_DATASET_ID=common.PIPELINE_TABLES,
        HPO_SITE_TABLE=common.SITE_MASKING_TABLE_ID)
    hpo_response = query(hpo_table_query)
    hpo_table_contents = response2rows(hpo_response)
    for hpo_table_row in hpo_table_contents:
        hpo_id = hpo_table_row[bq_consts.HPO_ID.lower()].lower()
        hpo_state = hpo_table_row[bq_consts.HPO_STATE]
        if hpo_id:
            hpo_dict = {"hpo_id": hpo_id, "state": hpo_state}
            hpo_list.append(hpo_dict)
    return hpo_list


@deprecated(reason='Use resources.has_primary_key(table) instead')
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


@deprecated(reason='create_snapshot_dataset is deprecated')
def create_snapshot_dataset(project_id, dataset_id, snapshot_dataset_id):
    """

    :param dataset_id:
    :param project_id:
    :param snapshot_dataset_id:
    :return:
    """
    dataset_result = create_dataset(project_id=project_id,
                                    dataset_id=snapshot_dataset_id,
                                    description=f'Snapshot of {dataset_id}',
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
