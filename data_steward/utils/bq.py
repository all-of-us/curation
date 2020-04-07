"""
A utility to standardize use of the BigQuery python client library.
"""
# Python Imports
import logging
import os
import time

# Third-party imports
from google.cloud import bigquery
from google.api_core.exceptions import GoogleAPIError, Conflict, NotFound

# Project Imports
from app_identity import PROJECT_ID
from constants.utils import bq as consts
from resources import fields_for

LOGGER = logging.getLogger(__name__)


def get_client(project_id=None):
    """
    Get a client for a specified project.

    :param project_id:  Name of the project to create a bigquery library client for

    :return:  A bigquery Client object.  It is being nice for now, but will begin to
        require users to provide the project_id.
    """
    if project_id is None:
        LOGGER.info(f'You should specify project_id for a reliable experience.'
                    f'Defaulting to {os.environ.get(PROJECT_ID)}.')
        return bigquery.Client()
    return bigquery.Client(project=project_id)


def job_status_done(project_id, job_id):
    """
    Check if the job is complete without errors

    Raises exceptions if they are found while checking the job state

    :param project_id: project containing the job status we want to check
    :param job_id: the job id
    :return: a bool indicating whether the job is done
    :raises:  any exceptions the job encounters
    """
    client = get_client(project_id)
    job = client.get_job(job_id)
    if job.exception():
        raise (job.exception())
    return job.done()


def get_table_schema(table_name):
    """
    A helper function to create big query SchemaFields for dictionary definitions.

    Given the table name, reads the schema from the schema definition file
    and returns a list of SchemaField objects that can be used for table
    creation.

    :param table_name:  the table name to get BigQuery SchemaField information
        for.
    :returns:  a list of SchemaField objects representing the table's schema.
    """
    fields = fields_for(table_name)

    schema = []
    for column in fields:
        name = column.get('name')
        field_type = column.get('type')
        mode = column.get('mode')
        description = column.get('description')
        column_def = bigquery.SchemaField(name, field_type, mode, description)

        schema.append(column_def)

    return schema


def create_tables(project_id=None, fq_table_names=None, exists_ok=False):
    """
    Create an empty table(s) in a project.

    Relies on a list of fully qualified table names.  This is a list of
    strings formatted as 'project-id.dataset-id.table-name`.  This will
    allow the table to be created using the schema defined in a definition
    file without requiring the user to read the file or submit the filepath.

    :param project_id:  The project that will contain the created table.
    :param fq_table_names: A list of fully qualified table names.
    :param exists_ok: A flag to throw an error if the table already exists.
        Defaults to raising an error if the table already exists.

    :raises RuntimeError: a runtime error if table creation fails for any
        table in the list.

    :return: A list of created table objects.
    """

    if not project_id or not isinstance(project_id, str):
        raise RuntimeError("Must specify the project to create the tables in")

    if not fq_table_names:
        raise RuntimeError(
            "Must specify a list of fully qualified table names to create")

    if not isinstance(fq_table_names, list):
        if not isinstance(fq_table_names, str):
            raise RuntimeError("fq_table_names expects a list of strings")

        fq_table_names = list(fq_table_names)

    client = get_client(project_id=project_id)

    successes = []
    failures = []
    for table_name in fq_table_names:
        schema = get_table_schema(table_name.split('.')[2])

        try:
            table = bigquery.Table(table_name, schema=schema)
            table = client.create_table(table, exists_ok)
        except (GoogleAPIError, OSError, AttributeError, TypeError, ValueError):
            LOGGER.exception(f'Unable to create table {table_name}')
            failures.append(table_name)
        else:
            successes.append(table)

    if failures:
        raise RuntimeError(f"Unable to create tables: {failures}")

    return successes


def wait_on_jobs(project_id, job_ids, retry_count=10, max_poll_interval=300):
    """
    Exponential backoff wait for jobs to complete

    :param project_id: project containing the jobs we want to wait on
    :param job_ids: a list of job ids to check for completion
    :param retry_count:  the maximum number of retry attempts
    :param max_poll_interval:  the maximum amount of time to wait on a job to finish
    :return: list of jobs that failed to complete or empty list if all completed
    """
    if not isinstance(job_ids, list):
        _job_ids = [job_ids]

    _job_ids = job_ids
    poll_interval = 1
    for i in range(retry_count):
        LOGGER.info('Waiting %s seconds for completion of job(s): %s' %
                    (poll_interval, _job_ids))
        time.sleep(poll_interval)
        _job_ids = [
            job_id for job_id in _job_ids
            if not job_status_done(project_id, job_id)
        ]

        if not _job_ids:
            return []

        if poll_interval < max_poll_interval:
            poll_interval = 2**i

    LOGGER.info('Job(s) failed to complete: %s' % _job_ids)
    return _job_ids


def query(query, project_id=None, use_cache=False):
    """
    Run the specified query.

    :param query: the query to execute
    :param project_id:  the project identifier to run the query in.
    :param use_cache:  allow using cached query objects.

    :return:  The results of executing the query in the given project.
    """
    client = get_client(project_id)
    query_job_config = bigquery.job.QueryJobConfig(use_query_cache=use_cache)
    return client.query(query, job_config=query_job_config)


def list_datasets(project_id):
    """
    Lists all datasets existing in a project.

    :return: List of dataset objects
    """
    client = get_client(project_id)
    datasets = list(client.list_datasets())
    return datasets


def get_table_info_for_dataset(project_id, dataset_id):
    """
    Get df of INFORMATION_SCHEMA.COLUMNS for a specified dataset

    :param project_id: identifies the project
    :param dataset_id: identifies the dataset
    :return df containing table column information
    :raises BadRequest
    """
    table_info_query = consts.TABLE_INFO_QUERY.format(project=project_id,
                                                      dataset=dataset_id)
    result_df = query(table_info_query, project_id)
    return result_df


def get_dataset(project_id, dataset_id):
    """
    Returns the dataset object associated with the dataset_id

    :param project_id: identifies the project
    :param dataset_id: identifies the dataset
    :return: dataset object
    """
    client = get_client(project_id)
    return client.get_dataset(dataset_id)


def create_dataset(project_id,
                   dataset_id,
                   description,
                   labels={},
                   exists_ok=False):
    """
    Create the dataset reference.

    :param project_id:  string name of the project to search for a dataset
    :param dataset_id:  string name of the dataset id to return a reference of

    :return: a dataset reference object.

    :raises: any GoogleAPIError that is not a 404 error.
        google.api_core.exceptions.Conflict if the dataset already exists
    """
    if description.isspace() or not description:
        raise RuntimeError("you must provide a description.")

    if not project_id:
        raise RuntimeError(
            "You must specify the project_id for the project containing the dataset"
        )

    if not dataset_id:
        raise RuntimeError("You must provide a dataset_id")

    client = get_client(project_id)

    dataset_id = "{}.{}".format(client.project, dataset_id)

    # Construct a full Dataset object to send to the API.
    dataset = bigquery.Dataset(dataset_id)

    # TODO(developer): Specify the geographic location where the dataset should reside.
    dataset.location = "US"

    try:
        dataset = client.create_dataset(dataset,
                                        exists_ok)  # Make an API request.
    except Conflict as err:
        LOGGER.exception("Dataset %s already exists.  Returning that dataset",
                         dataset_id)
        return client.get_dataset(dataset_id)

    return dataset


def get_or_create_dataset(project_id, dataset_id):
    """
    Get the dataset reference if it exists.  Else create it.

    :param project_id:  string name of the project to search for a dataset
    :param dataset_id:  string name of the dataset id to return a reference of

    :return: a dataset reference object.

    :raises: any GoogleAPIError that is not a 404 error
    """
    client = get_client(project_id)

    try:
        return get_dataset(project_id, dataset_id)
    except NotFound as err:
        if err.code != 404:
            raise err

        desc = 'dataset automatically generated because it doesnt exist yet'
        return create_dataset(project_id, dataset_id, desc)


def delete_dataset(project_id,
                   dataset_id,
                   delete_contents=True,
                   not_found_ok=True):
    """
    Delete a dataset in a project. Delete all contents and ignore not found error by default

    :param project_id: Identifies the project the containing the dataset
    :param dataset_id: Identifies the dataset to delete
    :param delete_contents: If set True, deletes all contents within the dataset
            Defaults to True
    :param not_found_ok: If set True, does not raise error if dataset cannot be found
            Defaults to True
    :return:
    """
    client = get_client(project_id)
    client.delete_dataset(dataset_id,
                          delete_contents=delete_contents,
                          not_found_ok=not_found_ok)
    LOGGER.info(f'Deleted dataset {project_id}.{dataset_id}')


def is_validation_dataset_id(dataset_id):
    """
    Checks if dataset_id is a validation dataset

    :param dataset_id: identifies the dataset
    :return: a bool indicating whether dataset is a validation_dataset
    """
    return consts.VALIDATION_PREFIX in dataset_id


def get_latest_validation_dataset_id(project_id):
    """
    Get the latest validation_dataset_id based on most recent creation time

    :param project_id: identifies the project
    :return: the most recent validation_dataset_id
    """

    dataset_id = os.environ.get(consts.MATCH_DATASET, consts.BLANK)
    if dataset_id == consts.BLANK:
        validation_datasets = []
        for dataset in list_datasets(project_id):
            dataset_id = dataset.dataset_id
            if is_validation_dataset_id(dataset_id):
                dataset = get_dataset(project_id, dataset_id)
                validation_datasets.append((dataset.created, dataset_id))

        if validation_datasets:
            return sorted(validation_datasets, key=lambda x: x[0],
                          reverse=True)[0][1]
    return None
