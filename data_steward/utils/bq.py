"""
A utility to standardize use of the BigQuery python client library.
"""
# Python Imports
import logging
import os

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
        It is being nice for now, but will begin to require users to provide
        the project_id.

    :return:  A bigquery Client object.
    """
    if project_id is None:
        LOGGER.info(
            'You should specify project_id for a reliable experience.'
            'Defaulting to %s.', os.environ.get(PROJECT_ID))
        return bigquery.Client()
    return bigquery.Client(project=project_id)


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


def create_tables(client, project_id, fq_table_names, exists_ok=False):
    """
    Create an empty table(s) in a project.

    Relies on a list of fully qualified table names.  This is a list of
    strings formatted as 'project-id.dataset-id.table-name`.  This will
    allow the table to be created using the schema defined in a definition
    file without requiring the user to read the file or submit the filepath.

    :param client: an instantiated bigquery client object
    :param project_id:  The project that will contain the created table.
    :param fq_table_names: A list of fully qualified table names.
    :param exists_ok: A flag to throw an error if the table already exists.
        Defaults to raising an error if the table already exists.

    :raises RuntimeError: a runtime error if table creation fails for any
        table in the list.

    :return: A list of created table objects.
    """
    if not client:
        raise RuntimeError("Specify BigQuery client object")

    if not project_id or not isinstance(project_id, str):
        raise RuntimeError("Specify the project to create the tables in")

    if not fq_table_names or not isinstance(fq_table_names, list):
        raise RuntimeError("Specify a list for fq_table_names to create")

    successes = []
    failures = []
    for table_name in fq_table_names:
        schema = get_table_schema(table_name.split('.')[2])

        try:
            table = bigquery.Table(table_name, schema=schema)
            table = client.create_table(table, exists_ok)
        except (GoogleAPIError, OSError, AttributeError, TypeError, ValueError):
            LOGGER.exception('Unable to create table %s', table_name)
            failures.append(table_name)
        else:
            successes.append(table)

    if failures:
        raise RuntimeError(f"Unable to create tables: {failures}")

    return successes


def query(q, project_id=None, use_cache=False):
    client = get_client(project_id)
    query_job_config = bigquery.job.QueryJobConfig(use_query_cache=use_cache)
    return client.query(q, job_config=query_job_config).to_dataframe()


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


def define_dataset(project_id, dataset_id, description, labels):
    """
    Define the dataset reference.

    :param project_id:  string name of the project to search for a dataset
    :param dataset_id:  string name of the dataset id to return a reference of

    :return: a dataset reference object.

    :raises: google.api_core.exceptions.Conflict if the dataset already exists
    """
    if description.isspace() or not description:
        raise RuntimeError("Provide a description to create a dataset.")

    if not project_id:
        raise RuntimeError(
            "Specify the project_id for the project containing the dataset")

    if not dataset_id:
        raise RuntimeError("Provide a dataset_id")

    dataset_id = f"{project_id}.{dataset_id}"

    # Construct a full Dataset object to send to the API.
    dataset = bigquery.Dataset(dataset_id)
    dataset.description = description
    dataset.labels = labels
    dataset.location = "US"

    return dataset


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
    LOGGER.info('Deleted dataset %s.%s', project_id, dataset_id)


def is_validation_dataset_id(dataset_id):
    """
    Check if  bq_consts.VALIDATION_PREFIX is in the dataset_id

    :param dataset_id:

    :return: a bool indicating whether dataset is a validation_dataset
    """
    return consts.VALIDATION_PREFIX in dataset_id


def get_latest_validation_dataset_id(project_id):
    """
    Get the latest validation_dataset_id based on most recent creationTime.

    :param project_id: 

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


def dataset_exists(dataset_id, project_id):
    """
    Checks if the dataset exists via dataset id

    :param dataset_id:  name of the dataset to check
    :param project_id:  name of the project in which to create the dataset

    :returns:  True if the dataset exists
    :returns:  False if the dataset does not exist
    """
    dataset_exist_check = get_dataset(project_id, dataset_id)

    try:
        if dataset_exist_check is not None:
            return True
    except NotFound:
        if dataset_exist_check:
            return False


def create_dataset(dataset_id,
                   description,
                   label,
                   project_id):
    """
    Creates a new dataset

    :param project_id:  name of the project in which to create the dataset
    :param dataset_id:  name to give the new dataset, is required
    :param description:  dataset description, is required
    :param label:  dataset label, is required

    :raises: RuntimeError if the dataset does not have project_id
    :raises: RuntimeError if the dataset does not have dataset_id
    :raises: RuntimeError if the dataset does not have a description
    :raises: RuntimeError if the dataset does not have a label
    """
    if not project_id:
        raise RuntimeError(
            "Please specify a project in which to create the dataset")

    if not dataset_id:
        raise RuntimeError("Cannot create a dataset without a name")

    if description.isspace() or not description:
        raise RuntimeError("Please provide a description to create a dataset")

    if not label:
        raise RuntimeError("Label and/or tag is required to create a dataset")

    dataset_id = f"{project_id}.{dataset_id}"

    # Construct a full Dataset object to send to the API.
    dataset = bigquery.Dataset(dataset_id)
    dataset.description = description
    dataset.labels = label

    client = get_client(project_id)
    client.create_dataset(dataset, exists_ok=False)

    return client
