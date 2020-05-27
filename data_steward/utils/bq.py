"""
A utility to standardize use of the BigQuery python client library.
"""
# Python Imports
import logging
import os

# Third-party imports
from google.cloud import bigquery
from google.api_core.exceptions import GoogleAPIError

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
        LOGGER.info(f"You should specify project_id for a reliable experience."
                    f"Defaulting to {os.environ.get(PROJECT_ID)}.")
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
            LOGGER.exception(f"Unable to create table {table_name}")
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
    dataset = client.get_dataset(dataset_id)
    return dataset


def define_dataset(project_id, dataset_id, description, label_or_tag):
    """
    Define the dataset reference.

    :param project_id:  string name of the project to search for a dataset
    :param dataset_id:  string name of the dataset id to return a reference of
    :param description:  description for the dataset
    :param label_or_tag:  labels for the dataset = Dict[str, str]
                          tags for the dataset = Dict[str, '']

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

    if not label_or_tag:
        raise RuntimeError("Please provide a label or tag")

    dataset_id = f"{project_id}.{dataset_id}"

    # Construct a full Dataset object to send to the API.
    dataset = bigquery.Dataset(dataset_id)
    dataset.description = description
    dataset.labels = label_or_tag
    dataset.location = "US"

    return dataset


def update_labels_and_tags(dataset_id,
                           existing_labels_or_tags,
                           new_labels_or_tags,
                           overwrite_ok=False):
    """
    Updates labels or tags in dataset if not set or needing to be updated
    or overwrites existing labels or tags in the dataset

    :param dataset_id:  string name to identify the dataset
    :param existing_labels_or_tags:  labels already existing on the dataset = Dict[str, str]
                                     tags already existing on the dataset = Dict[str, '']
    :param new_labels_or_tags:  new labels to add to the dataset = Dict[str, str]
                                new tags to add to the dataset = Dict[str, '']
    :param overwrite_ok:  flag to signal if labels or tags are to be either
                             overwritten (False as default) or updated (True)

    :raises:  RuntimeError if parameters are not specified
    :raises:  RuntimeError if overwrite_ok is false and new value for label is provided

    :return:  a dictionary of new labels or tags
    """
    if not dataset_id:
        raise RuntimeError("Provide a dataset_id")

    if not new_labels_or_tags:
        raise RuntimeError("Please provide a label or tag")

    # excludes duplicate keys
    updates = dict(new_labels_or_tags.items() - existing_labels_or_tags.items())

    overwrite_keys = updates.keys() & existing_labels_or_tags.keys()

    if overwrite_keys:
        if not overwrite_ok:
            raise RuntimeError(f'Cannot update labels on dataset {dataset_id}'
                               f'without overwriting keys {overwrite_keys}')
        return {**existing_labels_or_tags, **updates}


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
    LOGGER.info(f"Deleted dataset {project_id}.{dataset_id}")


def is_validation_dataset_id(dataset_id):
    """
    Check if  bq_consts.VALIDATION_PREFIX is in the dataset_id

    :param dataset_id: ID (name) of the dataset to validate

    :return: a bool indicating whether dataset is a validation_dataset
    """
    return consts.VALIDATION_PREFIX in dataset_id


def get_latest_validation_dataset_id(project_id):
    """
    Get the latest validation_dataset_id based on most recent creationTime.

    :param project_id: ID (name) of the project containing the dataset

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


def create_dataset(project_id,
                   dataset_id,
                   description=None,
                   friendly_name=None,
                   label_or_tag=None,
                   overwrite_existing=False):
    """
    Creates a new dataset

    :param project_id: name of the project to create dataset in, defaults to the currently configured project if missing.
    :param dataset_id: name to give the new dataset - required
    :param description: dataset description - required
    :param friendly_name: user friendly name for dataset - optional
    :param label_or_tag: labels for the dataset = Dict[str, str]
                         tags for the dataset = Dict[str, '']
    :param overwrite_existing: determine if dataset should be overwritten if already exists, defaults to False.
                               Overwrites ony if explicitly told to
    :return: a new dataset returned from the API
    """
    client = get_client(project_id)

    # Check to see if dataset already exists if overwrite_existing is False
    if not overwrite_existing:
        all_datasets = [d.dataset_id for d in list_datasets(project_id)]
        if dataset_id in all_datasets:
            raise RuntimeError("Dataset already exists")

    # Construct a full dataset object to send to the API using define_dataset.
    dataset = define_dataset(project_id, dataset_id, description, label_or_tag)

    # Set friendly_name
    if friendly_name:
        dataset.friendly_name = friendly_name

    failures = []
    try:
        dataset = client.create_dataset(
            dataset, exists_ok=overwrite_existing)
    except (GoogleAPIError, OSError, AttributeError, TypeError, ValueError):
        LOGGER.exception(f"Unable to create dataset {dataset_id}")
        failures.append(dataset_id)
    else:
        LOGGER.info(f"Created dataset {client.project}.{dataset.dataset_id}")

    if failures:
        raise RuntimeError(f"Unable to create tables: {failures}")

    return dataset
