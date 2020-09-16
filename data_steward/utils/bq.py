"""
A utility to standardize use of the BigQuery python client library.
"""
# Python Imports
import logging
import os
import typing
import warnings

# Third-party imports
from google.api_core.exceptions import GoogleAPIError, BadRequest
from google.cloud import bigquery
from google.auth import default
import jinja2

# Project Imports
from app_identity import PROJECT_ID
from utils import auth
from constants.utils import bq as consts
from resources import fields_for

LOGGER = logging.getLogger(__name__)
JINJA_ENV = jinja2.Environment(
    # block tags on their own lines
    # will not cause extra white space
    trim_blocks=True,
    lstrip_blocks=True,
    # syntax highlighting should be better
    # with these comment delimiters
    comment_start_string='--',
    comment_end_string=' --',
    # in jinja2 autoescape is for html; jinjasql supports autoescape for sql
    # TODO Look into jinjasql for sql templating
    autoescape=False)

CREATE_OR_REPLACE_TABLE_TPL = JINJA_ENV.from_string("""
CREATE OR REPLACE TABLE `{{project_id}}.{{dataset_id}}.{{table_id}}` (
{% for field in schema -%}
  {{ field.name }} {{ field.field_type }} {% if field.mode == 'required' -%} NOT NULL {%- endif %}
  {% if field.description %} OPTIONS (description="{{ field.description }}") {%- endif %} 
  {% if loop.nextitem %},{% endif -%}
{%- endfor %} )
{% if opts -%} 
OPTIONS (
    {% for opt_name, opt_val in opts.items() -%}
    {{opt_name}}=
        {% if opt_val is string %}
        "{{opt_val}}"
        {% elif opt_val is mapping %}
        [
            {% for opt_val_key, opt_val_val in opt_val.items() %}
                ("{{opt_val_key}}", "{{opt_val_val}}"){% if loop.nextitem is defined %},{% endif %}       
            {% endfor %}
        ]
        {% endif %}
        {% if loop.nextitem is defined %},{% endif %} 
    {%- endfor %} )
{%- endif %}
{% if cluster_by_cols -%}
CLUSTER BY 
{% for col in cluster_by_cols -%}
    {{col}}{% if loop.nextitem is defined %},{% endif %}
{%- endfor %}  
{%- endif -%}
-- Note clustering/partitioning in conjunction with AS query_expression is -- 
-- currently unsupported (see https://bit.ly/2VeMs7e) --
{% if query -%} AS {{ query }} {%- endif %}
""")

DATASET_COLUMNS_TPL = JINJA_ENV.from_string(consts.DATASET_COLUMNS_QUERY)


def get_client(project_id=None, scopes=None):
    """
    Get a client for a specified project.

    :param project_id:  Name of the project to create a bigquery library client for
        It is being nice for now, but will begin to require users to provide
        the project_id.
    :param scopes: List of Google scopes as strings

    :return:  A bigquery Client object.
    """
    if scopes:
        credentials, project_id = default()
        credentials = auth.delegated_credentials(credentials, scopes=scopes)
        return bigquery.Client(project=project_id, credentials=credentials)
    if project_id is None:
        LOGGER.info(f"You should specify project_id for a reliable experience."
                    f"Defaulting to {os.environ.get(PROJECT_ID)}.")
        return bigquery.Client()
    return bigquery.Client(project=project_id)


def get_table_schema(table_name, fields=None):
    """
    A helper function to create big query SchemaFields for dictionary definitions.

    Given the table name, reads the schema from the schema definition file
    and returns a list of SchemaField objects that can be used for table
    creation.

    :param table_name:  the table name to get BigQuery SchemaField information
        for.
    :param fields: An optional argument to provide fields/schema as a list of JSON objects
    :returns:  a list of SchemaField objects representing the table's schema.
    """
    if fields:
        fields = fields
    else:
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


def upload_csv_data_to_bq_table(client, dataset_id, table_name, fq_file_path,
                                write_disposition):
    """
    Uploads data from local csv file to bigquery table

    :param client: an instantiated bigquery client object
    :param dataset_id: identifies the dataset
    :param table_name: identifies the table name where data needs to be uploaded
    :param fq_file_path: Fully qualified path to the csv file which needs to be uploaded
    :param write_disposition: Write disposition for job choose b/w write_empty, write_append, write_truncate
    :return: job result
    """
    dataset_ref = bigquery.DatasetReference(client.project, dataset_id)
    table_ref = bigquery.TableReference(dataset_ref, table_name)
    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.CSV
    job_config.skip_leading_rows = 1
    job_config.autodetect = True
    job_config.write_disposition = write_disposition

    LOGGER.info(f"Uploading {fq_file_path} data to {dataset_id}.{table_name}")
    with open(fq_file_path, "rb") as source_file:
        job = client.load_table_from_file(source_file,
                                          table_ref,
                                          job_config=job_config)
    try:
        result = job.result()  # Waits for table load to complete.
    except (BadRequest, OSError, AttributeError, TypeError, ValueError) as exp:
        message = f"Unable to load data to table {table_name}"
        LOGGER.exception(message)
        raise exp

    return result


def _to_standard_sql_type(field_type: str) -> str:
    """
    Get standard SQL type corresponding to a SchemaField type

    :param field_type: type in SchemaField object (can be legacy or standard SQL type)
    :return: standard SQL type name
    """
    upper_field_type = field_type.upper()
    standard_sql_type_code = bigquery.schema.LEGACY_TO_STANDARD_TYPES.get(
        upper_field_type)
    if not standard_sql_type_code:
        raise ValueError(f'{field_type} is not a valid field type')
    standard_sql_type = bigquery.StandardSqlDataTypes(standard_sql_type_code)
    return standard_sql_type.name


def _to_sql_field(field: bigquery.SchemaField) -> bigquery.SchemaField:
    """
    Convert all types in a schema field object to standard SQL types (not legacy)

    :param field: the schema field object
    :return: a converted schema field object
    """
    return bigquery.SchemaField(field.name,
                                _to_standard_sql_type(field.field_type),
                                field.mode, field.description, field.fields)


def get_create_or_replace_table_ddl(project_id: str,
                                    dataset_id: str,
                                    table_id: str,
                                    schema: typing.List[
                                        bigquery.SchemaField] = None,
                                    cluster_by_cols: typing.List[str] = None,
                                    as_query: str = None,
                                    **table_options) -> str:
    """
    Generate CREATE OR REPLACE TABLE DDL statement

    Note: Reference https://bit.ly/3fgkCPg for supported syntax

    :param project_id: identifies the project containing the table
    :param dataset_id: identifies the dataset containing the table
    :param table_id: identifies the table to be created or replaced
    :param schema: list of schema fields (optional). if not provided, attempts to
                   use a schema associated with the table_id.
    :param cluster_by_cols: columns defining the table clustering (optional)
    :param as_query: query used to populate the table (optional)
    :param table_options: options e.g. description and labels (optional)
    :return: DDL statement as string
    """
    _schema = get_table_schema(table_id) if schema is None else schema
    _schema = [_to_sql_field(field) for field in _schema]
    return CREATE_OR_REPLACE_TABLE_TPL.render(project_id=project_id,
                                              dataset_id=dataset_id,
                                              table_id=table_id,
                                              schema=_schema,
                                              cluster_by_cols=cluster_by_cols,
                                              query=as_query,
                                              opts=table_options)


def create_tables(client,
                  project_id,
                  fq_table_names,
                  exists_ok=False,
                  fields=None):
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
    :param fields: An optional argument to provide a list of a list of JSON objects for the fields/schema
            ex:[
                   [{
                        "type": "integer",
                        "name": "condition_occurrence_id",
                        "mode": "nullable",
                        "description": ""
                    },
                    {
                        "type": "string",
                        "name": "src_dataset_id",
                        "mode": "nullable",
                        "description": ""
                    }]
                ]
            if not provided resources.get_table_schema will be called to get schema.

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
    for index, table_name in enumerate(fq_table_names):
        schema = get_table_schema(
            table_name.split('.')[2], fields[index] if fields else None)

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
    """
    Deprecated: Execute a query and get results as a dataframe 
     
    :param q: the query to execute
    :param project_id: identifies the project associated with the query
    :param use_cache: if set to True, allow cached results
    :return: the results as a dataframe
    """
    warnings.warn(
        "Function utils.bq.query is deprecated and will be removed in a future version. "
        "Use `bigquery.Client` object directly and its `to_dataframe()` method if needed.",
        PendingDeprecationWarning,
        stacklevel=2,
    )
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


def dataset_columns_query(project_id: str, dataset_id: str) -> str:
    """
    Get INFORMATION_SCHEMA.COLUMNS query for a specified dataset
 
    :param project_id: identifies the project containing the dataset
    :param dataset_id: identifies the dataset whose metadata is queried
    :return the query as a string 
    """
    return DATASET_COLUMNS_TPL.render(project_id=project_id,
                                      dataset_id=dataset_id)


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
    if not description or description.isspace():
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
                   label_or_tag=None,
                   friendly_name=None,
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

    # Check to see if dataset already exists
    all_datasets = [d.dataset_id for d in list_datasets(project_id)]
    if dataset_id in all_datasets:
        if overwrite_existing:
            delete_dataset(project_id, dataset_id)
        else:
            raise RuntimeError("Dataset already exists")

    # Construct a full dataset object to send to the API using define_dataset.
    dataset = define_dataset(project_id, dataset_id, description, label_or_tag)

    # Set friendly_name
    if friendly_name:
        dataset.friendly_name = friendly_name

    failures = []
    try:
        dataset = client.create_dataset(dataset, exists_ok=overwrite_existing)
    except (GoogleAPIError, OSError, AttributeError, TypeError, ValueError):
        LOGGER.exception(f"Unable to create dataset {dataset_id}")
        failures.append(dataset_id)
    else:
        LOGGER.info(f"Created dataset {client.project}.{dataset.dataset_id}")

    if failures:
        raise RuntimeError(f"Unable to create dataset: {failures}")

    return dataset


def query_sheet_linked_bq_table(project_id, table_content_query,
                                external_data_scopes):
    """
    Queries Google Sheet sourced BigQuery Table and returns results dataframe

    :param project_id: identifies the project
    :param table_content_query: query to retrieve table contents
    :param external_data_scopes: scopes needed to query the external data sourced table
    :return: result dataframe
    """
    # add Google OAuth2.0 scopes
    client = get_client(project_id, external_data_scopes)
    query_job_config = bigquery.job.QueryJobConfig(use_query_cache=False)
    result_df = client.query(table_content_query,
                             job_config=query_job_config).to_dataframe()

    return result_df
