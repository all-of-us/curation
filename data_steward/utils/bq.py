"""
A utility to standardize use of the BigQuery python client library.
"""
# Python Imports
from datetime import datetime
import logging
import os
import typing
import warnings

# Third-party imports
from google.api_core.exceptions import GoogleAPIError, BadRequest
from google.cloud import bigquery
from google.auth import default
from deprecated import deprecated

# Project Imports
from utils import auth
from constants.utils import bq as consts
from resources import fields_for
from common import JINJA_ENV

_MAX_RESULTS_PADDING = 100
"""Constant added to table count in order to list all table results"""
LOGGER = logging.getLogger(__name__)

CREATE_OR_REPLACE_TABLE_TPL = JINJA_ENV.from_string("""
CREATE OR REPLACE TABLE `{{project_id}}.{{dataset_id}}.{{table_id}}` (
{% for field in schema -%}
  {{ field.name }} {{ field.field_type }} {% if field.mode.lower() == 'required' -%} NOT NULL {%- endif %}
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

TABLE_COUNT_TPL = JINJA_ENV.from_string(
    "SELECT COUNT(1) table_count FROM `{{dataset.project}}.{{dataset.dataset_id}}.__TABLES__`"
)
"""Query template to retrieve the number of tables in a dataset.
Requires parameter `dataset`: :class:`DatasetReference` and
yields a scalar result with column `table_count`: :class:`int`."""

FIELDS_TMPL = JINJA_ENV.from_string("""
    {{name}} {{col_type}} {{mode}} OPTIONS(description="{{desc}}")
""")


@deprecated(reason='replaced by gcloud.bq.BigQueryClient()')
def get_client(project_id, scopes=None, credentials=None):
    """
    Get a client for a specified project.

    :param project_id:  Name of the project to create a bigquery library client for
    :param scopes: List of Google scopes as strings
    :param credentials: Google credentials object (ignored if scopes is defined,
        uses delegated credentials instead)

    :return:  A bigquery Client object.
    """
    if scopes:
        credentials, project_id = default()
        credentials = auth.delegated_credentials(credentials, scopes=scopes)
    return bigquery.Client(project=project_id, credentials=credentials)


@deprecated(
    reason=
    'Use gcloud.bq.BigQueryClient.get_table_schema(self, table_name: str, fields=None) instead'
)
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
        column_def = bigquery.SchemaField(name,
                                          field_type).from_api_repr(column)

        schema.append(column_def)

    return schema


@deprecated(
    reason=
    'Use gcloud.bq.BigQueryClient.upload_csv_data_to_bq_table(self, dataset_id, table_name, fq_file_path, write_disposition) instead'
)
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


@deprecated(
    reason=
    'Use gcloud.bq.BigQueryClient._to_standard_sql_type(self, field_type: str) instead'
)
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


@deprecated(
    reason=
    'Use gcloud.bq.BigQueryClient._to_sql_field(self,field: bigquery.SchemaField) instead'
)
def _to_sql_field(field: bigquery.SchemaField) -> bigquery.SchemaField:
    """
    Convert all types in a schema field object to standard SQL types (not legacy)

    :param field: the schema field object
    :return: a converted schema field object
    """
    return bigquery.SchemaField(field.name,
                                _to_standard_sql_type(field.field_type),
                                field.mode, field.description, field.fields)


@deprecated(reason="""
    Use gcloud.bq.BigQueryClient.get_create_or_replace_table_ddl(self,
                                                                  dataset_id: str,
                                                                  table_id: str,
                                                                  schema: typing.List[
                                                                  bigquery.SchemaField] = None,
                                                                  cluster_by_cols: typing.List[str] = None,
                                                                  as_query: str = None,
                                                                  **table_options)
                                                                instead
    """)
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


@deprecated(
    reason=
    'Use gcloud.bq.BigQueryClient.create_tables(self, fq_table_names, exists_ok=False, fields=None) instead'
)
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


@deprecated(
    reason=
    'Use gcloud.bq.BigQueryClient.dataset_columns_query(self, dataset_id: str) instead'
)
def dataset_columns_query(project_id: str, dataset_id: str) -> str:
    """
    Get INFORMATION_SCHEMA.COLUMNS query for a specified dataset

    :param project_id: identifies the project containing the dataset
    :param dataset_id: identifies the dataset whose metadata is queried
    :return the query as a string
    """
    return DATASET_COLUMNS_TPL.render(project_id=project_id,
                                      dataset_id=dataset_id)


@deprecated(
    reason=
    'Use gcloud.bq.BigQueryClient.define_dataset(self, dataset_id: str, description: str, label_or_tag: dict) instead'
)
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


@deprecated(
    reason=
    'Use gcloud.bq.BigQueryClient.update_labels_and_tags(self, dataset_id, existing_labels_or_tags, new_labels_or_tags, overwrite_ok=False) instead'
)
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

    return {**existing_labels_or_tags, **updates}


@deprecated(reason='is_validation_dataset_id is deprecated')
def is_validation_dataset_id(dataset_id):
    """
    Check if  bq_consts.VALIDATION_PREFIX is in the dataset_id

    :param dataset_id: ID (name) of the dataset to validate

    :return: a bool indicating whether dataset is a validation_dataset
    """
    return consts.VALIDATION_PREFIX in dataset_id


@deprecated(reason='get_latest_validation_dataset_id is deprecated')
def get_latest_validation_dataset_id(project_id):
    """
    Get the latest validation_dataset_id based on most recent creationTime.

    :param project_id: ID (name) of the project containing the dataset

    :return: the most recent validation_dataset_id
    """

    client = get_client(project_id)
    dataset_id = os.environ.get(consts.MATCH_DATASET, consts.BLANK)
    if dataset_id == consts.BLANK:
        validation_datasets = []
        for dataset in client.list_datasets(project_id):
            dataset_id = dataset.dataset_id
            if is_validation_dataset_id(dataset_id):
                dataset = client.get_dataset(dataset_id)
                validation_datasets.append((dataset.created, dataset_id))

        if validation_datasets:
            return sorted(validation_datasets, key=lambda x: x[0],
                          reverse=True)[0][1]
    return None


@deprecated(
    reason=
    'see: https://cloud.google.com/python/docs/reference/bigquery/latest/google.cloud.bigquery.client.Client#google_cloud_bigquery_client_Client_create_dataset'
)
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
    all_datasets = [d.dataset_id for d in client.list_datasets(project_id)]
    if dataset_id in all_datasets:
        if overwrite_existing:
            client.delete_dataset(dataset_id,
                                  delete_contents=True,
                                  not_found_ok=True)
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


@deprecated(
    reason=
    'Use gcloud.bq.BigQueryClient.to_scalar(self, result: typing.Union[bigquery.table.RowIterator,bigquery.QueryJob]) instead'
)
def to_scalar(
    result: typing.Union[bigquery.table.RowIterator, bigquery.QueryJob]
) -> typing.Any:
    """
    Get a scalar query result

    :param result: a query job or a resultant :class:`bigquery.table.RowIterator`
    :return: the singular result value
    """
    row_iter = None
    if isinstance(result, bigquery.table.RowIterator):
        row_iter = result
    elif isinstance(result, bigquery.QueryJob):
        row_iter = result.result()
    else:
        raise ValueError(f'Scalar result requires a RowIterator or QueryJob '
                         f'but `{type(result)}` was supplied.')
    if row_iter.total_rows != 1:
        raise ValueError(f'Cannot get scalar result from '
                         f'row iterator with {row_iter.total_rows} rows.')
    _, row = next(enumerate(row_iter))
    if len(row_iter.schema) == 1:
        return row[0]

    return dict(row.items())


@deprecated(
    reason=
    'Use gcloud.bq.BigQueryClient.get_table_count(self, dataset: bigquery.DatasetReference) instead'
)
def get_table_count(client: bigquery.Client,
                    dataset: bigquery.DatasetReference) -> int:
    """
    Get the number of tables currently in a specified dataset

    :param client: active bigquery client
    :param dataset: the dataset
    :return: number of tables
    :raises:
        google.cloud.exceptions.GoogleCloudError:
            If the job failed.
        concurrent.futures.TimeoutError:
            If the job did not complete in the given timeout.
    """
    q = TABLE_COUNT_TPL.render(dataset=dataset)
    return to_scalar(client.query(q))


@deprecated(
    reason=
    'Use gcloud.bq.BigQueryClient.list_tables(self, dataset: bigquery.DatasetReference) instead'
)
def list_tables(
    client: bigquery.Client, dataset: bigquery.DatasetReference
) -> typing.Iterator[bigquery.table.TableListItem]:
    """
    List all tables in a dataset

    NOTE: Ensures all results are retrieved by first getting total
    table count and setting max_results in list tables API call

    :param client: active bigquery client object
    :param dataset: the dataset containing the tables
    :return: tables contained within the requested dataset
    """
    table_count = get_table_count(client, dataset)
    return client.list_tables(dataset=dataset,
                              max_results=table_count + _MAX_RESULTS_PADDING)


@deprecated(
    reason=
    'Use gcloud.bq.BigQueryClient.copy_dataset(self, input_dataset, output_dataset) instead'
)
def copy_datasets(client: bigquery.Client, input_dataset, output_dataset):
    """
    Copies tables from source dataset to a destination datasets

    :param client: an instantiated bigquery client object
    :param input_dataset: name of the input dataset
    :param output_dataset: name of the output dataset
    :return:
    """
    # Copy input dataset tables to backup and staging datasets
    tables = client.list_tables(input_dataset)
    for table in tables:
        staging_table = f'{output_dataset}.{table.table_id}'
        client.copy_table(table, staging_table)


@deprecated(reason='Use resources.validate_date_string(date_string) instead')
def validate_bq_date_string(date_string):
    """
    Validates the date string is a valid date in the YYYY-MM-DD format.

    If the string is valid, the string is returned.  Otherwise, strptime
    raises either a ValueError or TypeError.

    :param date_string: The string to validate adheres to YYYY-MM-DD format

    :return:  a bq conforming date string
    :raises:  A ValueError if the date string is not a valid date or
        doesn't conform to the specified format.
    """
    datetime.strptime(date_string, '%Y-%m-%d')
    return date_string


@deprecated(
    reason=
    'Use gcloud.bq.BigQueryClient.build_and_copy_contents(self, src_dataset, dest_dataset) instead'
)
def build_and_copy_contents(client, src_dataset, dest_dataset):
    """
    Uses google client object to copy non-schemaed data to schemaed table.

    :param client: google client object with permissions
    :param src_dataset: The dataset to copy data from
    :param des_dataset: The dataset to copy data to.  It's tables are
        created with valid schemas before inserting data.
    """
    LOGGER.info(f'Beginning copy of data from unschemaed dataset, '
                f'`{src_dataset}`, to schemaed dataset, `{dest_dataset}`.')
    table_list = client.list_tables(src_dataset)

    for table_item in table_list:
        # create empty schemaed tablle with client object
        try:
            schema_list = get_table_schema(table_item.table_id)
        except RuntimeError as re:
            schema_list = None
            LOGGER.warning(f"No schema available for {table_item.table_id}."
                           f"Creating table without specifying schema.")
        dest_table = f'{client.project}.{dest_dataset}.{table_item.table_id}'
        dest_table = bigquery.Table(dest_table, schema=schema_list)
        dest_table = client.create_table(dest_table)  # Make an API request.
        LOGGER.info(
            f'Created empty table `{dest_table.project}.{dest_table.dataset_id}.{dest_table.table_id}`'
        )

        if schema_list:
            fields_name_str = ',\n'.join([item.name for item in schema_list])

            # copy contents from non-schemaed source to schemaed dest
            sql = (
                f'SELECT {fields_name_str} '
                f'FROM `{table_item.project}.{table_item.dataset_id}.{table_item.table_id}`'
            )
        else:
            sql = (
                f'SELECT * '
                f'FROM `{table_item.project}.{table_item.dataset_id}.{table_item.table_id}`'
            )
        job_config = bigquery.job.QueryJobConfig(
            write_disposition=bigquery.job.WriteDisposition.WRITE_EMPTY,
            priority=bigquery.job.QueryPriority.BATCH,
            destination=dest_table,
            labels={
                'table_name': table_item.table_id.lower(),
                'copy_from': table_item.dataset_id.lower(),
                'copy_to': dest_dataset.lower()
            })
        job_id = (f'schemaed_copy_{table_item.table_id.lower()}_'
                  f'{datetime.now().strftime("%Y%m%d_%H%M%S")}')
        job = client.query(sql, job_config=job_config, job_id=job_id)
        job.result()  # Wait for the job to complete.

        LOGGER.info(
            f'Table contents `{table_item.project}.{table_item.dataset_id}.'
            f'{table_item.table_id}` were copied to `{dest_table.project}.'
            f'{dest_table.dataset_id}.{dest_table.table_id}`')

    LOGGER.info(f'Completed copy of data from unschemaed dataset, '
                f'`{src_dataset}`, to schemaed dataset, `{dest_dataset}`.')


@deprecated(reason='Use resources.get_bq_col_type(col_type) instead')
def get_bq_col_type(col_type):
    """
    Return correct SQL column type representation.

    :param col_type: The type of column as defined in json schema files.

    :return: A SQL column type compatible with BigQuery
    """
    lower_col_type = col_type.lower()
    if lower_col_type == 'integer':
        return 'INT64'

    if lower_col_type == 'string':
        return 'STRING'

    if lower_col_type == 'float':
        return 'FLOAT64'

    if lower_col_type == 'numeric':
        return 'DECIMAL'

    if lower_col_type == 'time':
        return 'TIME'

    if lower_col_type == 'timestamp':
        return 'TIMESTAMP'

    if lower_col_type == 'date':
        return 'DATE'

    if lower_col_type == 'datetime':
        return 'DATETIME'

    if lower_col_type == 'bool':
        return 'BOOL'

    return 'UNSET'


@deprecated(reason='Use resources.get_bq_mode(mode) instead')
def get_bq_mode(mode):
    """
    Return correct SQL for column mode.

    :param mode:  either nullable or required as defined in json schema files.

    :return: NOT NULL or empty string
    """
    lower_mode = mode.lower()
    if lower_mode == 'nullable':
        return ''

    if lower_mode == 'required':
        return 'NOT NULL'

    return 'UNSET'


@deprecated(reason='Use resources.get_bq_fields_sql(fields) instead')
def get_bq_fields_sql(fields):
    """
    Get the SQL compliant fields definition from json fields object.

    :param fields: table schema in json format

    :return: a string that can be added to SQL to generate a correct
        table.
    """
    fields_list = []
    for field in fields:
        rendered = FIELDS_TMPL.render(name=field.get('name'),
                                      col_type=get_bq_col_type(
                                          field.get('type')),
                                      mode=get_bq_mode(field.get('mode')),
                                      desc=field.get('description'))

        fields_list.append(rendered)

    fields_str = ','.join(fields_list)
    return fields_str
