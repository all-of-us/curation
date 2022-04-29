"""
Interact with Google Cloud BigQuery
"""
# Python stl imports
import typing

# Third-party imports
from google.cloud import bigquery
from google.cloud.bigquery import Client
from google.auth import default
from opentelemetry import trace
from opentelemetry.exporter.cloud_trace import CloudTraceSpanExporter
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from google.api_core.exceptions import GoogleAPIError, BadRequest

# Project imports
from utils import auth
from resources import fields_for
from constants.utils import bq as consts
from common import JINJA_ENV


class BigQueryClient(Client):
    """
    A client that extends GCBQ functionality
    See https://googleapis.dev/python/bigquery/latest/generated/google.cloud.bigquery.client.Client.html#google.cloud.bigquery.client.Client
    """

    def __init__(self, project_id: str, scopes=None, credentials=None):
        """
        :param project_id: Identifies the project to create a cloud BigQuery client for
        :param scopes: List of Google scopes as strings
        :param credentials: Google credentials object (ignored if scopes is defined,
            uses delegated credentials instead)

        :return:  A BigQueryClient instance
        """
        tracer_provider = TracerProvider()
        cloud_trace_exporter = CloudTraceSpanExporter(project_id=project_id)
        tracer_provider.add_span_processor(
            BatchSpanProcessor(cloud_trace_exporter))
        trace.set_tracer_provider(tracer_provider)
        tracer = trace.get_tracer(__name__)
        with tracer.start_as_current_span(project_id):
            if scopes:
                credentials, project_id = default()
                credentials = auth.delegated_credentials(credentials,
                                                         scopes=scopes)
            super().__init__(project=project_id, credentials=credentials)

    def get_table_schema(self, table_name: str, fields=None) -> list:
        """
        A helper method to create big query SchemaFields for dictionary definitions.

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

    def _to_standard_sql_type(self, field_type: str) -> str:
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
        standard_sql_type = bigquery.StandardSqlDataTypes(
            standard_sql_type_code)
        return standard_sql_type.name

    def _to_sql_field(self,
                      field: bigquery.SchemaField) -> bigquery.SchemaField:
        """
        Convert all types in a schema field object to standard SQL types (not legacy)

        :param field: the schema field object
        :return: a converted schema field object
        """
        return bigquery.SchemaField(
            field.name, self._to_standard_sql_type(field.field_type),
            field.mode, field.description, field.fields)

    def get_create_or_replace_table_ddl(
            self,
            dataset_id: str,
            table_id: str,
            schema: typing.List[bigquery.SchemaField] = None,
            cluster_by_cols: typing.List[str] = None,
            as_query: str = None,
            **table_options) -> str:
        """
        Generate CREATE OR REPLACE TABLE DDL statement

        Note: Reference https://bit.ly/3fgkCPg for supported syntax

        :param dataset_id: identifies the dataset containing the table
        :param table_id: identifies the table to be created or replaced
        :param schema: list of schema fields (optional). if not provided, attempts to
                    use a schema associated with the table_id.
        :param cluster_by_cols: columns defining the table clustering (optional)
        :param as_query: query used to populate the table (optional)
        :param table_options: options e.g. description and labels (optional)
        :return: DDL statement as string
        """
        CREATE_OR_REPLACE_TABLE_TPL = JINJA_ENV.from_string(
            consts.CREATE_OR_REPLACE_TABLE_QUERY)
        _schema = self.get_table_schema(table_id) if schema is None else schema
        _schema = [self._to_sql_field(field) for field in _schema]
        return CREATE_OR_REPLACE_TABLE_TPL.render(
            project_id=self.project,
            dataset_id=dataset_id,
            table_id=table_id,
            schema=_schema,
            cluster_by_cols=cluster_by_cols,
            query=as_query,
            opts=table_options)

    def dataset_columns_query(self, dataset_id: str) -> str:
        """
        Get INFORMATION_SCHEMA.COLUMNS query for a specified dataset

        :param dataset_id: identifies the dataset whose metadata is queried
        :return the query as a string
        """
        DATASET_COLUMNS_TPL = JINJA_ENV.from_string(
            consts.DATASET_COLUMNS_QUERY)
        return DATASET_COLUMNS_TPL.render(project_id=self.project,
                                          dataset_id=dataset_id)

    def define_dataset(self, dataset_id: str, description: str,
                       label_or_tag: dict) -> bigquery.Dataset:
        """
        Define the dataset reference.

        :param dataset_id:  string name of the dataset id to return a reference of
        :param description:  description for the dataset
        :param label_or_tag:  labels for the dataset = Dict[str, str]
                            tags for the dataset = Dict[str, '']

        :return: a dataset reference object.
        :raises: google.api_core.exceptions.Conflict if the dataset already exists
        """
        if not description or description.isspace():
            raise RuntimeError("Provide a description to create a dataset.")

        if not dataset_id:
            raise RuntimeError("Provide a dataset_id")

        if not label_or_tag:
            raise RuntimeError("Please provide a label or tag")

        dataset_id = f"{self.project}.{dataset_id}"

        # Construct a full Dataset object to send to the API.
        dataset = bigquery.Dataset(dataset_id)
        dataset.description = description
        dataset.labels = label_or_tag
        dataset.location = "US"

        return dataset

    def copy_dataset(self, input_dataset: str, output_dataset: str):
        """
        Copies tables from source dataset to a destination datasets

        :param input_dataset: name of the input dataset
        :param output_dataset: name of the output dataset
        :return:
        """
        # Copy input dataset tables to backup and staging datasets
        tables = super(BigQueryClient, self).list_tables(input_dataset)
        for table in tables:
            staging_table = f'{self.project}.{output_dataset}.{table.table_id}'
            self.copy_table(table, staging_table)

    def list_tables(
        self, dataset: typing.Union[bigquery.DatasetReference, str]
    ) -> typing.Iterator[bigquery.table.TableListItem]:
        """
        List all tables in a dataset

        NOTE: Ensures all results are retrieved by first getting total
        table count and setting max_results in list tables API call.
        Without setting max_results, the API has a bug causing it to
        randomly return 0 tables.

        :param dataset: the dataset containing the tables
        :return: tables contained within the requested dataset
        """
        _MAX_RESULTS_PADDING = 100
        dataset = self.get_dataset(dataset)
        table_count = self.get_table_count(dataset)
        return super(BigQueryClient, self).list_tables(dataset=dataset,
                                                       max_results=table_count +
                                                       _MAX_RESULTS_PADDING)

    def get_table_count(self, dataset: bigquery.DatasetReference) -> int:
        """
        Get the number of tables currently in a specified dataset

        :param dataset: the dataset
        :return: number of tables
        :raises:
            google.cloud.exceptions.GoogleCloudError:
                If the job failed.
            concurrent.futures.TimeoutError:
                If the job did not complete in the given timeout.
        """

        TABLE_COUNT_TPL = JINJA_ENV.from_string(
            "SELECT COUNT(1) table_count FROM `{{dataset.project}}.{{dataset.dataset_id}}.__TABLES__`"
        )
        """Query template to retrieve the number of tables in a dataset.
        Requires parameter `dataset`: :class:`DatasetReference` and
        yields a scalar result with column `table_count`: :class:`int`."""

        q = TABLE_COUNT_TPL.render(dataset=dataset)
        return self.to_scalar(self.query(q))

    def to_scalar(
        self, result: typing.Union[bigquery.table.RowIterator,
                                   bigquery.QueryJob]
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
            raise ValueError(
                f'Scalar result requires a RowIterator or QueryJob '
                f'but `{type(result)}` was supplied.')
        if row_iter.total_rows != 1:
            raise ValueError(f'Cannot get scalar result from '
                             f'row iterator with {row_iter.total_rows} rows.')
        _, row = next(enumerate(row_iter))
        if len(row_iter.schema) == 1:
            return row[0]

        return dict(row.items())

    def upload_csv_data_to_bq_table(self, dataset_id, table_name, fq_file_path,
                                    write_disposition):
        """
        Uploads data from local csv file to bigquery table

        :param dataset_id: identifies the dataset
        :param table_name: identifies the table name where data needs to be uploaded
        :param fq_file_path: Fully qualified path to the csv file which needs to be uploaded
        :param write_disposition: Write disposition for job choose b/w write_empty, write_append, write_truncate
        :return: job result
        """
        dataset_ref = bigquery.DatasetReference(self.project, dataset_id)
        table_ref = bigquery.TableReference(dataset_ref, table_name)
        job_config = bigquery.LoadJobConfig()
        job_config.source_format = bigquery.SourceFormat.CSV
        job_config.skip_leading_rows = 1
        job_config.autodetect = True
        job_config.write_disposition = write_disposition

        with open(fq_file_path, "rb") as source_file:
            job = self.load_table_from_file(source_file,
                                            table_ref,
                                            job_config=job_config)
        try:
            result = job.result()  # Waits for table load to complete.
        except (BadRequest, OSError, AttributeError, TypeError,
                ValueError) as exp:
            raise exp

        return result

    def create_tables(self, fq_table_names, exists_ok=False, fields=None):
        """
        Create an empty table(s) in a project.

        Relies on a list of fully qualified table names.  This is a list of
        strings formatted as 'project-id.dataset-id.table-name`.  This will
        allow the table to be created using the schema defined in a definition
        file without requiring the user to read the file or submit the filepath.

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

        if not fq_table_names or not isinstance(fq_table_names, list):
            raise RuntimeError("Specify a list for fq_table_names to create")

        successes = []
        failures = []
        for index, table_name in enumerate(fq_table_names):
            schema = self.get_table_schema(
                table_name.split('.')[2], fields[index] if fields else None)

            try:
                table = bigquery.Table(table_name, schema=schema)
                table = self.create_table(table, exists_ok)
            except (GoogleAPIError, OSError, AttributeError, TypeError,
                    ValueError):
                failures.append(table_name)
            else:
                successes.append(table)

        if failures:
            raise RuntimeError(f"Unable to create tables: {failures}")

        return successes

    def update_labels_and_tags(self,
                               dataset_id,
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
        updates = dict(new_labels_or_tags.items() -
                       existing_labels_or_tags.items())

        overwrite_keys = updates.keys() & existing_labels_or_tags.keys()

        if overwrite_keys:
            if not overwrite_ok:
                raise RuntimeError(
                    f'Cannot update labels on dataset {dataset_id}'
                    f'without overwriting keys {overwrite_keys}')
            return {**existing_labels_or_tags, **updates}

        return {**existing_labels_or_tags, **updates}