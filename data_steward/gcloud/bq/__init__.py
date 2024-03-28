"""
Interact with Google Cloud BigQuery
"""
# Python stl imports
import os
from datetime import datetime
import typing
import logging
from time import sleep

# Third-party imports
from google.api_core import retry
from google.cloud import bigquery
from google.cloud.bigquery import Client
from google.cloud.bigquery.job import CopyJobConfig, WriteDisposition, QueryJobConfig

from google.auth import default
from google.api_core.exceptions import GoogleAPIError, BadRequest, Conflict
from google.cloud.exceptions import NotFound

# Project imports
from utils import auth
from resources import fields_for, get_and_validate_schema_fields, replace_special_characters_for_labels, \
    is_rdr_dataset, is_mapping_table
from constants.utils import bq as consts
from common import JINJA_ENV, IDENTITY_MATCH, PARTICIPANT_MATCH, PIPELINE_TABLES, SITE_MASKING_TABLE_ID, NPH_TABLES, \
    NPH_VOCABULARY_TABLES
from resources import get_bq_col_type


BIGQUERY_DATA_TYPES = {
    'integer': 'INT64',
    'float': 'FLOAT64',
    'string': 'STRING',
    'date': 'DATE',
    'timestamp': 'TIMESTAMP',
    'bool': 'BOOLEAN',
    'datetime': 'DATETIME',
    'record': 'RECORD',
    'numeric': 'NUMERIC'
}


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
            if table_name.endswith(IDENTITY_MATCH):
                fields = fields_for(IDENTITY_MATCH)
            elif table_name.endswith(PARTICIPANT_MATCH):
                fields = fields_for(PARTICIPANT_MATCH)
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

    def _to_sql_field(self,
                      field: bigquery.SchemaField) -> bigquery.SchemaField:
        """
        Convert all types in a schema field object to standard SQL types (not legacy)

        :param field: the schema field object
        :return: a converted schema field object
        """
        return bigquery.SchemaField(name=field.name,
                                    field_type=get_bq_col_type(
                                        field.field_type),
                                    mode=field.mode,
                                    description=field.description,
                                    fields=field.fields)

    def get_validated_schema_fields(
            schema_filepath: str) -> typing.List[bigquery.SchemaField]:
        """
        Read and validate the table schema file

        Will require users to provide field descriptions for new tables.

        :param schema_filepath: path to the json schema file to load
        :return [bigquery.SchemaField]: A list of BigQuery SchemaField objects
        :raises ValueError: Raised when a field is missing a required type or the description
            of a field is blank
        """
        return [
            bigquery.SchemaField.from_api_repr(field)
            for field in get_and_validate_schema_fields(schema_filepath)
        ]

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
        dataset.location = "us-central1"

        return dataset

    def copy_dataset(self,
                     input_dataset: str,
                     output_dataset: str,
                     job_config: CopyJobConfig = None) -> list:
        """
        Copies tables from source dataset to a destination datasets

        :param input_dataset: fully qualified name of the input(source) dataset
        :param output_dataset: fully qualified name of the output(destination) dataset
        :param job_config: An optional google.cloud.bigquery.job.CopyJobConfig
        :return: incomplete jobs
        """
        job_config = job_config if job_config else CopyJobConfig(
            write_disposition=WriteDisposition.WRITE_EMPTY)
        # Copy input dataset tables to backup and staging datasets
        tables = super(BigQueryClient, self).list_tables(input_dataset)
        job_list = []
        for table in tables:
            staging_table = f'{output_dataset}.{table.table_id}'
            job_config.labels.update({
                'table_name':
                    replace_special_characters_for_labels(table.table_id),
                'copy_from':
                    replace_special_characters_for_labels(input_dataset),
                'copy_to':
                    replace_special_characters_for_labels(output_dataset)
            })
            job = self.copy_table(table, staging_table, job_config=job_config)
            job_list.append(job.job_id)
        self.wait_on_jobs(job_list)
        return job_list

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

    def upload_csv_data_to_bq_table(self, dataset_id: str, table_name: str,
                                    fq_file_path: str, write_disposition: str):
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

    def create_tables(self, fq_table_names: list, exists_ok=False, fields=None):
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
                               dataset_id: str,
                               existing_labels_or_tags: dict,
                               new_labels_or_tags: dict,
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

        if not existing_labels_or_tags:
            raise RuntimeError("Please provide existing label or tag")

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

    def build_and_copy_contents(self, src_dataset: str, dest_dataset: str):
        """
        Copy non-schemaed data to schemaed table.

        :param src_dataset: The dataset to copy data from
        :param dest_dataset: The dataset to copy data to.  Its tables are
            created with valid schemas before inserting data.
        """
        table_list = self.list_tables(src_dataset)

        tables_to_copy = NPH_VOCABULARY_TABLES + NPH_TABLES

        table_list = [table_item for table_item in table_list if table_item.table_id in tables_to_copy]

        for table_item in table_list:
            # create empty schemaed table with client object
            try:
                if is_rdr_dataset(src_dataset) and is_mapping_table(
                        table_item.table_id):
                    raise RuntimeError
                schema_list = self.get_table_schema(table_item.table_id)
            except RuntimeError as re:
                schema_list = None
            dest_table = f'{self.project}.{dest_dataset}.{table_item.table_id}'
            dest_table = bigquery.Table(dest_table, schema=schema_list)
            dest_table = self.create_table(dest_table)  # Make an API request.

            if schema_list:
                sc_list = []
                for item in schema_list:
                    field_cast = f'CAST({item.name} AS {BIGQUERY_DATA_TYPES[item.field_type.lower()]}) AS {item.name}'
                    sc_list.append(field_cast)

                fields_name_str = ',\n'.join(sc_list)

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
            job = self.query(sql, job_config=job_config, job_id=job_id)
            job.result()  # Wait for the job to complete.

    def table_exists(self, table_id: str, dataset_id: str = None) -> bool:
        """
        Determine whether a bigquery table exists

        :param table_id: id of the table
        :param dataset_id: id of the dataset
        :return: `True` if the table exists, `False` otherwise
        """
        if not table_id or table_id.isspace():
            raise RuntimeError('Please provide a table_id')
        if not dataset_id or dataset_id.isspace():
            dataset_id = os.environ.get('BIGQUERY_DATASET_ID')

        table = f'{self.project}.{dataset_id}.{table_id}'
        try:
            self.get_table(table)
            return True
        except NotFound:
            return False

    def serialize_jobs(self, job_list: list):
        """
        Waits on jobs until completion one by one

        :param job_list: list of job_ids
        """
        for job_id in job_list:
            job_info = self.get_job(job_id)
            if job_info.state.upper() != 'DONE':
                logging.info(f"Waiting on job {job_id} to complete")
                job_info.result()

    def wait_on_jobs(self,
                     job_list: list = None,
                     retry_limit: int = 300,
                     backoff_limit: int = 2**8) -> list:
        """
        Waits on jobs until all are 'DONE' until backoff_limit is reached

        :param job_list: list of job_ids. If not set, defaults to the last 10 jobs
        :param retry_limit: Max time to wait in retry strategy
        :param backoff_limit: Max time to wait in backoff strategy
        :return jobs: list of incomplete job ids
        """
        incomplete_jobs = job_list
        list_jobs_retry = retry.Retry(deadline=retry_limit)
        backoff = 1
        while incomplete_jobs and backoff <= backoff_limit:
            bq_jobs = self.list_jobs(max_results=len(job_list) * 3,
                                     state_filter='DONE',
                                     retry=list_jobs_retry)
            bq_job_ids = [job.job_id for job in bq_jobs]
            result = [job_id for job_id in job_list if job_id in bq_job_ids]
            incomplete_jobs = list(set(job_list) - set(result))
            if not incomplete_jobs:
                break
            logging.info(f"Waiting on jobs {incomplete_jobs} to complete")
            sleep(backoff)
            backoff *= 2
        return incomplete_jobs

    def restore_from_time(self,
                          datasets: typing.List[str],
                          days_ago: int,
                          job_config: QueryJobConfig = None) -> typing.List:
        """
        restore a list of datasets from a specific point in time.
        tables within each dataset are restored.

        :param datasets: the dataset list
        :param days_ago: The table's contents so many days ago
        :param job_config: An optional config
        :return: the time of creation
        """
        #* Create, evaluate, and template time information
        if days_ago > 6:
            raise ValueError(
                f'`days_ago` is too far in the past: {days_ago} > 6')

        time_travel_q = JINJA_ENV.from_string("""
            CREATE TABLE `{{project_id}}.{{dataset_id}}_restore.{{table_id}}_restore`
            CLONE `{{project_id}}.{{dataset_id}}.{{table_id}}`
                FOR SYSTEM_TIME AS OF
                TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {{days_ago}} DAYS)
        """)

        #* Labels and Job information
        job_config = job_config if job_config else QueryJobConfig()
        job_config.labels.update({'temp': ''})

        for dset in datasets:
            # for each dataset, get a list of tables and create the recovery dataset
            # org -> original, rest -> restore
            orginal_dataset = self.get_dataset(dset)
            orginal_tables = list(self.list_tables(orginal_dataset))

            try:
                restored_dataset = self.create_dataset(
                    f'{orginal_dataset.dataset_id}_restore')
            except Conflict:
                raise RuntimeError((
                    f'The dataset {orginal_dataset.dataset_id}_restore already exists.  '
                    f'Delete the dataset first to prevent an accidental overwrite.'
                ))
            # add the label to include 'temp'
            restored_dataset.labels.update({'temp': ''})
            self.update_dataset(restored_dataset, ['labels'])

            # Generate a list of queries for each table from a template
            queries = []
            job_list = []
            for table in orginal_tables:
                queries.append(
                    time_travel_q.render(project_id=self.project,
                                         dataset_id=restored_dataset.dataset_id,
                                         table_id=table.table_id,
                                         days_ago=days_ago))

            # Execute each query
            for query in queries:
                job = self.query(query, job_config=job_config)
                job_list.append(job.job_id)
            self.wait_on_jobs(job_list)
        return job_list

    def get_hpo_bucket_info(self):
        hpo_list = []
        hpo_table_query = consts.GET_HPO_CONTENTS_QUERY.format(
            project_id=self.project,
            TABLES_DATASET_ID=consts.LOOKUP_TABLES_DATASET_ID,
            HPO_SITE_TABLE=consts.HPO_ID_BUCKET_NAME_TABLE_ID)
        hpo_response = self.query(hpo_table_query)
        for hpo_table_row in hpo_response:
            hpo_id = hpo_table_row[consts.HPO_ID.lower()].lower()
            hpo_bucket = hpo_table_row[consts.BUCKET_NAME].lower()
            if hpo_id:
                hpo_dict = {"hpo_id": hpo_id, "bucket_name": hpo_bucket}
                hpo_list.append(hpo_dict)
        return hpo_list

    def get_hpo_site_state_info(self):
        hpo_list = []
        hpo_table_query = consts.GET_HPO_CONTENTS_QUERY.format(
            project_id=self.project,
            TABLES_DATASET_ID=PIPELINE_TABLES,
            HPO_SITE_TABLE=SITE_MASKING_TABLE_ID)
        hpo_response = self.query(hpo_table_query)
        for hpo_table_row in hpo_response:
            hpo_id = hpo_table_row[consts.HPO_ID.lower()].lower()
            hpo_state = hpo_table_row[consts.HPO_STATE]
            if hpo_id:
                hpo_dict = {"hpo_id": hpo_id, "state": hpo_state}
                hpo_list.append(hpo_dict)
        return hpo_list
