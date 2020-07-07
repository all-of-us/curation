"""
 Load an Athena vocabulary bundle located in a GCS bucket into a BQ dataset

"""
import argparse
import datetime
import logging
from typing import List, Iterable, Union

import jinja2
from google.cloud import storage
from google.cloud.bigquery import Client, Dataset, SchemaField, LoadJob, LoadJobConfig, QueryJob, \
    QueryJobConfig, Table

from common import VOCABULARY_TABLES
from sandbox import get_sandbox_dataset_id
from utils import bq

LOGGER = logging.getLogger(__name__)
DATE_TIME_TYPES = ['date', 'timestamp', 'datetime']
MAX_BAD_RECORDS = 0
FIELD_DELIMITER = '\t'
JINJA_ENV = jinja2.Environment()
SELECT_TPL = JINJA_ENV.from_string("""
    SELECT 
    {% for field in fields %}
        {% if field.field_type == 'date' %}
            PARSE_DATE('%Y%m%d', {{ field['name'] }})
        {% else %}
            {{ field.name }}
        {% endif %}
        AS {{ field.name }} {% if loop.nextitem is defined %} , {% endif %}
    {% endfor %}
    FROM `{{ project_id }}.{{ dataset_id }}.{{ table }}`
""")


def wait_jobs(jobs: Iterable[Union[QueryJob, LoadJob]]):
    """
    Run multiple jobs to completion

    :param jobs: jobs to run
    :return: the completed jobs

    :raises RuntimeError if a LoadJob completes with errors
    :raises google.cloud.exceptions.GoogleCloudError if a QueryJob fails
    """
    _jobs = []
    for job in jobs:
        _job = job.result()
        _jobs.append(_job)
        if hasattr(_job, 'errors') and _job.errors:
            LOGGER.error(f"Error running job {_job.job_id}: {_job.errors}")
            raise RuntimeError(f"Error running job {_job.job_id}: {_job.errors}")
    return _jobs


def safe_schema_for(table: str) -> List[SchemaField]:
    """
    Get schema fields whose date[time] fields are converted to strings so load will work

    :param table: name of the table
    :return: a list of SchemaField objects
    """
    return [SchemaField(f.name, 'string' if f.field_type in DATE_TIME_TYPES else f.field_type,
                        f.mode,
                        f.description) for f in bq.get_table_schema(table)]


def _filename_to_table_name(filename: str) -> str:
    return filename.replace('.csv', '').lower()


def load_stage(dst_dataset: Dataset,
               bq_client: Client,
               bucket_name: str) -> List[LoadJob]:
    """
    Stage files from a bucket to a dataset

    :param dst_dataset: reference to destination dataset object
    :param bq_client: a BigQuery client object
    :param bucket_name: the location in GCS containing the vocabulary files
    :return: list of completed load jobs
    """
    gcs_client = storage.Client()
    blobs = list(gcs_client.list_blobs(bucket_name))

    table_blobs = [_filename_to_table_name(blob.name) for blob in blobs]
    missing_blobs = [table for table in VOCABULARY_TABLES if table not in table_blobs]
    if missing_blobs:
        raise RuntimeError(f'Bucket {bucket_name} is missing files for tables {missing_blobs}')

    load_jobs = []
    for blob in blobs:
        table_name = _filename_to_table_name(blob.name)
        # ignore any non-vocabulary files
        if table_name not in VOCABULARY_TABLES:
            continue
        destination = dst_dataset.table(table_name)
        safe_schema = safe_schema_for(table_name)
        job_config = LoadJobConfig()
        job_config.schema = safe_schema
        job_config.skip_leading_rows = 1
        job_config.field_delimiter = FIELD_DELIMITER
        job_config.max_bad_records = MAX_BAD_RECORDS
        job_config.quote_character = ''
        source_uri = f'gs://{bucket_name}/{blob.name}'
        load_job = bq_client.load_table_from_uri(source_uri, destination, job_config=job_config)
        load_jobs.append(load_job)
        LOGGER.info(f'table:{destination} job_id:{load_job.job_id}')
    wait_jobs(load_jobs)
    return load_jobs


def load(project_id, bq_client, src_dataset_id, dst_dataset_id, overwrite_ok=False):
    """
    Transform safely loaded tables and store results in target dataset.

    :param project_id:
    :param bq_client:
    :param src_dataset_id:
    :param dst_dataset_id:
    :param overwrite_ok: if True and the dest dataset already exists the dataset is recreated
    :return:
    """
    if overwrite_ok:
        bq_client.delete_dataset(dst_dataset_id, delete_contents=True, not_found_ok=True)
    bq_client.create_dataset(dst_dataset_id)
    src_tables = list(bq_client.list_tables(dataset=src_dataset_id))

    job_config = QueryJobConfig()
    query_jobs = []
    for src_table in src_tables:
        schema = bq.get_table_schema(src_table.table_id)
        destination = f'{project_id}.{dst_dataset_id}.{src_table.table_id}'
        table = bq_client.create_table(Table(destination, schema=schema), exists_ok=True)
        job_config.destination = table
        query = SELECT_TPL.render(project_id=project_id,
                                  dataset_id=src_dataset_id,
                                  table=src_table.table_id,
                                  fields=schema)
        query_job = bq_client.query(query, job_config=job_config)
        LOGGER.info(f'table:{destination} job_id:{query_job.job_id}')
        query_jobs.append(query_job)
    wait_jobs(query_jobs)


def main(project_id: str, bucket_name: str, dst_dataset_id: str):
    """
    Transform and load vocabulary files from Athena to a BigQuery dataset

    :param project_id:
    :param bucket_name:
    :param dst_dataset_id:
    """
    bq_client = bq.get_client(project_id=project_id)
    sandbox_dataset_id = get_sandbox_dataset_id(dst_dataset_id)
    sandbox_dataset = bq.define_dataset(project_id, sandbox_dataset_id,
                                        f'Vocabulary loaded from gs://{bucket_name}',
                                        label_or_tag={'type': 'vocabulary'})
    sandbox_dataset = bq_client.create_dataset(sandbox_dataset, exists_ok=True)
    load_stage(sandbox_dataset, bq_client, bucket_name)
    load(project_id, bq_client, sandbox_dataset_id, dst_dataset_id, overwrite_ok=True)


def get_arg_parser() -> argparse.ArgumentParser:
    """
    Ex:
    Typical usage

        load_vocab -p my_project -b my_bucket -r 20200701

    :return: the parser
    """
    argument_parser = argparse.ArgumentParser(description=__doc__)
    argument_parser.add_argument(
        '-p',
        '--project_id',
        dest='project_id',
        action='store',
        help='Identifies the project containing the target dataset',
        required=True)
    argument_parser.add_argument(
        '-b',
        '--bucket_name',
        dest='bucket_name',
        action='store',
        help='Bucket containing CSV files downloaded from Athena',
        required=True)
    argument_parser.add_argument(
        '-r',
        '--release_date',
        dest='release_date',
        action='store',
        help='Vocabulary release date in format yyyymmdd. Defaults to today',
        required=False)
    argument_parser.add_argument(
        '-t',
        '--target_dataset_id',
        dest='target_dataset_id',
        action='store',
        help='Identifies the target dataset where the vocabulary is to be loaded',
        required=False)
    return argument_parser


def parse_args() -> argparse.Namespace:
    """

    """
    argument_parser = get_arg_parser()
    return argument_parser.parse_args()


def get_release_date(release_date: datetime.date = None) -> str:
    """
    Get the name of a vocabulary release based on date

    :param release_date: date the vocabulary is released
    :return: name of vocabulary release
    """
    if not release_date:
        release_date = datetime.date.today()
    release_date_str = release_date.strftime("%Y%m%D")
    return release_date_str


def get_target_dataset_id(release_tag: str) -> str:
    return f'vocab_{release_tag}'


if __name__ == '__main__':
    import sys

    ARGS = parse_args()

    RELEASE_TAG = ARGS.release_date or get_release_date()
    TARGET_DATASET_ID = ARGS.target_dataset_id or get_target_dataset_id(RELEASE_TAG)

    handlers = [logging.StreamHandler(sys.stdout)]
    logging.basicConfig(
        level=logging.DEBUG,
        format='[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s',
        handlers=handlers
    )

    main(ARGS.project_id, ARGS.bucket_name, TARGET_DATASET_ID)
