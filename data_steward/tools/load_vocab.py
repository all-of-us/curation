"""
 Load an Athena vocabulary bundle located in a GCS bucket into a BQ dataset

"""
import argparse
import datetime
import hashlib
import json
import logging
from pathlib import Path
from typing import List, Dict

from google.cloud import storage
from google.cloud.exceptions import NotFound
from google.cloud.bigquery import Client, Dataset, SchemaField, LoadJob, LoadJobConfig
from google.cloud.bigquery import QueryJobConfig, Table, AccessEntry

from common import VOCABULARY_TABLES, JINJA_ENV, CONCEPT, VOCABULARY, VOCABULARY_UPDATES
from resources import AOU_VOCAB_PATH
from utils import bq, pipeline_logging

LOGGER = logging.getLogger(__name__)
DATE_TIME_TYPES = ['date', 'timestamp', 'datetime']
MAX_BAD_RECORDS = 0
FIELD_DELIMITER = '\t'
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


def hash_dir(vocab_folder_path: Path) -> str:
    """
    Generate an MD5 digest from the contents of a directory

    :param vocab_folder_path: Path
    :returns MD5 digest
    """
    hash_obj = hashlib.sha256()
    for vocab_file in vocab_folder_path.glob('*.csv'):
        with vocab_file.open('rb') as fp:
            hash_obj.update(fp.read())
    return hash_obj.hexdigest()


def update_aou_vocabs(vocab_folder_path: Path):
    """
    Add vocabularies AoU_General and AoU_Custom to the vocabulary at specified path

    :param vocab_folder_path: directory containing vocabulary files updated with cpt4 concepts
    """
    concept_file = _table_name_to_filename(CONCEPT)
    concept_path = vocab_folder_path / concept_file
    aou_custom_path = Path(AOU_VOCAB_PATH) / concept_file
    with aou_custom_path.open('r') as custom_concept, concept_path.open(
            'a') as vocab_concept:
        for line in custom_concept.readlines()[1:]:
            vocab_concept.write(line)
        LOGGER.info(f'Successfully updated file {str(concept_path)}')

    vocabulary_file = _table_name_to_filename(VOCABULARY)
    vocabulary_path = vocab_folder_path / vocabulary_file
    aou_vocab_version = hash_dir(vocab_folder_path)
    with vocabulary_path.open('a') as vocab_vocabulary:
        for _, vocab_list in VOCABULARY_UPDATES.items():
            vocab_list[-2] = aou_vocab_version
            vocab_vocabulary.write(f'{FIELD_DELIMITER.join(vocab_list)}\n')
        LOGGER.info(f'Successfully updated file {str(vocabulary_path)}')
    return


def upload_stage(bucket_name: str, vocab_folder_path: Path,
                 gcs_client: storage.Client):
    """
    Upload vocabulary tables to cloud storage

    :param bucket_name: the location in GCS containing the vocabulary files
    :param vocab_folder_path: points to the directory containing files downloaded from athena with CPT4 applied
    :param gcs_client: google cloud storage client
    """
    bucket = gcs_client.get_bucket(bucket_name)
    LOGGER.info(f'GCS bucket {bucket_name} found successfully')
    for table in VOCABULARY_TABLES:
        file_name = _table_name_to_filename(table)
        file_path = vocab_folder_path / file_name
        blob = bucket.blob(file_name)
        blob.upload_from_filename(str(file_path))
        LOGGER.info(f'Vocabulary file {str(file_path)} uploaded '
                    f'successfully to GCS bucket {bucket_name}')
    return


def check_and_create_staging_dataset(dst_dataset_id, bucket_name, bq_client):
    """

    :param dst_dataset_id: final destination to load the vocabulary in BigQuery
    :param bucket_name: the location in GCS containing the vocabulary files
    :param bq_client: google bigquery client
    :return: staging dataset object
    """
    staging_dataset_id = f'{dst_dataset_id}_staging'
    staging_dataset = Dataset(f'{bq_client.project}.{staging_dataset_id}')
    try:
        bq_client.get_dataset(staging_dataset)
    except NotFound:
        staging_dataset.description = f'Vocabulary loaded from gs://{bucket_name}'
        staging_dataset.labels = {'type': 'vocabulary', 'phase': 'staging'}
        staging_dataset.location = "US"
        staging_dataset = bq_client.create_dataset(staging_dataset)
        LOGGER.info(f'Successfully created dataset {staging_dataset_id}')
    return staging_dataset


def safe_schema_for(table: str) -> List[SchemaField]:
    """
    Get schema fields whose date[time] fields are converted to strings so load will work

    :param table: name of the table
    :return: a list of SchemaField objects
    """
    return [
        SchemaField(
            f.name,
            'string' if f.field_type in DATE_TIME_TYPES else f.field_type,
            f.mode, f.description) for f in bq.get_table_schema(table)
    ]


def _filename_to_table_name(filename: str) -> str:
    return filename.replace('.csv', '').lower()


def _table_name_to_filename(table_name: str) -> str:
    return f'{table_name.upper()}.csv'


def load_stage(dst_dataset: Dataset, bq_client: Client, bucket_name: str,
               gcs_client: storage.Client) -> List[LoadJob]:
    """
    Stage files from a bucket to a dataset

    :param dst_dataset: reference to destination dataset object
    :param bq_client: a BigQuery client object
    :param bucket_name: the location in GCS containing the vocabulary files
    :param gcs_client: a Cloud Storage client object
    :return: list of completed load jobs
    """
    blobs = list(gcs_client.list_blobs(bucket_name))

    table_blobs = [_filename_to_table_name(blob.name) for blob in blobs]
    missing_blobs = [
        table for table in VOCABULARY_TABLES if table not in table_blobs
    ]
    if missing_blobs:
        raise RuntimeError(
            f'Bucket {bucket_name} is missing files for tables {missing_blobs}')

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
        job_config.source_format = 'CSV'
        job_config.quote_character = ''
        source_uri = f'gs://{bucket_name}/{blob.name}'
        load_job = bq_client.load_table_from_uri(source_uri,
                                                 destination,
                                                 job_config=job_config)
        LOGGER.info(f'table:{destination} job_id:{load_job.job_id}')
        load_jobs.append(load_job)
        load_job.result()
    return load_jobs


# TODO Move this to another module (auth, admin?). It can be reused
#  if/when setting standard properties on other datasets
def dataset_properties_from_file(json_path: str) -> Dict[str, object]:
    """
    Read and validate a JSON file containing dataset properties to update
    (structure described at https://tinyurl.com/269xprpe)

    :param json_path: path to a JSON file that defines dataset access
      (follows API resource structure described at https://tinyurl.com/269xprpe)

    :return: a dict which maps (dataset field name => value)
    """
    with open(json_path, 'r', encoding='utf-8') as json_fp:
        resource_properties = json.load(json_fp)
    if 'access' not in resource_properties:
        raise RuntimeError(f'Missing "access" field in {json_path}')
    if not isinstance(resource_properties['access'], list):
        raise TypeError(f'Field "access" in {json_path} must refer to a list')
    dataset_properties = dict()
    dataset_properties['access_entries'] = [
        AccessEntry.from_api_repr(access_entry)
        for access_entry in resource_properties['access']
    ]
    return dataset_properties


def load(project_id, bq_client, src_dataset_id, dst_dataset_id,
         dataset_properties):
    """
    Transform safely loaded tables and store
    :param project_id: Identifies the BQ project
    :param bq_client: a BigQuery client object
    :param src_dataset_id: reference to source dataset object
    :param dst_dataset_id: reference to destination dataset object results in target dataset.
    :param dataset_properties: a dict specifying target dataset properties to
      set (i.e. access_entries)
    :return: List of BQ job_ids
    """
    dst_dataset = Dataset(f'{bq_client.project}.{dst_dataset_id}')
    dst_dataset.description = f'Vocabulary cleaned and loaded from {src_dataset_id}'
    dst_dataset.labels = {'type': 'vocabulary'}
    dst_dataset.location = "US"
    dst_dataset = bq_client.create_dataset(dst_dataset, exists_ok=True)
    src_tables = list(bq_client.list_tables(dataset=src_dataset_id))

    job_config = QueryJobConfig()
    query_jobs = []
    for src_table in src_tables:
        schema = bq.get_table_schema(src_table.table_id)
        destination = f'{project_id}.{dst_dataset_id}.{src_table.table_id}'
        table = bq_client.create_table(Table(destination, schema=schema),
                                       exists_ok=True)
        job_config.destination = table
        query = SELECT_TPL.render(project_id=project_id,
                                  dataset_id=src_dataset_id,
                                  table=src_table.table_id,
                                  fields=schema)
        query_job = bq_client.query(query, job_config=job_config)
        LOGGER.info(f'table:{destination} job_id:{query_job.job_id}')
        query_jobs.append(query_job)
        query_job.result()

    # to prevent sharing a dataset that fails to load
    # only set dataset access policy at the end
    dst_dataset.access_entries = dataset_properties['access_entries']
    bq_client.update_dataset(dst_dataset, ['access_entries'])

    return query_jobs


def main(project_id: str, bucket_name: str, vocab_folder_path: str,
         dst_dataset_id: str, dataset_json_path: str):
    """
    Load and transform vocabulary files in GCS to a BigQuery dataset

    :param project_id: Identifies the BQ project
    :param bucket_name: refers to the bucket containing vocabulary files
    :param vocab_folder_path: points to the directory containing files downloaded from athena with CPT4 applied
    :param dst_dataset_id: final destination to load the vocabulary in BigQuery
    :param dataset_json_path: path to a JSON file that defines dataset access
      (must follow API resource structure described at https://tinyurl.com/269xprpe)
    """
    # reading file first to ensure early failure on bad input
    dataset_props = dataset_properties_from_file(dataset_json_path)
    bq_client = bq.get_client(project_id)
    gcs_client = storage.Client(project_id)
    vocab_folder_path = Path(vocab_folder_path)
    update_aou_vocabs(vocab_folder_path)
    upload_stage(bucket_name, vocab_folder_path, gcs_client)
    staging_dataset = check_and_create_staging_dataset(dst_dataset_id,
                                                       bucket_name, bq_client)
    load_stage(staging_dataset, bq_client, bucket_name, gcs_client)
    load(project_id, bq_client, staging_dataset.dataset_id, dst_dataset_id,
         dataset_props)
    return


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
        '-f',
        '--vocab_folder_path',
        dest='vocab_folder_path',
        action='store',
        help='Identifies the path to the folder containing the vocabulary files',
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
        help=
        'Identifies the target dataset where the vocabulary is to be loaded',
        required=False)
    # this file should be in curation-devops
    argument_parser.add_argument(
        '--dataset_access_json_path',
        dest='dataset_access_json_path',
        action='store',
        help='Path to a JSON file that defines dataset access',
        required=True)
    return argument_parser


def get_release_date(release_date: datetime.date = None) -> str:
    """
    Get the name of a vocabulary release based on date

    :param release_date: date the vocabulary is released
    :return: name of vocabulary release
    """
    if not release_date:
        release_date = datetime.date.today()
    release_date_str = release_date.strftime("%Y%m%d")
    return release_date_str


def get_target_dataset_id(release_tag: str) -> str:
    return f'vocabulary{release_tag}'


if __name__ == '__main__':
    ARGS = get_arg_parser().parse_args()
    RELEASE_TAG = ARGS.release_date or get_release_date()
    TARGET_DATASET_ID = ARGS.target_dataset_id or get_target_dataset_id(
        RELEASE_TAG)
    pipeline_logging.configure(add_console_handler=True)
    main(ARGS.project_id, ARGS.bucket_name, ARGS.vocab_folder_path,
         TARGET_DATASET_ID, ARGS.dataset_access_json_path)
