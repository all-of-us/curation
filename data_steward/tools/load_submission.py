"""
Loads a submission into bq from the archive
"""
import logging
import argparse

from google.cloud.bigquery import LoadJobConfig, Table
from google.cloud.storage import Client

from common import AOU_REQUIRED, JINJA_ENV
from utils import pipeline_logging
from utils.bq import get_table_schema, get_client
import constants.bq_utils as bq_consts

LOGGER = logging.getLogger(__name__)

BUCKET_NAME_QUERY = JINJA_ENV.from_string("""
SELECT bucket_name
FROM `{{project}}.{{dataset}}.{{bucket_names_table}}`
WHERE hpo_id = '{{hpo_id}}'
""")


def get_bucket(client, hpo_id):
    bucket_name_query = BUCKET_NAME_QUERY.render(
        project=client.project,
        dataset=bq_consts.LOOKUP_TABLES_DATASET_ID,
        bucket_names_table=bq_consts.HPO_ID_BUCKET_NAME_TABLE_ID,
        hpo_id=hpo_id.upper())

    bucket_name_result = client.query(bucket_name_query).result()
    bucket_name = bucket_name_result.to_dataframe()['bucket_name'].to_list()[0]
    return bucket_name


def _filename_to_table_name(filename: str) -> str:
    return filename.split('/')[-1].replace('.csv', '').lower()


def load_stage(dst_dataset, bq_client, bucket_name, prefix, gcs_client, hpo_id):
    """
    Stage files from a bucket to a dataset

    :param dst_dataset: reference to destination dataset object
    :param bq_client: a BigQuery client object
    :param bucket_name: the location in GCS containing the vocabulary files
    :param gcs_client: a Cloud Storage client object
    :return: list of completed load jobs
    """
    blobs = list(gcs_client.list_blobs(bucket_name, prefix=prefix))

    load_jobs = []
    for blob in blobs:
        table_name = _filename_to_table_name(blob.name)
        if table_name not in AOU_REQUIRED:
            continue
        schema = get_table_schema(table_name)
        hpo_table_name = f'{hpo_id}_{_filename_to_table_name(blob.name)}'
        fq_hpo_table = f'{bq_client.project}.{dst_dataset}.{hpo_table_name}'
        destination = Table(fq_hpo_table, schema=schema)
        destination = bq_client.create_table(destination, exists_ok=True)
        job_config = LoadJobConfig()
        job_config.schema = schema
        job_config.skip_leading_rows = 1
        job_config.source_format = 'CSV'
        source_uri = f'gs://{bucket_name}/{blob.name}'
        load_job = bq_client.load_table_from_uri(source_uri,
                                                 destination,
                                                 job_config=job_config)
        LOGGER.info(f'table:{destination} job_id:{load_job.job_id}')
        load_jobs.append(load_job)
        load_job.result()
    return load_jobs


def get_arg_parser():
    argument_parser = argparse.ArgumentParser(description=__doc__)
    argument_parser.add_argument(
        '-p',
        '--project_id',
        dest='project_id',
        action='store',
        help='Identifies the project containing the target dataset',
        required=True)
    argument_parser.add_argument('-d',
                                 '--dataset_id',
                                 dest='dataset_id',
                                 action='store',
                                 help='Identifies the target dataset',
                                 required=True)
    argument_parser.add_argument('-b',
                                 '--bucket_name',
                                 dest='bucket_name',
                                 action='store',
                                 help='Bucket containing archive folders',
                                 required=True)
    argument_parser.add_argument('-i',
                                 '--hpo_id',
                                 dest='hpo_id',
                                 action='store',
                                 help='Identifies the hpo_id of the site',
                                 required=True)
    argument_parser.add_argument('-f',
                                 '--folder_name',
                                 dest='folder_name',
                                 action='store',
                                 help='Name of the submission folder to load',
                                 required=True)
    return argument_parser


def main(project_id, dataset_id, bucket_name, hpo_id, folder_name):
    bq_client = get_client(project_id)
    gcs_client = Client(project_id)
    site_bucket = get_bucket(bq_client, hpo_id)
    prefix = f'{hpo_id}/{site_bucket}/{folder_name}'
    load_stage(dataset_id, bq_client, bucket_name, prefix, gcs_client, hpo_id)


if __name__ == '__main__':
    ARGS = get_arg_parser().parse_args()
    pipeline_logging.configure(add_console_handler=True)
    main(ARGS.project_id, ARGS.dataset_id, ARGS.bucket_name, ARGS.hpo_id,
         ARGS.folder_name)
