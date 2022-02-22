"""
Loads a submission into bq from the archive
"""
import logging
import argparse
from typing import List
from concurrent.futures import TimeoutError as TOError

from google.cloud.bigquery import LoadJobConfig, LoadJob, Table, Client as BQClient
from gcloud.gcs import StorageClient
from google.cloud.exceptions import GoogleCloudError

from common import AOU_REQUIRED, JINJA_ENV
from utils.bq import get_table_schema, get_client
import constants.bq_utils as bq_consts

LOGGER = logging.getLogger(__name__)

BUCKET_NAME_QUERY = JINJA_ENV.from_string("""
SELECT bucket_name
FROM `{{project}}.{{dataset}}.{{bucket_names_table}}`
WHERE hpo_id = '{{hpo_id}}'
""")


def get_bucket(client: BQClient, hpo_id: str) -> str:
    """
    Retrieves bucket name for site

    :param client: Bigquery Client object
    :param hpo_id: Identifies the HPO site
    :return: bucket name for the HPO site as a string
    :raises GoogleCloudError/TimeoutError
    """
    bucket_name_query = BUCKET_NAME_QUERY.render(
        project=client.project,
        dataset=bq_consts.LOOKUP_TABLES_DATASET_ID,
        bucket_names_table=bq_consts.HPO_ID_BUCKET_NAME_TABLE_ID,
        hpo_id=hpo_id.upper())

    try:
        bucket_names_result = client.query(bucket_name_query).result()
        bucket_names = bucket_names_result.to_dataframe(
        )['bucket_name'].to_list()
        if len(bucket_names) > 1:
            LOGGER.warning(
                f'Found more than one bucket name for site {hpo_id}: {bucket_names}'
            )
        bucket_name = bucket_names[0]
    except (GoogleCloudError, TOError) as e:
        LOGGER.error(f'Job failed with error {str(e)}')
        raise e
    return bucket_name


def _filename_to_table_name(filename: str) -> str:
    """
    Converts a file path/URI to a table name

    :param filename: filename or URI as /path/to/file.csv
    :return: table_name as string, e.g. 'file' for /path/to/file.csv
    """
    return filename.split('/')[-1].replace('.csv', '').lower()


def load_folder(dst_dataset: str, bq_client: BQClient, bucket_name: str,
                prefix: str, gcs_client: StorageClient,
                hpo_id: str) -> List[LoadJob]:
    """
    Stage files from a bucket to a dataset

    :param dst_dataset: Identifies the destination dataset
    :param bq_client: a BigQuery client object
    :param bucket_name: the bucket in GCS containing the archive files
    :param prefix: prefix of the filepath URI
    :param gcs_client: a StorageClient object
    :param hpo_id: Identifies the HPO site
    :return: list of completed load jobs
    """
    blobs = list(gcs_client.list_blobs(bucket_name, prefix=prefix))

    load_jobs = []
    for blob in blobs:
        table_name = _filename_to_table_name(blob.name)
        if table_name not in AOU_REQUIRED:
            LOGGER.debug(f'Skipping file for {table_name}')
            continue
        schema = get_table_schema(table_name)
        hpo_table_name = f'{hpo_id}_{table_name}'
        fq_hpo_table = f'{bq_client.project}.{dst_dataset}.{hpo_table_name}'
        destination = Table(fq_hpo_table, schema=schema)
        destination = bq_client.create_table(destination)
        job_config = LoadJobConfig()
        job_config.schema = schema
        job_config.skip_leading_rows = 1
        job_config.source_format = 'CSV'
        source_uri = f'gs://{bucket_name}/{blob.name}'
        load_job = bq_client.load_table_from_uri(
            source_uri,
            destination,
            job_config=job_config,
            job_id_prefix=f"{__file__.split('/')[-1].split('.')[0]}_")
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
    """
    Main function to load submission into dataset

    :param project_id: Identifies the project
    :param dataset_id: Identifies the destination dataset
    :param bucket_name: the bucket in GCS containing the archive files
    :param hpo_id: Identifies the HPO site
    :param folder_name: Name of the submission folder to load
    :return:
    """
    bq_client = get_client(project_id)
    gcs_client = StorageClient(project_id)
    site_bucket = get_bucket(bq_client, hpo_id)
    prefix = f'{hpo_id}/{site_bucket}/{folder_name}'
    LOGGER.info(
        f'Starting jobs for loading {bucket_name}/{prefix} into {dataset_id}')
    _ = load_folder(dataset_id, bq_client, bucket_name, prefix, gcs_client,
                    hpo_id)
    LOGGER.info(f'Successfully loaded {bucket_name}/{prefix} into {dataset_id}')


if __name__ == '__main__':
    from utils import pipeline_logging

    ARGS = get_arg_parser().parse_args()
    pipeline_logging.configure(add_console_handler=True)
    main(ARGS.project_id, ARGS.dataset_id, ARGS.bucket_name, ARGS.hpo_id,
         ARGS.folder_name)
