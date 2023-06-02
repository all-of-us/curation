#!/usr/bin/env bash

# Imports RDR ETL results from GCS into a dataset in BigQuery.
# Assumes you have already activated a service account that is able to
# access the files in GCS.

from argparse import ArgumentParser
from datetime import datetime
import logging

from google.cloud import bigquery
from google.api_core.exceptions import NotFound

from utils import auth
from gcloud.bq import BigQueryClient
from utils import pipeline_logging
from common import CDR_SCOPES
from resources import replace_special_characters_for_labels, validate_date_string, cdm_schemas, rdr_specific_schemas, \
    rdr_src_id_schemas

LOGGER = logging.getLogger(__name__)


def parse_rdr_args(raw_args=None):
    parser = ArgumentParser(
        description='Arguments pertaining to an RDR raw load')

    parser.add_argument(
        '--rdr_bucket',
        action='store',
        dest='bucket',
        help='Bucket directory not including the "gs://" portion of the name',
        required=True)
    parser.add_argument('--run_as',
                        action='store',
                        dest='run_as_email',
                        help='Service account email address to impersonate',
                        required=True)
    parser.add_argument('--export_date',
                        action='store',
                        type=validate_date_string,
                        dest='export_date',
                        help='Date the RDR dump was exported to curation.',
                        required=True)
    parser.add_argument('--curation_project',
                        action='store',
                        dest='curation_project_id',
                        help='Curation project to load the RDR data into.',
                        required=True)
    parser.add_argument(
        '--vocab_dataset',
        action='store',
        dest='vocabulary',
        help='Vocabulary dataset used by RDR to create this data dump.',
        required=True)
    parser.add_argument(
        '--with_src_id',
        action='store',
        dest='with_src_id',
        help=
        'Boolean Flag that indicates if the files have src_id column. Choose between TRUE/FALSE.',
        required=True)
    parser.add_argument('-l',
                        '--console_log',
                        dest='console_log',
                        action='store_true',
                        required=False,
                        help='Log to the console as well as to a file.')

    return parser.parse_args(raw_args)


def create_rdr_tables(client, rdr_dataset, bucket, with_src_id=False):
    """
    Create tables from the data in the RDR bucket.

    Uses the client to load data directly from the bucket into
    a table.

    :param client: a BigQueryClient
    :param rdr_dataset: The existing dataset to load file data into
    :param bucket: the gcs bucket containing the file data.
    :param with_src_id: Boolean flag that indicates if the files have src_id column.
    """

    schema_dict = cdm_schemas()
    schema_dict.update(rdr_specific_schemas())
    if with_src_id:
        schema_dict.update(rdr_src_id_schemas())

    for table, schema in schema_dict.items():
        schema_list = client.get_table_schema(table, schema)
        table_id = f'{client.project}.{rdr_dataset}.{table}'
        job_config = bigquery.LoadJobConfig(
            schema=schema_list,
            skip_leading_rows=1,
            source_format=bigquery.SourceFormat.CSV,
            field_delimiter=',',
            allow_quoted_newlines=True,
            quote_character='"',
            write_disposition=bigquery.job.WriteDisposition.WRITE_TRUNCATE)
        if table == 'observation_period':
            job_config.allow_jagged_rows = True

        for schema_item in schema_list:
            if 'person_id' in schema_item.name and table.lower(
            ) != 'pid_rid_mapping':
                job_config.clustering_fields = 'person_id'
                job_config.time_partitioning = bigquery.table.TimePartitioning(
                    type_='DAY')

        # path to bucketed csv file
        uri = f'gs://{bucket}/{table}.csv'

        # job_id defined to the second precision
        job_id = f'rdr_load_{table.lower()}_{datetime.now().strftime("%Y%m%d_%H%M%S")}'

        LOGGER.info(f'Loading `{uri}` into `{table_id}`')
        try:
            load_job = client.load_table_from_uri(
                uri, table_id, job_config=job_config,
                job_id=job_id)  # Make an API request.

            load_job.result()  # Waits for the job to complete.
        except NotFound:
            LOGGER.info(
                f'{table} not provided by RDR team.  Creating empty table '
                f'in dataset: `{rdr_dataset}`')

            LOGGER.info(f'Creating empty CDM table, `{table}`')
            destination_table = bigquery.Table(table_id, schema=schema_list)
            destination_table = client.create_table(destination_table)
            LOGGER.info(f'Created empty table `{destination_table.table_id}`')
        else:
            destination_table = client.get_table(
                table_id)  # Make an API request.
        LOGGER.info(f'Loaded {destination_table.num_rows} rows into '
                    f'`{destination_table.table_id}`.')

    LOGGER.info(f"Finished RDR table LOAD from bucket gs://{bucket}")


def copy_vocab_tables(client, rdr_dataset, vocab_dataset):
    """
    Copy vocabulary tables into the new RDR dataset.

    Assumes the vocabulary and rdr datasets reside in the project the
    client object is built to access.  This is just a copy for now,
    because these tables are not partitioned yet.

    :param client: a BigQueryClient
    :param rdr_dataset:  the rdr dataset that tables will be copied into
    :param vocab_dataset: the vocabulary dataset id the tables will be copied from
    """
    LOGGER.info(
        f'Beginning COPY of vocab tables from `{vocab_dataset}` to `{rdr_dataset}`'
    )
    # get list of tables
    vocab_tables = client.list_tables(vocab_dataset)

    # create a copy job config
    job_config = bigquery.job.CopyJobConfig(
        write_disposition=bigquery.job.WriteDisposition.WRITE_EMPTY)

    for table_item in vocab_tables:
        job_config.labels = {
            'table_name':
                replace_special_characters_for_labels(table_item.table_id),
            'copy_from':
                replace_special_characters_for_labels(vocab_dataset),
            'copy_to':
                replace_special_characters_for_labels(rdr_dataset)
        }

        destination_table = f'{client.project}.{rdr_dataset}.{table_item.table_id}'
        # job_id defined to the second precision
        job_id = (f'rdr_vocab_copy_{table_item.table_id.lower()}_'
                  f'{datetime.now().strftime("%Y%m%d_%H%M%S")}')
        # copy each table to rdr dataset
        client.copy_table(table_item.reference,
                          destination_table,
                          job_id=job_id,
                          job_config=job_config)

    LOGGER.info(
        f'Vocabulary table COPY from `{vocab_dataset}` to `{rdr_dataset}` is complete'
    )


def main(raw_args=None):
    """
    Run a full RDR import.

    Assumes you are passing arguments either via command line or a
    list.
    """
    args = parse_rdr_args(raw_args)

    pipeline_logging.configure(level=logging.INFO,
                               add_console_handler=args.console_log)

    description = f'RDR DUMP loaded from {args.bucket} dated {args.export_date}'
    export_date = args.export_date.replace('-', '')
    new_dataset_name = f'rdr{export_date}'
    with_src_id = True if args.with_src_id == 'TRUE' else False

    # get credentials and create client
    impersonation_creds = auth.get_impersonation_credentials(
        args.run_as_email, CDR_SCOPES)

    bq_client = BigQueryClient(args.curation_project_id,
                               credentials=impersonation_creds)

    dataset_object = bq_client.define_dataset(new_dataset_name, description,
                                              {'export_date': args.export_date})
    bq_client.create_dataset(dataset_object)

    create_rdr_tables(bq_client, new_dataset_name, args.bucket, with_src_id)
    copy_vocab_tables(bq_client, new_dataset_name, args.vocabulary)


if __name__ == '__main__':
    main()
