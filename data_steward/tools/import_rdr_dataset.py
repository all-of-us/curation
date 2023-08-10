#!/usr/bin/env bash

# Imports RDR ETL results into a dataset in BigQuery.
# Assumes you have already activated a service account that is able to
# access the dataset in BigQuery.

# Python imports
from argparse import ArgumentParser
from datetime import datetime
import logging

# Third party imports
from google.cloud import bigquery
from google.api_core.exceptions import NotFound

# Project imports
from utils import auth, pipeline_logging
from gcloud.bq import BigQueryClient
from common import CDR_SCOPES
from resources import replace_special_characters_for_labels, validate_date_string, rdr_src_id_schemas, cdm_schemas
from tools.snapshot_by_query import BIGQUERY_DATA_TYPES
from tools.import_rdr_omop import copy_vocab_tables

LOGGER = logging.getLogger(__name__)


def parse_rdr_args(raw_args=None):
    parser = ArgumentParser(
        description='Arguments pertaining to an RDR raw load')

    parser.add_argument('--rdr_project',
                        action='store',
                        dest='rdr_project_id',
                        help='RDR project to load RDR data from.',
                        required=True)
    parser.add_argument('--rdr_dataset',
                        action='store',
                        dest='rdr_dataset',
                        help='rdr source dataset name.',
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
    parser.add_argument('-l',
                        '--console_log',
                        dest='console_log',
                        action='store_true',
                        required=False,
                        help='Log to the console as well as to a file.')

    return parser.parse_args(raw_args)


def create_rdr_tables(client, destination_dataset, rdr_project,
                      rdr_source_dataset):
    """
    Create tables from the data in the RDR dataset.

    Uses the client to load data directly from the dataset into
    a table.

    :param client: a BigQueryClient
    :param destination_dataset: the existing local dataset to load file data into
    :param rdr_project: the source rdr project containing the data
    :param rdr_source_dataset: the source rdr dataset containing the data
    """
    schema_dict = cdm_schemas()
    schema_dict.update(rdr_src_id_schemas())

    for table, schema in schema_dict.items():

        destination_table_id = f'{client.project}.{destination_dataset}.{table}'
        source_table_id = f'{rdr_project}.{rdr_source_dataset}.{table}'

        # rdr consent table is ingested as consent_validation
        if table == 'consent_validation':
            consent_table = 'consent'
            source_table_id = f'{rdr_project}.{rdr_source_dataset}.{consent_table}'

        schema_list = client.get_table_schema(table, schema)
        destination_table = bigquery.Table(destination_table_id,
                                           schema=schema_list)

        for schema_item in schema_list:
            if 'person_id' in schema_item.name and table.lower(
            ) != 'pid_rid_mapping':
                destination_table.clustering_fields = 'person_id'
                destination_table.time_partitioning = bigquery.table.TimePartitioning(
                    type_='DAY')

        LOGGER.info(
            f'Loading `{source_table_id}` into `{destination_table_id}`')

        try:
            LOGGER.info(f'Get table `{source_table_id}` in RDR')
            table_ref = client.get_table(source_table_id)

            LOGGER.info(f'Creating empty CDM table, `{table}`')
            destination_table = client.create_table(
                destination_table)  # Make an API request.

            LOGGER.info(
                f'Copying source table `{source_table_id}` to destination table `{destination_table_id}`'
            )

            if table_ref.num_rows == 0:
                raise NotFound(f'`{source_table_id}` has No data To copy from')

            sc_list = []
            for item in schema_list:
                if item.field_type.lower(
                ) == 'string' and item.name != 'place_of_service_source_value':
                    field_cast = f'CAST(COLLATE({item.name}, "") AS {BIGQUERY_DATA_TYPES[item.field_type.lower()]}) AS \
                 {item.name}'

                else:
                    field_cast = f'CAST({item.name} AS {BIGQUERY_DATA_TYPES[item.field_type.lower()]}) AS {item.name}'
                sc_list.append(field_cast)

            fields_name_str = ',\n'.join(sc_list)

            # copy contents from source dataset to destination dataset
            sql = (f'SELECT {fields_name_str} ' f'FROM `{source_table_id}`')
            LOGGER.info(f'Running query: {sql}')

            job_config = bigquery.job.QueryJobConfig(
                default_dataset=f'{client.project}.{destination_dataset}',
                write_disposition=bigquery.job.WriteDisposition.WRITE_EMPTY,
                priority=bigquery.job.QueryPriority.BATCH,
                destination=destination_table,
                labels={
                    'table_name':
                        table.lower(),
                    'copy_from':
                        replace_special_characters_for_labels(source_table_id),
                    'copy_to':
                        replace_special_characters_for_labels(
                            destination_table_id)
                })
            job_id = (f'schemaed_copy_{table}_'
                      f'{datetime.now().strftime("%Y%m%d_%H%M%S")}')
            job = client.query(sql, job_config=job_config, job_id=job_id)
            job.result()  # Wait for the job to complete.
        except NotFound:
            LOGGER.info(
                f'Created empty table in dataset: `{destination_dataset}`')
        else:
            destination_table = client.get_table(
                destination_table_id)  # Make an API request.
        LOGGER.info(f'Loaded {destination_table.num_rows} rows into '
                    f'`{destination_table.table_id}`.')

    LOGGER.info(
        f"Finished RDR table LOAD from dataset {rdr_project}.{rdr_source_dataset}"
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

    description = f'RDR DUMP loaded from {args.rdr_project_id}.{args.rdr_dataset} dated {args.export_date}'
    export_date = args.export_date.replace('-', '')
    new_dataset_name = f'rdr{export_date}'

    # get credentials and create client
    impersonation_creds = auth.get_impersonation_credentials(
        args.run_as_email, CDR_SCOPES)

    bq_client = BigQueryClient(args.curation_project_id,
                               credentials=impersonation_creds)

    dataset_object = bq_client.define_dataset(new_dataset_name, description,
                                              {'export_date': args.export_date})
    bq_client.create_dataset(dataset_object)

    create_rdr_tables(bq_client, new_dataset_name, args.rdr_project_id,
                      args.rdr_dataset)
    copy_vocab_tables(bq_client, new_dataset_name, args.vocabulary)


if __name__ == '__main__':
    main()
