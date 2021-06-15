#!/usr/bin/env bash
# This Script automates the process of generating the rdr_snapshot and apply rdr cleaning rules

from argparse import ArgumentParser
from datetime import datetime
import logging

from google.cloud import bigquery

from cdr_cleaner import clean_cdr
from cdr_cleaner.args_parser import add_kwargs_to_args
from utils import auth
from utils import bq
from utils import pipeline_logging

LOGGER = logging.getLogger(__name__)

SCOPES = [
    'https://www.googleapis.com/auth/bigquery',
    'https://www.googleapis.com/auth/devstorage.read_write',
]


def parse_rdr_args(raw_args=None):
    parser = ArgumentParser(
        description='Arguments pertaining to cleaning an RDR dataset.')

    parser.add_argument('--run_as',
                        action='store',
                        dest='run_as_email',
                        help='Service account email address to impersonate',
                        required=True)
    parser.add_argument(
        '--release_tag',
        action='store',
        dest='release_tag',
        help='Release tag for naming and labeling the cleaned dataset with.',
        required=True)
    parser.add_argument('--curation_project',
                        action='store',
                        dest='curation_project_id',
                        help='Curation project to load the RDR data into.',
                        required=True)
    parser.add_argument('--rdr_dataset',
                        action='store',
                        dest='rdr_dataset',
                        help='RDR dataset to backup and clean.',
                        required=True)
    parser.add_argument('-l',
                        '--console_log',
                        dest='console_log',
                        action='store_true',
                        required=False,
                        help='Log to the console as well as to a file.')
    parser.add_argument(
        '--truncation_date',
        action='store',
        dest='truncation_date',
        required=False,
        help=('date to truncate the RDR data to.  The cleaning rule defaults '
              'to the current date if unset.'))
    parser.add_argument(
        '--export_date',
        action='store',
        dest='export_date',
        required=False,
        help=
        'Raw rdr export date. store_pid_rid_mappings default to current date if unset'
    )

    common_args, unknown_args = parser.parse_known_args(raw_args)
    custom_args = clean_cdr._get_kwargs(unknown_args)
    return common_args, custom_args


def main(raw_args=None):
    """
    Clean an RDR import.

    Assumes you are passing arguments either via command line or a
    list.
    """
    args, kwargs = parse_rdr_args(raw_args)

    pipeline_logging.configure(level=logging.INFO,
                               add_console_handler=args.console_log)

    # specific check on truncation_date. It should not cause a failure if it is not set.
    if not args.truncation_date:
        LOGGER.info('truncation_date is unset.  It will default to the current '
                    'date in the truncation cleaning rule.')

    # validate we've got all required data before continuing
    cleaning_classes = clean_cdr.DATA_STAGE_RULES_MAPPING.get('rdr')
    clean_cdr.validate_custom_params(cleaning_classes, **kwargs)

    # get credentials and create client
    impersonation_creds = auth.get_impersonation_credentials(
        args.run_as_email, SCOPES)

    client = bq.get_client(args.curation_project_id,
                           credentials=impersonation_creds)

    # create staging, sandbox, and clean datasets with descriptions and labels
    datasets = create_datasets(client, args.rdr_dataset, args.release_tag)

    # copy raw data into staging dataset
    copy_raw_rdr_tables(client, args.rdr_dataset, datasets.get('staging'))

    # clean the RDR staging dataset
    cleaning_args = [
        '-p', args.curation_project_id, '-d',
        datasets.get('staging', 'UNSET'), '-b',
        datasets.get('sandbox',
                     'UNSET'), '--data_stage', 'rdr', '--truncation_date',
        args.truncation_date, '--export_date', args.export_date
    ]

    all_cleaning_args = add_kwargs_to_args(cleaning_args, kwargs)
    clean_cdr.main(args=all_cleaning_args)

    bq.build_and_copy_contents(client, datasets.get('staging', 'UNSET'),
                               datasets.get('clean', 'UNSET'))

    # update sandbox description and labels
    sandbox_dataset = client.get_dataset(datasets.get(
        'sandbox', 'UNSET'))  # Make an API request.
    sandbox_dataset.description = (
        f'Sandbox created for storing records affected by the cleaning '
        f'rules applied to {datasets.get("clean")}')
    sandbox_dataset.labels['phase'] = 'sandbox'
    sandbox_dataset = client.update_dataset(
        sandbox_dataset, ["description"])  # Make an API request.

    full_dataset_id = f'{sandbox_dataset.project}.{sandbox_dataset.dataset_id}'
    LOGGER.info(
        f'Updated dataset `{full_dataset_id}` with description `{sandbox_dataset.description}`'
    )

    LOGGER.info(f'RDR snapshot and cleaning, '
                f'`{client.project}.{datasets.get("clean")}`, is complete.')


def copy_raw_rdr_tables(client, rdr_dataset, rdr_staging):
    LOGGER.info(
        f'Beginning COPY of raw rdr tables from `{rdr_dataset}` to `{rdr_staging}`'
    )
    # get list of tables
    src_tables = client.list_tables(rdr_dataset)

    # create a copy job config
    job_config = bigquery.job.CopyJobConfig(
        write_disposition=bigquery.job.WriteDisposition.WRITE_EMPTY)

    for table_item in src_tables:
        job_config.labels = {
            'table_name': table_item.table_id,
            'copy_from': rdr_dataset,
            'copy_to': rdr_staging
        }

        destination_table = f'{client.project}.{rdr_staging}.{table_item.table_id}'
        # job_id defined to the second precision
        job_id = (f'rdr_staging_copy_{table_item.table_id.lower()}_'
                  f'{datetime.now().strftime("%Y%m%d_%H%M%S")}')
        # copy each table to rdr dataset
        client.copy_table(table_item.reference,
                          destination_table,
                          job_id=job_id,
                          job_config=job_config)

    LOGGER.info(
        f'RDR raw table COPY from `{rdr_dataset}` to `{rdr_staging}` is complete'
    )


def create_datasets(client, rdr_dataset, release_tag):
    rdr_clean = f'{release_tag}_rdr'
    rdr_staging = f'{rdr_clean}_staging'
    rdr_sandbox = f'{rdr_clean}_sandbox'

    staging_desc = f'Intermediary dataset to apply cleaning rules on {rdr_dataset}'
    labels = {
        "phase": "staging",
        "release_tag": release_tag,
        "de_identified": "false"
    }
    staging_dataset_object = bq.define_dataset(client.project, rdr_staging,
                                               staging_desc, labels)
    client.create_dataset(staging_dataset_object)
    LOGGER.info(f'Created dataset `{client.project}.{rdr_staging}`')

    sandbox_desc = (f'Sandbox created for storing records affected by the '
                    f'cleaning rules applied to {rdr_staging}')
    labels["phase"] = "sandbox"
    sandbox_dataset_object = bq.define_dataset(client.project, rdr_sandbox,
                                               sandbox_desc, labels)
    client.create_dataset(sandbox_dataset_object)
    LOGGER.info(f'Created dataset `{client.project}.{rdr_sandbox}`')

    version = 'implement getting software version'
    clean_desc = (f'{version} clean version of {rdr_dataset}')
    labels["phase"] = "clean"
    clean_dataset_object = bq.define_dataset(client.project, rdr_clean,
                                             clean_desc, labels)
    client.create_dataset(clean_dataset_object)
    LOGGER.info(f'Created dataset `{client.project}.{rdr_clean}`')

    return {'clean': rdr_clean, 'staging': rdr_staging, 'sandbox': rdr_sandbox}


if __name__ == '__main__':
    main()
