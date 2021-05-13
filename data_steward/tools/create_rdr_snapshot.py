#!/usr/bin/env bash
# This Script automates the process of generating the rdr_snapshot and apply rdr cleaning rules

from argparse import ArgumentParser
from datetime import datetime
import logging

from google.cloud import bigquery
from google.api_core.exceptions import NotFound

from cdr_cleaner import clean_cdr
from cdr_cleaner.args_parser import add_kwargs_to_args
from utils import auth
from utils import bq
from utils import pipeline_logging
import cdm
import resources

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

    common_args, unknown_args = parser.parse_known_args(args)
    custom_args = clean_cdr._get_kwargs(unknown_args)
    #    return parser.parse_args(raw_args)
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
    copy_raw_rdr_tables(client, rdr_dataset, datasets[1])

    # clean the RDR staging dataset
    cleaning_args = [
        '-p', args.curation_project_id, '-d', datasets[1], '-b', datasets[2],
        '--data_stage', 'rdr'
    ]

    all_cleaning_args = add_kwargs_to_args(cleaning_args, kwargs)
    clean_cdr.main(args=all_cleaning_args)


def copy_raw_rdr_tables(client, rdr_dataset, rdr_staging):
    LOGGER.info(
        f'Beginning COPY of raw rdr tables from `{rdr_dataset}` to `{rdr_staging}`'
    )
    # get list of tables
    rdr_tables = client.list_tables(rdr_dataset)

    # create a copy job config
    job_config = bigquery.job.CopyJobConfig(
        write_disposition=bigquery.job.WriteDisposition.WRITE_EMPTY)

    for table_item in vocab_tables:
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

    sandbox_desc = (f'Sandbox created for storing records affected by the '
                    f'cleaning rules applied to {rdr_staging}')
    labels["phase"] = "sandbox"
    sandbox_dataset_object = bq.define_dataset(client.project, rdr_sandbox,
                                               sandbox_desc, labels)
    client.create_dataset(sandbox_dataset_object)

    version = 'implement getting software version'
    clean_desc = (f'{version} clean version of {rdr_dataset}')
    clean_labels["phase"] = "clean"
    clean_dataset_object = bq.define_dataset(client.project, rdr_clean,
                                             clean_desc, labels)
    client.create_dataset(clean_dataset_object)

    return (rdr_clean, rdr_staging, rdr_sandbox)


if __name__ == '__main__':
    main()
#tag=$(git describe --abbrev=0 --tags)
#version=${tag}

#echo "--------------------------> Snapshotting  and cleaning RDR Dataset"
#rdr_clean="${dataset_release_tag}_rdr"
#rdr_clean_staging="${rdr_clean}_staging"
#rdr_sandbox="${rdr_clean}_sandbox"
##set BIGQUERY_DATASET_ID variable to dataset name where the vocabulary exists
#export BIGQUERY_DATASET_ID="${rdr_clean_staging}"
#export RDR_DATASET_ID="${rdr_clean_staging}"
#echo "Cleaning the RDR data"
#data_stage="rdr"
#
#echo "--------------------------> applying cleaning rules on staging"

# Create a snapshot dataset with the result
#python "${TOOLS_DIR}/snapshot_by_query.py" --project_id "${app_id}" --dataset_id "${rdr_clean_staging}" --snapshot_dataset_id "${rdr_clean}"

# created at this point in the new code
####bq update --description "${version} clean version of ${rdr_dataset}" --set_label "phase:clean" --set_label "release_tag:${dataset_release_tag}" --set_label "de_identified:false" ${app_id}:${rdr_clean}

# Update sandbox description
#bq update --description "Sandbox created for storing records affected by the cleaning rules applied to ${rdr_clean}" --set_label "phase:sandbox" --set_label "release_tag:${dataset_release_tag}" --set_label "de_identified:false" "${app_id}":"${rdr_sandbox}"
