# Python Imports
import logging
from argparse import ArgumentParser
from datetime import datetime, timedelta
import json

import curation_bq_utils
from common import CDR_SCOPES, NOTE_NLP, VOCABULARY_TABLES, ALL_ADDITIONAL_TABLES
from curation_utils import auth, pipeline_logging
from gcloud.bq import BigQueryClient
from constants.cdr_cleaner import clean_cdr as consts
from create_tier import create_datasets, get_dataset_name, TIER_LIST, DEID_STAGE_LIST
from cdr_cleaner import clean_cdr
from cdr_cleaner.args_parser import add_kwargs_to_args
from resources import get_git_tag, fields_for
from tools.run_deid import main as deid_main
from tools import add_cdr_metadata

LOGGER = logging.getLogger(__name__)


def parse_deid_args(raw_args=None):
    """
    Parse command line arguments for the deid runner
    """
    parser = ArgumentParser(
        description='Arguments pertaining to a de-identification process')

    parser.add_argument('--key_file',
                        action='store',
                        dest='key_file',
                        help='Path to service account key file',
                        required=True)
    parser.add_argument('--idataset',
                        action='store',
                        dest='idataset',
                        help='Combined dataset name',
                        required=True)
    parser.add_argument('--run_as',
                        action='store',
                        dest='run_as_email',
                        help='Service account email address to impersonate',
                        required=True)
    parser.add_argument('--pmi_email',
                        action='store',
                        dest='pmi_email',
                        help='PMI email address to use',
                        required=True)
    parser.add_argument('--deid_questionnaire_response_map_dataset',
                        action='store',
                        dest='deid_questionnaire_response_map_dataset',
                        help='De-id questionnaire response map dataset name',
                        required=True)
    parser.add_argument('--vocab_dataset',
                        action='store',
                        dest='vocab_dataset',
                        help='Vocabulary dataset name',
                        required=True)
    parser.add_argument('--release_tag',
                        action='store',
                        dest='release_tag',
                        help='Release tag for the CDR',
                        required=True)
    parser.add_argument('-t',
                        '--tier',
                        action='store',
                        dest='tier',
                        help='controlled or registered tier',
                        required=True,
                        choices=TIER_LIST)
    parser.add_argument('-d',
                        '--deid_stage',
                        action='store',
                        dest='deid_stage',
                        help='deid stage (deid, base or clean)',
                        required=True,
                        choices=DEID_STAGE_LIST)
    parser.add_argument('--deid_max_age',
                        action='store',
                        type=int,
                        dest='deid_max_age',
                        help='Maximum age for de-identified participants',
                        required=True)
    parser.add_argument('-l',
                        '--console_log',
                        dest='console_log',
                        action='store_true',
                        required=False,
                        help='Log to the console as well as to a file.')

    common_args, unknown_args = parser.parse_known_args(raw_args)
    custom_args = clean_cdr._get_kwargs(unknown_args)
    return common_args, custom_args


def get_project_id(key_file):
    """
    Extract project ID from service account key file
    """
    with open(key_file) as f:
        return json.load(f)['quota_project_id']


def main(raw_args=None):
    """
    Main function to run the de-identification process
    """
    args, kwargs = parse_deid_args(raw_args)
    pipeline_logging.configure(level=logging.INFO,
                               add_console_handler=args.console_log)

    # Get necessary variables
    project_id = get_project_id(args.key_file)
    handoff_date = (datetime.now() + timedelta(days=2)).strftime('%Y-%m-%d')

    # Setup credentials and client
    impersonation_creds = auth.get_impersonation_credentials(
        args.run_as_email, CDR_SCOPES)
    client = BigQueryClient(project_id, credentials=impersonation_creds)

    # Create datasets using helper function
    final_dataset_name = get_dataset_name(args.tier, args.release_tag,
                                          args.deid_stage)
    datasets = create_datasets(client,
                               final_dataset_name,
                               args.idataset,
                               args.tier,
                               args.release_tag,
                               backup=True)

    # Copy tables from combined dataset
    LOGGER.info(
        f"Copying tables from combined dataset to {datasets[consts.BACKUP]}")
    client.copy_dataset(args.idataset, datasets[consts.BACKUP])

    # Copy and setup vocabulary
    LOGGER.info('Setting up vocabulary tables...')
    client.copy_dataset(args.vocab_dataset, datasets[consts.BACKUP])

    # Run pre-deid cleaning rules
    LOGGER.info('Running cleaning rules...')
    cleaning_args = [
        '-p', project_id, '-d', datasets[consts.BACKUP], '-b',
        datasets[consts.SANDBOX], '--data_stage', 'registered_tier_pre_deid',
        '--run_as', args.run_as_email, '-s'
    ]

    all_cleaning_args = add_kwargs_to_args(cleaning_args, kwargs)
    clean_cdr.main(args=all_cleaning_args)

    # Run de-identification
    LOGGER.info('Running de-identification process...')
    deid_args = [
        '-i', datasets[consts.BACKUP], '--run_as', args.run_as_email, '-p',
        args.key_file, '-o', datasets[consts.STAGING], '-a', 'submit',
        '--interactive', '-c', '-m',
        str(args.deid_max_age)
    ]
    deid_main(raw_args=deid_args)

    # Copy Note_nlp, Vocabulary, derived, health_eco, & metadata tables in Staging
    LOGGER.info(f'Copying {NOTE_NLP} table to {datasets[consts.STAGING]}...')
    client.copy_table(f'{project_id}.{datasets[consts.BACKUP]}.{NOTE_NLP}',
                      f'{project_id}.{datasets[consts.STAGING]}.{NOTE_NLP}')

    for vocab_table in VOCABULARY_TABLES:
        LOGGER.info(
            f'Copying {vocab_table} table to {datasets[consts.STAGING]}...')
        client.copy_table(
            f'{project_id}.{datasets[consts.BACKUP]}.{vocab_table}',
            f'{project_id}.{datasets[consts.STAGING]}.{vocab_table}')

    # Create empty tables for Other_additional_tables
    for additional_table in ALL_ADDITIONAL_TABLES:
        field_list = fields_for(additional_table)
        # create a table schema only.
        curation_bq_utils.create_table(additional_table,
                                       field_list,
                                       drop_existing=True,
                                       dataset_id=datasets[consts.STAGING])

    # Run cleaning rules
    LOGGER.info('Running cleaning rules...')
    cleaning_args = [
        '-p', project_id, '-d', datasets[consts.STAGING], '-b',
        datasets[consts.SANDBOX], '--data_stage', 'registered_tier_deid',
        '--mapping_dataset_id', datasets[consts.STAGING],
        '--deid_questionnaire_response_map_dataset',
        args.deid_questionnaire_response_map_dataset, '--run_as',
        args.run_as_email, '-s'
    ]

    all_cleaning_args = add_kwargs_to_args(cleaning_args, kwargs)
    clean_cdr.main(args=all_cleaning_args)

    LOGGER.info(
        f'Copying Cleaned tables from Staging to Clean {datasets[consts.CLEAN]}'
    )
    client.build_and_copy_contents(datasets[consts.STAGING],
                                   datasets[consts.CLEAN])

    # Handle CDR metadata
    LOGGER.info(f'Adding cdr_metadata table to {datasets[consts.CLEAN]}')
    # First create the metadata table
    add_cdr_metadata.main([
        '--component', add_cdr_metadata.CREATE, '--project_id', project_id,
        '--target_dataset', datasets[consts.CLEAN]
    ])

    # Insert metadata with appropriate values
    today = datetime.now().strftime('%Y-%m-%d')
    git_version = str(get_git_tag())

    # Copy metadata from source CDR and add deid-specific fields
    add_cdr_metadata.main([
        '--component', add_cdr_metadata.COPY, '--project_id', project_id,
        '--source_dataset', datasets[consts.STAGING], '--target_dataset',
        datasets[consts.CLEAN]
    ],
                          bq_client=client)

    add_cdr_metadata.main([
        '--component', add_cdr_metadata.INSERT, '--project_id', project_id,
        '--target_dataset', datasets[consts.CLEAN], '--qa_handoff_date',
        handoff_date, '--etl_version', git_version, '--cdr_generation_date',
        today
    ],
                          bq_client=client)

    LOGGER.info(
        f'De-identification process complete for {datasets[consts.CLEAN]}')


if __name__ == '__main__':
    main()
