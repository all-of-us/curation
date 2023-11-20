# Python Imports
import logging
from argparse import ArgumentParser

from google.cloud import bigquery

from common import CDR_SCOPES, MAPPING_PREFIX, AOU_DEATH
from resources import CDM_TABLES
from utils import auth, pipeline_logging
from gcloud.bq import BigQueryClient
from tools.pipeline_utils import create_datasets, create_cdm_tables
from cdr_cleaner import clean_cdr
from cdr_cleaner.args_parser import add_kwargs_to_args

LOGGER = logging.getLogger(__name__)


def parse_unioned_ehr_args(raw_args=None):
    parser = ArgumentParser(
        description='Arguments pertaining to an Unioned EHR dataset generation')

    parser.add_argument('--run_as',
                        action='store',
                        dest='run_as_email',
                        help='Service account email address to impersonate',
                        required=True)
    parser.add_argument('--pmi_email',
                        action='store',
                        dest='pmi_email;',
                        help='PMI email address to use for mapping',
                        required=True)
    parser.add_argument(
        '--project_id',
        action='store',
        dest='project_id',
        help='Curation project to create a unioned_ehr_dataset in.',
        required=True)
    parser.add_argument(
        '--vocab_dataset',
        action='store',
        dest='vocab_dataset',
        help='Vocabulary dataset used by RDR to create this data dump.',
        required=True)
    parser.add_argument(
        '--release_tag',
        action='store',
        dest='release_tag',
        help='Release tag for naming and labeling the cleaned dataset with.',
        required=True)
    parser.add_argument('--ehr_snapshot',
                        action='store',
                        dest='ehr_snapshot',
                        help='ehr dataset used to generate unioned_ehr dataset',
                        required=True)
    parser.add_argument('--ehr_cutoff_date',
                        action='store',
                        dest='ehr_cutoff_date',
                        required=True,
                        help='date to truncate the unioned_ehr data to')
    parser.add_argument('-l',
                        '--console_log',
                        dest='console_log',
                        action='store_true',
                        required=False,
                        help='Log to the console as well as to a file.')

    common_args, unknown_args = parser.parse_known_args(raw_args)
    custom_args = clean_cdr._get_kwargs(unknown_args)
    return common_args, custom_args


def copy_mapping_tables(client, source_dataset, destination_dataset):
    """
    copies mapping tables from ehr_snapshot dataset to unioned_ehr_backup dataset
    :param client: a BigQueryClient
    :param source_dataset: the source dataset to copy from
    :param destination_dataset: the destination dataset to copy to
    """
    for table in [
            table.table_id
            for table in client.list_tables(source_dataset)
            if table.table_id.startswith(MAPPING_PREFIX)
    ]:
        source_table = f'{source_dataset}.{table}'
        destination_table = f'{destination_dataset}.{table}'
        LOGGER.info(f'Copying Table: {source_table}')
        if client.table_exists(table, source_dataset):
            job = client.copy_table(source_table, destination_table)
            job.result()
            LOGGER.info(
                f'Copied {source_table} to {destination_table} successfully')
        else:
            LOGGER.info(f'{source_table} does not exist')


def copy_unioned_ehr_tables(client, source_dataset, destination_dataset):
    """
    copies unioned_ehr tables from ehr_snapshot dataset to unioned_ehr_backup dataset

    :param client: a BigQueryClient
    :param source_dataset: the source dataset to copy from
    :param destination_dataset: the destination dataset to copy to
    
    """
    for table in CDM_TABLES + [AOU_DEATH]:
        source_table_id = f'unioned_ehr_{table}'
        source_table = f'{source_dataset}.{source_table_id}'
        destination_table = f'{destination_dataset}.{table}'
        if client.table_exists(source_table_id, source_dataset):
            job = client.copy_table(source_table,
                                    destination_table,
                                    job_config=bigquery.job.CopyJobConfig(
                                        write_disposition=bigquery.job.
                                        WriteDisposition.WRITE_EMPTY))
            job.result()
            LOGGER.info(
                f'Copied {source_table} to {destination_table} successfully')
        else:
            LOGGER.info(f'{source_table} does not exist')


def main(raw_args=None):
    args, kwargs = parse_unioned_ehr_args(raw_args)
    pipeline_logging.configure(level=logging.INFO,
                               add_console_handler=args.console_log)

    # validate we've got all required data before continuing
    cleaning_classes = clean_cdr.DATA_STAGE_RULES_MAPPING.get('unioned')
    clean_cdr.validate_custom_params(cleaning_classes, **kwargs)

    impersonation_creds = auth.get_impersonation_credentials(
        args.run_as_email, CDR_SCOPES)

    client = BigQueryClient(args.project_id, credentials=impersonation_creds)

    unioned_ehr_backup = create_datasets(client, args.release_tag, 'unioned',
                                         'backup')

    LOGGER.info('Creating destination CDM tables...')
    create_cdm_tables(client, unioned_ehr_backup)

    LOGGER.info(
        f'Copying vocabulary tables from {args.vocab_dataset} to {unioned_ehr_backup}'
    )
    client.copy_dataset(args.vocab_dataset, unioned_ehr_backup)
    LOGGER.info(
        f'Finished Copying vocabulary tables from {args.vocab_dataset} to {unioned_ehr_backup}'
    )
    LOGGER.info(
        f'copying clinical tables from {args.ehr_snapshot} to {unioned_ehr_backup}...'
    )
    copy_unioned_ehr_tables(client, args.ehr_snapshot, unioned_ehr_backup)
    LOGGER.info(f'Finished Copying clinical tables.')

    LOGGER.info(
        f'Copying _mapping_* tables from {args.ehr_snapshot} to {unioned_ehr_backup}...'
    )
    copy_mapping_tables(client, args.ehr_snapshot, unioned_ehr_backup)

    # Creating staging, sandbox and clean datasets for unioned_ehr cleaning process
    unioned_ehr_staging = create_datasets(client, args.release_tag, 'unioned',
                                          'staging')
    unioned_ehr_sandbox = create_datasets(client, args.release_tag, 'unioned',
                                          'sandbox')
    unioned_ehr = create_datasets(client, args.release_tag, 'unioned', 'clean')

    LOGGER.info(
        f' Copying unioned_ehr raw tables from `{unioned_ehr_backup}` to `{unioned_ehr_staging}`...'
    )
    client.copy_dataset(f'{client.project}.{unioned_ehr_backup}',
                        f'{client.project}.{unioned_ehr_staging}')
    LOGGER.info(
        f'unioned_ehr raw tables COPY from `{unioned_ehr_backup}` to `{unioned_ehr_staging}` is complete'
    )

    # Applying cleaning rules on unioned_ehr staging dataset
    cleaning_args = [
        '-p', client.project, '-d', unioned_ehr_staging, '-b',
        unioned_ehr_sandbox, '--data_stage', 'unioned', "--cutoff_date",
        args.ehr_cutoff_date, '--run_as', args.run_as_email, '-s'
    ]

    all_cleaning_args = add_kwargs_to_args(cleaning_args, kwargs)
    clean_cdr.main(args=all_cleaning_args)

    client.build_and_copy_contents(unioned_ehr_staging, unioned_ehr)

    LOGGER.info(f'Cleaning `{client.project}.{unioned_ehr}` is complete.')


if __name__ == '__main__':
    main()
