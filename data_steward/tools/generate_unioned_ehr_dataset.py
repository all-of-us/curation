# Python Imports
import logging
from argparse import ArgumentParser

# Third party imports
from google.cloud.exceptions import Conflict
from google.cloud import bigquery

from common import CDR_SCOPES, MAPPING_PREFIX
from resources import (get_git_tag, CDM_TABLES)
from utils import auth, pipeline_logging
from gcloud.bq import BigQueryClient
from cdr_cleaner import clean_cdr
from cdr_cleaner.args_parser import add_kwargs_to_args
from tools.create_combined_backup_dataset import create_cdm_tables

LOGGER = logging.getLogger(__name__)


def create_dataset(client, release_tag, dataset_type) -> str:
    """
    Create a dataset for the specified dataset type in the unioned_ehr stage. 

    :param client: a BigQueryClient
    :param release_tag: the release tag for this CDR
    :param dataset_type: the type of the dataset this function creates.
        It has to be clean, backup, staging, sandbox, or release.
    :returns: The name of the dataset.
    """
    version = get_git_tag()

    dataset_definition = {
        'clean': {
            'name':
                f'{release_tag}_unioned_ehr',
            'desc':
                f'{version} Clean version of {release_tag}_unioned_ehr_backup',
            'labels': {
                "owner": "curation",
                "phase": "clean",
                "release_tag": release_tag,
                "de_identified": "false"
            }
        },
        'backup': {
            'name':
                f'{release_tag}_unioned_ehr_backup',
            'desc':
                f'Combined raw version of {release_tag}_rdr + {release_tag}_unioned_ehr',
            'labels': {
                "owner": "curation",
                "phase": "backup",
                "release_tag": release_tag,
                "de_identified": "false"
            }
        },
        'staging': {
            'name':
                f'{release_tag}_unioned_ehr_staging',
            'desc':
                f'Intermediary dataset to apply cleaning rules on {release_tag}_unioned_ehr_backup',
            'labels': {
                "owner": "curation",
                "phase": "staging",
                "release_tag": release_tag,
                "de_identified": "false"
            }
        },
        'sandbox': {
            'name': f'{release_tag}_unioned_ehr_sandbox',
            'desc':
                (f'Sandbox created for storing records affected by the '
                 f'cleaning rules applied to {release_tag}_unioned_ehr_staging'
                ),
            'labels': {
                "owner": "curation",
                "phase": "sandbox",
                "release_tag": release_tag,
                "de_identified": "false"
            }
        }
    }

    LOGGER.info(
        f"Creating unioned_ehr {dataset_type} dataset if not exists: `{dataset_definition[dataset_type]['name']}`"
    )

    dataset_object = client.define_dataset(
        dataset_definition[dataset_type]['name'],
        dataset_definition[dataset_type]['desc'],
        dataset_definition[dataset_type]['labels'])

    try:
        client.create_dataset(dataset_object, exists_ok=False)
        LOGGER.info(
            f"Created dataset `{client.project}.{dataset_definition[dataset_type]['name']}`"
        )
    except Conflict:
        LOGGER.info(
            f"The dataset `{client.project}.{dataset_definition[dataset_type]['name']}` already exists. "
        )

    return dataset_definition[dataset_type]['name']


def parse_unioned_ehr_args(raw_args=None):
    parser = ArgumentParser(
        description='Arguments pertaining to an Unioned EHR dataset generation')

    parser.add_argument('--key_file',
                        action='store',
                        dest='key_file',
                        help='Path to key file for service account',
                        required=True)
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
    parser.add_argument(
        '--ehr_snapshot',
        action='store',
        dest='ehr_snapshot',
        help='unioned_ehr dataset dataset used to generate unioned_ehr dataset',
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
        if client.table_exists(source_table, source_dataset):
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


def copy_unioned_ehr_tables(client, source_dataset, destination_dataset):
    """
    copies unioned_ehr tables from ehr_snapshot dataset to unioned_ehr_backup dataset

    :param client: a BigQueryClient
    :param source_dataset: the source dataset to copy from
    :param destination_dataset: the destination dataset to copy to
    
    """
    for table in CDM_TABLES:
        source_table_id = f'{source_dataset}.unioned_ehr_{table}'
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

    impersonation_creds = auth.get_impersonation_credentials(
        args.run_as_email, CDR_SCOPES)

    client = BigQueryClient(args.project_id, credentials=impersonation_creds)

    unioned_ehr_backup = create_dataset(client, args.release_tag, 'backup')

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
    unioned_ehr_staging = create_dataset(client, args.release_tag, 'staging')
    unioned_ehr_sandbox = create_dataset(client, args.release_tag, 'sandbox')
    unioned_ehr = create_dataset(client, args.release_tag, 'clean')

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

    # update sandbox description and labels
    sandbox_dataset = client.get_dataset(unioned_ehr_sandbox)
    sandbox_dataset.description = (
        f'Sandbox created for storing records affected by the cleaning '
        f'rules applied to {unioned_ehr}')
    sandbox_dataset.labels['phase'] = 'sandbox'
    sandbox_dataset = client.update_dataset(sandbox_dataset, ["description"])

    LOGGER.info(
        f'Updated dataset `{sandbox_dataset.full_dataset_id}` with description `{sandbox_dataset.description}`'
    )
    LOGGER.info(f'Cleaning `{client.project}.{unioned_ehr}` is complete.')


if __name__ == '__main__':
    main()
