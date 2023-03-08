"""
This Script automates the process of generating the combined_staging and apply combined cleaning rules.
"""

# Python imports
import logging
from argparse import ArgumentParser

# Third party imports
from google.cloud.exceptions import Conflict

# Project imports
from cdr_cleaner import clean_cdr
from cdr_cleaner.args_parser import add_kwargs_to_args
import resources
from common import CDR_SCOPES
from gcloud.bq import BigQueryClient
from utils import auth
from utils import pipeline_logging

LOGGER = logging.getLogger(__name__)


def parse_combined_args(raw_args=None):
    parser = ArgumentParser(
        description='Arguments pertaining to cleaning an combined dataset.')

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
                        help='Curation project to load the combined data into.',
                        required=True)
    parser.add_argument(
        '--combined_backup_dataset',
        action='store',
        dest='combined_backup_dataset',
        help='combined backup dataset for creating clean and staging datasets.',
        required=True)
    parser.add_argument('--cutoff_date',
                        action='store',
                        dest='cutoff_date',
                        required=True,
                        help='date to truncate the combined data to')
    parser.add_argument('--validation_dataset_id',
                        action='store',
                        dest='validation_dataset_id',
                        required=True,
                        help='Validation dataset ID')
    parser.add_argument('--api_project_id',
                        action='store',
                        dest='api_project_id',
                        required=True,
                        help='Name of the Participant summary API project.')
    parser.add_argument('-l',
                        '--console_log',
                        dest='console_log',
                        action='store_true',
                        required=False,
                        help='Log to the console as well as to a file.')

    parser.add_argument('--ehr_dataset_id',
                        action='store',
                        dest='ehr_dataset_id',
                        required=True,
                        help='The EHR snapshot dataset ID')

    common_args, unknown_args = parser.parse_known_args(raw_args)
    custom_args = clean_cdr._get_kwargs(unknown_args)
    return common_args, custom_args


def main(raw_args=None):
    """
    Clean combined import.

    Assumes you are passing arguments either via command line or a
    list.
    """
    args, kwargs = parse_combined_args(raw_args)

    pipeline_logging.configure(level=logging.INFO,
                               add_console_handler=args.console_log)

    # validate we've got all required data before continuing
    cleaning_classes = clean_cdr.DATA_STAGE_RULES_MAPPING.get('combined')
    clean_cdr.validate_custom_params(cleaning_classes, **kwargs)

    # get credentials and create client
    impersonation_creds = auth.get_impersonation_credentials(
        args.run_as_email, CDR_SCOPES)

    bq_client = BigQueryClient(args.curation_project_id,
                               credentials=impersonation_creds)

    # create clean, staging, and release datasets with descriptions and labels.
    combined_clean = create_dataset(bq_client, args.release_tag, 'clean')
    combined_staging = create_dataset(bq_client, args.release_tag, 'staging')
    combined_release = create_dataset(bq_client, args.release_tag, 'release')
    # sandbox is already created by create_combined_backup_dataset.py.
    # create_dataset is called here to get the dataset name.
    combined_sandbox = create_dataset(bq_client, args.release_tag, 'sandbox')

    # copy raw data into staging dataset
    bq_client.copy_dataset(
        f'{bq_client.project}.{args.combined_backup_dataset}',
        f'{bq_client.project}.{combined_staging}')
    LOGGER.info(
        f'combined raw table COPY from `{args.combined_backup_dataset}` to `{combined_staging}` is complete'
    )

    # clean the combined staging dataset
    cleaning_args = [
        '-p', args.curation_project_id, '-d', combined_staging, '-b',
        combined_sandbox, '--data_stage', 'combined', "--cutoff_date",
        args.cutoff_date, '--validation_dataset_id', args.validation_dataset_id,
        '--ehr_dataset_id', args.ehr_dataset_id, '--api_project_id',
        args.api_project_id, '--run_as', args.run_as_email, '-s'
    ]

    all_cleaning_args = add_kwargs_to_args(cleaning_args, kwargs)
    clean_cdr.main(args=all_cleaning_args)

    bq_client.build_and_copy_contents(combined_staging, combined_clean)

    # update sandbox description and labels
    sandbox_dataset = bq_client.get_dataset(combined_sandbox)
    sandbox_dataset.description = (
        f'Sandbox created for storing records affected by the cleaning '
        f'rules applied to {combined_clean}')
    sandbox_dataset.labels['phase'] = 'sandbox'
    sandbox_dataset = bq_client.update_dataset(sandbox_dataset, ["description"])

    LOGGER.info(
        f'Updated dataset `{sandbox_dataset.full_dataset_id}` with description `{sandbox_dataset.description}`'
    )
    LOGGER.info(f'Cleaning `{bq_client.project}.{combined_clean}` is complete.')

    bq_client.copy_dataset(combined_clean, combined_release)
    LOGGER.info(
        f' Snapshotting `{combined_clean}` into {combined_release} is completed.'
    )


def create_dataset(client, release_tag, dataset_type) -> str:
    """
    Create a dataset for the specified dataset type in the combined stage. 

    :param client: a BigQueryClient
    :param release_tag: the release tag for this CDR
    :param dataset_type: the type of the dataset this function creates.
        It has to be clean, backup, staging, sandbox, or release.
    :returns: The name of the dataset.
    """
    version = resources.get_git_tag()

    dataset_definition = {
        'clean': {
            'name': f'{release_tag}_combined',
            'desc': f'{version} Clean version of {release_tag}_combined_backup',
            'labels': {
                "owner": "curation",
                "phase": "clean",
                "release_tag": release_tag,
                "de_identified": "false"
            }
        },
        'backup': {
            'name':
                f'{release_tag}_combined_backup',
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
                f'{release_tag}_combined_staging',
            'desc':
                f'Intermediary dataset to apply cleaning rules on {release_tag}_combined_backup',
            'labels': {
                "owner": "curation",
                "phase": "staging",
                "release_tag": release_tag,
                "de_identified": "false"
            }
        },
        'sandbox': {
            'name': f'{release_tag}_combined_sandbox',
            'desc':
                (f'Sandbox created for storing records affected by the '
                 f'cleaning rules applied to {release_tag}_combined_staging'),
            'labels': {
                "owner": "curation",
                "phase": "sandbox",
                "release_tag": release_tag,
                "de_identified": "false"
            }
        },
        'release': {
            'name': f'{release_tag}_combined_release',
            'desc': f'{version} Release version of {release_tag}_combined',
            'labels': {
                "owner": "curation",
                "phase": "release",
                "release_tag": release_tag,
                "de_identified": "false"
            }
        }
    }

    LOGGER.info(
        f"Creating Combined {dataset_type} dataset if not exists: `{dataset_definition[dataset_type]['name']}`"
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


if __name__ == '__main__':
    main()
