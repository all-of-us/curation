"""
This Script automates the process of generating the combined_staging and apply combined cleaning rules.
"""

# Python Imports
import logging
from argparse import ArgumentParser

# Project imports
from cdr_cleaner import clean_cdr
import resources
from cdr_cleaner.args_parser import add_kwargs_to_args
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
    parser.add_argument('--combined_dataset',
                        action='store',
                        dest='combined_dataset',
                        help='combined dataset to backup and clean.',
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

    # create staging, sandbox, and clean datasets with descriptions and labels
    datasets = create_datasets(bq_client, args.combined_dataset,
                               args.release_tag)

    # copy raw data into staging dataset
    bq_client.copy_dataset(f'{bq_client.project}.{args.combined_dataset}',
                           f'{bq_client.project}.{datasets.get("staging")}')
    LOGGER.info(
        f'combined raw table COPY from `{args.combined_dataset}` to `{datasets.get("staging")}` is complete'
    )

    # clean the combined staging dataset
    cleaning_args = [
        '-p', args.curation_project_id, '-d',
        datasets.get('staging', 'UNSET'), '-b',
        datasets.get('sandbox',
                     'UNSET'), '--data_stage', 'combined', "--cutoff_date",
        args.cutoff_date, '--validation_dataset_id', args.validation_dataset_id,
        '--ehr_dataset_id', args.ehr_dataset_id, '--api_project_id',
        args.api_project_id, '--run_as', args.run_as_email, '-s'
    ]

    all_cleaning_args = add_kwargs_to_args(cleaning_args, kwargs)
    clean_cdr.main(args=all_cleaning_args)

    bq_client.build_and_copy_contents(datasets.get('staging', 'UNSET'),
                                      datasets.get('clean', 'UNSET'))

    # update sandbox description and labels
    sandbox_dataset = bq_client.get_dataset(datasets.get(
        'sandbox', 'UNSET'))  # Make an API request.
    sandbox_dataset.description = (
        f'Sandbox created for storing records affected by the cleaning '
        f'rules applied to {datasets.get("clean")}')
    sandbox_dataset.labels['phase'] = 'sandbox'
    sandbox_dataset = bq_client.update_dataset(
        sandbox_dataset, ["description"])  # Make an API request.

    LOGGER.info(
        f'Updated dataset `{sandbox_dataset.full_dataset_id}` with description `{sandbox_dataset.description}`'
    )
    LOGGER.info(
        f'Cleaning, `{bq_client.project}.{datasets.get("clean")}`, is complete.'
    )

    bq_client.copy_dataset(datasets.get("clean"), datasets.get("release"))
    LOGGER.info(
        f' Snapshotting `{datasets.get("clean")}` into {datasets.get("release")} is completed.'
    )


def create_datasets(client, combined_dataset, release_tag):
    combined_clean = f'{release_tag}_combined'
    combined_staging = f'{combined_clean}_staging'
    combined_sandbox = f'{combined_clean}_sandbox'
    combined_release = f'{combined_clean}_release'

    staging_desc = f'Intermediary dataset to apply cleaning rules on {combined_dataset}'
    labels = {
        "phase": "staging",
        "release_tag": release_tag,
        "de_identified": "false"
    }
    staging_dataset_object = client.define_dataset(combined_staging,
                                                   staging_desc, labels)
    client.create_dataset(staging_dataset_object)
    LOGGER.info(f'Created dataset `{client.project}.{combined_staging}`')

    sandbox_desc = (f'Sandbox created for storing records affected by the '
                    f'cleaning rules applied to {combined_staging}')
    labels["phase"] = "sandbox"
    sandbox_dataset_object = client.define_dataset(combined_sandbox,
                                                   sandbox_desc, labels)
    client.create_dataset(sandbox_dataset_object)
    LOGGER.info(f'Created dataset `{client.project}.{combined_sandbox}`')

    version = resources.get_git_tag()
    clean_desc = f'{version} Clean version of {combined_dataset}'
    labels["phase"] = "clean"
    clean_dataset_object = client.define_dataset(combined_clean, clean_desc,
                                                 labels)
    client.create_dataset(clean_dataset_object)
    LOGGER.info(f'Created dataset `{client.project}.{combined_clean}`')

    release_desc = f'{version} Release version of {combined_clean}'
    labels["phase"] = "release"
    release_dataset_object = client.define_dataset(combined_release,
                                                   release_desc, labels)
    client.create_dataset(release_dataset_object)
    LOGGER.info(f'Created dataset `{client.project}.{combined_release}`')

    return {
        'clean': combined_clean,
        'staging': combined_staging,
        'sandbox': combined_sandbox,
        'release': combined_release
    }


if __name__ == '__main__':
    main()
