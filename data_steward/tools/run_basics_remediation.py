"""
This is a wrapper script to complete hot-fix for DC-3016.

You can use this script for remediation for the following datasets:
    - RDR dataset
    - COMBINED / COMBINED RELEASE datasets
    - [CT|RT] DEID / DEID BASE / DEID CLEAN datasets

Original Issues: DC-3016 and its subtasks.
"""

# Python imports
from argparse import ArgumentParser
import logging

# Third party imports
from google.cloud.exceptions import Conflict

# Project imports
from cdr_cleaner import clean_cdr
from cdr_cleaner.args_parser import add_kwargs_to_args
from cdr_cleaner.clean_cdr_engine import clean_dataset
from cdr_cleaner.manual_cleaning_rules.remediate_basics import RemediateBasics
from common import CDR_SCOPES
from constants.cdr_cleaner.clean_cdr import DATA_CONSISTENCY
from gcloud.bq import BigQueryClient
from resources import ask_if_continue, get_new_dataset_name
from retraction.retract_utils import is_combined_dataset, is_deid_dataset
from utils import pipeline_logging
from utils.auth import get_impersonation_credentials

LOGGER = logging.getLogger(__name__)


def create_dataset(client: BigQueryClient, src_dataset_name: str,
                   release_tag: str) -> None:
    """
    Create an empty new dataset based on the source dataset name and the new
    release tag. Definition for the new dataset is copied and updated from the
    old dataset.
    If the new dataset already exists, it skips creating and updating the new
    dataset and its definition.
    Args:
        client: BigQueryClient
        src_dataset_name: Name of the source dataset.
        release_tag: Release tag for the new datasets.
    """
    dataset_name = get_new_dataset_name(src_dataset_name, release_tag)

    LOGGER.info(
        f"Creating an empty dataset {dataset_name} from {src_dataset_name}.")

    src_dataset_obj = client.get_dataset(src_dataset_name)
    src_desc, src_labels = src_dataset_obj.description, src_dataset_obj.labels

    labels: dict = src_labels
    labels["release_tag"] = release_tag

    dataset_obj = client.define_dataset(
        dataset_name, f"Hot fix applied to {src_dataset_name}. \n"
        f"-- \n{src_dataset_name}'s description for reference -> {src_desc}",
        labels)

    try:
        client.create_dataset(dataset_obj, exists_ok=False)
        LOGGER.info(f"Created empty dataset `{client.project}.{dataset_name}`")
    except Conflict:
        LOGGER.info(
            f"The dataset `{client.project}.{dataset_name}` already exists. Skipping the creation."
        )


def copy_dataset(client: BigQueryClient, src_dataset_name, release_tag) -> None:
    """
    Copy data from the source dataset to the new dataset.
    Args:
        client: BigQueryClient
        src_dataset_name: Name of the source dataset.
        release_tag: Release tag for the new datasets.
    """
    dataset_name = get_new_dataset_name(src_dataset_name, release_tag)

    LOGGER.info(f"Copying data from {src_dataset_name} to {dataset_name}.")

    client.copy_dataset(f'{client.project}.{src_dataset_name}',
                        f'{client.project}.{dataset_name}')

    LOGGER.info(f"Copied data from {src_dataset_name} to {dataset_name}.")


def create_sandbox_dataset(client: BigQueryClient, release_tag) -> None:
    """
    Create an empty new sandbox dataset for retraction. This sandbox dataset
    will be shared by all the new datasets. If the sandbox dataset already
    exists, it skips creating and updating the new dataset and its definition.
    Args:
        client: BigQueryClient
        release_tag: Release tag for the new datasets.
    """
    sb_dataset_name = f"{release_tag}_sandbox"

    dataset_obj = client.define_dataset(
        sb_dataset_name,
        f'Sandbox created for storing records affected by retraction for {release_tag}.',
        {
            "phase": "sandbox",
            "release_tag": release_tag,
            "de_identified": "false"
        })

    try:
        client.create_dataset(dataset_obj, exists_ok=False)
        LOGGER.info(f"Created dataset `{client.project}.{sb_dataset_name}`")
    except Conflict:
        LOGGER.info(
            f"The dataset `{client.project}.{sb_dataset_name}` already exists. Skipping the creation."
        )


def parse_args(raw_args=None):

    parser = ArgumentParser(description='Remediation script for hot-fix')

    parser.add_argument('--run_as',
                        action='store',
                        dest='run_as_email',
                        help='Service account email address to impersonate',
                        required=True)
    parser.add_argument('--project_id',
                        action='store',
                        dest='project_id',
                        help='Curation project ID for running retraction.',
                        required=True)
    parser.add_argument('--source_dataset_id',
                        action='store',
                        dest='source_dataset_id',
                        help=('Dataset that needs remediation.'),
                        required=True)
    parser.add_argument(
        '--incremental_dataset_id',
        action='store',
        dest='incremental_dataset_id',
        help=(
            'Dataset that needs to be loaded together with source_dataset_id.'),
        required=True)
    parser.add_argument(
        '--dataset_with_largest_observation_id',
        action='store',
        dest='dataset_with_largest_observation_id',
        help=(
            'Dataset that has the largest observation_id among all the datasets.'
        ),
        required=True)
    parser.add_argument(
        '--new_release_tag',
        action='store',
        dest='new_release_tag',
        required=True,
        help='Release tag for the new datasets after remediation.')
    parser.add_argument('-l',
                        '--console_log',
                        dest='console_log',
                        action='store_true',
                        required=False,
                        help='Log to the console as well as to a file.')

    return parser.parse_args(raw_args)


def main():
    """
    Read through LOGGER.info() messages for what it does.
    """
    args = parse_args()

    tag: str = args.new_release_tag
    project_id: str = args.project_id

    dataset: str = args.source_dataset_id
    new_dataset: str = get_new_dataset_name(dataset, tag)
    sb_dataset: str = f"{tag}_sandbox"

    pipeline_logging.configure(level=logging.INFO,
                               add_console_handler=args.console_log)

    impersonation_creds = get_impersonation_credentials(args.run_as_email,
                                                        CDR_SCOPES)
    client = BigQueryClient(args.project_id, credentials=impersonation_creds)

    LOGGER.info(
        f"Starting the basics remediation.\n"
        f"This script will copy the following dataset to the new dataset with "
        f"the new release tag {tag}, and run remediation on the new dataset. \n"
        f"\n"
        f"Source dataset: {dataset}. \n"
        f"New dataset: {new_dataset}. \n"
        f"\n"
        f"No change will be made to the source datasets, and all the remediation "
        f"will be sandboxed to {sb_dataset}.\n"
        f"\n"
        f"Steps:\n"
        f"[1/5] Create the empty new dataset if not exist\n"
        f"[2/5] Copy data from the source dataset to the new dataset\n"
        f"[3/5] Create an empty sandbox dataset if not exists\n"
        f"[4/5] Run remediation and cleaning rules on the new dataset\n"
        f"-- Pause execution and run validation notebook --\n"
        f"[5/5] Run cleaning rules on the new datasets\n"
        f"\n"
        f"If you answer 'Y', [1/5] will start. \n"
        f"You will need to answer 'Y' after each step completes.\n")

    LOGGER.info(f"Starting [1/5] Create the empty new dataset if not exist.")
    ask_if_continue()
    create_dataset(client, dataset, tag)
    LOGGER.info(f"Completed [1/5] Create the empty new dataset if not exist.\n")

    LOGGER.info(f"Starting [2/5] Copy data to the new dataset.")
    ask_if_continue()
    copy_dataset(client, dataset, tag)
    LOGGER.info(f"Completed [2/5] Copy data to the new dataset.\n")

    LOGGER.info(
        f"Starting [3/5] Create an empty sandbox dataset if not exists.")
    ask_if_continue()
    create_sandbox_dataset(client, tag)
    LOGGER.info(
        f"Completed [3/5] Completed an empty sandbox dataset if not exists.\n")

    LOGGER.info(
        f"Starting [4/5] Run remediation and cleaning rules on the new dataset."
    )
    ask_if_continue()

    LOGGER.info(f"Running remediation for {new_dataset}...")
    clean_dataset(project_id,
                  new_dataset,
                  f"{tag}_sandbox", [(RemediateBasics,)],
                  incremental_dataset_id=args.incremental_dataset_id,
                  dataset_with_largest_observation_id=args.
                  dataset_with_largest_observation_id)

    LOGGER.info(f"Completed running remediation for {new_dataset}...")

    LOGGER.info(
        f"[4/5] Run remediation and cleaning rules on the new datasets completed.\n"
    )

    LOGGER.info(
        "-- Stop for now and run validation notebook. --\n"
        "-- Once validation is completed, type 'Y' and proceed to 5. --\n")
    ask_if_continue()

    LOGGER.info(f"Starting [5/5] Run cleaning rules on {new_dataset}.")
    ask_if_continue()
    cleaning_args = [
        '-p', project_id, '-d', new_dataset, '-b', sb_dataset, '--data_stage',
        DATA_CONSISTENCY, '--run_as', args.run_as_email, '-s'
    ]
    all_cleaning_args = add_kwargs_to_args(cleaning_args, None)
    clean_cdr.main(args=all_cleaning_args)
    LOGGER.info(f"Completed [5/5] Run cleaning rules on {new_dataset}.\n")

    LOGGER.info(f"Remediation completed.\n")


if __name__ == '__main__':
    main()
