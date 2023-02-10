"""
Use this script for dropping certain participants from datasets.
It creates new datasets and run retraction on them. The original datasets will
not be modified.

You can use this script for retraction for the following datasets:
    - RDR dataset
    - EHR dataset
    - UNIONED EHR dataset
    - COMBINED / COMBINED RELEASE datasets
    - [CT|RT] DEID / DEID BASE / DEID CLEAN datasets
    - [CT|RT] FITBIT datasets

You cannot use this script for retraction for the following datasets.
You need to re-run the dataset creation script after source datasets are retracted:
    - [CT] antibody quest dataset

Original Issues: DC-2801, DC-2865
"""

# Python imports
from argparse import ArgumentParser
import logging
import re

# Third party imports
from google.cloud.exceptions import Conflict

# Project imports
from cdr_cleaner import clean_cdr
from cdr_cleaner.args_parser import add_kwargs_to_args
from common import CDR_SCOPES
from constants.cdr_cleaner.clean_cdr import DATA_CONSISTENCY
from gcloud.bq import BigQueryClient
from resources import ask_if_continue, get_new_dataset_name
from retraction.retract_data_bq import run_bq_retraction, RETRACTION_ONLY_EHR, RETRACTION_RDR_EHR
from retraction.retract_utils import is_fitbit_dataset
from utils import pipeline_logging
from utils.auth import get_impersonation_credentials
from utils.parameter_validators import validate_release_tag_param

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
        dataset_name,
        f"Certain participants removed from {src_dataset_name} based on the retraction rule. \n"
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


def create_lookup_table(client: BigQueryClient, sql_file_path: str) -> str:
    """
    Run 'create table' from a local file and create a lookup table for retraction.
    The local 'create table' file needs to meet the following criteria:
        1. It's written for creating a table in BigQuery. The statement can start by
           'CREATE TABLE', 'CREATE TABLE IF NOT EXISTS', or 'CREATE OR REPLACE TABLE'
        2. Only one statement is written in the file
        3. The table is fully qualified and hard-coded
        4. Dataset name is f"{release_tag}_sandbox"
        5. The table contains 'person_id' and 'research_id' columns
    You can see the sample SQL file in the JIRA page for DC-2806.
    Args:
        client: BigQueryClient
        sql_file_path: Path of the SQL file you store locally.
    Returns: Name of the created lookup table
    """
    with open(sql_file_path, 'r') as f:
        create_statement = f.read()

    LOGGER.info(f"Running the following SQL: \n{create_statement}\n")

    job = client.query(create_statement)
    job.result()

    # Read the 'create table' statement and get the table name.
    regex_created_table = re.search(
        r"create( +or +replace)?[ ]+table( +if +not +exists)?[ ]+`?[a-z0-9-_]*.[a-z0-9-_]*.([a-z0-9-_]*)`?",
        create_statement, re.IGNORECASE)
    lookup_table_name = regex_created_table.group(3)

    LOGGER.info(
        f"Created the lookup table {lookup_table_name} in the sandbox dataset.")
    return lookup_table_name


def parse_args(raw_args=None):
    parser = ArgumentParser(
        description='Retraction script for BigQuery datasets')

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
    parser.add_argument(
        '--source_datasets',
        nargs='+',
        action='store',
        dest='source_datasets',
        help=('Datasets that need retraction. If there are more than one, '
              'seperate them by whitespaces.'),
        required=True)
    parser.add_argument(
        '--new_release_tag',
        action='store',
        dest='new_release_tag',
        required=True,
        type=validate_release_tag_param,
        help='Release tag for the new datasets after retraction.')
    parser.add_argument(
        '--lookup_creation_sql_file_path',
        action='store',
        dest='lookup_creation_sql_file_path',
        required=True,
        help='Path of the SQL file that has DDL for the lookup table.')
    parser.add_argument('-l',
                        '--console_log',
                        dest='console_log',
                        action='store_true',
                        required=False,
                        help='Log to the console as well as to a file.')
    parser.add_argument(
        '-i',
        '--hpo_id',
        action='store',
        dest='hpo_id',
        help=('Identifies the site to retract data from. '
              'Specify this argument when retracting from EHR dataset. '
              'This argument is effective only for EHR dataset. '
              'For other datasets, it gets ignored.'),
        required=False)
    parser.add_argument(
        '-r',
        '--retraction_type',
        action='store',
        dest='retraction_type',
        help=(
            f'Identifies whether all data needs to be removed, including RDR, '
            f'or if RDR data needs to be kept intact. Can take the values '
            f'"{RETRACTION_RDR_EHR}" or "{RETRACTION_ONLY_EHR}".'),
        required=True)
    parser.add_argument(
        '--skip_sandboxing',
        dest='skip_sandboxing',
        action='store_true',
        required=False,
        help=
        'Specify this option if you do not want this script to sanbox the retracted records.'
    )

    return parser.parse_args(raw_args)


def main():
    """
    Read through LOGGER.info() messages for what it does.
    """
    args = parse_args()

    tag: str = args.new_release_tag
    sql_file_path: str = args.lookup_creation_sql_file_path
    project_id: str = args.project_id

    datasets: list = args.source_datasets
    new_datasets: list = [
        get_new_dataset_name(dataset, tag) for dataset in datasets
    ]
    sb_dataset: str = f"{tag}_sandbox"

    pipeline_logging.configure(level=logging.INFO,
                               add_console_handler=args.console_log)

    impersonation_creds = get_impersonation_credentials(args.run_as_email,
                                                        CDR_SCOPES)
    client = BigQueryClient(args.project_id, credentials=impersonation_creds)

    LOGGER.info(
        f"Starting retraction.\n"
        f"This script will copy the following datasets to the new datasets with "
        f"the new release tag {tag}, and run retraction on the new datasets. \n"
        f"\n"
        f"Source datasets: {', '.join(datasets)}. \n"
        f"New datasets: {', '.join(new_datasets)}. \n"
        f"\n"
        f"No change will be made to the source datasets, and all the retractions "
        f"will be sandboxed to {sb_dataset}.\n"
        f"\n"
        f"Steps:\n"
        f"[1/6] Create empty new datasets if not exist\n"
        f"[2/6] Copy data from the source datasets to the new datasets\n"
        f"[3/6] Create an empty sandbox dataset if not exists. This sandbox dataset will be shared by all the new datasets\n"
        f"[4/6] Create a lookup table that has PIDs/RIDs to remove\n"
        f"[5/6] Run retraction on the new datasets\n"
        f"[6/6] Run cleaning rules on the new datasets\n"
        f"\n"
        f"If you answer 'Y', [1/6] will start. \n"
        f"You will need to answer 'Y' after each step completes.\n")

    LOGGER.info(f"Starting [1/6] Create empty new datasets if not exist.")
    ask_if_continue()
    for dataset in datasets:
        create_dataset(client, dataset, tag)
    LOGGER.info(f"Completed [1/6] Create empty new datasets if not exist.\n")

    LOGGER.info(f"Starting [2/6] Copy data to the new datasets.")
    ask_if_continue()
    for dataset in datasets:
        copy_dataset(client, dataset, tag)
    LOGGER.info(f"Completed [2/6] Copy data to the new datasets.\n")

    LOGGER.info(
        f"Starting [3/6] Create an empty sandbox dataset if not exists.")
    ask_if_continue()
    create_sandbox_dataset(client, tag)
    LOGGER.info(
        f"Completed [3/6] Completed an empty sandbox dataset if not exists.\n")

    LOGGER.info(
        f"Starting [4/6] Create a lookup table that has PIDs/RIDs to remove.")
    ask_if_continue()
    lookup_table = create_lookup_table(client, sql_file_path)
    LOGGER.info(
        f"Completed [4/6] Create a lookup table that has PIDs/RIDs to remove.\n"
    )

    LOGGER.info(f"Starting [5/6] Run retraction on the new datasets.")
    ask_if_continue()
    run_bq_retraction(project_id,
                      sb_dataset,
                      lookup_table,
                      args.hpo_id,
                      new_datasets,
                      args.retraction_type,
                      skip_sandboxing=args.skip_sandboxing,
                      bq_client=client)
    LOGGER.info(f"Completed [5/6] Run retraction on the new datasets.\n")

    LOGGER.info(f"Starting [6/6] Run cleaning rules on the new datasets.")
    ask_if_continue()
    for dataset in new_datasets:
        if is_fitbit_dataset(dataset):
            LOGGER.info(
                f"Skipping running CR for {dataset} since it's a fitbit dataset."
            )
            continue
        LOGGER.info(f"Running CRs for {dataset}...")
        cleaning_args = [
            '-p', project_id, '-d', dataset, '-b', sb_dataset, '--data_stage',
            DATA_CONSISTENCY, '--run_as', args.run_as_email, '-s'
        ]
        all_cleaning_args = add_kwargs_to_args(cleaning_args, None)
        clean_cdr.main(args=all_cleaning_args)
        LOGGER.info(f"Completed running CRs for {dataset}...")
    LOGGER.info(f"Completed [6/6] Run cleaning rules on the new datasets.\n")

    LOGGER.info(f"Retraction completed.\n")


if __name__ == '__main__':
    main()
