"""

This script is not a part of any of the datastages' CR.
This script is used on-demand.

TODO more details

Original Issues: DC-2801
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
from gcloud.bq import BigQueryClient
from retraction.retract_data_bq import run_bq_retraction, RETRACTION_EHR, RETRACTION_RDR_EHR
from utils import pipeline_logging
from utils.auth import get_impersonation_credentials

LOGGER = logging.getLogger(__name__)


def ask_if_continue():
    """abc
    """
    confirm = input("\nContinue? [Y/N]:\n\n")
    if confirm != 'Y':
        raise RuntimeError('User canceled the execution.')


def get_new_dataset_name(src_dataset_name, release_tag):
    """abc
    """
    return re.sub(r'\d{4}q\dr\d', release_tag, src_dataset_name)


def create_dataset(client: BigQueryClient, src_dataset_name, release_tag):
    """abc
    """
    dataset_name = get_new_dataset_name(src_dataset_name, release_tag)

    LOGGER.info(
        f"Creating an empty dataset {dataset_name} for {src_dataset_name}.")

    src_dataset_obj = client.get_dataset(src_dataset_name)
    src_desc, src_labels = src_dataset_obj.description, src_dataset_obj.labels

    dataset_obj = client.define_dataset(
        dataset_name,
        f"Certain participants removed from {src_dataset_name} based on the AoU's decision. \n"
        f"{src_dataset_name}'s description for reference: {src_desc}", {
            "phase": src_labels["phase"],
            "release_tag": release_tag,
            "de_identified": src_labels["de_identified"]
        })

    try:
        client.create_dataset(dataset_obj, exists_ok=False)
        LOGGER.info(f"Created empty dataset `{client.project}.{dataset_name}`")
    except Conflict:
        LOGGER.info(
            f"The dataset `{client.project}.{dataset_name}` already exists. Skipping the creation."
        )


def copy_dataset(client: BigQueryClient, src_dataset_name, release_tag):
    """abc
    """
    dataset_name = get_new_dataset_name(src_dataset_name, release_tag)

    LOGGER.info(f"Copying data from {src_dataset_name} to {dataset_name}.")

    client.copy_dataset(f'{client.project}.{src_dataset_name}',
                        f'{client.project}.{dataset_name}')

    LOGGER.info(f"Copied data from {src_dataset_name} to {dataset_name}.")


def create_sandbox_dataset(client: BigQueryClient, release_tag):
    """abc
    """
    sb_dataset_name = f"{release_tag}_sandbox"

    dataset_obj = client.define_dataset(
        sb_dataset_name,
        f'Sandbox created for storing records affected by retraction applied to {release_tag}.',
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
            f"The dataset `{client.project}.{sb_dataset_name}` already exists. "
        )


def create_lookup_table(client: BigQueryClient, sql_file_path):
    """abc
    """
    with open(sql_file_path, 'r') as f:
        create_statement = f.read()

    LOGGER.info(f"Running the following SQL: \n{create_statement}")

    job = client.query(create_statement)
    job.result()

    return re.search(
        r"create( or replace)?[ ]+table[ ]+`?[a-z0-9-_]*.[a-z0-9-_]*.([a-z0-9-_]*)`?",
        create_statement, re.IGNORECASE).group(2)


def parse_args(raw_args=None):
    parser = ArgumentParser(description='abc')

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
    parser.add_argument('--source_datasets',
                        nargs='+',
                        action='store',
                        dest='source_datasets',
                        help='abc.',
                        required=True)
    parser.add_argument('--new_release_tag',
                        action='store',
                        dest='new_release_tag',
                        required=True,
                        help='abc.')
    parser.add_argument('--lookup_creation_sql_file_path',
                        action='store',
                        dest='lookup_creation_sql_file_path',
                        required=True,
                        help='abc.')
    parser.add_argument('-l',
                        '--console_log',
                        dest='console_log',
                        action='store_true',
                        required=False,
                        help='Log to the console as well as to a file.')
    parser.add_argument('-i',
                        '--hpo_id',
                        action='store',
                        dest='hpo_id',
                        help='Identifies the site to retract data from',
                        required=True)
    parser.add_argument(
        '-r',
        '--retraction_type',
        action='store',
        dest='retraction_type',
        help=(
            f'Identifies whether all data needs to be removed, including RDR, '
            f'or if RDR data needs to be kept intact. Can take the values '
            f'"{RETRACTION_RDR_EHR}" or "{RETRACTION_EHR}"'),
        required=True)

    return parser.parse_args(raw_args)


def main():
    """pass
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
        f"[3/6] Create an empty sandbox datasets if not exist. This sandbox dataset will be shared by all the new datasets\n"
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
    LOGGER.info(f"Completed [1/6] Create empty new datasets if not exist.")

    LOGGER.info(f"Starting [2/6] Copy data to the new datasets.")
    ask_if_continue()
    for dataset in datasets:
        copy_dataset(client, dataset, tag)
    LOGGER.info(f"Completed [2/6] Copy data to the new datasets.")

    LOGGER.info(
        f"Starting [3/6] Create an empty sandbox datasets if not exist.")
    ask_if_continue()
    create_sandbox_dataset(client, tag)
    LOGGER.info(
        f"Completed [3/6] Completed an empty sandbox datasets if not exist.")

    LOGGER.info(
        f"Starting [4/6] Create a lookup table that has PIDs/RIDs to remove.")
    ask_if_continue()
    lookup_table = create_lookup_table(client, sql_file_path)
    LOGGER.info(
        f"Completed [4/6] Create a lookup table that has PIDs/RIDs to remove.")

    LOGGER.info(f"Starting [5/6] Run retraction on the new datasets.")
    ask_if_continue()
    # TODO add sandbox option to this.
    run_bq_retraction(project_id, sb_dataset, project_id, lookup_table,
                      args.hpo_id, new_datasets, args.retraction_type)
    LOGGER.info(f"Completed [5/6] Run retraction on the new datasets.")

    LOGGER.info(f"Starting [6/6] Run cleaning rules on the new datasets.")
    ask_if_continue()
    for dataset in new_datasets:
        cleaning_args = [
            '-p', project_id, '-d', dataset, '-b', sb_dataset, '--data_stage',
            'retraction', '--run_as', args.run_as_email, '-s'
        ]
        all_cleaning_args = add_kwargs_to_args(cleaning_args, None)
        clean_cdr.main(args=all_cleaning_args)
    LOGGER.info(f"Completed [6/6] Run cleaning rules on the new datasets.")


if __name__ == '__main__':
    main()
