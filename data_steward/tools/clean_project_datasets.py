# Python imports
import argparse
import logging
import sys

# Third party imports
from googleapiclient.errors import HttpError

# Project imports
from utils import bq

logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")


def get_datasets_with_substrings(datasets_list, name_substrings):
    """
    Filters list of datasets with specified substrings (e.g. github usernames) in them

    :param datasets_list: list of dataset_ids
    :param name_substrings: identifies substrings that help identify datasets to delete
    :return: list of dataset_ids with any substring in their dataset_id
    """
    datasets_with_substrings = []
    for dataset in datasets_list:
        if any(name_substring in dataset for name_substring in name_substrings):
            datasets_with_substrings.append(dataset)
    return datasets_with_substrings


def delete_datasets(project_id, datasets_to_delete_list):
    """
    Deletes datasets using their dataset_ids

    :param project_id: identifies the project
    :param datasets_to_delete_list: list of dataset_ids to delete
    :return:
    """
    failed_to_delete = []
    for dataset in datasets_to_delete_list:
        try:
            bq.delete_dataset(project_id, dataset)
            logging.info(f'Deleted dataset {dataset}')
        except HttpError:
            logging.exception(f'Could not delete dataset {dataset}')
            failed_to_delete.append(dataset)
    logging.info(
        f'The following datasets could not be deleted: {failed_to_delete}')


def run_deletion(project_id, name_substrings):
    """
    Deletes datasets from project containing any of the name_substrings

    :param project_id: identifies the project
    :param name_substrings: Identifies substrings that help identify datasets to delete
    :return:
    """
    all_datasets = [
        dataset.dataset_id for dataset in bq.list_datasets(project_id)
    ]
    datasets_with_substrings = get_datasets_with_substrings(
        all_datasets, name_substrings)
    logging.info(f'Datasets marked for deletion: {datasets_with_substrings}')
    logging.info('Proceed?')
    response = get_response()
    if response == "Y":
        delete_datasets(project_id, datasets_with_substrings)
    elif response.lower() == "n":
        logging.info("Aborting deletion")


# Make sure user types Y to proceed
def get_response():
    """Return input from user denoting yes/no"""
    prompt_text = 'Please press Y/n\n'
    response = input(prompt_text)
    while response not in ('Y', 'n', 'N'):
        response = input(prompt_text)
    return response


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description=
        'Deletes datasets containing specific strings in the dataset_id.',
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('-p',
                        '--project_id',
                        action='store',
                        dest='project_id',
                        help='Identifies the project to delete datasets from',
                        required=True)
    parser.add_argument(
        '-n',
        '--name_substrings',
        nargs='+',
        dest='name_substrings',
        help='Identifies substrings that help identify datasets to delete. '
        'A dataset containing any of these substrings within in their dataset_id will be deleted. ',
        required=True)
    args = parser.parse_args()

    run_deletion(args.project_id, args.name_substrings)
