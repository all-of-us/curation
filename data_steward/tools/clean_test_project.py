# Python imports
import logging
import sys

# Third party imports
from googleapiclient.errors import HttpError

# Project imports
import app_identity
import bq_utils

logging.basicConfig(
    stream=sys.stdout,
    level=logging.DEBUG,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

GITHUB_USERNAMES = []


def get_circle_datasets(datasets_list):
    """
    Filters list of datasets with github usernames in them

    :param datasets_list: list of dataset_ids
    :return: list of dataset_ids with github usernames
    """
    circle_datasets = []
    for dataset in datasets_list:
        if any(gh_username in dataset for gh_username in GITHUB_USERNAMES):
            circle_datasets.append(dataset)
    return circle_datasets


def delete_datasets(project_id, datasets_to_delete_list):
    """
    Deletes datasets using their dataset_ids

    :param project_id: identifies the project
    :param datasets_to_delete_list: list of dataset_ids to delete
    :return: 
    """
    deleted, not_deleted = 0, 0
    for dataset in datasets_to_delete_list:
        try:
            bq_utils.delete_dataset(project_id, dataset)
            deleted += 1
            logging.info('Deleted dataset %s' % dataset)
        except HttpError:
            logging.info('Could not delete dataset %s' % dataset)
            not_deleted += 1
    logging.info('Deleted %s datasets, failed to delete %s datasets' %
                 (deleted, not_deleted))


def run_deletion(project_id):
    """
    Retrieves all datasets in project,
    filters the list based on criteria (github username to identify circle datasets in this case)
    deletes the filtered datasets

    :param project_id: identifies the project
    :return: 
    """
    all_dataset_objs = bq_utils.list_datasets(project_id)
    all_datasets = [
        bq_utils.get_dataset_id_from_obj(dataset_obj)
        for dataset_obj in all_dataset_objs
    ]
    circle_datasets = get_circle_datasets(all_datasets)
    delete_datasets(project_id, circle_datasets)


if __name__ == '__main__':
    project_id = app_identity.get_application_id()
    run_deletion(project_id)
