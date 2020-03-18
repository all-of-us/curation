# Python Imports
import logging
import os

# Third-party imports
from google.cloud import bigquery

# Project Imports
from app_identity import GOOGLE_CLOUD_PROJECT
from constants.utils import bq as bq_consts


def get_client(project_id=None):
    """
    Get a client for a specified project.
    """
    if project_id is None:
        logging.info(f'You should specify project_id for a reliable experience.'
                     f'Defaulting to {os.environ.get(GOOGLE_CLOUD_PROJECT)}.')
        return bigquery.Client()
    return bigquery.Client(project=project_id)


def query(q, project_id=None, use_cache=False):
    client = get_client(project_id)
    query_job_config = bigquery.job.QueryJobConfig(use_query_cache=use_cache)
    return client.query(q, job_config=query_job_config).to_dataframe()


def list_datasets(project_id):
    """
    Lists all datasets existing in a project.

    :return: List of dataset objects
    """
    client = get_client(project_id)
    datasets = list(client.list_datasets())
    return datasets


def get_dataset(project_id, dataset_id):
    """
    Return the dataset object
    :param project_id: 
    :param dataset_id: 
    :return: the dataset object
    """
    client = get_client(project_id)
    return client.get_dataset(dataset_id)


def delete_dataset(project_id,
                   dataset_id,
                   delete_contents=True,
                   not_found_ok=True):
    """
    Delete a dataset in a project. Delete all contents and ignore not found error by default

    :param project_id: Identifies the project the containing the dataset
    :param dataset_id: Identifies the dataset to delete
    :param delete_contents: If set True, deletes all contents within the dataset
            Defaults to True
    :param not_found_ok: If set True, does not raise error if dataset cannot be found
            Defaults to True
    :return:
    """
    client = get_client(project_id)
    client.delete_dataset(dataset_id,
                          delete_contents=delete_contents,
                          not_found_ok=not_found_ok)
    logging.info(f'Deleted dataset {project_id}.{dataset_id}')


def is_validation_dataset_id(dataset_id):
    """
    Check if  bq_consts.VALIDATION_PREFIX is in the dataset_id
    :param dataset_id: 
    :return: a bool indicating whether dataset is a validation_dataset
    """
    return bq_consts.VALIDATION_PREFIX in dataset_id


def get_latest_validation_dataset_id(project_id):
    """
    Get the latest validation_dataset_id based on most recent creationTime. 
    :param project_id: 
    :return: the most recent validatioN_dataset_id
    """

    dataset_id = os.environ.get(bq_consts.MATCH_DATASET, bq_consts.BLANK)
    if dataset_id == bq_consts.BLANK:
        validation_datasets = []
        for dataset in list_datasets(project_id):
            dataset_id = dataset.dataset_id
            if is_validation_dataset_id(dataset_id):
                dataset = get_dataset(project_id, dataset_id)
                validation_datasets.append((dataset.created, dataset_id))

        if validation_datasets:
            return sorted(validation_datasets, key=lambda x: x[0],
                          reverse=True)[0][1]
    return None
