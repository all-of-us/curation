# Python Imports
import logging
import os

# Third-party imports
from google.cloud import bigquery

# Project Imports
from app_identity import GOOGLE_CLOUD_PROJECT


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


def delete_dataset(project_id,
                   dataset_id,
                   delete_contents=True,
                   not_found_ok=True):
    """
    Delete a dataset in a project

    :param project_id: Identifies the project the containing the dataset
    :param dataset_id: Identifies the dataset to delete
    :param delete_contents: If set True, deletes all contents within the dataset
    :param not_found_ok: If set True, does not raise error if dataset cannot be found
    :return:
    """
    client = get_client(project_id)
    client.delete_dataset(dataset_id,
                          delete_contents=delete_contents,
                          not_found_ok=not_found_ok)
    logging.info(f'Deleted dataset {project_id}.{dataset_id}')
