# Python Imports
from google.cloud import bigquery
import logging
import os

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
    else:
        return bigquery.Client(project=project_id)


def query(q, project_id=None, use_cache=False):
    client = get_client(project_id)
    query_job_config = bigquery.job.QueryJobConfig(use_query_cache=use_cache)
    return client.query(q, job_config=query_job_config).to_dataframe()
