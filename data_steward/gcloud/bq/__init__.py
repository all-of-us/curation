"""
Interact with Google Cloud BigQuery
"""
# Python stl imports
import os

# Third-party imports
from google.cloud.bigquery import Client
from google.auth import default

# Project imports
from utils import auth


class BigQueryClient(Client):
    """
    A client that extends GCBQ functionality
    See https://googleapis.dev/python/bigquery/latest/generated/google.cloud.bigquery.client.Client.html#google.cloud.bigquery.client.Client
    """

    def __init__(self, project_id: str, scopes=None, credentials=None):
        """
        :param project_id: Identifies the project to create a cloud BigQuery client for
        :param scopes: List of Google scopes as strings
        :param credentials: Google credentials object (ignored if scopes is defined,
            uses delegated credentials instead)

        :return:  A BigQueryClient instance
        """
        if scopes:
            credentials, project_id = default()
            credentials = auth.delegated_credentials(credentials, scopes=scopes)
        super().__init__(project=project_id, credentials=credentials)
