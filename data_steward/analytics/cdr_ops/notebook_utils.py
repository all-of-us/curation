"""
Utility functions for notebooks
"""

from sqlalchemy import create_engine
from gcloud.gsm import SecretManager

IMPERSONATION_SCOPES = [
    'https://www.googleapis.com/auth/drive.readonly',
    'https://www.googleapis.com/auth/bigquery'
]

POSTGRES_USER_NAME = "pdr_psql_username"
POSTGRES_PASSWORD = "pdr_psql_password"


class KeyConfigurationError(RuntimeError):
    """
    Raised when the required Mandrill API key is not properly configured
    """

    def __init__(self, msg):
        super(KeyConfigurationError, self).__init__()
        self.msg = msg


def _get_secret_from_secret_manager(secret, project_id):
    """
    Get the token used to interact with the Mandrill API

    :raises:
      KeyConfigurationError: secret is not configured
    :return: configured Mandrill API key as str
    """
    smc = SecretManager()
    secret = smc.access_secret_version(
        request={'name': smc.build_secret_full_name(secret, project_id)})
    if not secret:
        raise KeyConfigurationError(
            f"Secret: `{secret}` is not set in secret manager")
    return secret.payload.data.decode("UTF-8")


def execute(client, query, max_rows=False):
    """
    Execute a bigquery command and return the results in a dataframe

    :param client: an instantiated bigquery client object
    :param query: the query to execute
    :param max_rows: Boolean option to manually turn on max rows display(default -> false)
    :return pandas dataframe object
    """
    import pandas as pd
    print(query)

    res = client.query(query).to_dataframe()
    if max_rows:
        pd.set_option('display.max_rows', res.shape[0] + 1)
    return res


def pdr_client(project_id):
    """
    Gets the username and password for postgres instance and creates a connection
    which can be used to query the views in PDR project.
s
    NOTE: This expects cloud_sql_proxy is already installed in the google-cloud-sdk
    """
    username = _get_secret_from_secret_manager(POSTGRES_USER_NAME, project_id)
    password = _get_secret_from_secret_manager(POSTGRES_PASSWORD, project_id)
    port = 7005

    db_conn = create_engine(
        f'postgresql://{username}:{password}@localhost:{port}/drc')
    return db_conn


def execute(client, query, max_rows=False):
    """
    Execute a bigquery command and return the results in a dataframe

    :param client: an instantiated bigquery client object
    :param query: the query to execute
    :param max_rows: Boolean option to manually turn on max rows display(default -> false)
    :return pandas dataframe object
    """
    import pandas as pd
    print(query)

    res = client.query(query).to_dataframe()
    if max_rows:
        pd.set_option('display.max_rows', res.shape[0] + 1)
    return res
