"""
Utility functions for notebooks related to CDR generation
"""

IMPERSONATION_SCOPES = [
    'https://www.googleapis.com/auth/drive.readonly',
    'https://www.googleapis.com/auth/bigquery'
]


def execute(client, query):
    """
    Execute a bigquery command and return the results in a dataframe

    :param client: an instantiated bigquery client object
    :param query: the query to execute
    :return pandas dataframe object
    """
    print(query)
    return client.query(query).to_dataframe()
