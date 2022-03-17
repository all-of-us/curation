"""
Utility functions for notebooks related to CDR generation
"""

IMPERSONATION_SCOPES = [
    'https://www.googleapis.com/auth/drive.readonly',
    'https://www.googleapis.com/auth/bigquery'
]


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
