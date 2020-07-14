# Python imports
import requests

# Third party imports

# Project imports

METADATA_URL = 'http://metadata.google.internal/computeMetadata/v1/'
METADATA_HEADERS = {'Metadata-Flavor': 'Google'}
SERVICE_ACCOUNT = 'default'


def get_access_token(scopes):
    """
    Retrieves an access_token in App Engine for the default service account

    :param scopes: List of Google scopes as strings
    :return: access token as string
    """
    url = f'{METADATA_URL}instance/service-accounts/{SERVICE_ACCOUNT}/token?scopes={scopes}'
    # Request an access token from the metadata server.
    r = requests.get(url, headers=METADATA_HEADERS)
    r.raise_for_status()
    # Extract the access token from the response.
    access_token = r.json()['access_token']
    return access_token
