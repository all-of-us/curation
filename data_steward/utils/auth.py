# Python imports
import requests as req

# Third party imports
from google.auth import iam
from google.auth.transport import requests
from google.oauth2 import service_account

METADATA_URL = 'http://metadata.google.internal/computeMetadata/v1/'
METADATA_HEADERS = {'Metadata-Flavor': 'Google'}
SERVICE_ACCOUNT = 'default'
TOKEN_URI = 'https://accounts.google.com/o/oauth2/token'


def get_access_token(scopes):
    """
    Retrieves an access_token in App Engine for the default service account

    Source: https://cloud.google.com/functions/docs/securing/function-identity#gcloud
    and https://cloud.google.com/compute/docs/access/create-enable-service-accounts-for-instances#applications
    :param scopes: List of Google scopes as strings
    :return: access token as string to be used in a request header as
        headers = {'Authorization': f'Bearer {access_token}'}
    """
    scopes_str = ','.join(scopes)
    url = f'{METADATA_URL}instance/service-accounts/{SERVICE_ACCOUNT}/token?scopes={scopes_str}'
    # Request an access token from the metadata server.
    r = req.get(url, headers=METADATA_HEADERS)
    r.raise_for_status()
    # Extract the access token from the response.
    access_token = r.json()['access_token']
    return access_token


def delegated_credentials(credentials, subject, scopes):
    """
    Generate credentials with scopes. This needs the 'SA Token Creator' role for the SA

    Source: https://stackoverflow.com/a/57092533
    :param credentials: Credentials object to add scopes/subject to
    :param subject: Subject to add to credentials
    :param scopes: Scopes to add to credentials
    :return: Updated credentials object with access token for scopes
    """
    try:
        # If using service account credentials from json file
        updated_credentials = credentials.with_subject(subject).with_scopes(
            scopes)
    except AttributeError:
        # If using GCE/GAE default credentials
        request = requests.Request()

        # Refresh the default credentials. This ensures that the information
        # about this account, notably the email, is populated.
        credentials.refresh(request)

        # Create an IAM signer using the default credentials.
        signer = iam.Signer(request, credentials,
                            credentials.service_account_email)

        # Create OAuth 2.0 Service Account credentials using the IAM-based
        # signer and the bootstrap_credential's service account email
        updated_credentials = service_account.Credentials(
            signer,
            credentials.service_account_email,
            TOKEN_URI,
            scopes=scopes,
            subject=subject)

    return updated_credentials
