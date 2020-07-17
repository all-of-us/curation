# Third party imports
from google.auth import iam
from google.auth.transport import requests
from google.oauth2 import service_account

TOKEN_URI = 'https://accounts.google.com/o/oauth2/token'


def delegated_credentials(credentials, scopes, subject=None):
    """
    Generate scoped credentials. This needs the 'SA Token Creator' role for the SA

    Source: https://stackoverflow.com/a/57092533
    :param credentials: Credentials object to add scopes/subject to
    :param scopes: Scopes to add to credentials
    :param subject: Subject to add to credentials
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
