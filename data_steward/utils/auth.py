"""
A BigQuery authentication utility module.

This module is not intended to be a stand alone script.  It's functions are
intended to be imported and used by other scripts.


"""
# Python imports
import logging
import warnings

# Third party imports
from google.auth import default, iam, impersonated_credentials
from google.auth.transport import requests
from google.oauth2 import service_account

TOKEN_URI = 'https://accounts.google.com/o/oauth2/token'
"""Maximum lifetime of an impersonated credential.  1 hour."""
IMPERSONATION_LIFETIME = 3600
"""Default scopes provide read only access."""
DEFAULT_SCOPES = [
    'https://www.googleapis.com/auth/devstorage.read_only',
    'https://www.googleapis.com/auth/bigquery.read_only',
]

LOGGER = logging.getLogger(__name__)


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


def get_impersonation_credentials(target_principal,
                                  target_scopes=None,
                                  key_file=None):
    """
    Get useful impersonation credentials.

    The target principal email address is required.  If a key_file path is not provided,
    attempts will be made to determine your valid credentials from the environment.

    Potential target scopes when using impersonation are:
        'https://www.googleapis.com/auth/bigquery',
        'https://www.googleapis.com/auth/devstorage.read_write',

    :param target_principal:  email address to impersonate.  typically, this is an
        email address associated with a google service account.
    :param target_scopes: a list of scopes that the impersonating account needs
        to perform requested actions.  if no list is provided, it defaults to a
        limited set of functionality.
    :param key_file: path to a json service account google key file.  If provided, this key
        file will be used to create source credentials.  If missing and the
        GOOGLE_APPLICATION_CREDENTIALS environment variable is set, that service
        account key file will be used.  If this environment variable is unset, your
        user account default application credentials will be used.  This usage
        is preferable.

    :returns: valid impersonated credentials
    """
    if not target_principal:
        raise RuntimeError(
            "Cannot impersonate.  No impersonation target specified.")

    if not target_scopes:
        LOGGER.warning(f"Using default scopes: {DEFAULT_SCOPES}")
        target_scopes = DEFAULT_SCOPES

    if not isinstance(target_scopes, (list, tuple)):
        LOGGER.warning(f"Target scopes requires an iterable.  Currently "
                       f"for lists and tuples.  Using "
                       f"default scopes: {DEFAULT_SCOPES}.")
        target_scopes = DEFAULT_SCOPES

    if key_file:
        try:
            creds = (service_account.Credentials.from_service_account_file(
                key_file, scopes=target_scopes))
        except (OSError, ValueError) as exc:
            LOGGER.exception(f"{key_file} is an invalid file path.")
            raise (exc)

        # if the wrong key file is provided, this will cause a RefreshError
        # when the credentials are used, not at creation.
        return impersonated_credentials.Credentials(
            source_credentials=creds,
            target_principal=target_principal,
            target_scopes=target_scopes,
            lifetime=IMPERSONATION_LIFETIME)

    else:
        with warnings.catch_warnings():
            # the library sends a UserWarning about end user credentials.
            # this context manager will ignore that one warning
            warnings.simplefilter("ignore", category=UserWarning)
            creds, _ = default()

        try:
            if creds.client_secret:
                LOGGER.info(
                    f"Using end user account to impersonate: {target_principal}"
                )

            target_credentials = impersonated_credentials.Credentials(
                source_credentials=creds,
                target_principal=target_principal,
                target_scopes=target_scopes,
                lifetime=IMPERSONATION_LIFETIME)

            # scopes must be reset when using end user credentials
            target_credentials._source_credentials._scopes = creds.scopes
            return target_credentials
        except AttributeError:
            if creds.service_account_email:
                LOGGER.info(
                    f"Using service account to impersonate: {target_principal}")

            return impersonated_credentials.Credentials(
                source_credentials=creds,
                target_principal=target_principal,
                target_scopes=target_scopes,
                lifetime=IMPERSONATION_LIFETIME)
        except (OSError, ValueError, TypeError, RuntimeError) as exc:
            LOGGER.exception("An unexpected error was encountered")
            raise (exc)
