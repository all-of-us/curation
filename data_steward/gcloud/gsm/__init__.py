import os

# external imports
from google.cloud import secretmanager


class SecretManager(secretmanager.SecretManagerServiceClient):
    """
    Simple extension of upstream GSM client
    """

    @staticmethod
    def build_secret_full_name(secret_name: str, project_id: str = os.getenv('GOOGLE_CLOUD_PROJECT'),
                               secret_version: str = 'latest') -> str:
        """
        build_secret_full_name constructs the fqn for a given secret within Google Secrets Engine

        :param project_id: [required] Name of Google project containing secret
        :param secret_name: [required] Specific name of secret
        :param secret_version: [optional] Version of secret to retrieve.  Defaults to "latest"
        :return: FQN of secret based on provided values
        """
        return f'projects/{project_id}/secrets/{secret_name}/versions/{secret_version}'
