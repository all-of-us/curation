"""
Test  dataset resources for integration tests.

Contains functions for setting up and tearing down test datasets for integration testing.
Also contains test dataset deletion functions.  Dataset modification functions
should be added here as well.
"""
from google.oauth2 import service_account

from gcloud.bq import BigQueryClient

CLIENT = None
"""Bigquery  client object"""


def get_client(project_id, app_creds):
    """
    Ensure only one client is created and reused

    :param project_id: project to get a client for
    :param app_creds: Filepath to credentials file used to create the client
    :returns: A BigQueryClient object
    """
    global CLIENT
    if not CLIENT:
        credentials = service_account.Credentials.from_service_account_file(
            app_creds)
        CLIENT = BigQueryClient(project_id=project_id, credentials=credentials)

    return CLIENT


def create_dataset(client, dataset_id, description, tags):
    """
    Create a dataset with the given  parameters.

    :param client: A BigQueryClient
    :param dataset_id: The string to name the dataset with.
    :param description: A string to use to describe the dataset.
    :param tags: The list of tags/labels to apply to the dataset.
    """
    # Construct a full Dataset object to send to the API.
    dataset = client.define_dataset(dataset_id, description, tags)

    dataset = client.create_dataset(dataset, exists_ok=True)
    print(f"Created dataset {client.project}.{dataset_id}")


def create_datasets(client, config, datasets):
    """
    Create datasets defined in an iterable.

    :param client: A BigQueryClient
    :param config: A dictionary of values from the environment.
    :param datasets: An iterable containing strings that name the  datasets.
    """
    username = config.get('USERNAME', 'default')
    for dataset in datasets:
        dataset_name = dataset.split('_')[0]
        description = f"Test {dataset_name} dataset for {username}"
        dataset_id = config.get(dataset)
        create_dataset(client, dataset_id, description, {'automated_test': ''})


def remove_datasets(client, config, datasets):
    """
    Remove datasets from the defined project.

    :param  client: A BigQueryClient
    :param config: A dictionary of values from the environment
    :param datasets: An iterable of strings that represent dataset names.  If
        they exist in the given project, the named datasets will be deleted.
    """
    for dataset in datasets:
        dataset_id = config.get(dataset)
        fq_dataset_id = f'{client.project}.{dataset_id}'
        client.delete_dataset(fq_dataset_id,
                              delete_contents=True,
                              not_found_ok=True)
        print(f"Deleted dataset '{fq_dataset_id}'")


def copy_vocab_tables(client, vocab_dataset, dest_prefix):
    """
    Copy vocabulary tables from a vocabulary dataset to the test dataset.

    :param client: A BigQueryClient
    :param vocab_dataset:  The  vocabulary dataset to copy tables  from
    :param dest_prefix: The dataset to copy  vocabulary tables  into
    """
    tables = client.list_tables(vocab_dataset)
    for table in tables:
        vocab_table = f"{vocab_dataset}.{table.table_id}"
        dest_table = f"{dest_prefix}.{table.table_id}"
        client.copy_table(vocab_table, dest_table)
        print(f"copied '{vocab_table}' to '{dest_table}'")


def create_test_datasets(config, datasets):
    """
    Create test datasets for automated integration  tests.

    :param config: A dictionary of environment variables  relevant to creating
        new datasets.
    :param datasets: An iterable of strings where each string represents a dataset
        name  to create.
    """
    project = config.get('APPLICATION_ID')
    credentials = config.get('GOOGLE_APPLICATION_CREDENTIALS')
    client = get_client(project, credentials)
    remove_datasets(client, config, datasets)
    create_datasets(client, config, datasets)
    vocab_dataset = f"{project}.{config.get('VOCABULARY_DATASET')}"
    dest_prefix = f"{project}.{config.get('BIGQUERY_DATASET_ID')}"
    copy_vocab_tables(client, vocab_dataset, dest_prefix)


if __name__ == "__main__":
    # only import this if this is running as a standalone module
    from ci.test_setup import get_environment_config, DATASET_NAMES

    config = get_environment_config()
    create_test_datasets(config, DATASET_NAMES)
