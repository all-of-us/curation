"""
Test  dataset resources for integration tests.

Contains functions for setting up and tearing down test datasets for integration testing.
Also contains test dataset deletion functions.  Dataset modification functions
should be added here as well.
"""
from google.oauth2 import service_account
from google.cloud import bigquery

from common import JINJA_ENV
from utils import bq

CLIENT = None
"""Bigquery  client object"""


def get_client(project_id, app_creds):
    """
    Ensure only one client is created and reused

    :param project_id:  project to get a client for
    :returns: a big query client object
    """
    global CLIENT
    if not CLIENT:
        credentials = service_account.Credentials.from_service_account_file(
            app_creds)
        CLIENT = bigquery.Client(project=project_id, credentials=credentials)

    return CLIENT


def create_dataset(project, dataset_id, description, tags, app_creds):
    """
    Create a dataset with the given  parameters.

    :param project:  The project_id used to define the dataset.
    :param dataset_id: The string to name the dataset with.
    :param description: A string to use to describe the dataset.
    :param tags: The list of tags/labels to apply to the dataset.
    :parm app_creds: Filepath to credentials file used to create the dataset
    """
    # Construct a full Dataset object to send to the API.
    dataset = bq.define_dataset(project, dataset_id, description, tags)

    client = get_client(project, app_creds)
    dataset = client.create_dataset(dataset, exists_ok=True)
    print(f"Created dataset {project}.{dataset_id}")


def create_datasets(project, config, datasets):
    """
    Create datasets defined in an iterable.

    :param project: The project_id used to create the dataset in.
    :param config: A dictionary of values from the environment.
    :param datasets: An iterable containing strings that name the  datasets.
    """
    username = config.get('USERNAME', 'default')
    for dataset in datasets:
        dataset_name = dataset.split('_')[0]
        description = f"Test {dataset_name} dataset for {username}"
        dataset_id = config.get(dataset)
        create_dataset(project, dataset_id, description, {'automated_test': ''},
                       config.get('GOOGLE_APPLICATION_CREDENTIALS'))


def remove_datasets(project, creds_path, config, datasets):
    """
    Remove datasets from the defined project.

    :param  project: The project_id to delete datasets from.
    :param creds_path: The filepath to an appropriate credentials file
    :param config: A dictionary of values from the environment
    :param datasets: An iterable of strings that represent dataset names.  If
        they exist in the given project, the named datasets will be deleted.
    """
    client = get_client(project, creds_path)
    for dataset in datasets:
        dataset_id = config.get(dataset)
        fq_dataset_id = f'{project}.{dataset_id}'
        client.delete_dataset(fq_dataset_id,
                              delete_contents=True,
                              not_found_ok=True)
        print(f"Deleted dataset '{fq_dataset_id}'")


def copy_vocab_tables(vocab_dataset, dest_prefix):
    """
    Copy vocabulary tables from a vocabulary dataset to the test dataset.

    :param vocab_dataset:  The  vocabulary dataset to copy tables  from
    :param dest_prefix: The dataset to copy  vocabulary tables  into
    """
    client = get_client(None, None)
    tables = client.list_tables(vocab_dataset)
    for table in tables:
        vocab_table = f"{vocab_dataset}.{table.table_id}"
        dest_table = f"{dest_prefix}.{table.table_id}"
        client.copy_table(vocab_table, dest_table)
        print(f"copied '{vocab_table}' to '{dest_table}'")


def create_test_datasets(config, dataset_keys):
    """
    Create test datasets for automated integration  tests.

    :param config: A dictionary of environment variables  relevant to creating
        new datasets.
    :param datasets: An iterable of strings where each string represents a dataset
        name  to create.
    """
    project = config.get('APPLICATION_ID')
    datasets = [config.get(dataset_key) for dataset_key in dataset_keys]
    vocab_dataset = f"{project}.{config.get('VOCABULARY_DATASET')}"
    dst_dataset = f"{project}.{config.get('BIGQUERY_DATASET_ID')}"
    username = config.get('USERNAME', 'default')
    tpl = JINJA_ENV.from_string('''
    {% for dataset in datasets %}
      {% set dataset_name = dataset.split('_')[0] %}
      DROP SCHEMA IF EXISTS `{{ project }}.{{ dataset }}` CASCADE;
      CREATE SCHEMA `{{ project }}.{{ dataset }}`
       OPTIONS(
         description="Test {{ dataset_name }} dataset for {{ username }}"
       );
    {% endfor %}

    {% for src_table in src_tables %}
      CREATE OR REPLACE TABLE `{{ dst_dataset }}.{{ src_table.table_id }}`
      COPY `{{ src_table.project }}.{{ src_table.dataset_id }}.{{ src_table.table_id }}`;
    {% endfor %}
    ''')
    creds_path = config.get('GOOGLE_APPLICATION_CREDENTIALS')
    client = get_client(project, creds_path)
    src_tables = client.list_tables(vocab_dataset)
    script = tpl.render(project=project,
                        username=username,
                        datasets=datasets,
                        src_tables=src_tables,
                        dst_dataset=dst_dataset)
    setup_job = client.query(script)
    print(f'BigQuery job {setup_job.job_id} started to set up test datasets {datasets}.')
    return setup_job.result()


if __name__ == "__main__":
    # only import this if this is running as a standalone module
    from ci.test_setup import get_environment_config, DATASET_NAMES

    config = get_environment_config()
    create_test_datasets(config, DATASET_NAMES)
