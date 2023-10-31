import logging
from google.cloud.exceptions import Conflict
from google.cloud import bigquery

from gcloud.bq import BigQueryClient
from resources import get_git_tag, CDM_TABLES, AOU_DEATH

LOGGER = logging.getLogger(__name__)


def create_datasets(client, release_tag, data_stage, dataset_type) -> str:
    """
    Create a dataset for the specified dataset type in the unioned_ehr and combined stage.

    :param client: a BigQueryClient
    :param release_tag: the release tag for this CDR
    :param data_stage: the pipeline stage of the dataset('unioned', 'combined' etc...).
    :param dataset_type: the type of the dataset this function creates.
        It has to be clean, backup, staging, sandbox, or release.
    :returns: The name of the dataset.
    """
    if data_stage == 'unioned':
        dataset_tag = 'unioned_ehr'
    elif data_stage == 'combined':
        dataset_tag = 'combined'
    else:
        raise ValueError(
            f'Invalid data_stage: {data_stage}. It has to be unioned or combined.'
        )

    version = get_git_tag()

    dataset_definition = {
        'clean': {
            'name':
                f'{release_tag}_{dataset_tag}',
            'desc':
                f'{version} Clean version of {release_tag}_{dataset_tag}_backup',
            'labels': {
                "owner": "curation",
                "phase": "clean",
                "release_tag": release_tag,
                "de_identified": "false"
            }
        },
        'backup': {
            'name':
                f'{release_tag}_{dataset_tag}_backup',
            'desc':
                f"{'Raw version of {release_tag}_rdr + {release_tag}_unioned_ehr' if data_stage == 'combined' \
                    else 'Raw version of {dataset_tag} dataset'}",
            'labels': {
                "owner": "curation",
                "phase": "backup",
                "release_tag": release_tag,
                "de_identified": "false"
            }
        },
        'staging': {
            'name':
                f'{release_tag}_{dataset_tag}_staging',
            'desc':
                f'Intermediary dataset to apply cleaning rules on {release_tag}_{dataset_tag}_backup',
            'labels': {
                "owner": "curation",
                "phase": "staging",
                "release_tag": release_tag,
                "de_identified": "false"
            }
        },
        'sandbox': {
            'name': f'{release_tag}_{dataset_tag}_sandbox',
            'desc': (
                f'Sandbox created for storing records affected by the '
                f'cleaning rules applied to {release_tag}_{dataset_tag}_staging'
            ),
            'labels': {
                "owner": "curation",
                "phase": "sandbox",
                "release_tag": release_tag,
                "de_identified": "false"
            }
        },
        'release': {
            'name': f'{release_tag}_{dataset_tag}_release',
            'desc': f'{version} Release version of {release_tag}_{dataset_tag}',
            'labels': {
                "owner": "curation",
                "phase": "release",
                "release_tag": release_tag,
                "de_identified": "false"
            }
        }
    }

    LOGGER.info(
        f"Creating {dataset_tag} {dataset_type} dataset if not exists: `{dataset_definition[dataset_type]['name']}`"
    )

    dataset_object = client.define_dataset(
        dataset_definition[dataset_type]['name'],
        dataset_definition[dataset_type]['desc'],
        dataset_definition[dataset_type]['labels'])

    try:
        client.create_dataset(dataset_object, exists_ok=False)
        LOGGER.info(
            f"Created dataset `{client.project}.{dataset_definition[dataset_type]['name']}`"
        )
    except Conflict:
        LOGGER.info(
            f"The dataset `{client.project}.{dataset_definition[dataset_type]['name']}` already exists. "
        )

    return dataset_definition[dataset_type]['name']


def create_cdm_tables(client: BigQueryClient, dataset: str):
    """
    Create all CDM tables in the specified dataset.

    :param client: BigQueryClient
    :param dataset: Target dataset name
    :return: None

    Note: Recreates any existing tables
    """
    for table in CDM_TABLES + [AOU_DEATH]:
        LOGGER.info(f'Creating table {dataset}.{table}...')
        schema_list = client.get_table_schema(table_name=table)
        dest_table = f'{client.project}.{dataset}.{table}'
        dest_table = bigquery.Table(dest_table, schema=schema_list)
        table = client.create_table(dest_table)  # Make an API request.
        LOGGER.info(f"Created table: `{table.table_id}`")
