"""
Script to generate the validation dataset in the form validationYYYYMMDD
containing tables in the form {hpo_id}_identity_match using the latest
partitions from drc_ops.
"""
# Python imports
import argparse
import logging

# Third party imports
from google.cloud.bigquery import Table, Dataset

# Project imports
from utils import auth, pipeline_logging
from gcloud.bq import BigQueryClient
from common import DRC_OPS, CDR_SCOPES, IDENTITY_MATCH, DE_IDENTIFIED
from constants.validation.participants.snapshot_validaiton_dataset import (
    PARTITIONS_QUERY, SNAPSHOT_TABLE_QUERY)
from bq_utils import get_hpo_info, get_table_id

LOGGER = logging.getLogger(__name__)


def get_partition_date_df(df, hpo_id):
    """
    Filter dataframe to retrieve row for hpo_id

    :param df: Dataframe
    :param hpo_id: _description_

    :return: filtered df for hpo_id
    """
    return df[(df['table_name'] == f'{IDENTITY_MATCH}_{hpo_id}')]


def create_id_match_tables(client: BigQueryClient, dataset_id: str) -> None:
    """
    Generate id_match tables in the specified snapshot dataset

    :param client: a BigQueryClient
    :param dataset_id: Identifies the snapshot dataset

    :return: None
    """
    job = client.query(
        PARTITIONS_QUERY.render(project_id=client.project,
                                drc_ops_dataset=DRC_OPS))
    partitions_df = job.result().to_dataframe()
    LOGGER.info(f'Fetched latest partitions from {DRC_OPS}')

    for hpo_dict in get_hpo_info():
        hpo_id = hpo_dict["hpo_id"]
        hpo_partition_df = get_partition_date_df(partitions_df, hpo_id)

        source_table = f'{IDENTITY_MATCH}_{hpo_id}'
        dest_table = get_table_id(hpo_id, IDENTITY_MATCH)

        fq_source_table = f'{client.project}.{DRC_OPS}.{source_table}'
        fq_dest_table = f'{client.project}.{dataset_id}.{dest_table}'

        if hpo_partition_df['partition_id'].count() != 1:
            LOGGER.info(
                f'Skipping {hpo_id} since {fq_source_table} does not exist')
            continue

        partition_date = hpo_partition_df['partition_id'].iloc[0]

        schema = client.get_table_schema(IDENTITY_MATCH)
        dest_table = Table(fq_dest_table, schema=schema)
        client.create_table(dest_table)
        LOGGER.info(f'Created table {fq_dest_table} for {hpo_id}')

        snapshot_table_job = client.query(
            SNAPSHOT_TABLE_QUERY.render(project_id=client.project,
                                        fq_source_table=fq_source_table,
                                        fq_dest_table=fq_dest_table,
                                        partition_date=partition_date))
        snapshot_table_job.result()
        LOGGER.info(
            f'Populated table {fq_dest_table} from latest partition in {fq_source_table}'
        )


def create_snapshot(client: BigQueryClient, release_tag: str) -> str:
    """
    Generates the snapshot dataset based on the CDR run release tag

    :param client: a BigQueryClient
    :param release_tag: Release tag for the CDR run
    :return: dataset_id: Identifies the snapshot dataset
    """
    dataset_id = f"{release_tag}_validation"
    dataset = Dataset(f'{client.project}.{dataset_id}')
    dataset.description = f'{DRC_OPS} + {release_tag}_ehr'
    dataset.labels = {
        'owner': 'curation',
        'release_tag': release_tag,
        DE_IDENTIFIED: 'false',
        'phase': 'clean'
    }
    dataset = client.create_dataset(dataset_id, exists_ok=True)
    return dataset.dataset_id


def get_arg_parser():
    parser = argparse.ArgumentParser(
        description="""Generate validation snapshot""")
    parser.add_argument(
        '-p',
        '--project_id',
        action='store',
        dest='project_id',
        help='Project associated with drc_ops and output dataset',
        required=True)
    parser.add_argument('-t',
                        '--release_tag',
                        action='store',
                        dest='release_tag',
                        help='Release tag for the CDR run',
                        required=True)
    parser.add_argument('-r',
                        '--run_as_email',
                        action='store',
                        dest='run_as_email',
                        help='Service account to impersonate',
                        required=True)
    return parser


def main():
    parser = get_arg_parser()
    args = parser.parse_args()

    # Set up pipeline logging
    pipeline_logging.configure(add_console_handler=True)

    # get credentials and create client
    impersonation_creds = auth.get_impersonation_credentials(
        args.run_as_email, CDR_SCOPES)

    bq_client = BigQueryClient(args.project_id, credentials=impersonation_creds)

    dataset_id = create_snapshot(bq_client, args.release_tag)
    create_id_match_tables(bq_client, dataset_id)


if __name__ == '__main__':
    main()
