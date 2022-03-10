"""
Script to generate the validation dataset in the form validationYYYYMMDD
containing tables in the form {hpo_id}_identity_match using the latest
partitions from drc_ops.
"""

import argparse
import logging

from utils import bq, auth, pipeline_logging
from common import JINJA_ENV, DRC_OPS, CDR_SCOPES, IDENTITY_MATCH
from bq_utils import get_hpo_info, get_table_id

LOGGER = logging.getLogger(__name__)

partitions_query = JINJA_ENV.from_string("""
SELECT table_name, partition_id
FROM (SELECT 
    table_name,
    partition_id,
    ROW_NUMBER() OVER(PARTITION BY table_name ORDER BY partition_id DESC) r
FROM {{project_id}}.{{drc_ops_dataset}}.INFORMATION_SCHEMA.PARTITIONS
WHERE table_name LIKE '%identity_match%'
AND partition_id NOT IN ("__NULL__", "__UNPARTITIONED__")
) WHERE r = 1
""")

create_table_query = JINJA_ENV.from_string("""
CREATE TABLE {{project_id}}.{{dest_table}}
LIKE {{project_id}}.{{source_table}}
AS SELECT *
FROM {{project_id}}.{{source_table}}
WHERE FORMAT_TIMESTAMP("%Y%m%d%H", _PARTITIONTIME) = "{{partition_date}}"
""")


def get_partition_date(df, hpo_id):
    """
    Filter dataframe to retrieve row for hpo_id

    :param df: Dataframe
    :param hpo_id: _description_

    :return: filtered df for hpo_id
    """
    return df[(df['table_name'] == f'{IDENTITY_MATCH}_{hpo_id}')]


def create_id_match_tables(client, dataset_id):
    """
    Generate id_match tables in the specified snapshot dataset

    :param client: BigQuery client
    :param dataset_id: Identifies the snapshot dataset

    :return: None
    """
    job = client.query(partitions_query)
    partitions_df = job.result().to_dataframe()
    LOGGER.info(f'Fetched latest partitions from {DRC_OPS}')

    for hpo_id in get_hpo_info():
        hpo_partition_df = get_partition_date(partitions_df, hpo_id)

        source_table = f'{IDENTITY_MATCH}_{hpo_id}'
        dest_table = get_table_id(hpo_id, IDENTITY_MATCH)

        fq_source_table = f'{client.project}.{DRC_OPS}.{source_table}'
        fq_dest_table = f'{client.project}.{dataset_id}.{dest_table}'

        if hpo_partition_df['table_name'].count() != 1:
            LOGGER.info(
                f'Skipping {hpo_id} since {fq_source_table} does not exist')
            continue

        partition_date = hpo_partition_df['table_name'].iloc[0]

        create_table_job = client.query(
            create_table_query.render(project_id=client.project,
                                      source_table=fq_source_table,
                                      dest_table=fq_dest_table,
                                      partition_date=partition_date))
        create_table_job.result()
        LOGGER.info(f'Created table {fq_dest_table} for {hpo_id}')


def create_snapshot(client, date_str):
    """
    Generates the snapshot dataset based on date passed

    :param client: BigQuery client
    :param date_str: date string to snapshot on
    :return: 
    """
    dataset_id = f"validation{date_str.replace('-', '')}"
    dataset = client.create_dataset(dataset_id, exists_ok=True)
    return dataset.dataset_id


def get_arg_parser():
    parser = argparse.ArgumentParser(description=""".""")
    parser.add_argument(
        '-p',
        '--project_id',
        action='store',
        dest='project_id',
        help='Project associated with drc_ops and output dataset',
        required=True)
    parser.add_argument('-s',
                        '--snapshot_date',
                        action='store',
                        dest='snapshot_date',
                        help='Date to use for the snapshot as YYYY-MM-DD',
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
    pipeline_logging.configure(level=logging.DEBUG, add_console_handler=True)

    # get credentials and create client
    impersonation_creds = auth.get_impersonation_credentials(
        args.run_as_email, CDR_SCOPES)

    client = bq.get_client(args.project_id, credentials=impersonation_creds)

    dataset_id = create_snapshot(client, args.snapshot_date)
    create_id_match_tables(client, dataset_id)


if __name__ == '__main__':
    main()
