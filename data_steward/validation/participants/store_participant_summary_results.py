""" Module responsible for calling the Participant Summary Api for a set of sites and storing in tables.

Original Issue: DC-1214
"""

# Python imports
import argparse
import logging
from typing import List, Dict

# Third party imports
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

# Project imports
from utils.participant_summary_requests import get_org_participant_information, store_participant_data
from common import PS_API_VALUES, DRC_OPS
from utils import bq, pipeline_logging
from constants import bq_utils as bq_consts

LOGGER = logging.getLogger(__name__)
SCOPES = [
    'https://www.googleapis.com/auth/bigquery',
    'https://www.googleapis.com/auth/devstorage.read_write',
    'https://www.googleapis.com/auth/cloud-platform'
]


def get_hpo_info(project_id: str) -> List[Dict]:
    """ Returns a list of HPOs

    :param project_id
    :type project_id: str
    :return: a list of HPOs
    :rtype: List[Dict]
    """
    client = bq.get_client(project_id)
    hpo_list = []
    hpo_table_query = bq_consts.GET_HPO_CONTENTS_QUERY.format(
        project_id=project_id,
        LOOKUP_TABLES_DATASET_ID=bq_consts.LOOKUP_TABLES_DATASET_ID,
        HPO_SITE_ID_MAPPINGS_TABLE_ID=bq_consts.HPO_SITE_ID_MAPPINGS_TABLE_ID)
    hpo_job = client.query(hpo_table_query)
    hpo_table_contents = hpo_job.result()
    for hpo_table_row in hpo_table_contents:
        hpo_id = hpo_table_row[bq_consts.HPO_ID].lower()
        org_id = hpo_table_row[bq_consts.ORG_ID]
        hpo_name = hpo_table_row[bq_consts.SITE_NAME]
        if hpo_id and hpo_name:
            hpo_dict = {"hpo_id": hpo_id, "org_id": org_id}
            hpo_list.append(hpo_dict)
    return hpo_list


def get_org_id(project_id, hpo_id):
    """
    Fetch org_id for the hpo_id
    :param project_id: 
    :param hpo_id: 
    :return: 
    """

    hpo_list = get_hpo_info(project_id)

    org_id = None
    for hpo_dict in hpo_list:
        if hpo_id == hpo_dict['hpo_id']:
            org_id = hpo_dict['org_id']
            return org_id

    if not org_id:
        raise RuntimeError(
            f'Site {hpo_id} not found in table {bq_consts.HPO_SITE_ID_MAPPINGS_TABLE_ID}'
        )


def fetch_and_store_ps_hpo_data(client,
                                project_id,
                                rdr_project_id,
                                hpo_id,
                                dataset_id=DRC_OPS):
    """
    
    :param client: BQ client
    :param project_id: 
    :param rdr_project_id: PS API project
    :param dataset_id: contains table to store PS API data
    :param hpo_id: 
    :return: 
    """

    org_id = get_org_id(project_id, hpo_id)

    # Get participant summary data
    LOGGER.info(
        f'Getting participant summary data for HPO/ORG {hpo_id}/{org_id}')
    participant_info = get_org_participant_information(rdr_project_id, org_id)

    # Load schema and create ingestion time-partitioned table

    schema = bq.get_table_schema(PS_API_VALUES)
    # TODO use resources.get_table_id after updating it to flip hpo_id, table_name
    table_name = f'{PS_API_VALUES}_{hpo_id}'

    try:
        table = client.get_table(f'{project_id}.{dataset_id}.{table_name}')
    except NotFound:
        LOGGER.info(
            f'Creating HOUR partitioned table {project_id}.{dataset_id}.{table_name}'
        )

        table = bigquery.Table(f'{project_id}.{dataset_id}.{table_name}',
                               schema=schema)
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.HOUR)
        table = client.create_table(table)

    # Insert summary data into table
    LOGGER.info(
        f'Storing participant data for {hpo_id} in table {project_id}.{dataset_id}.{table.table_id}'
    )
    store_participant_data(participant_info,
                           project_id,
                           f'{dataset_id}.{table_name}',
                           schema=schema,
                           to_hour_partition=True)

    LOGGER.info(f'Done.')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description=""" Store participant summary api results in BigQuery tables.
            Pass --hpo_id to query a site.
            Environment variable GOOGLE_APPLICATION_CREDENTIALS must be set before running.
        """)
    parser.add_argument('--project_id', '-p', required=True)
    parser.add_argument('--rdr_project_id', '-r', required=True)
    parser.add_argument('--hpo_id', required=True)

    args = parser.parse_args()

    pipeline_logging.configure(level=logging.DEBUG, add_console_handler=True)

    client = bq.get_client(args.project_id)

    fetch_and_store_ps_hpo_data(client,
                                args.project_id,
                                args.rdr_project_id,
                                hpo_id=args.hpo_id)
