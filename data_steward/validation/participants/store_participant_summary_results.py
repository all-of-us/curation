""" Module responsible for calling the Participant Summary Api for a set of sites and storing in tables.

Original Issue: DC-1214
"""

# Python imports
import argparse
import logging
from typing import List, Dict
from requests import Session
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Third party imports
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

# Project imports
from utils.participant_summary_requests import (
    get_org_participant_information, get_paginated_participant_data,
    store_participant_data, process_api_data_to_df,
    FIELDS_OF_INTEREST_FOR_VALIDATION, MAX_RETRIES, BACKOFF_FACTOR,
    STATUS_FORCELIST)
from common import PS_API_VALUES, DRC_OPS, UNIONED
from utils import pipeline_logging
from gcloud.bq import BigQueryClient
from constants import bq_utils as bq_consts

LOGGER = logging.getLogger(__name__)
SCOPES = [
    'https://www.googleapis.com/auth/bigquery',
    'https://www.googleapis.com/auth/devstorage.read_write',
    'https://www.googleapis.com/auth/cloud-platform'
]


def get_hpo_org_info(client: BigQueryClient) -> List[Dict]:
    """ Returns a list of HPOs

    :param client: A BigQueryClient
    :type project_id: str
    :return: a list of HPOs
    :rtype: List[Dict]
    """
    hpo_list = []
    hpo_table_query = bq_consts.GET_HPO_CONTENTS_QUERY.format(
        project_id=client.project,
        LOOKUP_TABLES_DATASET_ID=bq_consts.LOOKUP_TABLES_DATASET_ID,
        HPO_SITE_TABLE=bq_consts.HPO_SITE_ID_MAPPINGS_TABLE_ID)
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


def get_org_id(client, hpo_id):
    """
    Fetch org_id for the hpo_id
    :param client: A BigQueryClient 
    :param hpo_id: identifies the hpo site
    :return: 
    """
    hpo_list = get_hpo_org_info(client)

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
                                rdr_project_id,
                                hpo_id,
                                dataset_id=DRC_OPS):
    """
    
    :param client: A BigQueryClient
    :param rdr_project_id: PS API project
    :param dataset_id: contains table to store PS API data
    :param hpo_id: identifies the hpo site
    :return: 
    """

    org_id = get_org_id(client, hpo_id)

    # Get participant summary data
    LOGGER.info(
        f'Getting participant summary data for HPO/ORG {hpo_id}/{org_id}')
    participant_info = get_org_participant_information(rdr_project_id, org_id)

    # Load schema and create ingestion time-partitioned table

    schema = client.get_table_schema(PS_API_VALUES)
    # TODO use resources.get_table_id after updating it to flip hpo_id, table_name
    table_name = f'{PS_API_VALUES}_{hpo_id}'

    try:
        table = client.get_table(f'{client.project}.{dataset_id}.{table_name}')
    except NotFound:
        LOGGER.info(
            f'Creating HOUR partitioned table {client.project}.{dataset_id}.{table_name}'
        )

        table = bigquery.Table(f'{client.project}.{dataset_id}.{table_name}',
                               schema=schema)
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.HOUR)
        table = client.create_table(table)

    # Insert summary data into table
    LOGGER.info(
        f'Storing participant data for {hpo_id} in table {client.project}.{dataset_id}.{table.table_id}'
    )
    store_participant_data(participant_info,
                           client,
                           f'{dataset_id}.{table_name}',
                           schema=schema,
                           to_hour_partition=True)

    LOGGER.info(f'Done.')


def fetch_and_store_full_ps_data(client,
                                 project_id,
                                 rdr_project_id,
                                 dataset_id=DRC_OPS):
    """
    Fetches PS API data for all participants and stores in drc_ops.ps_api_unioned table

    :param client: a BigQueryClient
    :param project_id: Identifies the project
    :param rdr_project_id: PS API project
    :param dataset_id: contains table to store PS API data
    :return: 
    """
    # Load schema
    schema = client.get_table_schema(PS_API_VALUES)
    table_name = f'{PS_API_VALUES}'
    fq_table_id = f'{project_id}.{dataset_id}.{table_name}'

    # Clear existing table to refresh data
    client.delete_table(fq_table_id, not_found_ok=True)
    LOGGER.info(f'Creating table {fq_table_id}')

    table = bigquery.Table(fq_table_id, schema=schema)
    table = client.create_table(table)

    done = False
    url = None

    params = {
        'suspensionStatus': 'NOT_SUSPENDED',
        'consentForElectronicHealthRecords': 'SUBMITTED',
        'withdrawalStatus': 'NOT_WITHDRAWN',
        '_sort': 'participantId',
        '_count': '10000'
    }

    # Create session for reuse
    session = Session()
    retries = Retry(total=MAX_RETRIES,
                    read=MAX_RETRIES,
                    connect=MAX_RETRIES,
                    backoff_factor=BACKOFF_FACTOR,
                    status_forcelist=STATUS_FORCELIST)
    session.mount('https://', HTTPAdapter(max_retries=retries))
    session.mount('http://', HTTPAdapter(max_retries=retries))

    while not done:
        # Get paginated participant summary data
        LOGGER.info(f'Getting paginated participant summary data')

        paginated_dict = get_paginated_participant_data(rdr_project_id,
                                                        params=params,
                                                        url=url,
                                                        session=session)
        participant_data = paginated_dict['data']
        url = paginated_dict['url']

        column_map = {'participant_id': 'person_id'}

        df = process_api_data_to_df(participant_data,
                                    FIELDS_OF_INTEREST_FOR_VALIDATION,
                                    column_map)
        # Insert paginated summary data into table
        LOGGER.info(
            f'Storing paginated participant data in table {fq_table_id}')
        store_participant_data(df,
                               client,
                               f'{dataset_id}.{table_name}',
                               schema=schema,
                               to_hour_partition=False,
                               append=True)
        if not url:
            done = True

    LOGGER.info(f'Done.')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description=""" Store participant summary api results in BigQuery tables.
            Pass --hpo_id to query a site.
            Environment variable GOOGLE_APPLICATION_CREDENTIALS must be set before running.
        """)
    parser.add_argument('--project_id', '-p', required=True)
    parser.add_argument('--rdr_project_id', '-r', required=True)
    # HPO to download data for. Use the keyword 'all_hpo' to get all participant summary data
    parser.add_argument('--hpo_id', required=True)

    args = parser.parse_args()

    pipeline_logging.configure(level=logging.DEBUG, add_console_handler=True)

    bq_client = BigQueryClient(args.project_id)

    if args.hpo_id.lower() == 'all_hpo':
        fetch_and_store_full_ps_data(bq_client, args.project_id,
                                     args.rdr_project_id)
    else:
        fetch_and_store_ps_hpo_data(bq_client,
                                    args.rdr_project_id,
                                    hpo_id=args.hpo_id)
