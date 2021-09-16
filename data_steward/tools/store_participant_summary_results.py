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

DATASET_ID = DRC_OPS


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


def main(project_id, rdr_project_id, org_id=None, hpo_id=None):

    #Get list of hpos
    LOGGER.info('Getting hpo list...')
    if org_id:
        hpo_list = [{"hpo_id": hpo_id, "org_id": org_id}]
    else:
        hpo_list = get_hpo_info(project_id)

    LOGGER.info(hpo_list)

    for hpo in hpo_list:
        org_id = hpo['org_id']
        hpo_id = hpo['hpo_id']
        # Get participant summary data
        LOGGER.info(f'Getting participant summary data for {org_id}...')
        participant_info = get_org_participant_information(
            rdr_project_id, org_id)

        # Load schema and create ingestion time-partitioned table

        schema = bq.get_table_schema(PS_API_VALUES)
        tablename = f'{PS_API_VALUES}_{hpo_id}'

        client = bq.get_client(project_id)
        try:
            table = client.get_table(f'{project_id}.{DATASET_ID}.{tablename}')
        except NotFound:
            LOGGER.info(
                f'Creating table {project_id}.{DATASET_ID}.{tablename}...')

            table = bigquery.Table(f'{project_id}.{DATASET_ID}.{tablename}',
                                   schema=schema)
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.HOUR)
            table = client.create_table(table)

        # Insert summary data into table
        LOGGER.info(
            f'Storing participant data for {org_id} in table {project_id}.{DATASET_ID}.{tablename}...'
        )
        store_participant_data(participant_info,
                               project_id,
                               f'{DATASET_ID}.{tablename}',
                               schema=schema)

    LOGGER.info(f'Done.')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description=""" Store participant summary api results in BigQuery tables.
            Pass --org_id and --hpo_id to query a single site. Otherwise, all sites are queried.
            Environment variable GOOGLE_APPLICATION_CREDENTIALS must be set before running.
        """)
    parser.add_argument('--project_id', '-p', required=True)
    parser.add_argument('--rdr_project_id', '-r', required=True)
    parser.add_argument('--org_id', required=False)
    parser.add_argument('--hpo_id', required=False)

    args = parser.parse_args()

    pipeline_logging.configure(level=logging.DEBUG, add_console_handler=True)

    if (args.org_id and not args.hpo_id) or (args.hpo_id and not args.org_id):
        parser.error(
            "--org_id requires --hpo_id and --hpo_id required --org_id.")

    main(args.project_id,
         args.rdr_project_id,
         org_id=args.org_id,
         hpo_id=args.hpo_id)
