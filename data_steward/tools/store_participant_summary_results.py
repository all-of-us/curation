# Python imports
import argparse
import logging

# Third party imports
from google.cloud import bigquery
from google.cloud.bigquery import dataset

# Project imports
from utils.participant_summary_requests import get_org_participant_information, store_participant_data
from bq_utils import table_exists, response2rows
from common import PS_API_VALUES, DRC_OPS
from utils import bq, auth, pipeline_logging
from constants import bq_utils as bq_consts

LOGGER = logging.getLogger(__name__)
SCOPES = [
    'https://www.googleapis.com/auth/bigquery',
    'https://www.googleapis.com/auth/devstorage.read_write',
    'https://www.googleapis.com/auth/cloud-platform'
]

DATASET_ID = DRC_OPS


def get_hpo_info(project_id, credentials=None):
    client = bq.get_client(project_id, credentials=credentials)
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
            hpo_dict = {"hpo_id": hpo_id, "org_id": org_id, "name": hpo_name}
            hpo_list.append(hpo_dict)
    return hpo_list


def main(project_id, rdr_project_id, curation_run_as_email, rdr_run_as_email,
         rdr_key_file):
    curation_impersonation_creds = auth.get_impersonation_credentials(
        curation_run_as_email, SCOPES)

    rdr_impersonation_creds = auth.get_impersonation_credentials(
        rdr_run_as_email, SCOPES, key_file=rdr_key_file)

    #Get list of hpos
    LOGGER.info('Getting hpo list...')
    hpo_list = get_hpo_info(project_id, credentials=None)
    hpo_list = [hpo for hpo in hpo_list if hpo['hpo_id']]
    print(hpo_list)

    for hpo in hpo_list:
        org_id = hpo['org_id']
        hpo_id = hpo['hpo_id']
        # Get participant summary data
        LOGGER.info(f'Getting participant summary data for {org_id}...')
        participant_info = get_org_participant_information(rdr_project_id,
                                                           org_id,
                                                           credentials=None)

        # Load schema and create ingestion time-partitioned table

        schema = bq.get_table_schema(PS_API_VALUES)
        tablename = f'{PS_API_VALUES}_{hpo_id}'

        if not table_exists(tablename, DATASET_ID):
            LOGGER.info(
                f'Creating table {project_id}.{DATASET_ID}.{tablename}...')

            client = bq.get_client(project_id, credentials=None)
            dataset_ref = bigquery.DatasetReference(project_id, DATASET_ID)
            table_ref = dataset_ref.table(tablename)
            table = bigquery.Table(table_ref, schema=schema)
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY)
            table = client.create_table(table)

        # Insert summary data into table
        LOGGER.info(
            f'Storing participant data for {org_id} in table {project_id}.{DATASET_ID}.{tablename}...'
        )
        store_participant_data(participant_info,
                               project_id,
                               f'{DATASET_ID}.{tablename}',
                               schema=schema,
                               credentials=None)

    LOGGER.info(f'Done.')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Store participant summary api results in BigQuery tables.")
    parser.add_argument('--project_id', '-p', required=True)
    parser.add_argument('--rdr_project_id', '-r', required=True)
    parser.add_argument(
        '--curation_run_as',
        action='store',
        dest='curation_run_as_email',
        help=
        'Service account email address to impersonate for accessing curation project',
        required=True)
    parser.add_argument(
        '--rdr_run_as',
        action='store',
        dest='rdr_run_as_email',
        help=
        'Service account email address to impersonate for accessing RDR project',
        required=True)
    parser.add_argument('--rdr_key_file',
                        '-s',
                        help="File containing a key for accessing the RDR API")
    args = parser.parse_args()

    pipeline_logging.configure(level=logging.DEBUG, add_console_handler=True)

    main(args.project_id, args.rdr_project_id, args.curation_run_as_email,
         args.rdr_run_as_email, args.rdr_key_file)
