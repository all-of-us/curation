"""
Background

The Genomics program requires stable research IDs (RIDs). This is a script that will
add only pid/rid mappings for participants that don't currently exist in the 
priamry pid_rid_mapping table. 

The regisered tier deid module contained the logic to generate a _deid_map table
containing person_id, research_id, and date_shift.  The date shift can be created
here as it was created there.

These records will be appended to the pipeline_tables.pid_rid_mapping table in BigQuery.
There cannot be duplicate mappings.
"""
# Python imports
import argparse
import inspect
import logging
import time

# Third party imports
from google.cloud import bigquery

# Project imports
from common import JINJA_ENV
from utils import auth, bq, pipeline_logging

LOGGER = logging.getLogger(__name__)

SCOPES = [
    'https://www.googleapis.com/auth/bigquery',
    'https://www.googleapis.com/auth/devstorage.read_write',
]

GET_NEW_MAPPINGS = JINJA_ENV.from_string("""
SELECT 
  person_id
  , research_id
-- generates random shifts between 1 and max_shift inclusive --
  , CAST(FLOOR({{max_shift}} * RAND() + 1) AS INT64) as shift
FROM `{{rdr_table.project}}.{{rdr_table.dataset_id}}.{{rdr_table.table_id}}`
WHERE person_id not in (
  SELECT person_id
  FROM `{{primary.project}}.{{primary.dataset_id}}.{{primary.table_id}}`)
-- This is just to make sure we don't duplicate either person_id OR research_id --
AND research_id not in (
  SELECT research_id
  FROM `{{primary.project}}.{{primary.dataset_id}}.{{primary.table_id}}`)
""")

MAX_SHIFT = 365
MAPPING_DATASET = 'pipeline_tables'
MAPPING_TABLE = 'pid_rid_mapping'


def store_to_primary_mapping_table(fq_rdr_mapping_table,
                                   client=None,
                                   run_as=None):
    """
    Method to generate mapping table rows to append to existing mapping table: pipeline_tables._deid_map table
    Retrieves participants from the participant summary API based on specific parameters. Only retrieving participants
    who were submitted during the cutoff date range with no age limit and also retrieving participants outside of the
    cutoff date range above the max age.

    Creates the research_ids and date shift based on previous logic used in DEID. Queries the current
    pipeline_tables._deid_map table to retrieve current research_ids to ensure all research_ids are unique.

    """
    project, dataset, table = fq_rdr_mapping_table.split('.')

    LOGGER.info(
        f'RDR mapping info: project -> {project}\tdataset -> {dataset}\ttable -> {table}'
    )
    LOGGER.info(f'Primary mapping info: project -> {project}\t'
                f'dataset -> {MAPPING_DATASET}\ttable -> {MAPPING_TABLE}')

    if not client and not run_as:
        LOGGER.error('Run cannot proceed without proper credentials')
        raise RuntimeError(
            'Provide either a client or a service account to impersonate.')

    # set up an impersonated client if one is not provided
    if not client:
        LOGGER.info(
            'Using impersonation credentials and creating a new client.')
        # get credentials and create client
        impersonation_creds = auth.get_impersonation_credentials(run_as, SCOPES)

        client = bq.get_client(project, credentials=impersonation_creds)
    else:
        LOGGER.info('Client object provided and being used.')

    # rdr table ref
    dataset_ref = bigquery.DatasetReference(project, dataset)
    rdr_table = bigquery.TableReference(dataset_ref, table)

    # Query job config
    labels = {
        'fq_rdr_mapping_table': fq_rdr_mapping_table,
        'module_name': __file__
    }

    job_prefix = inspect.currentframe().f_code.co_name
    query = GET_NEW_MAPPINGS.render(rdr_table=rdr_table,
                                    primary=primary_mapping_table,
                                    max_shift=MAX_SHIFT)

    LOGGER.info(f'Preparing to run query:\n{query}')

    config = bigquery.job.QueryJobConfig(
        destination=f'{project}.{MAPPING_DATASET}.{MAPPING_TABLE}',
        labels=labels,
        write_disposition='WRITE_APPEND')

    new_mappings_job = client.query(query,
                                    job_config=config,
                                    job_id_prefix=job_prefix)

    # wait for the query to finish
    LOGGER.info('Waiting for pid/rid/shift storage query to finish.')
    new_mappings_job.result()
    LOGGER.info('Query has finished.')

    # check if errors were encountered and report any
    if new_mappings_job.errors:
        LOGGER.error(f'Query job finished with errors.  See details of job '
                     f'with job_id_prefix {job_prefix} and labels {labels}')
    else:
        LOGGER.info('Query job finished without errors.')


def check_table_name(name_str):
    name_parts = name_str.split('.')
    if len(name_parts) != 3:
        raise ValueError(f'A fully qualified table name must be of the form '
                         f'<project_id>.<dataset_id>.<table_name> .  You '
                         f'provided {name_str}')

    return name_str


def check_email_address(address_str):
    if '@' not in address_str:
        raise ValueError(f'An email address must be specified.  '
                         f'You supplied {address_str}')

    return address_str


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Add new mappings to our primary pid/rid mapping table.',
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        '-r',
        '--fq_rdr_mapping',
        action='store',
        dest='rdr_mapping',
        help=('The fully qualified rdr mapping table name.  '
              'The project_id will be extracted from this table name.'),
        type=check_table_name,
        required=True)
    parser.add_argument(
        '-i',
        '--run_as',
        action='store',
        dest='run_as',
        help=('The email address of the service account to impersonate.'),
        type=check_email_address)
    args = parser.parse_args()

    pipeline_logging.configure()
    store_to_primary_mapping_table(args.rdr_mapping, run_as=args.run_as)
