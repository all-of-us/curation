"""
Background

The Genomics program requires stable research IDs (RIDs). This is a script that will
add only pid/rid mappings for participants that don't currently exist in the
priamry pid_rid_mapping table.

These records will be appended to the pipeline_tables.pid_rid_mapping table in BigQuery.
Duplicate mappings are not allowed.
"""
# Python imports
import argparse
import inspect
import logging
import time

# Third party imports
from google.cloud import bigquery

# Project imports
from common import (JINJA_ENV, MAX_DEID_DATE_SHIFT, PID_RID_MAPPING,
                    PIPELINE_TABLES)
from utils import auth, bq

LOGGER = logging.getLogger(__name__)

SCOPES = [
    'https://www.googleapis.com/auth/bigquery',
    'https://www.googleapis.com/auth/devstorage.read_write',
]

GET_NEW_MAPPINGS = JINJA_ENV.from_string("""
INSERT INTO  `{{primary.project}}.{{primary.dataset_id}}.{{primary.table_id}}`
(person_id, research_id, shift)
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


def store_to_primary_mapping_table(fq_rdr_mapping_table,
                                   client=None,
                                   run_as=None):
    """
    Store the provided mappings and create required date shifts.

    Curation must maintain a stable pid/rid mapping for participants, as well
    as a date shift integer.  Curation gets the pid/rid mapping table from the
    RDR team as part of their ETL process.  Curation must identify new pid/rid
    mapping pairs, create random date shifts for each pair, and store the three
    tuple to the pipeline_tables.pid_rid_mapping table.

    This script requires either a client object be passed as a parameter or an
    email address to impersonate be provided.  If both are missing, the script
    will not execute!

    The script assumes the newly provided mapping table exists in the same
    project as the primary mapping table.

    :param fq_rdr_mapping_table: a dot separated fully qualified name of the
        recently imported pid_rid_mapping table.
    :param client: a client object to use for querying both tables
    :param run_as: the email address of the service account to run as.  if
        impersonation is already set up, pass the existing client object instead.

    :return: None
    :raises: RuntimeError if client and run_as are both None.  BigQuery errors.
    """
    project, dataset, table = fq_rdr_mapping_table.split('.')

    LOGGER.info(
        f'RDR mapping info: project -> {project}\tdataset -> {dataset}\ttable -> {table}'
    )
    LOGGER.info(f'Primary mapping info: project -> {project}\t'
                f'dataset -> {PIPELINE_TABLES}\ttable -> {PID_RID_MAPPING}')

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

    # primary table ref
    dataset_ref = bigquery.DatasetReference(project, PIPELINE_TABLES)
    primary_mapping_table = bigquery.TableReference(dataset_ref,
                                                    PID_RID_MAPPING)

    # Query job config
    labels = {
        'rdr_mapping_table':
            '-'.join(fq_rdr_mapping_table.lower().split('.')[1:])[-63:],
        'module_name':
            __file__.lower().replace('/', '-').replace('.', '-')[-63:]
    }

    job_prefix = inspect.currentframe().f_code.co_name
    query = GET_NEW_MAPPINGS.render(rdr_table=rdr_table,
                                    primary=primary_mapping_table,
                                    max_shift=MAX_DEID_DATE_SHIFT)

    LOGGER.info(f'Preparing to run query:\n{query}')

    config = bigquery.job.QueryJobConfig(labels=labels)

    new_mappings_job = client.query(query,
                                    job_config=config,
                                    job_id_prefix=job_prefix)

    # wait for the query to finish
    LOGGER.info('Waiting for pid/rid/shift storage query to finish.')
    new_mappings_job.result()
    LOGGER.info('Query has finished.')

    LOGGER.info(f'{new_mappings_job.num_dml_affected_rows} mapping records '
                f'added to {primary_mapping_table}')

    # check if errors were encountered and report any
    if new_mappings_job.errors:
        LOGGER.error(f'Query job finished with errors.  See details of job '
                     f'with job_id_prefix {job_prefix} and labels {labels}')
    else:
        LOGGER.info('Query job finished without errors.')


def check_table_name(name_str):
    """
    Make sure the tablename provided follows the fully qualified format.

    If the table name cannot be split into three sections by splitting on a
    dot, '.', then reject the provided name as incomplete.

    :param name_str: The name of the table as provided by the end user.

    :return: a fully qualified table name string.
    :raises: ValueError if the name cannot be split.
    """
    name_parts = name_str.split('.')
    if len(name_parts) != 3:
        raise ValueError(f'A fully qualified table name must be of the form '
                         f'<project_id>.<dataset_id>.<table_name> .  You '
                         f'provided {name_str}')

    return name_str


def check_email_address(address_str):
    """
    Make sure the string provided looks like an email address.

    If the string does not contain `@`, then reject the provided string.

    :param address_str: The email address as provided by the end user.

    :return: a validated email address.
    :raises: ValueError if the address does not contain `@`.
    """
    if '@' not in address_str:
        raise ValueError(f'An email address must be specified.  '
                         f'You supplied {address_str}')

    return address_str


def process_mappings(raw_args=None):
    """
    Allow mapping arguments to be validated from other python modules.

    Use parser to validate arguments and then run mapping storage.  This
    is not strictly required, but will help ensure the
    `store_to_primary_mapping_table` function works as designed.

    :params raw_args: If provided, a list of arguments and values.
        If not provided, defaults to command line values.
    """
    LOGGER.info("Beginning pid/rid/shift storage process.")

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

    store_to_primary_mapping_table(args.rdr_mapping, run_as=args.run_as)

    LOGGER.info("Finished pid/rid/shift storage process.")


if __name__ == '__main__':
    from utils import pipeline_logging

    pipeline_logging.configure()
    process_mappings()
