# coding=utf-8
"""
Script to generate the serology dataset in the form C{release_tag}_antibody_quest
containing tables from R2020q4r1_antibody_quest as done in DC-1981.
These will be used to create C{release_tag}_serology in the output project.
Issue: DC-2263
"""
# Python imports
import argparse
import logging

# Third party imports
from google.cloud.bigquery import Dataset

# Project imports
from common import CDR_SCOPES, JINJA_ENV, PERSON
from utils import auth, pipeline_logging
from gcloud.bq import BigQueryClient

LOGGER = logging.getLogger(__name__)

ISSUE_NUMBER = 'DC2263'

S_TITER = 'titer'
S_ROCHE_ORTHO = 'roche_ortho'
S_TEST = 'test'
S_RESULT = 'result'

SEROLOGY_TABLES = [PERSON, S_TITER, S_ROCHE_ORTHO, S_TEST, S_RESULT]

PERSON_QUERY = JINJA_ENV.from_string("""
CREATE TABLE `{{project_id}}.{{dest_dataset_id}}.serology_person`
LIKE `{{project_id}}.{{source_dataset_id}}.person` AS
SELECT  
  serology_person_id 
  , collection_date
  , NULL AS sex_at_birth
  , NULL AS age
  , NULL AS race
  , NULL AS state,
  control_status,
  person_id
FROM `{{project_id}}.{{source_dataset_id}}.person`
WHERE control_status IN ('Negative', 'Non-Control')
AND person_id IN ( 
  SELECT person_id
  FROM `{{project_id}}.{{ct_dataset_id}}.person`)
UNION ALL
# non AoU pids
SELECT *
FROM `{{project_id}}.{{source_dataset_id}}.person`
WHERE control_status NOT IN ('Negative', 'Non-Control')""")

TITER_QUERY = JINJA_ENV.from_string("""
CREATE TABLE `{{project_id}}.{{dest_dataset_id}}.titer` 
LIKE `{{project_id}}.{{source_dataset_id}}.titer` AS
SELECT *
FROM `{{project_id}}.{{source_dataset_id}}.titer`
WHERE serology_person_id IN ( 
  SELECT serology_person_id
  FROM `{{project_id}}.{{dest_dataset_id}}.serology_person`)""")

ROCHE_ORTHO_QUERY = JINJA_ENV.from_string("""
CREATE TABLE `{{project_id}}.{{dest_dataset_id}}.roche_ortho`
LIKE `{{project_id}}.{{source_dataset_id}}.roche_ortho` AS
SELECT  *
FROM `{{project_id}}.{{source_dataset_id}}.roche_ortho`
WHERE serology_person_id IN ( 
  SELECT serology_person_id
  FROM `{{project_id}}.{{dest_dataset_id}}.serology_person`)""")

TEST_QUERY = JINJA_ENV.from_string("""
CREATE TABLE `{{project_id}}.{{dest_dataset_id}}.test`
LIKE `{{project_id}}.{{source_dataset_id}}.test` AS
SELECT *
FROM `{{project_id}}.{{source_dataset_id}}.test`
WHERE serology_person_id IN ( 
  SELECT serology_person_id
  FROM `{{project_id}}.{{dest_dataset_id}}.serology_person`)""")

RESULT_QUERY = JINJA_ENV.from_string("""
CREATE TABLE `{{project_id}}.{{dest_dataset_id}}.result`
LIKE `{{project_id}}.{{source_dataset_id}}.result` AS
SELECT *
FROM `{{project_id}}.{{source_dataset_id}}.result`
WHERE test_id IN ( 
  SELECT distinct test_id 
  FROM `{{project_id}}.{{dest_dataset_id}}.test`)""")

SEROLOGY_QUERIES = {
    PERSON: PERSON_QUERY,  # All table creation queries depend on person
    S_TITER: TITER_QUERY,
    S_ROCHE_ORTHO: ROCHE_ORTHO_QUERY,
    S_TEST: TEST_QUERY,
    S_RESULT: RESULT_QUERY  # Result table creation query depends on test
}


def create_serology_tables(client: BigQueryClient, snapshot_dataset_id: str,
                           src_serology_dataset_id: str,
                           ct_dataset_id: str) -> None:
    """
    Generate id_match tables in the specified snapshot dataset

    :param client: a BigQueryClient
    :param snapshot_dataset_id: Identifies the snapshot dataset (destination)
    :param src_serology_dataset_id: Identifies the source serology dataset
    :param ct_dataset_id: Identifies the Controlled tier dataset
    :return: None
    """
    for table in SEROLOGY_TABLES:
        job = client.query(SEROLOGY_QUERIES[table].render(
            project_id=client.project,
            source_dataset_id=src_serology_dataset_id,
            dest_dataset_id=snapshot_dataset_id,
            ct_dataset_id=ct_dataset_id))
        job.result()
        LOGGER.info(f'Created table {snapshot_dataset_id}.{table}')


def create_serology_snapshot(client: BigQueryClient, release_tag: str,
                             src_serology_dataset_id: str) -> str:
    """
    Generates the serology snapshot dataset based on the release tag

    :param client: a BigQueryClient
    :param release_tag: Release tag for the CDR run
    :param src_serology_dataset_id: Identifies the source serology dataset
    :return: str: Identifies the created snapshot serology dataset
    """
    dataset_id = f"C{release_tag}_antibody_quest"
    dataset = Dataset(f'{client.project}.{dataset_id}')
    dataset.description = f'Source dataset: {src_serology_dataset_id} *_ct views; JIRA issue number: {ISSUE_NUMBER}'
    dataset.labels = {
        'owner': 'curation',
        'release_tag': release_tag,
        'data_tier': 'controlled'
    }
    dataset = client.create_dataset(dataset)
    LOGGER.info(f'Successfully created empty dataset {dataset.dataset_id}')
    return dataset.dataset_id


def get_arg_parser():
    parser = argparse.ArgumentParser(description="""
    Creates a serology snapshot dataset as C{release_tag}_antibody_quest
    using CT and existing serology dataset, R2020q4r1_antibody_quest.""")
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
    parser.add_argument(
        '-s',
        '--src_serology_dataset_id',
        action='store',
        dest='src_serology_dataset_id',
        help='Source serology dataset, currently R2020q4r1_antibody_quest',
        required=True)
    parser.add_argument('-c',
                        '--ct_dataset_id',
                        action='store',
                        dest='ct_dataset_id',
                        help='Controlled tier dataset',
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

    # Create serology dataset
    dataset_id = create_serology_snapshot(bq_client, args.release_tag,
                                          args.src_serology_dataset_id)

    # Create serology tables
    create_serology_tables(bq_client, dataset_id, args.src_serology_dataset_id,
                           args.ct_dataset_id)


if __name__ == '__main__':
    main()
