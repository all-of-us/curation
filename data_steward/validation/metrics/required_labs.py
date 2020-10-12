# Python imports
import logging

# Third party imports
import googleapiclient
import oauth2client
from google.cloud.exceptions import NotFound

# Project imports
import app_identity
import bq_utils
import common
from constants import bq_utils as bq_consts
from utils import bq
from utils.bq import JINJA_ENV
from validation.metrics.required_labs_sql import (IDENTIFY_LABS_QUERY,
                                                  CHECK_REQUIRED_LAB_QUERY)

LOGGER = logging.getLogger(__name__)

MEASUREMENT_CONCEPT_SETS_TABLE = '_measurement_concept_sets'
MEASUREMENT_CONCEPT_SETS_DESCENDANTS_TABLE = '_measurement_concept_sets_descendants'

ROW_COUNT_QUERY = JINJA_ENV.from_string("""SELECT table_id, row_count
FROM `{{project_id}}.{{dataset_id}}.__TABLES__`
WHERE table_id IN ('concept', 'concept_ancestor')""")


def load_measurement_concept_sets_table(project_id, dataset_id):
    """
    Loads the required lab table from resource_files/measurement_concept_sets.csv
    into project_id.ehr_ops

    :param project_id: Project where the dataset resides
    :param dataset_id: Dataset where the required lab table needs to be created
    :return: None
    """

    table_name = f'{project_id}.{dataset_id}.{MEASUREMENT_CONCEPT_SETS_TABLE}'

    client = bq.get_client(project_id)
    dataset = client.dataset(dataset_id)
    table_ref = dataset.table(MEASUREMENT_CONCEPT_SETS_TABLE)

    # will check to see if MEASUREMENT_CONCEPT_SETS_TABLE exists, table will be created if it is not found
    try:
        client.get_table(table_ref)
    except NotFound:
        bq.create_tables(client=client,
                         project_id=project_id,
                         fq_table_names=[table_name],
                         exists_ok=False,
                         fields=None)

    try:
        LOGGER.info(
            'Upload {measurement_concept_sets_table}.csv to {dataset_id} in {project_id}'
            .format(
                measurement_concept_sets_table=MEASUREMENT_CONCEPT_SETS_TABLE,
                dataset_id=dataset_id,
                project_id=project_id))

        bq_utils.load_table_from_csv(project_id, dataset_id,
                                     MEASUREMENT_CONCEPT_SETS_TABLE)

    except (oauth2client.client.HttpAccessTokenRefreshError,
            googleapiclient.errors.HttpError):

        LOGGER.exception(
            f"FAILED:  CSV file could not be uploaded:\n{app_identity}")


def load_measurement_concept_sets_descendants_table(project_id, dataset_id):
    """
    Loads the measurement_concept_sets_descendants table using LOINC group and LOINC hierarchy
    into project_id.ehr_ops

    :param project_id: Project where the dataset resides
    :param dataset_id: Dataset where the required lab table needs to be created
    :return: None
    """

    descendants_table_name = f'{project_id}.{dataset_id}.{MEASUREMENT_CONCEPT_SETS_DESCENDANTS_TABLE}'

    client = bq.get_client(project_id)
    dataset = client.dataset(dataset_id)
    vocab_dataset = client.dataset(common.VOCABULARY_DATASET)

    # concept table and concept ancestor table source tables
    concept_source_table = vocab_dataset.table(common.CONCEPT)
    concept_ancestor_source_table = vocab_dataset.table(common.CONCEPT_ANCESTOR)

    # concept table and concept ancestor table destination tables
    concept_dest_table = dataset.table(common.CONCEPT)
    concept_ancestor_dest_table = dataset.table(common.CONCEPT_ANCESTOR)

    descendants_table_ref = dataset.table(
        MEASUREMENT_CONCEPT_SETS_DESCENDANTS_TABLE)
    concept_table_ref = dataset.table(common.CONCEPT)
    concept_ancestor_table_ref = dataset.table(common.CONCEPT_ANCESTOR)

    # query will check the row counts of each table in the specified dataset
    row_count_query = ROW_COUNT_QUERY.render(project_id=project_id,
                                             dataset_id=dataset_id)

    job = client.query(row_count_query)
    results = job.result()

    # will check if either the CONCEPT and CONCEPT_ANCESTOR tables is empty or not
    # If so, the CONCEPT and CONCEPT_ANCESTOR tables will be copied from the common.VOCABULARY
    # table to the destination dataset
    if results.total_rows == 0:
        client.copy_table(concept_source_table, concept_dest_table)
        client.copy_table(concept_ancestor_source_table,
                          concept_ancestor_dest_table)
    else:
        pass

    # will check to see if CONCEPT table exists, will be copied from the CONCEPT table in the
    # most recent VOCABULARY dataset if it is not found
    try:
        client.get_table(concept_table_ref)
    except NotFound:
        client.copy_table(concept_source_table, concept_dest_table)

    # will check to see if CONCEPT_ANCESTOR table exists, will be copied from the CONCEPT_ANCESTOR table
    # in the most recent VOCABULARY dataset if it is not found
    try:
        client.get_table(concept_ancestor_table_ref)
    except NotFound:
        client.copy_table(concept_ancestor_source_table,
                          concept_ancestor_dest_table)

    # will check to see if MEASUREMENT_CONCEPT_SETS_DESCENDANTS_TABLE exists, will be created if table is not found
    try:
        client.get_table(descendants_table_ref)
    except NotFound:
        bq.create_tables(client=client,
                         project_id=project_id,
                         fq_table_names=[descendants_table_name],
                         exists_ok=False,
                         fields=None)

    identify_labs_query = IDENTIFY_LABS_QUERY.format(
        project_id=project_id,
        ehr_ops_dataset_id=dataset_id,
        vocab_dataset_id=dataset_id,
        measurement_concept_sets=MEASUREMENT_CONCEPT_SETS_TABLE)

    try:
        LOGGER.info(f"Running query {identify_labs_query}")
        results = bq_utils.query(
            identify_labs_query,
            use_legacy_sql=False,
            destination_table_id=MEASUREMENT_CONCEPT_SETS_DESCENDANTS_TABLE,
            retry_count=bq_consts.BQ_DEFAULT_RETRY_COUNT,
            write_disposition=bq_consts.WRITE_TRUNCATE,
            destination_dataset_id=dataset_id,
            batch=None)

    except (oauth2client.client.HttpAccessTokenRefreshError,
            googleapiclient.errors.HttpError):
        LOGGER.exception(f"FAILED:  Clean rule not executed:\n{app_identity}")

    query_job_id = results['jobReference']['jobId']
    bq_utils.wait_on_jobs([query_job_id])

    updated_rows = results.get("totalRows")
    if updated_rows is not None:
        LOGGER.info(
            f"Query returned {updated_rows} rows for {dataset_id}.{MEASUREMENT_CONCEPT_SETS_DESCENDANTS_TABLE}"
        )


def get_lab_concept_summary_query(hpo_id):
    """
    Get the query that checks if the HPO site has submitted the required labs
    :param hpo_id: 
    :return: 
    """
    project_id = app_identity.get_application_id()
    dataset_id = bq_utils.get_dataset_id()
    hpo_measurement_table = bq_utils.get_table_id(hpo_id, common.MEASUREMENT)

    # Create measurement_concept_sets_table if not exist
    if not bq_utils.table_exists(MEASUREMENT_CONCEPT_SETS_TABLE, dataset_id):
        load_measurement_concept_sets_table(project_id, dataset_id)

    # Create measurement_concept_sets_descendants_table if not exist
    if not bq_utils.table_exists(MEASUREMENT_CONCEPT_SETS_DESCENDANTS_TABLE,
                                 dataset_id):
        load_measurement_concept_sets_descendants_table(project_id, dataset_id)

    return CHECK_REQUIRED_LAB_QUERY.format(
        project_id=project_id,
        ehr_ops_dataset_id=dataset_id,
        hpo_measurement_table=hpo_measurement_table,
        measurement_concept_sets_descendants=
        MEASUREMENT_CONCEPT_SETS_DESCENDANTS_TABLE)


if __name__ == '__main__':
    import cdr_cleaner.args_parser as parser

    ARGS = parser.parse_args()
    # Upload the required lab lookup table
    load_measurement_concept_sets_table(project_id=ARGS.project_id,
                                        dataset_id=ARGS.dataset_id)
    # Create the measurement concept sets descendant table
    load_measurement_concept_sets_descendants_table(project_id=ARGS.project_id,
                                                    dataset_id=ARGS.dataset_id)
