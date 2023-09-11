# Python imports
import logging

# Third party imports
import googleapiclient
import oauth2client

# Project imports
import app_identity
import bq_utils
import resources
import common
from constants import bq_utils as bq_consts
from gcloud.bq import BigQueryClient
from validation.metrics.required_labs_sql import (IDENTIFY_LABS_QUERY,
                                                  CHECK_REQUIRED_LAB_QUERY)

LOGGER = logging.getLogger(__name__)

MEASUREMENT_CONCEPT_SETS_TABLE = '_measurement_concept_sets'
MEASUREMENT_CONCEPT_SETS_DESCENDANTS_TABLE = '_measurement_concept_sets_descendants'

ROW_COUNT_QUERY = common.JINJA_ENV.from_string("""
SELECT table_id, row_count
FROM `{{project_id}}.{{dataset_id}}.__TABLES__`""")


def check_and_copy_tables(client, dataset_id):
    """
    Will check that all the required tables exist and if not, they will be created
    or copied from another table.

    :param client: a BigQueryClient, contains the project where the dataset resides
    :param dataset_id: Dataset where the required lab table needs to be created
    :return: None
    """

    descendants_table_name = f'{client.project}.{dataset_id}.{MEASUREMENT_CONCEPT_SETS_DESCENDANTS_TABLE}'
    concept_sets_table_name = f'{client.project}.{dataset_id}.{MEASUREMENT_CONCEPT_SETS_TABLE}'

    dataset = client.dataset(dataset_id)
    vocab_dataset = client.dataset(common.VOCABULARY_DATASET)

    # concept table and concept ancestor table source tables
    concept_source_table = vocab_dataset.table(common.CONCEPT)
    concept_ancestor_source_table = vocab_dataset.table(common.CONCEPT_ANCESTOR)

    # concept table and concept ancestor table destination tables
    concept_dest_table = dataset.table(common.CONCEPT)
    concept_ancestor_dest_table = dataset.table(common.CONCEPT_ANCESTOR)

    # query will check the row counts of each table in the specified dataset
    row_count_query = ROW_COUNT_QUERY.render(project_id=client.project,
                                             dataset_id=dataset_id)

    results_dataframe = client.query(row_count_query).to_dataframe()
    empty_results_dataframe = results_dataframe[(
        results_dataframe['row_count'] == 0)]

    # checks if CONCEPT and CONCEPT_ANCESTOR tables exist, if they don't, they are copied from the
    # CONCEPT and CONCEPT_ANCESTOR tables in common.VOCABULARY
    if common.CONCEPT not in (results_dataframe['table_id']).values:
        client.copy_table(concept_source_table, concept_dest_table)
    if common.CONCEPT_ANCESTOR not in (results_dataframe['table_id']).values:
        client.copy_table(concept_ancestor_source_table,
                          concept_ancestor_dest_table)

    # checks if CONCEPT and CONCEPT_ANCESTOR tables are empty, if they are, they are copied from the CONCEPT and
    # CONCEPT_ANCESTOR tables in common.VOCABULARY
    if common.CONCEPT in (empty_results_dataframe['table_id']).values:
        client.copy_table(concept_source_table, concept_dest_table)
    if common.CONCEPT_ANCESTOR in (empty_results_dataframe['table_id']).values:
        client.copy_table(concept_ancestor_source_table,
                          concept_ancestor_dest_table)

    # checks if MEASUREMENT_CONCEPT_SETS_TABLE and MEASUREMENT_CONCEPT_SETS_DESCENDANTS_TABLE exist, if they
    # do not exist, they will be created
    if MEASUREMENT_CONCEPT_SETS_TABLE not in results_dataframe[
            'table_id'].values:
        client.create_tables(fq_table_names=[concept_sets_table_name],
                             exists_ok=True,
                             fields=None)
    if MEASUREMENT_CONCEPT_SETS_DESCENDANTS_TABLE not in results_dataframe[
            'table_id'].values:
        client.create_tables(fq_table_names=[descendants_table_name],
                             exists_ok=True,
                             fields=None)


def load_measurement_concept_sets_table(client, dataset_id):
    """
    Loads the required lab table from resource_files/measurement_concept_sets.csv
    into project_id.ehr_ops

    :param client: a BigQueryClient, contains the project where the dataset resides
    :param dataset_id: Dataset where the required lab table needs to be created
    :return: None
    """

    check_and_copy_tables(client, dataset_id)

    try:
        LOGGER.info(
            'Upload {measurement_concept_sets_table}.csv to {dataset_id} in {project_id}'
            .format(
                measurement_concept_sets_table=MEASUREMENT_CONCEPT_SETS_TABLE,
                dataset_id=dataset_id,
                project_id=client.project))

        bq_utils.load_table_from_csv(client.project, dataset_id,
                                     MEASUREMENT_CONCEPT_SETS_TABLE)

    except (oauth2client.client.HttpAccessTokenRefreshError,
            googleapiclient.errors.HttpError):

        LOGGER.exception(
            f"FAILED:  CSV file could not be uploaded:\n{app_identity}")


def load_measurement_concept_sets_descendants_table(client, dataset_id):
    """
    Loads the measurement_concept_sets_descendants table using LOINC group and LOINC hierarchy
    into project_id.ehr_ops

    :param client: a BigQueryClient, contains the project where the dataset resides
    :param dataset_id: Dataset where the required lab table needs to be created
    :return: None
    """

    check_and_copy_tables(client, dataset_id)

    identify_labs_query = IDENTIFY_LABS_QUERY.format(
        project_id=client.project,
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


def get_lab_concept_summary_query(client, hpo_id):
    """
    Get the query that checks if the HPO site has submitted the required labs
    :param client: a BigQueryClient
    :param hpo_id: Identifies the HPO site
    :return: 
    """
    dataset_id = common.BIGQUERY_DATASET_ID
    hpo_measurement_table = resources.get_table_id(common.MEASUREMENT,
                                                   hpo_id=hpo_id)

    # Create measurement_concept_sets_table if not exist
    if not client.table_exists(MEASUREMENT_CONCEPT_SETS_TABLE, dataset_id):
        load_measurement_concept_sets_table(client, dataset_id)

    # Create measurement_concept_sets_descendants_table if not exist
    if not client.table_exists(MEASUREMENT_CONCEPT_SETS_DESCENDANTS_TABLE,
                               dataset_id):
        load_measurement_concept_sets_descendants_table(client, dataset_id)

    return CHECK_REQUIRED_LAB_QUERY.format(
        project_id=client.project,
        ehr_ops_dataset_id=dataset_id,
        hpo_measurement_table=hpo_measurement_table,
        measurement_concept_sets_descendants=
        MEASUREMENT_CONCEPT_SETS_DESCENDANTS_TABLE)


if __name__ == '__main__':
    import cdr_cleaner.args_parser as parser

    ARGS = parser.parse_args()
    bq_client = BigQueryClient(ARGS.project_id)
    # Upload the required lab lookup table
    load_measurement_concept_sets_table(client=bq_client,
                                        dataset_id=ARGS.dataset_id)
    # Create the measurement concept sets descendant table
    load_measurement_concept_sets_descendants_table(client=bq_client,
                                                    dataset_id=ARGS.dataset_id)
