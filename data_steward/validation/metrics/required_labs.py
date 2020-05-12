import app_identity
import bq_utils
import logging
import common
import oauth2client
import googleapiclient
from constants import bq_utils as bq_consts
from validation.metrics.required_labs_sql import (IDENTIFY_LABS_QUERY,
                                                  CHECK_REQUIRED_LAB_QUERY)

LOGGER = logging.getLogger(__name__)

MEASUREMENT_CONCEPT_SETS_TABLE = 'measurement_concept_sets'
MEASUREMENT_CONCEPT_SETS_DESCENDANTS_TABLE = 'measurement_concept_sets_descendants'


def load_measurement_concept_sets_table(project_id, dataset_id):
    """
    Loads the required lab table from resources/measurement_concept_sets.csv
    into project_id.ehr_ops

    :param project_id: Project where the dataset resides
    :param dataset_id: Dataset where the required lab table needs to be created
    :return: None
    """

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
