# coding=utf-8
"""
Removes operational Pii fields from rdr export.

Some new operational fields exists, that were not blacklisted in the RDR export. These rows needs to be dropped in the 
RDR load process so they do not make it to CDR. These do not have concept_id maps. 
The supplemental operational_pii_fields.csv shows all present PPI codes without a mapped concepts,
indicating which should be dropped in the “drop_value” column
"""
import logging

import bq_utils
import constants.cdr_cleaner.clean_cdr as cdr_consts
from utils import sandbox

LOGGER = logging.getLogger(__name__)

OPERATIONAL_PII_FIELDS_TABLE = '_operational_pii_fields'
INTERMEDIARY_TABLE = 'remove_operational_pii_fields_observation'

OPERATION_PII_FIELDS_INTERMEDIARY_QUERY = """
CREATE OR REPLACE TABLE
    `{project}.{sandbox}.{intermediary_table}` AS (
  SELECT
    *
  FROM
    `{project}.{dataset}.observation`
  WHERE
    observation_id IN (
    SELECT
      observation_id
    FROM
      `{project}.{sandbox}.{pii_fields_table}` as pii
    JOIN
      `{project}.{dataset}.observation` as ob
    ON
      (pii.observation_source_value=ob.observation_source_value)
    WHERE
      drop_value=TRUE))
"""

DELETE_OPERATIONAL_PII_FIELDS_QUERY = """
DELETE
FROM
    `{project}.{dataset}.observation`
  WHERE
    observation_id IN (
    SELECT
      observation_id
    FROM
      `{project}.{sandbox}.{pii_fields_table}` as pii
    JOIN
      `{project}.{dataset}.observation` as ob
    ON
      (pii.observation_source_value=ob.observation_source_value)
    WHERE
      drop_value=TRUE)
"""


def load_operational_pii_fields_lookup_table(project_id, sandbox_dataset_id):
    """
        Loads the operational pii fields from resource_files/operational_pii_fields.csv
        into project_id.sandbox_dataset_id.operational_pii_fields in BQ

        :param project_id: Project where the sandbox dataset resides
        :param sandbox_dataset_id: Dataset where the smoking lookup table needs to be created
        :return: None
    """
    bq_utils.load_table_from_csv(project_id,
                                 sandbox_dataset_id,
                                 OPERATIONAL_PII_FIELDS_TABLE,
                                 csv_path=None,
                                 fields=None)


def _get_intermediary_table_query(dataset_id, project_id, sandbox_dataset_id):
    """
    parses the intermediary query used to store records being deleted.

    :param project_id: project id associated with the dataset to run the queries on
    :param dataset_id: dataset id to run the queries on
    :param sandbox_dataset_id: dataset id of the sandbox
    :return: query string
    """
    return OPERATION_PII_FIELDS_INTERMEDIARY_QUERY.format(
        dataset=dataset_id,
        project=project_id,
        intermediary_table=INTERMEDIARY_TABLE,
        pii_fields_table=OPERATIONAL_PII_FIELDS_TABLE,
        sandbox=sandbox_dataset_id)


def _get_delete_query(dataset_id, project_id, sandbox_dataset_id):
    """
    parses the query used to delete the operational pii.

    :param project_id: project id associated with the dataset to run the queries on
    :param dataset_id: dataset id to run the queries on
    :param sandbox_dataset_id: dataset id of the sandbox
    :return: query string
    """

    return DELETE_OPERATIONAL_PII_FIELDS_QUERY.format(
        dataset=dataset_id,
        project=project_id,
        pii_fields_table=OPERATIONAL_PII_FIELDS_TABLE,
        sandbox=sandbox_dataset_id)


def get_remove_operational_pii_fields_query(project_id, dataset_id,
                                            sandbox_dataset_id):
    """

    :param project_id: Name of the project
    :param dataset_id: Name of the dataset where the queries should be run
    :param sandbox_dataset_id: Name of the sandbox dataset
    :return:
    """
    # fetch sandbox_dataset_id
    if sandbox_dataset_id is None:
        sandbox_dataset_id = sandbox.get_sandbox_dataset_id(dataset_id)

    load_operational_pii_fields_lookup_table(
        project_id=project_id, sandbox_dataset_id=sandbox_dataset_id)

    queries_list = []

    # Save operational pii records being deleted in sandbox `dataset.intermediary_table` .
    query = dict()
    query[cdr_consts.QUERY] = _get_intermediary_table_query(
        dataset_id, project_id, sandbox_dataset_id)

    queries_list.append(query)

    # Delete operational pii records from observation table
    query = dict()

    query[cdr_consts.QUERY] = _get_delete_query(dataset_id, project_id,
                                                sandbox_dataset_id)
    queries_list.append(query)

    return queries_list


if __name__ == '__main__':
    import cdr_cleaner.args_parser as parser
    import cdr_cleaner.clean_cdr_engine as clean_engine

    ARGS = parser.parse_args()

    if ARGS.list_queries:
        clean_engine.add_console_logging()
        query_list = clean_engine.get_query_list(
            ARGS.project_id, ARGS.dataset_id, ARGS.sandbox_dataset_id,
            [(get_remove_operational_pii_fields_query,)])
        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(ARGS.project_id, ARGS.dataset_id,
                                   ARGS.sandbox_dataset_id,
                                   [(get_remove_operational_pii_fields_query,)])
