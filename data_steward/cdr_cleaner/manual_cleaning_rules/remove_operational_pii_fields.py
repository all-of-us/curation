# coding=utf-8
"""
Removes operational Pii fields from rdr export.

Some new operational fields exists, that were not blacklisted in the RDR export. These rows needs to be dropped in the 
RDR load process so they do not make it to CDR. These do not have concept_id maps. 
The supplemental operational_pii_fields.csv shows all present PPI codes without a mapped concepts,
indicating which should be dropped in the “drop_value” column
"""
import logging
import os

import bq_utils
import constants.cdr_cleaner.clean_cdr as cdr_consts
import sandbox

LOGGER = logging.getLogger(__name__)

module_name = os.path.basename(file[:-3])

OPERATIONAL_PII_FIELDS_TABLE = 'operational_pii_fields'
INTERMEDIARY_TABLE = module_name + '_observation'

OPERATION_PII_FIELDS_INTERMEDIARY_QUERY = """
CREATE OR REPLACE TABLE
    `{project}.{sandbox}.{intermediary_table}` AS (
  SELECT
    *
  FROM
    {dataset}.observation
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
    {dataset}.observation
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


def get_remove_operational_pii_fields_query(project_id, dataset_id, sandbox_dataset_id):
    """

    :param project_id: Name of the project
    :param dataset_id: Name of the dataset where the queries should be run
    :param sandbox_dataset_id: Name of the sandbox dataset
    :return:
    """

    bq_utils.load_table_from_csv(project_id=project_id, dataset_id=sandbox_dataset_id,
                                 table_name=OPERATIONAL_PII_FIELDS_TABLE, csv_path=None, fields=None)

    queries_list = []

    # Save operational pii records being deleted in sandbox `dataset.intermediary_table` .
    query = dict()
    query[cdr_consts.QUERY] = OPERATION_PII_FIELDS_INTERMEDIARY_QUERY.format(dataset=dataset_id,
                                                                             project=project_id,
                                                                             intermediary_table=INTERMEDIARY_TABLE,
                                                                             pii_fields_table=
                                                                             OPERATIONAL_PII_FIELDS_TABLE,
                                                                             sandbox=sandbox_dataset_id
                                                                             )
    queries_list.append(query)

    # Delete operational pii records from observation table
    query = dict()

    query[cdr_consts.QUERY] = DELETE_OPERATIONAL_PII_FIELDS_QUERY.format(dataset=dataset_id,
                                                                         project=project_id,
                                                                         pii_fields_table=
                                                                         OPERATIONAL_PII_FIELDS_TABLE,
                                                                         sandbox=sandbox_dataset_id
                                                                         )
    queries_list.append(query)

    return queries_list


if __name__ == '__main__':
    import cdr_cleaner.args_parser as parser
    import cdr_cleaner.clean_cdr_engine as clean_engine

    ARGS = parser.parse_args()

    sandbox_dataset_id = sandbox.create_sandbox_dataset(project_id=ARGS.project_id, dataset_id=ARGS.dataset_id)

    clean_engine.add_console_logging(ARGS.console_log)
    query_list = get_remove_operational_pii_fields_query(ARGS.project_id, ARGS.dataset_id, sandbox_dataset_id)
    clean_engine.clean_dataset(ARGS.project_id, query_list)
