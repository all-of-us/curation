"""
 Drop duplicate states from participant records.

 Currently there are some individuals with multiple state records from consent.
 We need to include only one state, dropping all but the most recent record.
"""
import os
import logging

import constants.cdr_cleaner.clean_cdr as cdr_consts

LOGGER = logging.getLogger(__name__)

module_name = os.path.basename(__file__[:-3])
INTERMEDIARY_TABLE = module_name + '_observation'

DROP_DUPLICATE_STATES_INTERMEDIARY_QUERY = """
CREATE OR REPLACE TABLE
  `{project}.{sandbox}.{intermediary_table}` AS
SELECT
  DISTINCT observation_id
FROM (
  SELECT
    observation_id,
    ROW_NUMBER() OVER(PARTITION BY person_id ORDER BY observation_datetime DESC) AS this_row
  FROM
    `{project}.{dataset}.observation`
  WHERE
    observation_source_concept_id = 1585249 )
WHERE
  this_row > 1
"""

DROP_DUPLICATE_STATES_QUERY = """
DELETE
FROM
  `{project}.{dataset}.{table}`
WHERE
  observation_id IN (
  SELECT
    observation_id
  FROM
    `{project}.{sandbox}.{intermediary_table}`)
"""


def get_drop_duplicate_states_queries(project_id, dataset_id,
                                      sandbox_dataset_id):
    """

    This function returns the parsed queries to delete the duplicate state records.

    :param project_id: Name of the project
    :param dataset_id: Name of the dataset where the queries should be run
    :param sandbox_dataset_id: Name of the sandbox dataset
    :return:
    """

    queries_list = []

    # Save duplicate state records being deleted in sandbox `dataset.intermediary_table` .
    query = dict()
    query[cdr_consts.QUERY] = DROP_DUPLICATE_STATES_INTERMEDIARY_QUERY.format(
        dataset=dataset_id,
        project=project_id,
        intermediary_table=INTERMEDIARY_TABLE,
        sandbox=sandbox_dataset_id)
    queries_list.append(query)

    # Delete duplicate state records from observation table and _mapping_observation_table
    for table in ['observation', '_mapping_observation']:
        query = dict()

        query[cdr_consts.QUERY] = DROP_DUPLICATE_STATES_QUERY.format(
            dataset=dataset_id,
            project=project_id,
            table=table,
            intermediary_table=INTERMEDIARY_TABLE,
            sandbox=sandbox_dataset_id)
        queries_list.append(query)

    return queries_list


if __name__ == '__main__':
    import cdr_cleaner.args_parser as parser
    import cdr_cleaner.clean_cdr_engine as clean_engine

    ARGS = parser.parse_args()

    if ARGS.list_queries:
        clean_engine.add_console_logging()
        query_list = clean_engine.get_query_list(
            ARGS.project_id, ARGS.dataset_id,
            [(get_drop_duplicate_states_queries,)])
        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(ARGS.project_id, ARGS.dataset_id,
                                   [(get_drop_duplicate_states_queries,)])
