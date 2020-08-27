"""
Background

It is possible for a participant to have multiple records of Physical Measurements. This typically occurs when earlier
entries are incorrect. Data quality would improve if these earlier entries were removed.

Scope: Develop a cleaning rule to remove all but the most recent of each Physical Measurement for all participants.
Relevant measurement_source_concept_ids are listed in query

"""
import logging

# Project imports
import constants.cdr_cleaner.clean_cdr as cdr_consts

LOGGER = logging.getLogger(__name__)

INTERMEDIARY_TABLE = 'DC617_dropped_mult_measurements'

INVALID_MULT_MEASUREMENTS = """
CREATE OR REPLACE TABLE
  `{project}.{sandbox_dataset}.{intermediary_table}` AS (
SELECT
  *
FROM
(SELECT *, ROW_NUMBER() OVER(PARTITION BY person_id, measurement_source_concept_id ORDER BY
      measurement_datetime DESC) AS row_num
 FROM `{project}.{dataset}.measurement`
 WHERE measurement_source_concept_id IN (903131,903119,903107,903124,903115,903126,903136,903118,903135,903132,
                                         903110,903112,903117,903109,903127,1586218,903133,903111,903120,903113,
                                         903129,903105,903125,903114,903134,903116,903106,903108,903123,903130,
                                         903128,903122,903121)
 ORDER BY person_id, measurement_source_concept_id, row_num)
--selection measurements to delete where row_num !=1
WHERE row_num != 1
)
"""

VALID_MEASUREMENTS = """
DELETE FROM
  `{project}.{dataset}.measurement`
WHERE
(person_id, measurement_concept_id, measurement_id, measurement_date, measurement_datetime)
IN( SELECT
    (person_id, measurement_concept_id, measurement_id, measurement_date, measurement_datetime)
    FROM `{project}.{sandbox_dataset}.{intermediary_table}` )
"""


def get_drop_multiple_measurement_queries(project_id, dataset_id,
                                          sandbox_dataset):
    """
    runs the query which removes all multiple me

    :param project_id: Name of the project
    :param dataset_id: Name of the dataset where the queries should be run
    :return:
    """
    queries_list = []

    invalid_measurements_query = dict()
    invalid_measurements_query[
        cdr_consts.QUERY] = INVALID_MULT_MEASUREMENTS.format(
            dataset=dataset_id,
            project=project_id,
            sandbox_dataset=sandbox_dataset,
            intermediary_table=INTERMEDIARY_TABLE)
    queries_list.append(invalid_measurements_query)

    valid_measurements_query = dict()
    valid_measurements_query[cdr_consts.QUERY] = VALID_MEASUREMENTS.format(
        dataset=dataset_id,
        project=project_id,
        sandbox_dataset=sandbox_dataset,
        intermediary_table=INTERMEDIARY_TABLE)
    queries_list.append(valid_measurements_query)

    return queries_list


if __name__ == '__main__':
    import cdr_cleaner.args_parser as parser
    import cdr_cleaner.clean_cdr_engine as clean_engine

    ARGS = parser.parse_args()

    if ARGS.list_queries:
        clean_engine.add_console_logging()
        query_list = clean_engine.get_query_list(
            ARGS.project_id, ARGS.dataset_id,
            [(get_drop_multiple_measurement_queries,)])
        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(ARGS.project_id, ARGS.dataset_id,
                                   [(get_drop_multiple_measurement_queries,)])
