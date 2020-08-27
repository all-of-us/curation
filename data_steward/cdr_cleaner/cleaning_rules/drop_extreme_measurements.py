"""
Background

Background: For physical measurements collected by the Program, warnings or limits exist for height, weight, and BMI;
however, there are still some outliers in the dataset from before the addition of these limits (or in cases where limits
are more relaxed). These outliers need to be dropped to improve data quality.

Scope: Create the following cleaning rule (all Weights are in KILOGRAMS all Heights are in CENTIMETERS): In the
measurements table, extreme values of height, weight, and BMI should be dropped along with related values for the
participant.

This is expected to drop a very small number of rows (less than 300 total) based on values in the current CDR.

"""
import logging

# Project imports
import constants.cdr_cleaner.clean_cdr as cdr_consts

LOGGER = logging.getLogger(__name__)

DELETE_HEIGHT_ROWS_QUERY = """
DELETE FROM `{project_id}.{dataset_id}.measurement` m
WHERE 
  EXISTS (
  --subquery to select associated bmi records
  WITH outbound_heights AS (
  SELECT person_id, measurement_datetime
  FROM `{project_id}.{dataset_id}.measurement`
  WHERE measurement_source_concept_id = 903133
  AND value_as_number NOT BETWEEN 90 AND 228
  )
--drop BMI row associated with PID where height is out of bounds
(SELECT person_id FROM outbound_heights
WHERE m.measurement_source_concept_id = 903124
AND m.measurement_datetime = outbound_heights.measurement_datetime)
)
--drop all height records out of bounds
OR (m.measurement_source_concept_id = 903133
AND value_as_number NOT BETWEEN 90 AND 228)
"""

DELETE_WEIGHT_ROWS_QUERY = """
DELETE FROM `{project_id}.{dataset_id}.measurement` m
WHERE EXISTS (
  --subquery to select associated bmi records
  WITH outbound_weights AS (
  SELECT person_id, measurement_datetime
  FROM `{project_id}.{dataset_id}.measurement`
  WHERE measurement_source_concept_id = 903121
  AND value_as_number NOT BETWEEN 30 AND 250
  )
--drop BMI row associated with PID where height is out of bounds
(SELECT person_id FROM outbound_weights
WHERE m.measurement_source_concept_id = 903124
AND m.measurement_datetime = outbound_weights.measurement_datetime)
)
--drop all height records out of bounds
OR (m.measurement_source_concept_id = 903121
AND value_as_number NOT BETWEEN 30 AND 250)

"""

DELETE_BMI_ROWS_QUERY = """
DELETE FROM `{project_id}.{dataset_id}.measurement` m
WHERE 
  EXISTS (
  --subquery to select associated height and weight records
  WITH outbound_bmi AS (
  SELECT person_id, measurement_datetime
  FROM `{project_id}.{dataset_id}.measurement`
  WHERE measurement_source_concept_id = 903124
  AND value_as_number NOT BETWEEN 10 AND 125
  )
--drop height & weight rows associated with PID where bmi is out of bounds
(SELECT person_id FROM outbound_bmi
WHERE m.measurement_source_concept_id IN(903133, 903121)
AND m.measurement_datetime = outbound_bmi.measurement_datetime)
)
--drop all bmi records out of bounds
OR (m.measurement_source_concept_id = 903124
AND value_as_number NOT BETWEEN 10 AND 125)
"""


def get_drop_extreme_measurement_queries(project_id, dataset_id):
    """
    runs the query which removes all multiple me

    :param project_id: Name of the project
    :param dataset_id: Name of the dataset where the queries should be run
    :return:
    """
    queries_list = []

    height_query = dict()
    height_query[cdr_consts.QUERY] = DELETE_HEIGHT_ROWS_QUERY.format(
        dataset_id=dataset_id, project_id=project_id)
    queries_list.append(height_query)

    weight_query = dict()
    weight_query[cdr_consts.QUERY] = DELETE_WEIGHT_ROWS_QUERY.format(
        dataset_id=dataset_id, project_id=project_id)
    queries_list.append(weight_query)

    bmi_query = dict()
    bmi_query[cdr_consts.QUERY] = DELETE_BMI_ROWS_QUERY.format(
        dataset_id=dataset_id, project_id=project_id)
    queries_list.append(bmi_query)

    return queries_list


if __name__ == '__main__':
    import cdr_cleaner.args_parser as parser
    import cdr_cleaner.clean_cdr_engine as clean_engine

    ARGS = parser.parse_args()

    if ARGS.list_queries:
        clean_engine.add_console_logging()
        query_list = clean_engine.get_query_list(
            ARGS.project_id, ARGS.dataset_id,
            [(get_drop_extreme_measurement_queries,)])
        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(ARGS.project_id, ARGS.dataset_id,
                                   [(get_drop_extreme_measurement_queries,)])
