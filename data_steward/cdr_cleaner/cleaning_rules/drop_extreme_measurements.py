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

# Project imports
import constants.cdr_cleaner.clean_cdr as cdr_consts

DELETE_HEIGHT_ROWS_QUERY = """
--drop all height recrods out of bounds
DELETE
FROM `{project_id}.{dataset_id}.measurement`
WHERE measurement_source_concept_id = 903133
AND value_as_number NOT BETWEEN 90 AND 228
UNION ALL
--drop BMI row associated with PID
DELETE
FROM `{project_id}.{dataset_id}.measurement`
WHERE person_id IN (SELECT person_id 
  FROM `{project_id}.{dataset_id}.measurement`
  WHERE measurement_source_concept_id = 903133
  AND value_as_number NOT BETWEEN 90 AND 228)
AND measurement_source_concept_id = 903124
"""

DELETE_WEIGHT_ROWS_QUERY = """
--drop all weight records out of bounds
DELETE
FROM `{project_id}.{dataset_id}.measurement`
WHERE measurement_source_concept_id = 903121 
AND value_as_number NOT BETWEEN 30 AND 250
UNION ALL
--drop BMI rows associated with PID
DELETE
FROM `{project_id}.{dataset_id}.measurement`
WHERE person_id IN (SELECT person_id 
  FROM `{project_id}.{dataset_id}.measurement`
  WHERE measurement_source_concept_id = 903121
  AND value_as_number NOT BETWEEN 30 AND 250)
AND measurement_source_concept_id = 903124

"""

DELETE_BMI_ROWS_QUERY = """
DELETE
FROM
  `{project_id}.{dataset_id}.measurement`
WHERE
  measurement_source_concept_id = 903124
  AND value_as_number NOT BETWEEN 10 AND 125
--drop height rows associated with PID
UNION ALL
DELETE
FROM `{project_id}.{dataset_id}.measurement`
WHERE person_id IN (SELECT person_id 
  FROM `{project_id}.{dataset_id}.measurement`
  WHERE measurement_source_concept_id = 903124
  AND value_as_number NOT BETWEEN 10 AND 125)
AND measurement_source_concept_id = 903133
--drop weight rows associated with PID
UNION ALL
DELETE
FROM `{project_id}.{dataset_id}.measurement`
WHERE person_id IN (SELECT person_id 
  FROM `{project_id}.{dataset_id}.measurement`
  WHERE measurement_source_concept_id = 903124
  AND value_as_number NOT BETWEEN 10 AND 125)
AND measurement_source_concept_id = 903121
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
    height_query[cdr_consts.QUERY] = DELETE_HEIGHT_ROWS_QUERY.format(dataset=dataset_id, project=project_id)
    queries_list.append(height_query)

    weight_query = dict()
    weight_query[cdr_consts.QUERY] = DELETE_WEIGHT_ROWS_QUERY.format(dataset=dataset_id, project=project_id)
    queries_list.append(weight_query)

    bmi_query = dict()
    height_query[cdr_consts.QUERY] = DELETE_BMI_ROWS_QUERY.format(dataset=dataset_id, project=project_id)
    queries_list.append(bmi_query)

    return queries_list


if __name__ == '__main__':
    import cdr_cleaner.args_parser as parser
    import cdr_cleaner.clean_cdr_engine as clean_engine

    ARGS = parser.parse_args()
    clean_engine.add_console_logging(ARGS.console_log)
    query_list = get_drop_extreme_measurement_queries(ARGS.project_id, ARGS.dataset_id)
    clean_engine.clean_dataset(ARGS.project_id, query_list)

