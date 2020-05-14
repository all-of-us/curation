import logging
import os

import bq_utils
import constants.bq_utils as bq_consts
import resources
from constants.cdr_cleaner import clean_cdr as cdr_consts

LOGGER = logging.getLogger(__name__)
UNIT_MAPPING_TABLE = 'unit_mapping'
UNIT_MAPPING_FILE = 'unit_mapping'
MEASUREMENT = 'measurement'
UNIT_MAPPING_FIELDS = [{
    "type": "integer",
    "name": "measurement_concept_id",
    "mode": "nullable"
}, {
    "type": "integer",
    "name": "unit_concept_id",
    "mode": "nullable"
}, {
    "type": "integer",
    "name": "set_unit_concept_id",
    "mode": "nullable"
}, {
    "type": "string",
    "name": "transform_value_as_number",
    "mode": "nullable"
}]

INSERT_UNITS_QUERY = """
INSERT INTO `{project_id}.{dataset_id}.{units_table_id}`
 (measurement_concept_id, unit_concept_id, set_unit_concept_id, transform_value_as_number)
VALUES {mapping_list}
"""

UNIT_NORMALIZATION_QUERY = """
SELECT
  measurement_id,
  person_id,
  measurement_concept_id,
  measurement_date,
  measurement_datetime,
  measurement_type_concept_id,
  operator_concept_id,
  CASE transform_value_as_number
    WHEN "(1/x)" THEN IF (value_as_number = 0, 0, 1/value_as_number)
    WHEN "(x-32)*(5/9)" THEN (value_as_number-32)*(5/9)
    WHEN "*0.02835" THEN value_as_number * 0.02835
    WHEN "*0.394" THEN value_as_number * 0.394
    WHEN "*0.4536" THEN value_as_number * 0.4536
    WHEN "*1" THEN value_as_number * 1
    WHEN "*10" THEN value_as_number * 10
    WHEN "*10^(-1)" THEN value_as_number * 0.1
    WHEN "*10^(-2)" THEN value_as_number * 0.01
    WHEN "*10^(3)" THEN value_as_number * 1000
    WHEN "*10^(-3)" THEN value_as_number * 0.001
    WHEN "*10^(6)" THEN value_as_number * 1000000
    WHEN "*10^(-6)" THEN value_as_number * 0.000001
    -- when transform_value_as_number is null due to left join
    ELSE value_as_number
END
  AS value_as_number,
  value_as_concept_id,
  COALESCE(set_unit_concept_id, unit_concept_id) AS unit_concept_id,
  CASE transform_value_as_number
    WHEN "(1/x)" THEN 1/range_low
    WHEN "(x-32)*(5/9)" THEN (range_low-32)*(5/9)
    WHEN "*0.02835" THEN range_low * 0.02835
    WHEN "*0.394" THEN range_low * 0.394
    WHEN "*0.4536" THEN range_low * 0.4536
    WHEN "*1" THEN range_low * 1
    WHEN "*10" THEN range_low * 10
    WHEN "*10^(-1)" THEN range_low * 0.1
    WHEN "*10^(-2)" THEN range_low * 0.01
    WHEN "*10^(3)" THEN range_low * 1000
    WHEN "*10^(-3)" THEN range_low * 0.001
    WHEN "*10^(6)" THEN range_low * 1000000
    WHEN "*10^(-6)" THEN range_low * 0.000001
    -- when transform_value_as_number is null due to left join
    ELSE range_low
END
  AS range_low,
  CASE transform_value_as_number
    WHEN "(1/x)" THEN 1/range_high
    WHEN "(x-32)*(5/9)" THEN (range_high-32)*(5/9)
    WHEN "*0.02835" THEN range_high * 0.02835
    WHEN "*0.394" THEN range_high * 0.394
    WHEN "*0.4536" THEN range_high * 0.4536
    WHEN "*1" THEN range_high * 1
    WHEN "*10" THEN range_high * 10
    WHEN "*10^(-1)" THEN range_high * 0.1
    WHEN "*10^(-2)" THEN range_high * 0.01
    WHEN "*10^(3)" THEN range_high * 1000
    WHEN "*10^(-3)" THEN range_high * 0.001
    WHEN "*10^(6)" THEN range_high * 1000000
    WHEN "*10^(-6)" THEN range_high * 0.000001
    -- when transform_value_as_number is null due to left join
    ELSE range_high
END
  AS range_high,
  provider_id,
  visit_occurrence_id,
  measurement_source_value,
  measurement_source_concept_id,
  unit_source_value,
  value_source_value
FROM
    `{project_id}.{dataset_id}.{measurement_table}`
LEFT JOIN
  `{project_id}.{dataset_id}.{unit_table_name}`
USING
  (measurement_concept_id, unit_concept_id)
"""


def get_mapping_list(unit_mappings_list):
    """
    Filters out name columns from unit_mappings.csv file and returns list of mappings suitable for BQ

    :param unit_mappings_list:
    :return: formatted list suitable for insert in BQ:
            (measurement_concept_id, unit_concept_id, set_unit_concept_id, transform_value_as_number)
    """
    pair_exprs = []
    for route_mapping_dict in unit_mappings_list:
        pair_expr = '({measurement_concept_id}, {unit_concept_id}, {set_unit_concept_id}, ' \
                    '\"{transform_value_as_number}\")'.format(**route_mapping_dict)
        pair_exprs.append(pair_expr)
    formatted_mapping_list = ', '.join(pair_exprs)
    return formatted_mapping_list


def create_unit_mapping_table(project_id, dataset_id):
    """
    This function creates the unit_mapping table and populate it with the values from resources/unit_mapping.csv
    :param project_id:
    :param dataset_id:
    :return:
    """
    bq_utils.create_table(table_id=UNIT_MAPPING_TABLE,
                          fields=UNIT_MAPPING_FIELDS,
                          drop_existing=True,
                          dataset_id=dataset_id)

    unit_mappings_csv = os.path.join(resources.resource_path,
                                     UNIT_MAPPING_FILE + ".csv")
    unit_mappings_list = resources.csv_to_list(unit_mappings_csv)
    unit_mappings_populate_query = INSERT_UNITS_QUERY.format(
        dataset_id=dataset_id,
        project_id=project_id,
        units_table_id=UNIT_MAPPING_TABLE,
        mapping_list=get_mapping_list(unit_mappings_list))
    result = bq_utils.query(unit_mappings_populate_query)
    LOGGER.info(f"Created {dataset_id}.{UNIT_MAPPING_TABLE}")
    return result


def get_unit_normalization_query(project_id, dataset_id):
    """
    :param project_id:
    :param dataset_id:
    :return:
    """
    queries = []

    query = dict()
    query[cdr_consts.QUERY] = UNIT_NORMALIZATION_QUERY.format(
        project_id=project_id,
        dataset_id=dataset_id,
        unit_table_name=UNIT_MAPPING_TABLE,
        measurement_table=MEASUREMENT)
    query[cdr_consts.DESTINATION_TABLE] = MEASUREMENT
    query[cdr_consts.DISPOSITION] = bq_consts.WRITE_TRUNCATE
    query[cdr_consts.DESTINATION_DATASET] = dataset_id
    queries.append(query)
    return queries


if __name__ == '__main__':
    import cdr_cleaner.args_parser as parser
    import cdr_cleaner.clean_cdr_engine as clean_engine

    ARGS = parser.parse_args()
    clean_engine.add_console_logging(ARGS.console_log)

    create_unit_mapping_table(ARGS.project_id, ARGS.dataset_id)
    query_list = get_unit_normalization_query(ARGS.project_id, ARGS.dataset_id)
    clean_engine.clean_dataset(ARGS.project_id, query_list)
