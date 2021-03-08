"""
Units for labs/measurements will be normalized.

In unit_mappings.csv for the measurement concepts Column A which have the unit concepts in column B,
standardize the unit concept to the value in Column C and transform the value of the
measurement according to the transformation in column D.

Original Issue: DC-414
"""

# Python imports
import logging
import os

# Project Imports
import constants.bq_utils as bq_consts
import resources
import utils.bq as bq
from common import JINJA_ENV
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule
from cdr_cleaner.cleaning_rules.clean_height_weight import CleanHeightAndWeight
from cdr_cleaner.cleaning_rules.measurement_table_suppression import (
    MeasurementRecordsSuppression)
from constants.cdr_cleaner import clean_cdr as cdr_consts

LOGGER = logging.getLogger(__name__)

UNIT_MAPPING_TABLE = '_unit_mapping'
UNIT_MAPPING_FILE = '_unit_mapping.csv'
MEASUREMENT = 'measurement'
UNIT_MAPPING_TABLE_DISPOSITION = bq.bigquery.job.WriteDisposition.WRITE_EMPTY

SANDBOX_UNITS_QUERY = JINJA_ENV.from_string("""
CREATE OR REPLACE TABLE
    `{{project_id}}.{{sandbox_dataset_id}}.{{intermediary_table}}` AS(
SELECT
    m.*
  FROM
  `{{project_id}}.{{dataset_id}}.{{measurement_table}}` as m
INNER JOIN
  `{{project_id}}.{{sandbox_dataset_id}}.{{unit_table_name}}` as um
USING
  (measurement_concept_id,
    unit_concept_id))
    """)

UNIT_NORMALIZATION_QUERY = JINJA_ENV.from_string("""SELECT
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
    -- when transform_value_as_number is null due to left join --
  ELSE
  value_as_number
END
  AS value_as_number,
  value_as_concept_id,
  COALESCE(set_unit_concept_id,
    unit_concept_id) AS unit_concept_id,
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
    -- when transform_value_as_number is null due to left join --
  ELSE
  range_low
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
    -- when transform_value_as_number is null due to left join --
  ELSE
  range_high
END
  AS range_high,
  provider_id,
  visit_occurrence_id,
  measurement_source_value,
  measurement_source_concept_id,
  unit_source_value,
  value_source_value
FROM
  `{{project_id}}.{{dataset_id}}.{{measurement_table}}`
LEFT JOIN
  `{{project_id}}.{{sandbox_dataset_id}}.{{unit_table_name}}`
USING
  (measurement_concept_id,
    unit_concept_id)""")


class UnitNormalization(BaseCleaningRule):
    """
    Units for labs/measurements will be normalized..
    """

    def __init__(self, project_id, dataset_id, sandbox_dataset_id):
        """
        Initialize the class with proper information.

        Set the issue numbers, description and affected datasets. As other tickets may affect
        this SQL, append them to the list of Jira Issues.
        DO NOT REMOVE ORIGINAL JIRA ISSUE NUMBERS!
        """
        desc = ('Units for labs/measurements will be normalized using '
                'unit_mapping lookup table.')
        super().__init__(
            issue_numbers=['DC414', 'DC700'],
            description=desc,
            affected_datasets=[
                cdr_consts.DEID_CLEAN, cdr_consts.CONTROLLED_TIER_DEID_CLEAN
            ],
            affected_tables=['measurement'],
            project_id=project_id,
            dataset_id=dataset_id,
            sandbox_dataset_id=sandbox_dataset_id,
            depends_on=[MeasurementRecordsSuppression, CleanHeightAndWeight])

    def setup_rule(self, client=None):
        """
        Load required resources prior to executing cleaning rule queries.

        Method to run data upload options before executing the first cleaning
        rule of a class.  For example, if your class requires loading a static
        table, that load operation should be defined here.  It SHOULD NOT BE
        defined as part of get_query_specs().
        :param client:
        :return:

        :raises:  BadRequest, OSError, AttributeError, TypeError, ValueError if
            the load job fails. Error raised from bq.upload_csv_data_to_bq_table
            helper function.
        """

        # creating _unit_mapping table
        unit_mapping_table = (f'{self.project_id}.'
                              f'{self.sandbox_dataset_id}.'
                              f'{UNIT_MAPPING_TABLE}')
        bq.create_tables(
            client,
            self.project_id,
            [unit_mapping_table],
        )
        # Uploading data to _unit_mapping table
        unit_mappings_csv_path = os.path.join(resources.resource_files_path,
                                              UNIT_MAPPING_FILE)
        result = bq.upload_csv_data_to_bq_table(client, self.sandbox_dataset_id,
                                                UNIT_MAPPING_TABLE,
                                                unit_mappings_csv_path,
                                                UNIT_MAPPING_TABLE_DISPOSITION)
        LOGGER.info(
            f"Created {self.sandbox_dataset_id}.{UNIT_MAPPING_TABLE} and "
            f"loaded data from {unit_mappings_csv_path}")

    def get_query_specs(self):
        """
        :return:
        """
        sandbox_query = dict()
        sandbox_query[cdr_consts.QUERY] = SANDBOX_UNITS_QUERY.render(
            project_id=self.project_id,
            sandbox_dataset_id=self.sandbox_dataset_id,
            intermediary_table=self.get_sandbox_tablenames()[0],
            dataset_id=self.dataset_id,
            unit_table_name=UNIT_MAPPING_TABLE,
            measurement_table=MEASUREMENT)

        update_query = dict()
        update_query[cdr_consts.QUERY] = UNIT_NORMALIZATION_QUERY.render(
            project_id=self.project_id,
            dataset_id=self.dataset_id,
            sandbox_dataset_id=self.sandbox_dataset_id,
            unit_table_name=UNIT_MAPPING_TABLE,
            measurement_table=MEASUREMENT)
        update_query[cdr_consts.DESTINATION_TABLE] = MEASUREMENT
        update_query[cdr_consts.DISPOSITION] = bq_consts.WRITE_TRUNCATE
        update_query[cdr_consts.DESTINATION_DATASET] = self.dataset_id
        return [sandbox_query, update_query]

    def setup_validation(self, client):
        """
        Run required steps for validation setup
        """
        pass

    def validate_rule(self, client):
        """
        Validates the cleaning rule which deletes or updates the data from the tables
        """
        pass

    def get_sandbox_table_name(self):
        return f'{self._issue_numbers[0].lower()}_measurement'

    def get_sandbox_tablenames(self):
        return [self.get_sandbox_table_name()]


if __name__ == '__main__':
    import cdr_cleaner.args_parser as parser
    import cdr_cleaner.clean_cdr_engine as clean_engine

    ARGS = parser.parse_args()

    if ARGS.list_queries:
        clean_engine.add_console_logging()
        query_list = clean_engine.get_query_list(ARGS.project_id,
                                                 ARGS.dataset_id,
                                                 ARGS.sandbox_dataset_id,
                                                 [(UnitNormalization,)])
        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(ARGS.project_id, ARGS.dataset_id,
                                   ARGS.sandbox_dataset_id,
                                   [(UnitNormalization,)])
