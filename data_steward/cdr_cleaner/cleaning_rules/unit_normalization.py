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

# Third party imports
from google.api_core.exceptions import Conflict

# Project Imports
import constants.bq_utils as bq_consts
import resources
from common import JINJA_ENV, MEASUREMENT
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule
from cdr_cleaner.cleaning_rules.clean_height_weight import CleanHeightAndWeight
from cdr_cleaner.cleaning_rules.measurement_table_suppression import (
    MeasurementRecordsSuppression)
from constants.cdr_cleaner import clean_cdr as cdr_consts
from gcloud.bq import BigQueryClient

LOGGER = logging.getLogger(__name__)

UNIT_MAPPING_TABLE = '_unit_mapping'
UNIT_MAPPING_FILE = '_unit_mapping.csv'
UNIT_MAPPING_TABLE_DISPOSITION = 'WRITE_EMPTY'

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
    (measurement_concept_id, unit_concept_id)
  )
""")

UNIT_NORMALIZATION_QUERY = JINJA_ENV.from_string("""
UPDATE `{{project_id}}.{{dataset_id}}.{{measurement_table}}` m1
  SET
  m1.value_as_number =  CASE ut.transform_value_as_number
    WHEN "(1/x)" THEN IF (m1.value_as_number = 0, 0, 1/m1.value_as_number)
    WHEN "(x-32)*(5/9)" THEN (m1.value_as_number-32)*(5/9)
    WHEN "*0.02835" THEN m1.value_as_number * 0.02835
    WHEN "*0.394" THEN m1.value_as_number * 0.394
    WHEN "*0.4536" THEN m1.value_as_number * 0.4536
    WHEN "*1" THEN m1.value_as_number * 1
    WHEN "*10" THEN m1.value_as_number * 10
    WHEN "*10^(-1)" THEN m1.value_as_number * 0.1
    WHEN "*10^(-2)" THEN m1.value_as_number * 0.01
    WHEN "*10^(3)" THEN m1.value_as_number * 1000
    WHEN "*10^(-3)" THEN m1.value_as_number * 0.001
    WHEN "*10^(6)" THEN m1.value_as_number * 1000000
    WHEN "*10^(-6)" THEN m1.value_as_number * 0.000001
    -- when ut.transform_value_as_number is null due to left join --
    ELSE m1.value_as_number
  END,

  m1.unit_concept_id = COALESCE(ut.set_unit_concept_id, ut.unit_concept_id),

  m1.range_low = CASE ut.transform_value_as_number
    WHEN "(1/x)" THEN 1/m1.range_low
    WHEN "(x-32)*(5/9)" THEN (m1.range_low-32)*(5/9)
    WHEN "*0.02835" THEN m1.range_low * 0.02835
    WHEN "*0.394" THEN m1.range_low * 0.394
    WHEN "*0.4536" THEN m1.range_low * 0.4536
    WHEN "*1" THEN m1.range_low * 1
    WHEN "*10" THEN m1.range_low * 10
    WHEN "*10^(-1)" THEN m1.range_low * 0.1
    WHEN "*10^(-2)" THEN m1.range_low * 0.01
    WHEN "*10^(3)" THEN m1.range_low * 1000
    WHEN "*10^(-3)" THEN m1.range_low * 0.001
    WHEN "*10^(6)" THEN m1.range_low * 1000000
    WHEN "*10^(-6)" THEN m1.range_low * 0.000001
    -- when ut.transform_value_as_number is null due to left join --
    ELSE m1.range_low
  END,

  m1.range_high = CASE ut.transform_value_as_number
    WHEN "(1/x)" THEN 1/m1.range_high
    WHEN "(x-32)*(5/9)" THEN (m1.range_high-32)*(5/9)
    WHEN "*0.02835" THEN m1.range_high * 0.02835
    WHEN "*0.394" THEN m1.range_high * 0.394
    WHEN "*0.4536" THEN m1.range_high * 0.4536
    WHEN "*1" THEN m1.range_high * 1
    WHEN "*10" THEN m1.range_high * 10
    WHEN "*10^(-1)" THEN m1.range_high * 0.1
    WHEN "*10^(-2)" THEN m1.range_high * 0.01
    WHEN "*10^(3)" THEN m1.range_high * 1000
    WHEN "*10^(-3)" THEN m1.range_high * 0.001
    WHEN "*10^(6)" THEN m1.range_high * 1000000
    WHEN "*10^(-6)" THEN m1.range_high * 0.000001
    -- when ut.transform_value_as_number is null due to left join --
    ELSE m1.range_high
  END

-- use unit normalization table to identify records to update with the WHERE clause below --
FROM
  `{{project_id}}.{{sandbox_dataset_id}}.{{unit_table_name}}` ut
WHERE
  m1.measurement_concept_id = ut.measurement_concept_id
  AND m1.unit_concept_id = ut.unit_concept_id
  AND m1.measurement_id IN
    (SELECT
       distinct measurement_id
     FROM `{{project_id}}.{{sandbox_dataset_id}}.{{intermediary_table}}`)
""")


class UnitNormalization(BaseCleaningRule):
    """
    Units for labs/measurements will be normalized.
    """

    def __init__(self,
                 project_id,
                 dataset_id,
                 sandbox_dataset_id,
                 table_namer=None):
        """
        Initialize the class with proper information.

        Set the issue numbers, description and affected datasets. As other tickets may affect
        this SQL, append them to the list of Jira Issues.
        DO NOT REMOVE ORIGINAL JIRA ISSUE NUMBERS!
        """
        desc = ('Units for labs/measurements will be normalized using '
                'unit_mapping lookup table.')
        super().__init__(
            issue_numbers=['DC414', 'DC700', 'DC2453', 'DC2454'],
            description=desc,
            affected_datasets=[
                cdr_consts.REGISTERED_TIER_DEID_CLEAN,
                cdr_consts.CONTROLLED_TIER_DEID_CLEAN
            ],
            affected_tables=[MEASUREMENT],
            project_id=project_id,
            dataset_id=dataset_id,
            sandbox_dataset_id=sandbox_dataset_id,
            depends_on=[MeasurementRecordsSuppression, CleanHeightAndWeight],
            table_namer=table_namer)

    def setup_rule(self, client: BigQueryClient):
        """
        Load required resources prior to executing cleaning rule queries.
        
        Here, UNIT_MAPPING_TABLE is created based off of the CSV file 
        UNIT_MAPPING_FILE. UNIT_MAPPING_TABLE is used in both [CT/RT] deid 
        clean tiers. If UNIT_MAPPING_TABLE is already there, setup_rule 
        skips the table preparation and the existing UNIT_MAPPING_TABLE will 
        be used for cleaning.

        Method to run data upload options before executing the first cleaning
        rule of a class.  For example, if your class requires loading a static
        table, that load operation should be defined here.  It SHOULD NOT BE
        defined as part of get_query_specs().

        :param client: A BigQueryClient
        :return:
        :raises: BadRequest, OSError, AttributeError, TypeError, ValueError if
            the load job fails. Error raised from client.upload_csv_data_to_bq_table
            helper method.
        """

        # creating _unit_mapping table
        unit_mapping_table = (f'{self.project_id}.'
                              f'{self.sandbox_dataset_id}.'
                              f'{UNIT_MAPPING_TABLE}')
        client.create_tables([unit_mapping_table], exists_ok=True)
        # Uploading data to _unit_mapping table
        unit_mappings_csv_path = os.path.join(resources.resource_files_path,
                                              UNIT_MAPPING_FILE)
        try:
            _ = client.upload_csv_data_to_bq_table(
                self.sandbox_dataset_id, UNIT_MAPPING_TABLE,
                unit_mappings_csv_path, UNIT_MAPPING_TABLE_DISPOSITION)

            LOGGER.info(
                f"Created {self.sandbox_dataset_id}.{UNIT_MAPPING_TABLE} and "
                f"loaded data from {unit_mappings_csv_path}")

        except Conflict as c:
            LOGGER.info(
                f"{self.sandbox_dataset_id}.{UNIT_MAPPING_TABLE} already exists. "
                f"{c.errors}")

    def get_query_specs(self):
        """
        :return:
        """
        sandbox_query = {
            cdr_consts.QUERY:
                SANDBOX_UNITS_QUERY.render(
                    project_id=self.project_id,
                    sandbox_dataset_id=self.sandbox_dataset_id,
                    intermediary_table=self.sandbox_table_for(MEASUREMENT),
                    dataset_id=self.dataset_id,
                    unit_table_name=UNIT_MAPPING_TABLE,
                    measurement_table=MEASUREMENT)
        }

        update_query = {
            cdr_consts.QUERY:
                UNIT_NORMALIZATION_QUERY.render(
                    project_id=self.project_id,
                    dataset_id=self.dataset_id,
                    sandbox_dataset_id=self.sandbox_dataset_id,
                    intermediary_table=self.sandbox_table_for(MEASUREMENT),
                    unit_table_name=UNIT_MAPPING_TABLE,
                    measurement_table=MEASUREMENT)
        }

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
        return self.sandbox_table_for('measurement')

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
