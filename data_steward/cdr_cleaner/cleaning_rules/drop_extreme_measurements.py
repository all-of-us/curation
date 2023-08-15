"""
Background: For physical measurements collected by the Program, warnings or limits exist for height, weight, and BMI;
however, there are still some outliers in the dataset from before the addition of these limits (or in cases where limits
are more relaxed). These outliers need to be dropped to improve data quality.

Scope: Create the following cleaning rule (all Weights are in KILOGRAMS all Heights are in CENTIMETERS): In the
measurements table, extreme values of height, weight, and BMI should be dropped along with related values for the
participant.

This is expected to drop a very small number of rows (less than 300 total) based on values in the current CDR.

Original Issue: DC-624 
"""

# Python Imports
import logging

# Project Imports
from common import JINJA_ENV, MEASUREMENT
import constants.cdr_cleaner.clean_cdr as cdr_consts
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule
from cdr_cleaner.cleaning_rules.calculate_bmi import CalculateBmi

LOGGER = logging.getLogger(__name__)

CREATE_SANDBOX = JINJA_ENV.from_string("""
CREATE OR REPLACE TABLE `{{project}}.{{sandbox_dataset}}.{{sandbox_table}}` AS (
SELECT * FROM `{{project}}.{{dataset}}.measurement` m
WHERE
    (
        -- select BMI row associated with extreme height --
        EXISTS (
            WITH extreme_heights AS (
                SELECT person_id, measurement_type_concept_id, measurement_datetime
                FROM `{{project}}.{{dataset}}.measurement`
                WHERE measurement_source_concept_id = 903133 AND value_as_number NOT BETWEEN 90 AND 228
            )
            SELECT 1 FROM extreme_heights
            WHERE m.measurement_source_concept_id = 903124
            AND m.person_id = extreme_heights.person_id
            AND m.measurement_type_concept_id = extreme_heights.measurement_type_concept_id
            AND m.measurement_datetime = extreme_heights.measurement_datetime
        )
        -- select all extreme height --
        OR (m.measurement_source_concept_id = 903133 AND m.value_as_number NOT BETWEEN 90 AND 228)
    ) OR (
        -- select BMI row associated with extreme weight --
        EXISTS (
            WITH extreme_weights AS (
                SELECT person_id, measurement_type_concept_id, measurement_datetime
                FROM `{{project}}.{{dataset}}.measurement`
                WHERE measurement_source_concept_id = 903121 AND value_as_number NOT BETWEEN 30 AND 250
            )
            SELECT 1 FROM extreme_weights
            WHERE m.measurement_source_concept_id = 903124
            AND m.person_id = extreme_weights.person_id
            AND m.measurement_type_concept_id = extreme_weights.measurement_type_concept_id
            AND m.measurement_datetime = extreme_weights.measurement_datetime
        )
        -- select all extreme weight --
        OR (m.measurement_source_concept_id = 903121 AND m.value_as_number NOT BETWEEN 30 AND 250)
    ) OR (
        -- select height & weight rows associated with extreme BMI --
        EXISTS (
            WITH extreme_bmi AS (
                SELECT person_id, measurement_type_concept_id, measurement_datetime
                FROM `{{project}}.{{dataset}}.measurement`
                WHERE measurement_source_concept_id = 903124 AND value_as_number NOT BETWEEN 10 AND 125
            )
            SELECT 1 FROM extreme_bmi
            WHERE m.measurement_source_concept_id IN (903133, 903121)
            AND m.person_id = extreme_bmi.person_id
            AND m.measurement_type_concept_id = extreme_bmi.measurement_type_concept_id
            AND m.measurement_datetime = extreme_bmi.measurement_datetime
        )
        -- select all extreme BMI --
        OR (m.measurement_source_concept_id = 903124 AND m.value_as_number NOT BETWEEN 10 AND 125)    
    )
)
""")

DELETE_SANDBOXED = JINJA_ENV.from_string("""
DELETE FROM `{{project}}.{{dataset}}.measurement`
WHERE measurement_id IN (
  SELECT measurement_id
  FROM `{{project}}.{{sandbox_dataset}}.{{sandbox_table}}`
)
""")


class DropExtremeMeasurements(BaseCleaningRule):

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

        desc = ('remove extreme physical measurement outliers')
        super().__init__(issue_numbers=['DC-624', 'DC-849'],
                         description=desc,
                         affected_datasets=[cdr_consts.RDR],
                         affected_tables=[MEASUREMENT],
                         project_id=project_id,
                         dataset_id=dataset_id,
                         sandbox_dataset_id=sandbox_dataset_id,
                         depends_on=[CalculateBmi],
                         table_namer=table_namer)

    def get_query_specs(self):
        """
        Return a list of dictionary query specifications.

        :return:  A list of dictionaries. Each dictionary contains a single query
            and a specification for how to execute that query. The specifications
            are optional but the query is required.
        """

        sandbox_query = {
            cdr_consts.QUERY:
                CREATE_SANDBOX.render(
                    project=self.project_id,
                    dataset=self.dataset_id,
                    sandbox_dataset=self.sandbox_dataset_id,
                    sandbox_table=self.sandbox_table_for(MEASUREMENT))
        }

        delete_query = {
            cdr_consts.QUERY:
                DELETE_SANDBOXED.render(
                    project=self.project_id,
                    dataset=self.dataset_id,
                    sandbox_dataset=self.sandbox_dataset_id,
                    sandbox_table=self.sandbox_table_for(MEASUREMENT))
        }

        return [sandbox_query, delete_query]

    def setup_rule(self, client):
        """
        Function to run any data upload options before executing a query.
        """
        pass

    def setup_validation(self, client):
        """
        Run required steps for validation setup
        """
        raise NotImplementedError("Please fix me.")

    def validate_rule(self, client):
        """
        Validates the cleaning rule which deletes or updates the data from the tables
        """
        raise NotImplementedError("Please fix me.")

    def get_sandbox_tablenames(self):
        return [self.sandbox_table_for(table) for table in self.affected_tables]


if __name__ == '__main__':
    import cdr_cleaner.args_parser as parser
    import cdr_cleaner.clean_cdr_engine as clean_engine

    ext_parser = parser.get_argument_parser()

    ARGS = ext_parser.parse_args()

    if ARGS.list_queries:
        clean_engine.add_console_logging()
        query_list = clean_engine.get_query_list(ARGS.project_id,
                                                 ARGS.dataset_id,
                                                 ARGS.sandbox_dataset_id,
                                                 [(DropExtremeMeasurements,)])
        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(ARGS.project_id, ARGS.dataset_id,
                                   ARGS.sandbox_dataset_id,
                                   [(DropExtremeMeasurements,)])
