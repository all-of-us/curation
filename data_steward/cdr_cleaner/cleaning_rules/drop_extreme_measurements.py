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
import constants.bq_utils as bq_consts
import constants.cdr_cleaner.clean_cdr as cdr_consts
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule
from cdr_cleaner.cleaning_rules.calculate_bmi import CalculateBmi

LOGGER = logging.getLogger(__name__)

SANDBOX_HEIGHT_WEIGHT_ROWS_QUERY = JINJA_ENV.from_string("""
SELECT
    *
FROM `{{project_id}}.{{dataset_id}}.measurement` m
WHERE
    (
    EXISTS (
        --subquery to select associated bmi records --
        WITH outbound_heights AS (
            SELECT person_id, measurement_datetime
            FROM `{{project_id}}.{{dataset_id}}.measurement`
            WHERE measurement_source_concept_id = 903133
            AND value_as_number NOT BETWEEN 90 AND 228
        )
        --drop BMI row associated with PID where height is out of bounds --
        (
            SELECT person_id FROM outbound_heights
            WHERE m.measurement_source_concept_id = 903124
            AND m.measurement_datetime = outbound_heights.measurement_datetime
        )
    )
    --drop all height records out of bounds --
    OR (m.measurement_source_concept_id = 903133 AND value_as_number NOT BETWEEN 90 AND 228)
) OR (
    EXISTS (
        --subquery to select associated bmi records --
        WITH outbound_weights AS (
            SELECT person_id, measurement_datetime
            FROM `{{project_id}}.{{dataset_id}}.measurement`
            WHERE measurement_source_concept_id = 903121
            AND value_as_number NOT BETWEEN 30 AND 250
        )
        --drop BMI row associated with PID where weight is out of bounds --
        (
            SELECT person_id FROM outbound_weights
            WHERE m.measurement_source_concept_id = 903124
            AND m.measurement_datetime = outbound_weights.measurement_datetime
        )
    )
    --drop all weight records out of bounds --
    OR (m.measurement_source_concept_id = 903121 AND value_as_number NOT BETWEEN 30 AND 250)
) OR (
    EXISTS (
        --subquery to select associated height and weight records --
        WITH outbound_bmi AS (
            SELECT person_id, measurement_datetime
            FROM `{{project_id}}.{{dataset_id}}.measurement`
            WHERE measurement_source_concept_id = 903124
            AND value_as_number NOT BETWEEN 10 AND 125
        )
        --drop height & weight rows associated with PID where bmi is out of bounds --
        (
            SELECT person_id FROM outbound_bmi
            WHERE m.measurement_source_concept_id IN (903133, 903121)
            AND m.measurement_datetime = outbound_bmi.measurement_datetime
        )
    )
    --drop all bmi records out of bounds --
    OR (m.measurement_source_concept_id = 903124 AND value_as_number NOT BETWEEN 10 AND 125)    
)
""")

DELETE_HEIGHT_ROWS_QUERY = JINJA_ENV.from_string("""
DELETE FROM `{{project_id}}.{{dataset_id}}.measurement` m
WHERE
    EXISTS (
        --subquery to select associated bmi records --
        WITH outbound_heights AS (
            SELECT person_id, measurement_datetime
            FROM `{{project_id}}.{{dataset_id}}.measurement`
            WHERE measurement_source_concept_id = 903133
            AND value_as_number NOT BETWEEN 90 AND 228
        )
        --drop BMI row associated with PID where height is out of bounds --
        (
            SELECT person_id FROM outbound_heights
            WHERE m.measurement_source_concept_id = 903124
            AND m.measurement_datetime = outbound_heights.measurement_datetime
        )
    )
    --drop all height records out of bounds --
    OR (m.measurement_source_concept_id = 903133 AND value_as_number NOT BETWEEN 90 AND 228)
""")

DELETE_WEIGHT_ROWS_QUERY = JINJA_ENV.from_string("""
DELETE FROM `{{project_id}}.{{dataset_id}}.measurement` m
WHERE
    EXISTS (
        --subquery to select associated bmi records --
        WITH outbound_weights AS (
            SELECT person_id, measurement_datetime
            FROM `{{project_id}}.{{dataset_id}}.measurement`
            WHERE measurement_source_concept_id = 903121
            AND value_as_number NOT BETWEEN 30 AND 250
        )
        --drop BMI row associated with PID where weight is out of bounds --
        (
            SELECT person_id FROM outbound_weights
            WHERE m.measurement_source_concept_id = 903124
            AND m.measurement_datetime = outbound_weights.measurement_datetime
        )
    )
    --drop all weight records out of bounds --
    OR (m.measurement_source_concept_id = 903121 AND value_as_number NOT BETWEEN 30 AND 250)
""")

DELETE_BMI_ROWS_QUERY = JINJA_ENV.from_string("""
DELETE FROM `{{project_id}}.{{dataset_id}}.measurement` m
WHERE
    EXISTS (
        --subquery to select associated height and weight records --
        WITH outbound_bmi AS (
            SELECT person_id, measurement_datetime
            FROM `{{project_id}}.{{dataset_id}}.measurement`
            WHERE measurement_source_concept_id = 903124
            AND value_as_number NOT BETWEEN 10 AND 125
        )
        --drop height & weight rows associated with PID where bmi is out of bounds --
        (
            SELECT person_id FROM outbound_bmi
            WHERE m.measurement_source_concept_id IN(903133, 903121)
            AND m.measurement_datetime = outbound_bmi.measurement_datetime
        )
    )
    --drop all bmi records out of bounds --
    OR (m.measurement_source_concept_id = 903124 AND value_as_number NOT BETWEEN 10 AND 125)
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

        queries_list = []

        sandbox_query = dict()
        sandbox_query[
            cdr_consts.QUERY] = SANDBOX_HEIGHT_WEIGHT_ROWS_QUERY.render(
                project_id=self.project_id, dataset_id=self.dataset_id)
        sandbox_query[cdr_consts.DISPOSITION] = bq_consts.WRITE_TRUNCATE
        sandbox_query[cdr_consts.DESTINATION_DATASET] = self.sandbox_dataset_id
        sandbox_query[cdr_consts.DESTINATION_TABLE] = self.sandbox_table_for(
            MEASUREMENT)
        queries_list.append(sandbox_query)

        height_query = dict()
        height_query[cdr_consts.QUERY] = DELETE_HEIGHT_ROWS_QUERY.render(
            dataset_id=self.dataset_id, project_id=self.project_id)
        queries_list.append(height_query)

        weight_query = dict()
        weight_query[cdr_consts.QUERY] = DELETE_WEIGHT_ROWS_QUERY.render(
            dataset_id=self.dataset_id, project_id=self.project_id)
        queries_list.append(weight_query)

        bmi_query = dict()
        bmi_query[cdr_consts.QUERY] = DELETE_BMI_ROWS_QUERY.render(
            dataset_id=self.dataset_id, project_id=self.project_id)
        queries_list.append(bmi_query)

        return queries_list

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
