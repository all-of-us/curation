"""
Background

It is possible for a participant to have multiple records of Physical Measurements. This typically occurs when earlier
entries are incorrect. Data quality would improve if these earlier entries were removed.

Scope: Develop a cleaning rule to remove all but the most recent of each Physical Measurement for all participants.
Relevant measurement_source_concept_ids are listed in query

For height, weight, and BMI, we must keep the most recent of both in-person PM and self-report PM.
"""

# Python Imports
import logging

# Project imports
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule
from cdr_cleaner.cleaning_rules.drop_extreme_measurements import DropExtremeMeasurements
from common import JINJA_ENV, MEASUREMENT
from constants.cdr_cleaner import clean_cdr as cdr_consts
from utils import pipeline_logging

LOGGER = logging.getLogger(__name__)

ISSUE_NUMBERS = ['DC617', 'DC847', 'DC3380']

SANDBOX_MULT_MEASUREMENTS = JINJA_ENV.from_string("""
CREATE OR REPLACE TABLE `{{project}}.{{sandbox_dataset}}.{{intermediary_table}}` AS 
SELECT *
FROM (
    SELECT
        *, 
        ROW_NUMBER() OVER(PARTITION BY
            person_id, measurement_source_concept_id
            ORDER BY measurement_datetime DESC
        ) AS row_num
    FROM `{{project}}.{{dataset}}.measurement`
    WHERE measurement_source_concept_id IN (
        903131, 903119, 903107, 903115, 903126, 903136, 903118, 903135, 903132,
        903110, 903112, 903117, 903109, 903127, 1586218, 903111, 903120, 903113,
        903129, 903105, 903125, 903114, 903134, 903116, 903106, 903108, 903123,
        903130, 903128, 903122
    )
)
WHERE row_num != 1
""")

# For height, weight, and BMI, we must consider in-person vs self-report
# by looking at measurement_type_concept_id
SANDBOX_HEIGHT_WEIGHT_BMI = JINJA_ENV.from_string("""
INSERT INTO `{{project}}.{{sandbox_dataset}}.{{intermediary_table}}`
SELECT *
FROM (
    SELECT 
        *, 
        ROW_NUMBER() OVER(PARTITION BY 
            person_id, measurement_source_concept_id, measurement_type_concept_id 
            ORDER BY measurement_datetime DESC
        ) AS row_num
    FROM `{{project}}.{{dataset}}.measurement`
    WHERE measurement_source_concept_id IN (903133, 903121, 903124)
)
WHERE row_num != 1
""")

REMOVE_MULT_MEASUREMENTS = JINJA_ENV.from_string("""
DELETE FROM `{{project}}.{{dataset}}.measurement`
WHERE measurement_id IN (
    SELECT measurement_id FROM `{{project}}.{{sandbox_dataset}}.{{intermediary_table}}`
)
""")


class DropMultipleMeasurements(BaseCleaningRule):
    """
    Removes all but the most recent of each Physical Measurement for all participants.
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
        desc = 'Removes all but the most recent of each Physical Measurement for all participants.'
        super().__init__(issue_numbers=ISSUE_NUMBERS,
                         description=desc,
                         affected_datasets=[cdr_consts.RDR],
                         affected_tables=[MEASUREMENT],
                         project_id=project_id,
                         dataset_id=dataset_id,
                         sandbox_dataset_id=sandbox_dataset_id,
                         depends_on=[DropExtremeMeasurements],
                         table_namer=table_namer)

    def get_query_specs(self):
        """
        Return a list of dictionary query specifications.

        :return:  A list of dictionaries. Each dictionary contains a single query
            and a specification for how to execute that query. The specifications
            are optional but the query is required.
        """

        sandbox_invalid_rows = {
            cdr_consts.QUERY:
                SANDBOX_MULT_MEASUREMENTS.render(
                    project=self.project_id,
                    dataset=self.dataset_id,
                    sandbox_dataset=self.sandbox_dataset_id,
                    intermediary_table=self.get_sandbox_tablenames()[0])
        }

        sandbox_height_weight_bmi = {
            cdr_consts.QUERY:
                SANDBOX_HEIGHT_WEIGHT_BMI.render(
                    project=self.project_id,
                    dataset=self.dataset_id,
                    sandbox_dataset=self.sandbox_dataset_id,
                    intermediary_table=self.get_sandbox_tablenames()[0])
        }

        delete_invalid_rows = {
            cdr_consts.QUERY:
                REMOVE_MULT_MEASUREMENTS.render(
                    project=self.project_id,
                    dataset=self.dataset_id,
                    sandbox_dataset=self.sandbox_dataset_id,
                    intermediary_table=self.get_sandbox_tablenames()[0])
        }

        return [
            sandbox_invalid_rows, sandbox_height_weight_bmi, delete_invalid_rows
        ]

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
        sandbox_table = self.sandbox_table_for(self.affected_tables[0])
        return [sandbox_table]


if __name__ == '__main__':
    import cdr_cleaner.args_parser as parser
    import cdr_cleaner.clean_cdr_engine as clean_engine

    pipeline_logging.configure()
    ARGS = parser.parse_args()

    if ARGS.list_queries:
        clean_engine.add_console_logging()
        query_list = clean_engine.get_query_list(ARGS.project_id,
                                                 ARGS.dataset_id,
                                                 ARGS.sandbox_dataset_id,
                                                 [(DropMultipleMeasurements,)])
        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(ARGS.project_id, ARGS.dataset_id,
                                   ARGS.sandbox_dataset_id,
                                   [(DropMultipleMeasurements,)])
