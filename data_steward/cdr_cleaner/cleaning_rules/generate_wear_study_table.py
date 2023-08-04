"""
The purpose of this cleaning rule is to generate the wear_study table.

Original Issues: DC-2364
"""

# Python imports
import logging

# Project imports
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule
from constants.cdr_cleaner import clean_cdr as cdr_consts
from common import JINJA_ENV, WEAR_STUDY
from utils import pipeline_logging

LOGGER = logging.getLogger(__name__)

JIRA_ISSUES = ['DC2364']

CREATION_QUERY = JINJA_ENV.from_string("""
CREATE OR REPLACE TABLE `{{project_id}}.{{dataset_id}}.wear_study` AS

WITH ranked_data AS (
  SELECT
    person_id,
    observation_source_concept_id,
    value_source_concept_id,
    observation_date,
    ROW_NUMBER() OVER (PARTITION BY person_id, observation_source_concept_id ORDER BY observation_date) AS row_num_asc,
    ROW_NUMBER() OVER (PARTITION BY person_id, observation_source_concept_id ORDER BY observation_date DESC) AS row_num_desc
  FROM
    `{{project_id}}.{{dataset_id}}.observation`
    WHERE observation_source_concept_id = 2100000010
)
SELECT
  person_id,
  'Yes' AS resultsconsent_wear,
  MAX(CASE WHEN value_source_concept_id = 2100000009 AND prev_value_source_concept_id = 2100000008 THEN observation_date END) AS first_yes_after_last_no,
  MAX(CASE WHEN value_source_concept_id = 2100000008 AND prev_value_source_concept_id = 2100000009 THEN observation_date END) AS last_no
FROM (
  SELECT
    person_id,
    observation_source_concept_id,
    value_source_concept_id,
    observation_date,
    LAG(value_source_concept_id) OVER (PARTITION BY person_id, observation_source_concept_id ORDER BY observation_date) AS prev_value_source_concept_id,
    row_num_asc,
    row_num_desc
  FROM ranked_data
)
WHERE
  (value_source_concept_id = 2100000009 AND (prev_value_source_concept_id = 2100000008 OR row_num_desc = 1))
  OR (value_source_concept_id = 2100000008 AND prev_value_source_concept_id = 2100000009)
""")


class GenerateWearStudyTable(BaseCleaningRule):

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
        desc = "The purpose of this cleaning rule is to create the wear_study table from observation records. " \
                "mapping lookup table."
        super().__init__(issue_numbers=JIRA_ISSUES,
                         description=desc,
                         affected_datasets=[
                             cdr_consts.CONTROLLED_TIER_DEID,
                             cdr_consts.REGISTERED_TIER_DEID
                         ],
                         affected_tables=[],
                         project_id=project_id,
                         dataset_id=dataset_id,
                         sandbox_dataset_id=sandbox_dataset_id,
                         table_namer=table_namer)

    def get_query_specs(self, *args, **keyword_args):
        """
        Interface to return a list of query dictionaries.

        :returns:  a list of query dictionaries.  Each dictionary specifies
            the query to execute and how to execute.  The dictionaries are
            stored in list order and returned in list order to maintain
            an ordering.
        """

        table_creation = {
            cdr_consts.QUERY:
                CREATION_QUERY.render(
                    project_id=self.project_id,
                    dataset_id=self.dataset_id)
        }

        return [table_creation]

    def validate_rule(self, client, *args, **keyword_args):
        """
        Validates the cleaning rule which deletes or updates the data from the tables

        Method to run validation on cleaning rules that will be updating the values.
        For example:
        if your class updates all the datetime fields you should be implementing the
        validation that checks if the date time values that needs to be updated no
        longer exists in the table.

        if your class deletes a subset of rows in the tables you should be implementing
        the validation that checks if the count of final final row counts + deleted rows
        should equals to initial row counts of the affected tables.

        Raises RunTimeError if the validation fails.
        """

        raise NotImplementedError("Please fix me.")

    def setup_rule(self, client, *args, **keyword_args):
        """
        Load required resources prior to executing cleaning rule queries.

        Method to run data upload options before executing the first cleaning
        rule of a class.  For example, if your class requires loading a static
        table, that load operation should be defined here.  It SHOULD NOT BE
        defined as part of get_query_specs().
        """
        pass

    def setup_validation(self, client, *args, **keyword_args):
        """
        Run required steps for validation setup

        Method to run to setup validation on cleaning rules that will be updating or deleting the values.
        For example:
        if your class updates all the datetime fields you should be implementing the
        logic to get the initial list of values which adhere to a condition we are looking for.

        if your class deletes a subset of rows in the tables you should be implementing
        the logic to get the row counts of the tables prior to applying cleaning rule

        """
        pass

    def get_sandbox_tablenames(self):
        pass

if __name__ == '__main__':
    import cdr_cleaner.args_parser as parser
    import cdr_cleaner.clean_cdr_engine as clean_engine

    ARGS = parser.parse_args()
    pipeline_logging.configure(level=logging.DEBUG, add_console_handler=True)

    if ARGS.list_queries:
        clean_engine.add_console_logging()
        query_list = clean_engine.get_query_list(
            ARGS.project_id, ARGS.dataset_id, ARGS.sandbox_dataset_id,
            [(GenerateWearStudyTable,)])

        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(ARGS.project_id, ARGS.dataset_id,
                                   ARGS.sandbox_dataset_id,
                                   [(GenerateWearStudyTable,)])
