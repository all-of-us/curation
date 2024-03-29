"""
Update Nov, Dec, and Feb COPE concepts

The wrong observation_source_concept_id is used in the Nov, Dec, and Feb COPE
Surveys.  This rule should store the original data and use it to update the
existing table.  It will set cdc_covid_19_9b to dmfs_27 for those three COPE surveys.

Original Issue: DC-1894
"""

# Python imports
import logging

# Project imports
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule
from constants.cdr_cleaner import clean_cdr as cdr_consts
from common import JINJA_ENV, OBSERVATION
from utils import pipeline_logging

LOGGER = logging.getLogger(__name__)

STORE_ROWS = JINJA_ENV.from_string("""
CREATE OR REPLACE TABLE `{{project_id}}.{{sandbox_dataset_id}}.{{sandbox_table_name}}` AS (
SELECT *
FROM `{{project_id}}.{{dataset_id}}.{{observation_table}}`
JOIN `{{project_id}}.{{dataset_id}}.cope_survey_semantic_version_map`
USING (questionnaire_response_id)
WHERE observation_source_value = 'cdc_covid_19_9b' 
AND lower(cope_month) IN ('nov', 'dec', 'feb')
)
""")

UPDATE_FLU_CONCEPTS = JINJA_ENV.from_string("""
UPDATE `{{project_id}}.{{dataset_id}}.{{observation_table}}`
SET
observation_source_value = 'dmfs_27',
observation_source_concept_id = 705047,
observation_concept_id = 705047
WHERE observation_id IN (SELECT observation_id FROM `{{project_id}}.{{sandbox_dataset_id}}.{{sandbox_table_name}}`)
""")


class UpdateCopeFluQuestionConcept(BaseCleaningRule):

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
        desc = ('Update concepts with code cdc_covid_19_9b to code dmfs_27 for '
                'the Nov, Dec, and Feb cope surveys.')
        super().__init__(issue_numbers=['DC1894'],
                         description=desc,
                         affected_datasets=[cdr_consts.RDR],
                         affected_tables=[OBSERVATION],
                         project_id=project_id,
                         dataset_id=dataset_id,
                         sandbox_dataset_id=sandbox_dataset_id,
                         table_namer=table_namer)

    def setup_rule(self, client, *args, **keyword_args):
        """
        Load required resources prior to executing cleaning rule queries.

        Method to run data upload options before executing the first cleaning
        rule of a class.  For example, if your class requires loading a static
        table, that load operation should be defined here.  It SHOULD NOT BE
        defined as part of get_query_specs().
        :param client:
        :return:
        """
        pass

    def get_query_specs(self, *args, **keyword_args):
        """
        Interface to return a list of query dictionaries.

        :returns:  a list of query dictionaries.  Each dictionary specifies
            the query to execute and how to execute.  The dictionaries are
            stored in list order and returned in list order to maintain
            an ordering.
        """
        save_query = dict()
        save_query[cdr_consts.QUERY] = STORE_ROWS.render(
            project_id=self.project_id,
            dataset_id=self.dataset_id,
            observation_table=OBSERVATION,
            sandbox_dataset_id=self.sandbox_dataset_id,
            sandbox_table_name=self.sandbox_table_for(OBSERVATION))

        update_query = dict()
        update_query[cdr_consts.QUERY] = UPDATE_FLU_CONCEPTS.render(
            project_id=self.project_id,
            dataset_id=self.dataset_id,
            observation_table=OBSERVATION,
            sandbox_dataset_id=self.sandbox_dataset_id,
            sandbox_table_name=self.sandbox_table_for(OBSERVATION))

        return [save_query, update_query]

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
        raise NotImplementedError("Please fix me.")

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

    def get_sandbox_tablenames(self):
        return [self.sandbox_table_for(name) for name in self.affected_tables]


if __name__ == '__main__':
    import cdr_cleaner.args_parser as parser
    import cdr_cleaner.clean_cdr_engine as clean_engine

    ARGS = parser.parse_args()
    pipeline_logging.configure(level=logging.DEBUG, add_console_handler=True)

    if ARGS.list_queries:
        clean_engine.add_console_logging()
        query_list = clean_engine.get_query_list(
            ARGS.project_id, ARGS.dataset_id, ARGS.sandbox_dataset_id,
            [(UpdateCopeFluQuestionConcept,)])

        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(ARGS.project_id, ARGS.dataset_id,
                                   ARGS.sandbox_dataset_id,
                                   [(UpdateCopeFluQuestionConcept,)])
