"""
In the PPI race/ethnicity question from the Basics (observation_source_concept_id = 1586140), participants who select
“None of these describe me” (value_source_concept_id = 1586148) should not be able to select any other answers.

Due to an error with the PTSC, ~36 participants were able to select “None of these describe me” and another response(s).
Due to the race/ethnicity generalization rule, this would lead to an inconsistency in expression of the data.
Therefore, all responses for these participants that are not “None of these describe me” must be dropped.

solution:
1. For observation_source_concept_id = 1586140, identify all participants who have a value_source_concept_id = 1586148.
2. For these participants, drop any additional rows for this observation_source_concept_id (i.e. all participants should
have ONLY one row and that row should have value_source_concept_id = 1586148).
"""

import logging

import constants.cdr_cleaner.clean_cdr as cdr_consts
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule
from common import JINJA_ENV

LOGGER = logging.getLogger(__name__)

JIRA_ISSUE_NUMBERS = ['DC503', 'DC832']
OBSERVATION = 'observation'

SANDBOX_ADDITIONAL_RESPONSES_OTHER_THAN_NOT = JINJA_ENV.from_string("""
CREATE OR REPLACE TABLE
  `{{project}}.{{sandbox_dataset}}.{{sandbox_table}}` AS
SELECT
  *
FROM
  `{{project}}.{{dataset}}.observation`
WHERE
  person_id IN (
  SELECT
    person_id
  FROM
    `{{project}}.{{dataset}}.observation`
  WHERE
    observation_concept_id = 1586140
    AND value_source_concept_id = 1586148)
  AND (observation_concept_id = 1586140
    AND value_source_concept_id != 1586148)
""")

REMOVE_ADDITIONAL_RESPONSES_OTHER_THAN_NOT = JINJA_ENV.from_string("""
DELETE
FROM
  `{{project}}.{{dataset}}.observation`
WHERE
  person_id IN (
  SELECT
    person_id
  FROM
    `{{project}}.{{dataset}}.observation`
  WHERE
    observation_concept_id = 1586140
    AND value_source_concept_id = 1586148)
  AND (observation_concept_id = 1586140
    AND value_source_concept_id != 1586148)
""")


class RemoveMultipleRaceEthnicityAnswersQueries(BaseCleaningRule):
    """
    Runs the query which removes the other than None of these identify me answers for gender/sex
    if a person answers None of these identify me as an option from observation table
    """

    def __init__(self, project_id, dataset_id, sandbox_dataset_id):
        """
        Initialize the class with proper information.

        Set the issue numbers, description and affected datasets. As other tickets may affect
        this SQL, append them to the list of Jira Issues.
        DO NOT REMOVE ORIGINAL JIRA ISSUE NUMBERS!
        """
        desc = "Runs the query which removes the other than None of these identify me answers for gender/sex \
    if a person answers None of these identify me as an option from observation table."

        super().__init__(issue_numbers=JIRA_ISSUE_NUMBERS,
                         description=desc,
                         affected_datasets=[cdr_consts.RDR],
                         affected_tables=[OBSERVATION],
                         project_id=project_id,
                         dataset_id=dataset_id,
                         sandbox_dataset_id=sandbox_dataset_id,
                         run_for_synthetic=True)

    def get_query_specs(self):
        """
        Return a list of dictionary query specifications.

        :return:  A list of dictionaries. Each dictionary contains a single query
            and a specification for how to execute that query. The specifications
            are optional but the query is required.
        """
        sandbox_query = dict()
        sandbox_query[
            cdr_consts.
            QUERY] = SANDBOX_ADDITIONAL_RESPONSES_OTHER_THAN_NOT.render(
                dataset=self.dataset_id,
                project=self.project_id,
                sandbox_dataset=self.sandbox_dataset_id,
                sandbox_table=self.get_sandbox_tablenames()[0])

        delete_query = dict()
        delete_query[cdr_consts.
                     QUERY] = REMOVE_ADDITIONAL_RESPONSES_OTHER_THAN_NOT.render(
                         dataset=self.dataset_id, project=self.project_id)

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

    def get_sandbox_table_name(self):
        return f'{self._issue_numbers[0].lower()}_{self.affected_tables[0]}'

    def get_sandbox_tablenames(self):
        return [self.get_sandbox_table_name()]


if __name__ == '__main__':
    import cdr_cleaner.args_parser as parser
    import cdr_cleaner.clean_cdr_engine as clean_engine

    ARGS = parser.parse_args()

    if ARGS.list_queries:
        clean_engine.add_console_logging()
        query_list = clean_engine.get_query_list(
            ARGS.project_id, ARGS.dataset_id, ARGS.sandbox_dataset_id,
            [(RemoveMultipleRaceEthnicityAnswersQueries,)])
        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(
            ARGS.project_id, ARGS.dataset_id, ARGS.sandbox_dataset_id,
            [(RemoveMultipleRaceEthnicityAnswersQueries,)])
