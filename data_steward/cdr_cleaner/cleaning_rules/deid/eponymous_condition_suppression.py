"""
Eponymous disease concepts that are in Note_NLP table should be suppressed
unless if the concept already exists in the condition_occurrence table.
This prevents privacy leaks due to patient/clinician names coinciding with eponymous diseases

Original Issue: DC-3916
"""

# Python imports
import logging

# Project imports
from cdr_cleaner.cleaning_rules.deid.concept_suppression import BaseCleaningRule
from constants.cdr_cleaner import clean_cdr as cdr_consts
from utils import pipeline_logging
from common import JINJA_ENV, NOTE_NLP

LOGGER = logging.getLogger(__name__)
JIRA_ISSUE_NUMBERS = ['DC3915']

EPONYM_SANDBOX_QUERY = JINJA_ENV.from_string("""
CREATE OR REPLACE TABLE `{{project_id}}.{{sandbox_dataset_id}}.{{sandbox_table}}` AS
SELECT s.*
FROM `{{project_id}}.{{dataset_id}}.note_nlp` s
JOIN `{{project_id}}.{{dataset_id}}.note` n ON n.note_id = s.note_id
JOIN `{{project_id}}.pipeline_tables.all_final_eponymous_concepts` c ON s.note_nlp_concept_id = c.concept_id
LEFT JOIN (
  SELECT DISTINCT person_id, condition_concept_id
  FROM `{{project_id}}.{{dataset_id}}.condition_occurrence`
  WHERE condition_source_value != 'note_nlp'
) co ON n.person_id = co.person_id AND s.note_nlp_concept_id = co.condition_concept_id
WHERE 
  c.concept_id IS NULL
  OR co.person_id IS NOT NULL;
""")

EPONYM_DELETE_QUERY = JINJA_ENV.from_string("""
DELETE FROM `{{project_id}}.{{dataset_id}}.note_nlp`
WHERE note_nlp_id IN (SELECT note_nlp_id
FROM `{{project_id}}.{{sandbox_dataset_id}}.{{sandbox_table}}`)
""")


class EponymousConditionSuppression(BaseCleaningRule):
    """
    Any record in the observation table with a free text concept should be sandboxed and suppressed
    """

    def __init__(self, project_id, dataset_id, sandbox_dataset_id):
        """
        Initialize the class with proper information.
        """
        desc = (
            f'Suppresses any eponymous condition concept from note_nlp which is not '
            f'already in the condition_occurrence table')
        super().__init__(
            issue_numbers=JIRA_ISSUE_NUMBERS,
            description=desc,
            affected_datasets=[cdr_consts.COMBINED],
            affected_tables=[NOTE_NLP],
            project_id=project_id,
            dataset_id=dataset_id,
            sandbox_dataset_id=sandbox_dataset_id,
        )

    def get_query_specs(self):
        """
        Return a list of dictionary query specifications.

        :return:  A list of dictionaries. Each dictionary contains a single query
            and a specification for how to execute that query. The specifications
            are optional but the query is required.
        """

        queries_list = []

        sandbox_query = dict()
        sandbox_query[cdr_consts.QUERY] = EPONYM_SANDBOX_QUERY.render(
            project_id=self.project_id,
            dataset_id=self.dataset_id,
            sandbox_dataset_id=self.sandbox_dataset_id,
            sandbox_table=self.get_sandbox_tablenames()[0])
        queries_list.append(sandbox_query)

        delete_query = dict()
        delete_query[cdr_consts.QUERY] = EPONYM_DELETE_QUERY.render(
            project_id=self.project_id,
            dataset_id=self.dataset_id,
            sandbox_dataset_id=self.sandbox_dataset_id,
            sandbox_table=self.get_sandbox_tablenames()[0])
        queries_list.append(delete_query)

        return queries_list

    def get_sandbox_tablenames(self) -> list:
        return [self.sandbox_table_for(table) for table in self.affected_tables]

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


if __name__ == '__main__':
    import cdr_cleaner.args_parser as parser
    import cdr_cleaner.clean_cdr_engine as clean_engine

    ARGS = parser.parse_args()
    pipeline_logging.configure(level=logging.DEBUG, add_console_handler=True)

    if ARGS.list_queries:
        clean_engine.add_console_logging()
        query_list = clean_engine.get_query_list(
            ARGS.project_id, ARGS.dataset_id, ARGS.sandbox_dataset_id,
            [(EponymousConditionSuppression,)])

        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(ARGS.project_id, ARGS.dataset_id,
                                   ARGS.sandbox_dataset_id,
                                   [(EponymousConditionSuppression,)])
