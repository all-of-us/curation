"""
Notes submitted by sites and RDR need to be suppressed since they are not being
released in the near future.

Original Issue: DC-3607
"""

# Python imports
import logging

# Project imports
from cdr_cleaner.cleaning_rules.deid.concept_suppression import BaseCleaningRule
from constants.cdr_cleaner import clean_cdr as cdr_consts
from utils import pipeline_logging
from common import JINJA_ENV, NOTE

LOGGER = logging.getLogger(__name__)
JIRA_ISSUE_NUMBERS = ['DC3607']

FREE_TEXT_UPDATE_QUERY = JINJA_ENV.from_string("""
UPDATE `{{project_id}}.{{dataset_id}}.note`
SET
  note_text = 'NO_TEXT',
  note_title = 'NO_TITLE'
WHERE True
""")


class ReplaceFreeTextNotes(BaseCleaningRule):
    """
    Any record in the observation table with a free text concept should be sandboxed and suppressed
    """

    def __init__(self, project_id, dataset_id, sandbox_dataset_id):
        """
        Initialize the class with proper information.
        """
        desc = (f'Replace any text in note_text and not_title with '
                f'NO_TEXT and NOT_TITLE respectively in the notes table')
        super().__init__(
            issue_numbers=JIRA_ISSUE_NUMBERS,
            description=desc,
            affected_datasets=[cdr_consts.COMBINED],
            affected_tables=[NOTE],
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
        query = dict()

        query[cdr_consts.QUERY] = FREE_TEXT_UPDATE_QUERY.render(
            project_id=self.project_id,
            dataset_id=self.dataset_id,
        )

        queries_list.append(query)

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
        pass


if __name__ == '__main__':
    import cdr_cleaner.args_parser as parser
    import cdr_cleaner.clean_cdr_engine as clean_engine

    ARGS = parser.parse_args()
    pipeline_logging.configure(level=logging.DEBUG, add_console_handler=True)

    if ARGS.list_queries:
        clean_engine.add_console_logging()
        query_list = clean_engine.get_query_list(ARGS.project_id,
                                                 ARGS.dataset_id,
                                                 ARGS.sandbox_dataset_id,
                                                 [(ReplaceFreeTextNotes,)])

        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(ARGS.project_id, ARGS.dataset_id,
                                   ARGS.sandbox_dataset_id,
                                   [(ReplaceFreeTextNotes,)])
