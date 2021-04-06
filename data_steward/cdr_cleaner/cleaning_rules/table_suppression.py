"""
Original Issues: DC-1360

As part of the controlled tier, some table data will be entirely suppressed.  When suppression happens, the table
needs to maintain it’s expected schema, but drop all of its data.

Apply table suppression to note, location, provider, and care_site tables.
table schemas should remain intact and match their data_steward/resource_files/schemas/<table>.json schema definition.

Should be added to list of CONTROLLED_TIER_DEID_CLEANING_CLASSES in data_steward/cdr_cleaner/clean_cdr.py
all data should be dropped from the tables
sandboxing not required
"""
import logging

# Project imports
import common
from constants.cdr_cleaner import clean_cdr as cdr_consts
from common import JINJA_ENV
from utils import pipeline_logging
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule, query_spec_list

LOGGER = logging.getLogger(__name__)

JIRA_ISSUE_NUMBERS = ['DC1360']

tables = [common.NOTE, common.LOCATION, common.PROVIDER, common.CARE_SITE]

# query to delete all rows from table
TABLE_SUPPRESSION_QUERY = JINJA_ENV.from_string("""
DELETE FROM
    `{{project_id}}.{{dataset_id}}.{{table}}`
WHERE
    true
""")


class TableSuppression(BaseCleaningRule):

    def __init__(self, project_id, dataset_id, sandbox_dataset_id=None):
        """
        Initialize the class with proper information.

        Set the issue numbers, description and affected datasets. As other tickets may affect
        this SQL, append them to the list of Jira Issues.
        DO NOT REMOVE ORIGINAL JIRA ISSUE NUMBERS!
        """
        desc = (
            'Returns queries to suppress all table data from note, location, provider and care_site tables.'
        )

        super().__init__(issue_numbers=JIRA_ISSUE_NUMBERS,
                         description=desc,
                         affected_datasets=[cdr_consts.CONTROLLED_TIER_DEID],
                         affected_tables=tables,
                         project_id=project_id,
                         dataset_id=dataset_id,
                         sandbox_dataset_id=sandbox_dataset_id)

    def get_query_specs(self, *args, **keyword_args) -> query_spec_list:
        """
        Return a list of dictionary query specifications.

        :return:  A list of dictionaries. Each dictionary contains a single query
            and a specification for how to execute that query. The specifications
            are optional but the query is required.
        """

        queries = []
        for table in tables:
            query = dict()
            query[cdr_consts.QUERY] = TABLE_SUPPRESSION_QUERY.render(
                project_id=self.project_id,
                dataset_id=self.dataset_id,
                table=table,
            )
            queries.append(query)

        return queries

    def setup_rule(self, client, *args, **keyword_args):
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
                                                 [(TableSuppression,)])
        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(ARGS.project_id, ARGS.dataset_id,
                                   ARGS.sandbox_dataset_id,
                                   [(TableSuppression,)])
