"""
Various cleaning rules will alter data in the observation table, both before
and as part of de-identification.
The survey_conduct table needs to remain in sync with the observation table,
meaning if a survey_conduct.survey_conduct_id exists then at least one
observation.questionnaire_response_id record with the same value should also exist.

This cleaning rule sandboxes and deletes records from the survey_conduct table
if the record cannot be joined to the observation table.

Original Issues: DC-2735
"""

# Python imports
import logging

# Project imports
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule
from constants.cdr_cleaner.clean_cdr import (COMBINED, CONTROLLED_TIER_DEID,
                                             CONTROLLED_TIER_DEID_CLEAN, QUERY,
                                             RDR, REGISTERED_TIER_DEID,
                                             REGISTERED_TIER_DEID_CLEAN)
from common import JINJA_ENV, SURVEY_CONDUCT
from utils import pipeline_logging

LOGGER = logging.getLogger(__name__)

SANDBOX_QUERY = JINJA_ENV.from_string("""
CREATE OR REPLACE TABLE `{{project_id}}.{{sandbox_dataset_id}}.{{sandbox_table_id}}` AS (
    SELECT *
    FROM `{{project_id}}.{{dataset_id}}.survey_conduct`
    WHERE survey_conduct_id NOT IN (
        SELECT DISTINCT questionnaire_response_id
        FROM `{{project_id}}.{{dataset_id}}.observation`
        WHERE questionnaire_response_id IS NOT NULL
    )
)
""")

DELETE_QUERY = JINJA_ENV.from_string("""
DELETE FROM `{{project_id}}.{{dataset_id}}.survey_conduct`
WHERE survey_conduct_id IN (
    SELECT DISTINCT survey_conduct_id
    FROM `{{project_id}}.{{sandbox_dataset_id}}.{{sandbox_table_id}}`
)
""")


class DropOrphanedSurveyConductIds(BaseCleaningRule):

    def __init__(self,
                 project_id,
                 dataset_id,
                 sandbox_dataset_id,
                 table_namer=None):
        """
        Initialize the class with proper information.
        """
        desc = ('Sandbox and delete records from the survey_conduct table '
                'if the record cannot be joined to the observation table.')
        super().__init__(issue_numbers=['DC2735'],
                         description=desc,
                         affected_datasets=[
                             CONTROLLED_TIER_DEID, CONTROLLED_TIER_DEID_CLEAN,
                             COMBINED, RDR, REGISTERED_TIER_DEID,
                             REGISTERED_TIER_DEID_CLEAN
                         ],
                         affected_tables=[SURVEY_CONDUCT],
                         project_id=project_id,
                         dataset_id=dataset_id,
                         sandbox_dataset_id=sandbox_dataset_id,
                         table_namer=table_namer,
                         run_for_synthetic=True)

    def setup_rule(self, client):
        """
        Function to run any data upload options before executing a query.
        """
        pass

    def setup_validation(self, client, *args, **keyword_args):
        """
        Run required steps for validation setup
        """
        raise NotImplementedError("Please fix me.")

    def validate_rule(self, client, *args, **keyword_args):
        """
        Validates the cleaning rule which deletes or updates the data from the tables
        """
        raise NotImplementedError("Please fix me.")

    def get_sandbox_tablenames(self):
        return [
            self.sandbox_table_for(affected_table)
            for affected_table in self.affected_tables
        ]

    def get_query_specs(self):
        """
        Return a list of dictionary query specifications.
        :return:  A list of dictionaries. Each dictionary contains a single query
            and a specification for how to execute that query. The specifications
            are optional but the query is required.
        """
        sandbox_queries, delete_queries = [], []

        sandbox_queries.append({
            QUERY:
                SANDBOX_QUERY.render(
                    project_id=self.project_id,
                    sandbox_dataset_id=self.sandbox_dataset_id,
                    sandbox_table_id=self.sandbox_table_for(SURVEY_CONDUCT),
                    dataset_id=self.dataset_id)
        })

        delete_queries.append({
            QUERY:
                DELETE_QUERY.render(
                    project_id=self.project_id,
                    sandbox_dataset_id=self.sandbox_dataset_id,
                    sandbox_table_id=self.sandbox_table_for(SURVEY_CONDUCT),
                    dataset_id=self.dataset_id)
        })

        return sandbox_queries + delete_queries


if __name__ == '__main__':
    import cdr_cleaner.args_parser as parser
    import cdr_cleaner.clean_cdr_engine as clean_engine

    ARGS = parser.parse_args()
    pipeline_logging.configure(level=logging.DEBUG, add_console_handler=True)

    if ARGS.list_queries:
        clean_engine.add_console_logging()
        query_list = clean_engine.get_query_list(
            ARGS.project_id, ARGS.dataset_id, ARGS.sandbox_dataset_id,
            [(DropOrphanedSurveyConductIds,)])

        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(ARGS.project_id, ARGS.dataset_id,
                                   ARGS.sandbox_dataset_id,
                                   [(DropOrphanedSurveyConductIds,)])