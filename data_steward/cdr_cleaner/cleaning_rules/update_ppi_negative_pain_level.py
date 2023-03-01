"""
Update answers where the participant skipped answering but the answer was registered as -1
"""
import logging

from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule, query_spec_list
from constants.cdr_cleaner import clean_cdr as cdr_consts
from common import JINJA_ENV, OBSERVATION
import constants.bq_utils as bq_consts

LOGGER = logging.getLogger(__name__)

JIRA_ISSUE_NUMBERS = ['DC536', 'DC1241']
JIRA_ISSUE_URL = [
    'https://precisionmedicineinitiative.atlassian.net/browse/DC-536',
    'https://precisionmedicineinitiative.atlassian.net/browse/DC-1241'
]
CLEANING_RULE_NAME = 'update_ppi_negative_pain_level'

SELECT_NEGATIVE_PPI_QUERY = JINJA_ENV.from_string("""
SELECT
  *
FROM
  `{{project_id}}.{{dataset_id}}.observation`
WHERE value_as_number = -1
AND observation_source_concept_id = 1585747
""")

UPDATE_NEGATIVE_PPI_QUERY = JINJA_ENV.from_string("""
UPDATE
  `{{project_id}}.{{dataset_id}}.observation`
SET value_as_number = NULL,
value_source_concept_id = 903096,
value_as_concept_id = 903096,
value_as_string = 'PMI Skip',
value_source_value = 'PMI_Skip'
WHERE value_as_number = -1
AND observation_source_concept_id = 1585747
""")


class UpdatePpiNegativePainLevel(BaseCleaningRule):
    """Cleaning Rule class to update the '-1' pain level."""

    def __init__(self,
                 project_id,
                 dataset_id,
                 sandbox_dataset_id,
                 table_namer=None):
        desc = (
            "Update OBSERVATION table records where negative pain value '-1' for 'value_as_number' to NULL. "
            "Accordingly, update the 'value_as_string' and 'value_source_value to 'PMI Skip', "
            "and value_source_concept_id and value_as_concept_id to '903096.")

        super().__init__(issue_numbers=JIRA_ISSUE_NUMBERS,
                         issue_urls=JIRA_ISSUE_URL,
                         description=desc,
                         affected_datasets=[cdr_consts.RDR],
                         affected_tables=[OBSERVATION],
                         project_id=project_id,
                         dataset_id=dataset_id,
                         sandbox_dataset_id=sandbox_dataset_id,
                         table_namer=table_namer)

    def setup_rule(self, client, *args, **keyword_args):
        """
        Run required steps for setup rule
        """
        pass

    def get_query_specs(self, *args, **keyword_args) -> query_spec_list:
        """
        This function generate the queries_list that updates negative ppi answers to pmi skip.

        This updates values in value_as_number, value_source_concept_id, value_as_concept_id,
        value_as_string, and value_source_value that matches record with observation_source_concept_id is 1585747 and
        value_as_number is -1.

        :return: a list of queries to execute
        """
        queries_list = []

        # Select all records where observation_source_concept_id = 1585747 and value_as_number = -1
        queries_list.append({
            cdr_consts.QUERY:
                SELECT_NEGATIVE_PPI_QUERY.render(dataset_id=self.dataset_id,
                                                 project_id=self.project_id),
            cdr_consts.DESTINATION_DATASET:
                self.sandbox_dataset_id,
            cdr_consts.DISPOSITION:
                bq_consts.WRITE_TRUNCATE,
            cdr_consts.DESTINATION_TABLE:
                CLEANING_RULE_NAME
        })

        # Update value_as_number, value_source_concept_id, value_as_concept_id, value_as_string, value_source_value.
        queries_list.append({
            cdr_consts.QUERY:
                UPDATE_NEGATIVE_PPI_QUERY.render(dataset_id=self.dataset_id,
                                                 project_id=self.project_id)
        })

        return queries_list

    def get_sandbox_tablenames(self):
        return [self.sandbox_table_for(OBSERVATION)]

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

    ARGS = parser.get_argument_parser().parse_args()

    if ARGS.list_queries:
        clean_engine.add_console_logging()
        query_list = clean_engine.get_query_list(
            ARGS.project_id, ARGS.dataset_id, ARGS.sandbox_dataset_id,
            [(UpdatePpiNegativePainLevel,)])
        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(ARGS.project_id, ARGS.dataset_id,
                                   ARGS.sandbox_dataset_id,
                                   [(UpdatePpiNegativePainLevel,)])
