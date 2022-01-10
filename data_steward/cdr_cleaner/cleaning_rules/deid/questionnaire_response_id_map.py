"""
Maps questionnaire_response_ids from the observation table to the research_response_id in the
_deid_questionnaire_response_map lookup table.

Original Issue: DC-1347, DC-518

The purpose of this cleaning rule is to create (if it does not already exist) the questionnaire mapping lookup table
and use that lookup table to remap the questionnaire_response_id in the observation table to the randomly
generated research_response_id in the _deid_questionnaire_response_map table.
"""

# Python imports
import logging

# Project imports
from utils import pipeline_logging
from common import OBSERVATION, JINJA_ENV
from constants.bq_utils import WRITE_TRUNCATE
from constants.cdr_cleaner import clean_cdr as cdr_consts
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule

LOGGER = logging.getLogger(__name__)

ISSUE_NUMBERS = ['DC1347', 'DC518']

# Creates _deid_questionnaire_response_map lookup table and populates with the questionnaire_response_id's
# from the observation table as well as randomly generates values for hte research_response_id column
LOOKUP_TABLE_CREATION_QUERY = JINJA_ENV.from_string("""
CREATE TABLE IF NOT EXISTS `{{project_id}}.{{shared_sandbox_id}}._deid_questionnaire_response_map` 
(questionnaire_response_id INT64, research_response_id INT64)
OPTIONS (description='lookup table for questionnaire response ids') AS
-- 1000000 used to start the research_response_id generation at 1mil --
SELECT DISTINCT questionnaire_response_id AS questionnaire_response_id, 1000000 + ROW_NUMBER() 
    OVER(ORDER BY GENERATE_UUID()) AS research_response_id
FROM `{{project_id}}.{{dataset_id}}.observation`
WHERE questionnaire_response_id IS NOT NULL
GROUP BY questionnaire_response_id
""")

# Map the research_response_id from _deid_questionnaire_response_map lookup table to the questionnaire_response_id in
# the observation table
QRID_RID_MAPPING_QUERY = JINJA_ENV.from_string("""
SELECT
    t.* EXCEPT (questionnaire_response_id),
    d.research_response_id as questionnaire_response_id,
FROM
    `{{project_id}}.{{dataset_id}}.observation` t
LEFT JOIN `{{project_id}}.{{shared_sandbox_id}}._deid_questionnaire_response_map` d
ON t.questionnaire_response_id = d.questionnaire_response_id
""")


class QRIDtoRID(BaseCleaningRule):
    """
    Create a deid questionnaire response mapping lookup table and remap the QID (questionnaire_response_id) from the
    observation table to the RID (research_response_id) found in that deid questionnaire response mapping lookup table
    """

    def __init__(self, project_id, dataset_id, sandbox_dataset_id):
        """
        Initialize the class with proper info.

        Set the issue numbers, description and affected datasets.  As other
        tickets may affect this SQL, append them to the list of Jira Issues.
        DO NOT REMOVE ORIGINAL JIRA ISSUE NUMBERS!
        """
        desc = f'Create a deid questionnaire response mapping lookup table and remap the QID ' \
               f'(questionnaire_response_id) from the observation table to the RID (research_response_id) found in ' \
               f'the deid questionnaire response mapping lookup table.'
        super().__init__(issue_numbers=ISSUE_NUMBERS,
                         description=desc,
                         affected_datasets=[
                             cdr_consts.CONTROLLED_TIER_DEID,
                             cdr_consts.REGISTERED_TIER_DEID
                         ],
                         affected_tables=OBSERVATION,
                         project_id=project_id,
                         dataset_id=dataset_id,
                         sandbox_dataset_id=sandbox_dataset_id)

    def get_query_specs(self, *args, **keyword_args):
        """
        Return a list of dictionary query specifications.

        :return:  A list of dictionaries.  Each dictionary contains a
            single query and a specification for how to execute that query.
            The specifications are optional but the query is required.
        """

        lookup_table_creation = {
            cdr_consts.QUERY:
                LOOKUP_TABLE_CREATION_QUERY.render(
                    project_id=self.project_id,
                    shared_sandbox_id=self.sandbox_dataset_id,
                    dataset_id=self.dataset_id)
        }

        mapping_query = {
            cdr_consts.QUERY:
                QRID_RID_MAPPING_QUERY.render(
                    project_id=self.project_id,
                    dataset_id=self.dataset_id,
                    shared_sandbox_id=self.sandbox_dataset_id),
            cdr_consts.DESTINATION_TABLE:
                OBSERVATION,
            cdr_consts.DESTINATION_DATASET:
                self.dataset_id,
            cdr_consts.DISPOSITION:
                WRITE_TRUNCATE
        }

        return [lookup_table_creation, mapping_query]

    def setup_rule(self, client, *args, **keyword_args):
        """
        Function to run any data upload options before executing a query.
        """
        pass

    def get_sandbox_tablenames(self):
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
                                                 [(QRIDtoRID,)])
        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(ARGS.project_id, ARGS.dataset_id,
                                   ARGS.sandbox_dataset_id, [(QRIDtoRID,)])
