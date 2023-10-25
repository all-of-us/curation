"""
Create a lookup table of AIAN participants.

We create this lookup table in case we need to run AIAN-specific processes, retraction for example.
We run this cleaning rule at the early stage of the RDR data stage so that we can include all the
potential AIAN participants in our datasets.

The criteria of AIAN defition comes from the existing cleaning rules and retractions from the past.
See DC-3402 and its related tickets and comments for more context.

Original JIRA ticket: DC-3402
"""
# Python imports
import logging

# Third party imports

# Project imports
import constants.cdr_cleaner.clean_cdr as cdr_consts
from common import AIAN_LIST, JINJA_ENV
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule

LOGGER = logging.getLogger(__name__)

CREATE_AIAN_LIST = JINJA_ENV.from_string("""
CREATE OR REPLACE TABLE `{{project_id}}.{{sandbox_dataset_id}}.{{storage_table_name}}` AS (
    SELECT DISTINCT person_id FROM `{{project_id}}.{{dataset_id}}.observation`
    WHERE (observation_source_concept_id = 1586140 AND value_source_concept_id = 1586141)
    OR observation_source_concept_id in (1586150, 1585599, 1586139, 1585604)
)""")


class CreateAIANLookup(BaseCleaningRule):
    """
    Create a lookup table of AIAN participants.
    """

    def __init__(self,
                 project_id,
                 dataset_id,
                 sandbox_dataset_id,
                 table_namer=None):
        desc = ('Create a lookup table of AIAN participants. '
                'We create it in case we need AIAN-specific ETL process '
                '(retraction, etc).')

        super().__init__(issue_numbers=['DC3402'],
                         description=desc,
                         affected_datasets=[],
                         project_id=project_id,
                         dataset_id=dataset_id,
                         sandbox_dataset_id=sandbox_dataset_id,
                         depends_on=[],
                         table_namer=table_namer)

    def get_query_specs(self):
        """
        :return: a list of SQL strings to run
        """
        create_sandbox_table = CREATE_AIAN_LIST.render(
            project_id=self.project_id,
            dataset_id=self.dataset_id,
            sandbox_dataset_id=self.sandbox_dataset_id,
            storage_table_name=self.sandbox_table_for(AIAN_LIST))

        create_sandbox_table_dict = {cdr_consts.QUERY: create_sandbox_table}

        return [create_sandbox_table_dict]

    def get_sandbox_tablenames(self):
        return [self.sandbox_table_for(AIAN_LIST)]

    def setup_rule(self, client):
        pass

    def setup_validation(self):
        pass

    def validate_rule(self):
        pass


if __name__ == '__main__':
    from utils import pipeline_logging

    import cdr_cleaner.args_parser as parser
    import cdr_cleaner.clean_cdr_engine as clean_engine

    ARGS = parser.parse_args()
    pipeline_logging.configure(level=logging.DEBUG, add_console_handler=True)

    if ARGS.list_queries:
        clean_engine.add_console_logging()
        query_list = clean_engine.get_query_list(ARGS.project_id,
                                                 ARGS.dataset_id,
                                                 ARGS.sandbox_dataset_id,
                                                 [(CreateAIANLookup,)])
        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(ARGS.project_id, ARGS.dataset_id,
                                   ARGS.sandbox_dataset_id,
                                   [(CreateAIANLookup,)])
