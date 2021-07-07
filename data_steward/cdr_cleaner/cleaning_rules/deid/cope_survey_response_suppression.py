"""
Ensures that the below list of observation_source_concept_id are sandboxed and suppressed. These concepts were
    identified from the RedCap codebooks as of 03/17/2021

| observation_source_concept_id |
|-------------------------------|
| 1333234                       |
| 1310066                       |
| 715725                        |
| 1310147                       |
| 702686                        |
| 1310054                       |
| 715726                        |
| 715724                        |
| 715714                        |
| 1310146                       |
| 1310058                       |
| 1310065                       |
---------------------------------

Original Issue: DC-1492

The intent of this cleaning rule is to sandbox and suppress all records with observation_source_concept_id in the
    above table which exist in the observation table. This will be done by providing a list containing the above
    observation_source_concept_id to the AbstractInMemoryLookupTableConceptSuppression class to be sandboxed and
    suppressed
"""

# Python imports
import logging

# Project imports
from common import OBSERVATION
from utils import pipeline_logging
import constants.cdr_cleaner.clean_cdr as cdr_consts
from cdr_cleaner.cleaning_rules.deid.concept_suppression import \
    AbstractInMemoryLookupTableConceptSuppression

LOGGER = logging.getLogger(__name__)


class CopeSurveyResponseSuppression(
        AbstractInMemoryLookupTableConceptSuppression):

    def __init__(self, project_id, dataset_id, sandbox_dataset_id):
        """
        Initialize the class with proper info.

        Set the issue numbers, description and affected datasets.  As other
        tickets may affect this SQL, append them to the list of Jira Issues.
        DO NOT REMOVE ORIGINAL JIRA ISSUE NUMBERS!
        """
        desc = 'Any record with an observation_source_concept_id equal to any of these values (1333234, 1310066, ' \
               '715725, 1310147, 702686, 1310054, 715726, 715724, 715714, 1310146, 1310058) will be ' \
               'sandboxed and dropped from the observation table'
        super().__init__(issue_numbers=['DC1492'],
                         description=desc,
                         affected_datasets=[cdr_consts.CONTROLLED_TIER_DEID],
                         project_id=project_id,
                         dataset_id=dataset_id,
                         sandbox_dataset_id=sandbox_dataset_id,
                         affected_tables=[OBSERVATION])

    def get_suppressed_concept_ids(self):
        # https://athena.ohdsi.org/search-terms/terms/1333234
        # https://athena.ohdsi.org/search-terms/terms/1310066
        # https://athena.ohdsi.org/search-terms/terms/715725
        # https://athena.ohdsi.org/search-terms/terms/1310147
        # https://athena.ohdsi.org/search-terms/terms/702686
        # https://athena.ohdsi.org/search-terms/terms/1310054
        # https://athena.ohdsi.org/search-terms/terms/715726
        # https://athena.ohdsi.org/search-terms/terms/715724
        # https://athena.ohdsi.org/search-terms/terms/715714
        # https://athena.ohdsi.org/search-terms/terms/1310146
        # https://athena.ohdsi.org/search-terms/terms/1310058
        # https://athena.ohdsi.org/search-terms/terms/1310065
        return [
            1333234, 1310066, 715725, 1310147, 702686, 1310054, 715726, 715724,
            715714, 1310146, 1310058, 1310065
        ]

    def setup_validation(self, client, *args, **keyword_args):
        pass

    def validate_rule(self, client, *args, **keyword_args):
        pass


if __name__ == '__main__':
    import cdr_cleaner.args_parser as parser
    import cdr_cleaner.clean_cdr_engine as clean_engine

    ARGS = parser.default_parse_args()
    pipeline_logging.configure(level=logging.DEBUG, add_console_handler=True)

    if ARGS.list_queries:
        clean_engine.add_console_logging()
        query_list = clean_engine.get_query_list(
            ARGS.project_id, ARGS.dataset_id, ARGS.sandbox_dataset_id,
            [(CopeSurveyResponseSuppression,)])
        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(ARGS.project_id, ARGS.dataset_id,
                                   ARGS.sandbox_dataset_id,
                                   [(CopeSurveyResponseSuppression,)])
