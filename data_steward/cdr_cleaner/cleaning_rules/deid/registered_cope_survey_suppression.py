"""
Ensures that the below list of observation_source_concept_id are sandboxed and suppressed. These concepts were
to be suppressed from the registered tier CDR

| observation_source_concept_id |                                                               |
|-------------------------------|---------------------------------------------------------------|
| 1310058                       | https://athena.ohdsi.org/search-terms/terms/1310058           |
| 1310065                       | https://athena.ohdsi.org/search-terms/terms/1310065           |
| 1333012                       | https://athena.ohdsi.org/search-terms/terms/1333012           |
| 1333234                       | https://athena.ohdsi.org/search-terms/terms/1333234           |
| 702686                        | https://athena.ohdsi.org/search-terms/terms/702686            |
| 1333327                       | https://athena.ohdsi.org/search-terms/terms/1333327           |
| 1333118                       | https://athena.ohdsi.org/search-terms/terms/1333118           |
| 1310054                       | https://athena.ohdsi.org/search-terms/terms/1310054           |
| 1333326                       | https://athena.ohdsi.org/search-terms/terms/1333326           |
| 310066                        | https://athena.ohdsi.org/search-terms/terms/310066            |
| 596884                        | https://athena.ohdsi.org/search-terms/terms/596884            |
| 596885                        | https://athena.ohdsi.org/search-terms/terms/596885            |
| 596886                        | https://athena.ohdsi.org/search-terms/terms/596886            |
| 596887                        | https://athena.ohdsi.org/search-terms/terms/596887            |
| 596888                        | https://athena.ohdsi.org/search-terms/terms/596888            |
| 596889                        | https://athena.ohdsi.org/search-terms/terms/596889            |
| 1310137                       | https://athena.ohdsi.org/search-terms/terms/1310137           |
| 1310146                       | https://athena.ohdsi.org/search-terms/terms/1310146           |
| 1333015                       | https://athena.ohdsi.org/search-terms/terms/1333015           |
| 1333023                       | https://athena.ohdsi.org/search-terms/terms/1333023           |
| 1333016                       | https://athena.ohdsi.org/search-terms/terms/1333016           |
| 715714                        | https://athena.ohdsi.org/search-terms/terms/715714            |
| 1310147                       | https://athena.ohdsi.org/search-terms/terms/1310147           |
| 715726                        | https://athena.ohdsi.org/search-terms/terms/715726            |
-------------------------------------------------------------------------------------------------

Original Issue: DC-1666, DC-1740

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

REGISTERED_COPE_SURVEY_SUPPRESS_CONCEPT_LIST = [
    1310058, 1310065, 1333012, 1333234, 702686, 1333327, 1333118, 1310054,
    1333326, 1310066, 596884, 596885, 596886, 596887, 596888, 596889, 1310137,
    1310146, 1333015, 1333023, 1333016, 715714, 1310147, 715726
]


class RegisteredCopeSurveyQuestionsSuppression(
        AbstractInMemoryLookupTableConceptSuppression):

    def __init__(self, project_id, dataset_id, sandbox_dataset_id):
        """
        Initialize the class with proper info.

        Set the issue numbers, description and affected datasets.  As other
        tickets may affect this SQL, append them to the list of Jira Issues.
        DO NOT REMOVE ORIGINAL JIRA ISSUE NUMBERS!
        """
        desc = f'Any record with an observation_source_concept_id equal to any concept_id in ' \
               f'{REGISTERED_COPE_SURVEY_SUPPRESS_CONCEPT_LIST} will be sandboxed and dropped from observation table.'
        super().__init__(issue_numbers=['DC1666', 'DC1740'],
                         description=desc,
                         affected_datasets=[cdr_consts.REGISTERED_TIER_DEID],
                         project_id=project_id,
                         dataset_id=dataset_id,
                         sandbox_dataset_id=sandbox_dataset_id,
                         affected_tables=[OBSERVATION])

    def get_suppressed_concept_ids(self):
        """
        returns a list of all concepts_ids that will need to be suppressed
        """

        return REGISTERED_COPE_SURVEY_SUPPRESS_CONCEPT_LIST

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
            [(RegisteredCopeSurveyQuestionsSuppression,)])
        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(
            ARGS.project_id, ARGS.dataset_id, ARGS.sandbox_dataset_id,
            [(RegisteredCopeSurveyQuestionsSuppression,)])
