"""
Original Issue: DC-1529

The intent of this cleaning rule is to suppress any rows in the observation table where the response is related to
 organ transplant

OrganTransplantDescription_OtherOrgan - 1585807 -> https://athena.ohdsi.org/search-terms/terms/1585807
OrganTransplantDescription_OtherTissue - 1585808 -> https://athena.ohdsi.org/search-terms/terms/1585808
"""

# Python imports
import logging

# Project imports
from common import AOU_REQUIRED
from utils import pipeline_logging
import constants.cdr_cleaner.clean_cdr as cdr_consts
from cdr_cleaner.cleaning_rules.deid.concept_suppression import AbstractInMemoryLookupTableConceptSuppression

LOGGER = logging.getLogger(__name__)


class OrganTransplantConceptSuppression(
        AbstractInMemoryLookupTableConceptSuppression):

    def __init__(self, project_id, dataset_id, sandbox_dataset_id):
        """
        Initialize the class with proper info.

        Set the issue numbers, description and affected datasets.  As other
        tickets may affect this SQL, append them to the list of Jira Issues.
        DO NOT REMOVE ORIGINAL JIRA ISSUE NUMBERS!
        """
        desc = ('Sandbox and record suppress all records with a concept_id '
                'relating to OrganTransplant in the observation table. ')
        super().__init__(issue_numbers=['DC1529'],
                         description=desc,
                         affected_datasets=[cdr_consts.CONTROLLED_TIER_DEID],
                         project_id=project_id,
                         dataset_id=dataset_id,
                         sandbox_dataset_id=sandbox_dataset_id,
                         affected_tables=AOU_REQUIRED)

    def get_suppressed_concept_ids(self):
        # OrganTransplantDescription_OtherOrgan - 1585807 -> https://athena.ohdsi.org/search-terms/terms/1585807
        # OrganTransplantDescription_OtherTissue - 1585808 -> https://athena.ohdsi.org/search-terms/terms/1585808
        return [1585807, 1585808]

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
            [(OrganTransplantConceptSuppression,)])
        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(ARGS.project_id, ARGS.dataset_id,
                                   ARGS.sandbox_dataset_id,
                                   [(OrganTransplantConceptSuppression,)])
