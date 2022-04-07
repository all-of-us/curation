"""
Ensures that all the Missing Standard concepts and the replaced concepts in vocabulary are being suppressed
 in Controlled tier datasets


Original Issue: DC-2278

The intent of this cleaning rule is ensure the concepts listed for suppression in the controlled tier are
 sandboxed and then suppressed.
"""

# Python imports
import logging

# Third party imports
from pandas import read_csv

# Project imports
from resources import MISSING_PRIVACY_STANDARD_CONCEPTS_PATH, REPLACED_PRIVACY_CONCEPTS_PATH
from common import CDM_TABLES
from utils import pipeline_logging
import constants.cdr_cleaner.clean_cdr as cdr_consts
from cdr_cleaner.cleaning_rules.deid.concept_suppression import \
    AbstractInMemoryLookupTableConceptSuppression

LOGGER = logging.getLogger(__name__)
ISSUE_NUMBERS = ['DC2278']


class MissingAndReplacedConceptSuppression(
        AbstractInMemoryLookupTableConceptSuppression):

    def __init__(self,
                 project_id,
                 dataset_id,
                 sandbox_dataset_id,
                 table_namer=None):
        """
        Initialize the class with proper info.

        Set the issue numbers, description and affected datasets.  As other
        tickets may affect this SQL, append them to the list of Jira Issues.
        DO NOT REMOVE ORIGINAL JIRA ISSUE NUMBERS!
        """
        desc = f'Any record with an concept_id equal to any of the values in ' \
               f'{ISSUE_NUMBERS} will be sandboxed and dropped from the domain tables'
        super().__init__(issue_numbers=ISSUE_NUMBERS,
                         description=desc,
                         affected_datasets=[cdr_consts.CONTROLLED_TIER_DEID],
                         project_id=project_id,
                         dataset_id=dataset_id,
                         sandbox_dataset_id=sandbox_dataset_id,
                         affected_tables=CDM_TABLES,
                         table_namer=table_namer)

    def get_suppressed_concept_ids(self):
        with open(MISSING_PRIVACY_STANDARD_CONCEPTS_PATH) as f:
            missing_concept_ids_df = read_csv(f, delimiter=',')
            missing_concept_ids = missing_concept_ids_df['concept_id'].to_list()
        with open(REPLACED_PRIVACY_CONCEPTS_PATH) as f:
            replaced_concept_ids_df = read_csv(f, delimiter=',')
            replaced_concept_ids = replaced_concept_ids_df[
                'concept_id'].to_list()

        return missing_concept_ids + replaced_concept_ids

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
            [(MissingAndReplacedConceptSuppression,)])
        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(ARGS.project_id, ARGS.dataset_id,
                                   ARGS.sandbox_dataset_id,
                                   [(MissingAndReplacedConceptSuppression,)])
