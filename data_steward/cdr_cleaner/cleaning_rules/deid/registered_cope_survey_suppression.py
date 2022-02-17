"""
Ensures that the below list of observation_source_concept_id are sandboxed and suppressed. These concepts were
to be suppressed from the registered tier CDR

These are stored in resources.RT_COPE_SUPPRESSION_CSV_PATH

Original Issue: DC-1666, DC-1740

The intent of this cleaning rule is to sandbox and suppress all records with observation_source_concept_id in the
    above table which exist in the observation table. This will be done by providing a list containing the above
    observation_source_concept_id to the AbstractInMemoryLookupTableConceptSuppression class to be sandboxed and
    suppressed
"""

# Python imports
import logging

# Third party imports
from pandas import read_csv

# Project imports
from common import OBSERVATION
from utils import pipeline_logging
import constants.cdr_cleaner.clean_cdr as cdr_consts
from cdr_cleaner.cleaning_rules.deid.concept_suppression import \
    AbstractInMemoryLookupTableConceptSuppression
from resources import RT_COPE_SUPPRESSION_CSV_PATH, RT_CT_COPE_SUPPRESSION_CSV_PATH

LOGGER = logging.getLogger(__name__)


class RegisteredCopeSurveyQuestionsSuppression(
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
        desc = f'Any record with an observation_source_concept_id equal to any concept_id in ' \
               f'resources.RT_COPE_SUPPRESSION_CSV_PATH will be sandboxed and dropped from observation table.'
        super().__init__(issue_numbers=[
            'DC1666', 'DC1740', 'DC1745', 'DC1747', 'DC1750', 'DC1783', 'DC2109'
        ],
                         description=desc,
                         affected_datasets=[cdr_consts.REGISTERED_TIER_DEID],
                         project_id=project_id,
                         dataset_id=dataset_id,
                         sandbox_dataset_id=sandbox_dataset_id,
                         affected_tables=[OBSERVATION],
                         table_namer=table_namer)

    def get_suppressed_concept_ids(self):
        """
        returns a list of all concepts_ids that will need to be suppressed
        """
        with open(RT_COPE_SUPPRESSION_CSV_PATH) as f:
            concept_ids_df = read_csv(f, delimiter=',')
            rt_concept_ids = concept_ids_df['concept_id'].to_list()
        with open(RT_CT_COPE_SUPPRESSION_CSV_PATH) as f:
            concept_ids_df = read_csv(f, delimiter=',')
            rt_ct_concept_ids = concept_ids_df['concept_id'].to_list()
        return rt_concept_ids + rt_ct_concept_ids

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
