"""

Original issue: DC-3098
"""
# Python imports
import logging

# Project imports
import constants.cdr_cleaner.clean_cdr as cdr_consts
from cdr_cleaner.cleaning_rules.backfill_survey_records import AbstractBackfillSurveyRecords
from common import OBSERVATION

LOGGER = logging.getLogger(__name__)

BACKFILL_CONCEPTS = []


class BackfillOverallHealth(AbstractBackfillSurveyRecords):

    def __init__(self,
                 project_id,
                 dataset_id,
                 sandbox_dataset_id,
                 table_namer=None):
        desc = ('')

        super().__init__(issue_numbers=['DC3098'],
                         description=desc,
                         affected_datasets=[cdr_consts.RDR],
                         affected_tables=[OBSERVATION],
                         project_id=project_id,
                         dataset_id=dataset_id,
                         sandbox_dataset_id=sandbox_dataset_id,
                         backfill_concepts=BACKFILL_CONCEPTS,
                         table_namer=table_namer)

    def setup_rule(self, client):
        pass

    def setup_validation(self, client):
        raise NotImplementedError("Please fix me.")

    def validate_rule(self, client):
        raise NotImplementedError("Please fix me.")


if __name__ == '__main__':
    import cdr_cleaner.args_parser as parser
    import cdr_cleaner.clean_cdr_engine as clean_engine

    ARGS = parser.parse_args()

    if ARGS.list_queries:
        clean_engine.add_console_logging()
        query_list = clean_engine.get_query_list(ARGS.project_id,
                                                 ARGS.dataset_id,
                                                 ARGS.sandbox_dataset_id,
                                                 [(BackfillOverallHealth,)])
        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(ARGS.project_id, ARGS.dataset_id,
                                   ARGS.sandbox_dataset_id,
                                   [(BackfillOverallHealth,)])
