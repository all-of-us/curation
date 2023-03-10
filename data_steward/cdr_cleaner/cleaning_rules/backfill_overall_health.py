"""
There are 16 questions that are expected to have answers for every participant
who has taken OverallHealth survey. If a participant has answered one or more
of these 16 questions, curation is expected to backfill the answers with skip
codes for any missing Q/A responses.

NOTE: The concept 1585784 should only be backfilled for female participants. 

This rule extends the abstract class for the skip record creation and run the
backfill.

Original issue: DC-3098
"""
# Python imports
import logging

# Project imports
import constants.cdr_cleaner.clean_cdr as cdr_consts
from cdr_cleaner.cleaning_rules.backfill_survey_records import AbstractBackfillSurveyRecords
from common import OBSERVATION

LOGGER = logging.getLogger(__name__)

BACKFILL_CONCEPTS = [
    1585766, 1585772, 1585778, 1585711, 1585717, 1585723, 1585729, 1585735,
    1585741, 1585747, 1585748, 1585754, 1585760, 1585803, 1585815, 1585784
]
ADDITIONAL_CONDITIONS = {1585784: 'gender_concept_id = 8532'}


class BackfillOverallHealth(AbstractBackfillSurveyRecords):

    def __init__(self,
                 project_id,
                 dataset_id,
                 sandbox_dataset_id,
                 table_namer=None):
        desc = (
            'Backfills OverallHealth survey data with skip codes for participants who have any missing Q/A responses. '
            'This rule extends the abstract class AbstractBackfillSurveyRecords for the skip record creation.'
        )

        super().__init__(issue_numbers=['DC3098'],
                         description=desc,
                         affected_datasets=[cdr_consts.RDR],
                         affected_tables=[OBSERVATION],
                         project_id=project_id,
                         dataset_id=dataset_id,
                         sandbox_dataset_id=sandbox_dataset_id,
                         backfill_concepts=BACKFILL_CONCEPTS,
                         additional_backfill_conditions=ADDITIONAL_CONDITIONS,
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
