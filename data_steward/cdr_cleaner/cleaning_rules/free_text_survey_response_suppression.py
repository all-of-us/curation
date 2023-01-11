"""
To ensure participant privacy, remove any records containing concepts related to free text responses

Original Issue: DC-1387, DC-2799
"""

# Python imports
import logging

from google.cloud.exceptions import GoogleCloudError

# Project imports
from utils import pipeline_logging
from common import JINJA_ENV, OBSERVATION
from constants.cdr_cleaner import clean_cdr as cdr_consts
from cdr_cleaner.cleaning_rules.deid.concept_suppression import AbstractBqLookupTableConceptSuppression

LOGGER = logging.getLogger(__name__)

SUPPRESSION_RULE_CONCEPT_TABLE = 'free_text_suppression_concept'

FREE_TEXT_CONCEPT_QUERY = JINJA_ENV.from_string("""
-- This query generates a lookup table that contains all the suppressed concepts relating to  --
-- free text generated using REGEX --
CREATE OR REPLACE TABLE `{{project_id}}.{{sandbox_dataset}}.{{concept_suppression_table}}` AS
(SELECT * FROM `{{project_id}}.{{dataset_id}}.concept`
WHERE REGEXP_CONTAINS(lower(concept_code), r'(freetext)|(textbox)') OR concept_code = 'notes')
""")


class FreeTextSurveyResponseSuppression(AbstractBqLookupTableConceptSuppression
                                       ):
    """
    Any record in the observation table with a free text concept should be sandboxed and suppressed
    """

    def __init__(self,
                 project_id,
                 dataset_id,
                 sandbox_dataset_id,
                 table_namer=None):
        """
        Initialize the class with proper information.

        Set the issue numbers, description and affected datasets. As other tickets may affect
        this SQL, append them to the list of Jira Issues.
        DO NOT REMOVE ORIGINAL JIRA ISSUE NUMBERS!
        """
        desc = (f'Sandbox and record suppress any records containing '
                f'concepts related to free text responses')
        super().__init__(
            issue_numbers=['DC1387', 'DC2799'],
            description=desc,
            affected_datasets=[
                cdr_consts.CONTROLLED_TIER_DEID, cdr_consts.REGISTERED_TIER_DEID
            ],
            affected_tables=[OBSERVATION],
            project_id=project_id,
            dataset_id=dataset_id,
            sandbox_dataset_id=sandbox_dataset_id,
            concept_suppression_lookup_table=SUPPRESSION_RULE_CONCEPT_TABLE,
            table_namer=table_namer)

    def create_suppression_lookup_table(self, client):
        """

        :param client:
        :return:

        raises google.cloud.exceptions.GoogleCloudError if a QueryJob fails
        """
        concept_suppression_lookup_query = FREE_TEXT_CONCEPT_QUERY.render(
            project_id=self.project_id,
            dataset_id=self.dataset_id,
            sandbox_dataset=self.sandbox_dataset_id,
            concept_suppression_table=self.concept_suppression_lookup_table)
        query_job = client.query(concept_suppression_lookup_query)
        result = query_job.result()

        if query_job.errors or query_job.error_result:
            LOGGER.error(f"Error running job {result.job_id}: {result.errors}")
            raise GoogleCloudError(
                f"Error running job {result.job_id}: {result.errors}")

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
        query_list = clean_engine.get_query_list(
            ARGS.project_id, ARGS.dataset_id, ARGS.sandbox_dataset_id,
            [(FreeTextSurveyResponseSuppression,)])

        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(ARGS.project_id, ARGS.dataset_id,
                                   ARGS.sandbox_dataset_id,
                                   [(FreeTextSurveyResponseSuppression,)])
