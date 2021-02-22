"""
This rule sandboxes and suppresses reccords whose concept_codes end in 
'History_WhichConditions', 'Condition_OtherCancer', ‘History_AdditionalDiagnosis’,
and 'OutsideTravel6MonthsWhere'.

Runs on the controlled tier.

Original Issues: DC-1381
"""

# Python imports
import logging

# Project imports
import constants.bq_utils as bq_consts
from cdr_cleaner.cleaning_rules.deid.concept_suppression import AbstractBqLookupTableConceptSuppression
from constants.cdr_cleaner import clean_cdr as cdr_consts
from common import JINJA_ENV, OBSERVATION
from utils import pipeline_logging

# Third party imports
from google.cloud.exceptions import GoogleCloudError

LOGGER = logging.getLogger(__name__)

SUPPRESSION_RULE_CONCEPT_TABLE = 'cancer_condition_concepts'

CANCER_CONCEPT_QUERY = JINJA_ENV.from_string("""
CREATE OR REPLACE TABLE `{{project_id}}.{{sandbox_id}}.{{concept_suppression_lookup_table}}` AS
SELECT  c.*
FROM `{{project_id}}.{{dataset_id}}.concept` c
WHERE REGEXP_CONTAINS(c.concept_code, 
    r'(History_WhichConditions)|(Condition_OtherCancer)|(History_AdditionalDiagnosis)|(OutsideTravel6MonthWhere)'
)
""")


class CancerConceptSuppression(AbstractBqLookupTableConceptSuppression):

    def __init__(self, project_id, dataset_id, sandbox_dataset_id):
        """
        Initialize the class with proper information.

        Set the issue numbers, description and affected datasets. As other tickets may affect
        this SQL, append them to the list of Jira Issues.
        DO NOT REMOVE ORIGINAL JIRA ISSUE NUMBERS!
        """
        desc = 'Sandbox and removes records for some cancer condition concepts.'
        super().__init__(
            issue_numbers=['DC1381'],
            description=desc,
            affected_datasets=[cdr_consts.CONTROLLED_TIER_DEID],
            affected_tables=[OBSERVATION],
            project_id=project_id,
            dataset_id=dataset_id,
            sandbox_dataset_id=sandbox_dataset_id,
            concept_suppression_lookup_table=SUPPRESSION_RULE_CONCEPT_TABLE)

    def create_suppression_lookup_table(self, client):
        concept_suppression_lookup_query = CANCER_CONCEPT_QUERY.render(
            project_id=self.project_id,
            dataset_id=self.dataset_id,
            sandbox_id=self.sandbox_dataset_id,
            concept_suppression_lookup_table=self.
            concept_suppression_lookup_table)
        query_job = client.query(concept_suppression_lookup_query)
        result = query_job.result()

        if hasattr(result, 'errors') and result.errors:
            LOGGER.error(f"Error running job {result.job_id}: {result.errors}")
            raise GoogleCloudError(
                f"Error running job {result.job_id}: {result.errors}")

    def setup_validation(self, client, *args, **keyword_args):
        """
        Run required steps for validation setup

        Method to run to setup validation on cleaning rules that will be updating or deleting the values.
        For example:
        if your class updates all the datetime fields you should be implementing the
        logic to get the initial list of values which adhere to a condition we are looking for.

        if your class deletes a subset of rows in the tables you should be implementing
        the logic to get the row counts of the tables prior to applying cleaning rule

        """
        raise NotImplementedError("Please fix me.")

    def validate_rule(self, client, *args, **keyword_args):
        """
        Validates the cleaning rule which deletes or updates the data from the tables

        Method to run validation on cleaning rules that will be updating the values.
        For example:
        if your class updates all the datetime fields you should be implementing the
        validation that checks if the date time values that needs to be updated no
        longer exists in the table.

        if your class deletes a subset of rows in the tables you should be implementing
        the validation that checks if the count of final final row counts + deleted rows
        should equals to initial row counts of the affected tables.

        Raises RunTimeError if the validation fails.
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
                                                 [(CancerConceptSuppression,)])

        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(ARGS.project_id, ARGS.dataset_id,
                                   ARGS.sandbox_dataset_id,
                                   [(CancerConceptSuppression,)])
