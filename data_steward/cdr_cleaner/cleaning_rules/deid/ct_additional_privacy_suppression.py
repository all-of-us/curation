"""
Ensures that all the newly identified concepts as of 02/29/2024 in vocabulary are being suppressed
in the Controlled tier dataset and sandboxed in the sandbox dataset


Original Issue: DC-3749

The intent of this cleaning rule is to ensure the concepts to suppress in CT are sandboxed and suppressed.
"""

# Python imports
import logging
import pandas as pd

# Project imports
from resources import CT_ADDITIONAL_PRIVACY_CONCEPTS_PATH
from gcloud.bq import bigquery
from common import AOU_DEATH, CDM_TABLES, PERSON
from utils import pipeline_logging
import constants.cdr_cleaner.clean_cdr as cdr_consts
from cdr_cleaner.cleaning_rules.deid.concept_suppression import \
    AbstractBqLookupTableConceptSuppression

# Third party imports
from google.cloud.exceptions import GoogleCloudError

LOGGER = logging.getLogger(__name__)
ISSUE_NUMBERS = ['dc3749']


class CTAdditionalPrivacyConceptSuppression(
        AbstractBqLookupTableConceptSuppression):

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
        ct_additional_privacy_concept_table = f'ct_additional_privacy_{ISSUE_NUMBERS[0]}'
        super().__init__(
            issue_numbers=ISSUE_NUMBERS,
            description=desc,
            affected_datasets=[cdr_consts.CONTROLLED_TIER_DEID],
            project_id=project_id,
            dataset_id=dataset_id,
            sandbox_dataset_id=sandbox_dataset_id,
            affected_tables=list(set(CDM_TABLES + [AOU_DEATH]) - {PERSON}),
            concept_suppression_lookup_table=ct_additional_privacy_concept_table,
            table_namer=table_namer)

    def create_suppression_lookup_table(self, client):
        df = pd.read_csv(CT_ADDITIONAL_PRIVACY_CONCEPTS_PATH)
        dataset_ref = bigquery.DatasetReference(self.project_id,
                                                self.sandbox_dataset_id)
        table_ref = dataset_ref.table(self.concept_suppression_lookup_table)
        result = client.load_table_from_dataframe(df, table_ref).result()

        if hasattr(result, 'errors') and result.errors:
            LOGGER.error(f"Error running job {result.job_id}: {result.errors}")
            raise GoogleCloudError(
                f"Error running job {result.job_id}: {result.errors}")

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


if __name__ == '__main__':
    import cdr_cleaner.args_parser as parser
    import cdr_cleaner.clean_cdr_engine as clean_engine

    ARGS = parser.default_parse_args()
    pipeline_logging.configure(level=logging.DEBUG, add_console_handler=True)

    if ARGS.list_queries:
        clean_engine.add_console_logging()
        query_list = clean_engine.get_query_list(
            ARGS.project_id, ARGS.dataset_id, ARGS.sandbox_dataset_id,
            [(CTAdditionalPrivacyConceptSuppression,)])
        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(ARGS.project_id, ARGS.dataset_id,
                                   ARGS.sandbox_dataset_id,
                                   [(CTAdditionalPrivacyConceptSuppression,)])
