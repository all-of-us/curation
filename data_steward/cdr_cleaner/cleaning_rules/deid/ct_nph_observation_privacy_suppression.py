"""
Ensures that all the concepts identified in the NPH privacy documentation are suppressed in the controlled tier dataset
and sandboxed in the sandbox dataset

Original Issue: DL504

"""

# Python imports
import logging
import pandas as pd

from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule
# Project imports
from resources import CT_NPH_OBSERVATION_PRIVACY_CONCEPTS_PATH
from gcloud.bq import bigquery
from common import OBSERVATION, JINJA_ENV
from utils import pipeline_logging
import constants.cdr_cleaner.clean_cdr as cdr_consts

# Third party imports
from google.cloud.exceptions import GoogleCloudError

LOGGER = logging.getLogger(__name__)
ISSUE_NUMBERS = ['dl504']

SANDBOX_OBS = JINJA_ENV.from_string("""
CREATE TABLE `{{project_id}}.{{sandbox_id}}.{{sandbox_table}}` AS (
WITH nph_sep_concepts AS (
SELECT LOWER(l.concept_code) concept_code, c.concept_id
FROM `{{project_id}}.{{sandbox_id}}.{{concept_sup_lookup}}` l 
LEFT JOIN `{{project_id}}.{{dataset_id}}.concept` c
ON l.concept_code = c.concept_code
)

SELECT o.*
FROM `{{project_id}}.{{dataset_id}}.observation` AS o
WHERE observation_concept_id IN (SELECT concept_id FROM nph_sep_concepts)
OR observation_source_concept_id IN (SELECT concept_id FROM nph_sep_concepts)
OR value_source_concept_id IN (SELECT concept_id FROM nph_sep_concepts)
OR value_as_concept_id IN (SELECT concept_id FROM nph_sep_concepts)
OR LOWER(observation_source_value) IN (SELECT concept_code FROM nph_sep_concepts)
OR LOWER(value_source_value) IN (SELECT concept_code FROM nph_sep_concepts)
)
""")

SUPPRESS_NPH_OBS = JINJA_ENV.from_string("""
DELETE
FROM `{{project_id}}.{{dataset_id}}.observation`
WHERE observation_id IN (
    SELECT observation_id
    FROM `{{project_id}}.{{sandbox_id}}.{{sandbox_table}}`)
""")


class CTNPHObservationPrivacySuppression(BaseCleaningRule):

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
        self.ct_nph_observation_suppressions_table = f'ct_nph_observation_suppressions_{ISSUE_NUMBERS[0]}'
        super().__init__(issue_numbers=ISSUE_NUMBERS,
                         description=desc,
                         affected_datasets=[cdr_consts.CONTROLLED_TIER_DEID],
                         project_id=project_id,
                         dataset_id=dataset_id,
                         sandbox_dataset_id=sandbox_dataset_id,
                         affected_tables=[OBSERVATION],
                         table_namer=table_namer)

    def setup_rule(self, client, *args, **keyword_args):
        """
        Create the suppression lookup table in the sandbox dataset
        :param client:
        """
        df = pd.read_csv(CT_NPH_OBSERVATION_PRIVACY_CONCEPTS_PATH)
        dataset_ref = bigquery.DatasetReference(self.project_id,
                                                self.sandbox_dataset_id)
        table_ref = dataset_ref.table(
            self.ct_nph_observation_suppressions_table)
        result = client.load_table_from_dataframe(df, table_ref).result()

        if hasattr(result, 'errors') and result.errors:
            LOGGER.error(f"Error running job {result.job_id}: {result.errors}")
            raise GoogleCloudError(
                f"Error running job {result.job_id}: {result.errors}")

    def get_query_specs(self, *args, **keyword_args):
        """
        Return a list of dictionary query specifications.

        :return:  A list of dictionaries. Each dictionary contains a single query
            and a specification for how to execute that query. The specifications
            are optional but the query is required.
        """

        queries_list = []
        sandbox_query = dict()
        sandbox_query[cdr_consts.QUERY] = SANDBOX_OBS.render(
            project_id=self.project_id,
            dataset_id=self.dataset_id,
            sandbox_id=self.sandbox_dataset_id,
            sandbox_table=self.sandbox_table_for(OBSERVATION),
            concept_sup_lookup=self.ct_nph_observation_suppressions_table)
        queries_list.append(sandbox_query)

        suppress_query = dict()
        suppress_query[cdr_consts.QUERY] = SUPPRESS_NPH_OBS.render(
            project_id=self.project_id,
            dataset_id=self.dataset_id,
            sandbox_id=self.sandbox_dataset_id,
            sandbox_table=self.sandbox_table_for(OBSERVATION),
        )
        queries_list.append(suppress_query)

        return queries_list

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

    def get_sandbox_tablenames(self):
        return [self.sandbox_table_for(table) for table in self.affected_tables]


if __name__ == '__main__':
    import cdr_cleaner.args_parser as parser
    import cdr_cleaner.clean_cdr_engine as clean_engine

    ARGS = parser.default_parse_args()
    pipeline_logging.configure(level=logging.DEBUG, add_console_handler=True)

    if ARGS.list_queries:
        clean_engine.add_console_logging()
        query_list = clean_engine.get_query_list(
            ARGS.project_id, ARGS.dataset_id, ARGS.sandbox_dataset_id,
            [(CTNPHObservationPrivacySuppression,)])
        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(ARGS.project_id, ARGS.dataset_id,
                                   ARGS.sandbox_dataset_id,
                                   [(CTNPHObservationPrivacySuppression,)])
