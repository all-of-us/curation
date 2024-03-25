"""
Ensures that all the newly identified concepts in vocabulary are being suppressed
in the Registered tier dataset and sandboxed in the sandbox dataset

For observation table, we need to ensure PPI concepts that are post-coordinated are not suppressed by this CR
For concepts that are suppressed in both PPI and EHR, it is handled by CTAdditionalPrivacyConceptSuppression

Original Issue: DC-3749

The intent of this cleaning rule is to ensure the post-coordinated concepts to suppress
in CT are sandboxed and suppressed.
"""

# Python imports
import logging
import pandas as pd

from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule
# Project imports
from resources import CT_OBSERVATION_PRIVACY_CONCEPTS_PATH, CT_ADDITIONAL_PRIVACY_CONCEPTS_PATH, \
    CT_RT_PUBLICLY_REPORTABLE_CONCEPTS_PATH
from gcloud.bq import bigquery
from common import OBSERVATION, JINJA_ENV
from utils import pipeline_logging
import constants.cdr_cleaner.clean_cdr as cdr_consts

# Third party imports
from google.cloud.exceptions import GoogleCloudError

LOGGER = logging.getLogger(__name__)
ISSUE_NUMBERS = ['dc3749']

SANDBOX_OBS = JINJA_ENV.from_string("""
CREATE TABLE `{{project_id}}.{{sandbox_id}}.{{sandbox_table}}` AS
SELECT
  d.*
FROM `{{project_id}}.{{dataset_id}}.observation` AS d
JOIN `{{project_id}}.{{dataset_id}}.observation_ext` AS m
  ON d.observation_id = m.observation_id
LEFT JOIN `{{project_id}}.{{sandbox_id}}.{{postc_concept_sup}}` AS s1
  ON d.observation_concept_id = s1.concept_id 
LEFT JOIN `{{project_id}}.{{sandbox_id}}.{{postc_concept_sup}}` AS s2
  ON d.observation_type_concept_id = s2.concept_id 
LEFT JOIN `{{project_id}}.{{sandbox_id}}.{{postc_concept_sup}}` AS s3
  ON d.value_as_concept_id = s3.concept_id 
LEFT JOIN `{{project_id}}.{{sandbox_id}}.{{postc_concept_sup}}` AS s4
  ON d.qualifier_concept_id = s4.concept_id 
LEFT JOIN `{{project_id}}.{{sandbox_id}}.{{postc_concept_sup}}` AS s5
  ON d.unit_concept_id = s5.concept_id 
LEFT JOIN `{{project_id}}.{{sandbox_id}}.{{postc_concept_sup}}` AS s6
  ON d.observation_source_concept_id = s6.concept_id 
LEFT JOIN `{{project_id}}.{{sandbox_id}}.{{postc_concept_sup}}` AS s7
  ON d.value_source_concept_id = s7.concept_id 
WHERE m.src_id LIKE "%EHR%"
AND COALESCE(
   s1.concept_id
,  s2.concept_id
,  s3.concept_id
,  s4.concept_id
,  s5.concept_id
,  s6.concept_id
,  s7.concept_id
) IS NOT NULL
UNION ALL
SELECT
  d.*
FROM `{{project_id}}.{{dataset_id}}.observation` AS d
JOIN `{{project_id}}.{{dataset_id}}.observation_ext` AS m
  ON d.observation_id = m.observation_id
LEFT JOIN `{{project_id}}.{{sandbox_id}}.{{rest_concept_sup}}` AS s1
  ON d.observation_concept_id = s1.concept_id 
LEFT JOIN `{{project_id}}.{{sandbox_id}}.{{rest_concept_sup}}` AS s2
  ON d.observation_type_concept_id = s2.concept_id 
LEFT JOIN `{{project_id}}.{{sandbox_id}}.{{rest_concept_sup}}` AS s3
  ON d.value_as_concept_id = s3.concept_id 
LEFT JOIN `{{project_id}}.{{sandbox_id}}.{{rest_concept_sup}}` AS s4
  ON d.qualifier_concept_id = s4.concept_id 
LEFT JOIN `{{project_id}}.{{sandbox_id}}.{{rest_concept_sup}}` AS s5
  ON d.unit_concept_id = s5.concept_id 
LEFT JOIN `{{project_id}}.{{sandbox_id}}.{{rest_concept_sup}}` AS s6
  ON d.observation_source_concept_id = s6.concept_id 
LEFT JOIN `{{project_id}}.{{sandbox_id}}.{{rest_concept_sup}}` AS s7
  ON d.value_source_concept_id = s7.concept_id 
WHERE COALESCE(
   s1.concept_id
,  s2.concept_id
,  s3.concept_id
,  s4.concept_id
,  s5.concept_id
,  s6.concept_id
,  s7.concept_id
) IS NOT NULL
""")

SUPPRESS_POSTC_OBS = JINJA_ENV.from_string("""
DELETE
FROM `{{project_id}}.{{dataset_id}}.observation`
WHERE observation_id IN (
    SELECT observation_id
    FROM `{{project_id}}.{{sandbox_id}}.{{sandbox_table}}`)
""")


class CTObservationPrivacySuppression(BaseCleaningRule):

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
        self.rt_observation_postc_concept_table = f'rt_observation_postc_{ISSUE_NUMBERS[0]}'
        self.rt_observation_rest_concept_table = f'rt_observation_rest_{ISSUE_NUMBERS[0]}'
        super().__init__(issue_numbers=ISSUE_NUMBERS,
                         description=desc,
                         affected_datasets=[cdr_consts.REGISTERED_TIER_DEID],
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
        df = pd.read_csv(CT_OBSERVATION_PRIVACY_CONCEPTS_PATH)
        dataset_ref = bigquery.DatasetReference(self.project_id,
                                                self.sandbox_dataset_id)
        table_ref = dataset_ref.table(self.rt_observation_postc_concept_table)
        result = client.load_table_from_dataframe(df, table_ref).result()

        if hasattr(result, 'errors') and result.errors:
            LOGGER.error(f"Error running job {result.job_id}: {result.errors}")
            raise GoogleCloudError(
                f"Error running job {result.job_id}: {result.errors}")

        df_all = pd.read_csv(CT_ADDITIONAL_PRIVACY_CONCEPTS_PATH)
        df_pr = pd.read_csv(CT_RT_PUBLICLY_REPORTABLE_CONCEPTS_PATH)
        df = pd.concat([df_all, df_pr], ignore_index=True)
        dataset_ref = bigquery.DatasetReference(self.project_id,
                                                self.sandbox_dataset_id)
        table_ref = dataset_ref.table(self.rt_observation_rest_concept_table)
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
            postc_concept_sup=self.rt_observation_postc_concept_table,
            rest_concept_sup=self.rt_observation_rest_concept_table,
        )
        queries_list.append(sandbox_query)

        suppress_query = dict()
        suppress_query[cdr_consts.QUERY] = SUPPRESS_POSTC_OBS.render(
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
            [(CTObservationPrivacySuppression,)])
        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(ARGS.project_id, ARGS.dataset_id,
                                   ARGS.sandbox_dataset_id,
                                   [(CTObservationPrivacySuppression,)])
