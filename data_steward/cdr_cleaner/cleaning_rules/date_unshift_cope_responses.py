"""
Reverse Date Shift for COPE Responses.

The COPE survey is longitudinal (administered monthly), so researchers will need a way to identify with which survey
monthly iteration data is associated. Although there is a long-term plan to allow for survey versioning,
this is anticipated to provide a short-term solution to support inclusion of COPE data in the CDR.

The Date unshift is applied on the cope survey responses by following steps:

1)lookup table with all the COPE survey module descendants were generated using the concept_relationship table.

2) Created a CTE with filtering out the observation table with only observation_concept_ids that were cope_concept_ids
 using the `pipeline_tables.cope_concepts` and then apply the date_shit to those records in the CTE.

3) Then Left joined this CTE temp table to the observation table post_deid using the observation_id.

4) Using the Left join used coalesce function to get the date_unshifted observation_date and observation_datetime for
 cope survey responses.

Original Issue: DC-938
"""

# Python imports
import logging

# Project Imports
import constants.bq_utils as bq_consts
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule
from constants.cdr_cleaner import clean_cdr as cdr_consts
from common import JINJA_ENV, OBSERVATION
from cdr_cleaner.manual_cleaning_rules.survey_version_info import COPESurveyVersionTask

LOGGER = logging.getLogger(__name__)

OBSERVATION_EXT = f'{OBSERVATION}_ext'

SANDBOX_COPE_SURVEY_OBS_QUERY = JINJA_ENV.from_string("""
CREATE OR REPLACE TABLE
  `{{project_id}}.{{sandbox_dataset}}.{{intermediary_obs_table}}` AS
SELECT
  *
FROM
  `{{project_id}}.{{dataset_id}}.{{observation_table}}`
INNER JOIN
  `{{project_id}}.{{dataset_id}}.{{observation_ext_table}}`
USING
  (observation_id)
WHERE
  survey_version_concept_id IN (2100000002, 2100000003, 2100000004, 2100000005, 2100000006,
 2100000007, 905047, 905055, 765936) 
    """)

SANDBOX_COPE_SURVEY_SC_QUERY = JINJA_ENV.from_string("""
CREATE OR REPLACE TABLE
  `{{project_id}}.{{sandbox_dataset}}.{{intermediary_sc_table}}` AS
SELECT
  *
FROM
  `{{project_id}}.{{dataset_id}}.{{sc_table}}`
WHERE
  survey_source_concept_id IN (2100000002, 2100000003, 2100000004, 2100000005, 2100000006,
 2100000007, 905047, 905055, 765936) 
    """)

DATE_UNSHIFT_OBS_QUERY = JINJA_ENV.from_string("""
WITH
  cope_unshift AS (
  SELECT
    observation_id,
    observation_date,
    observation_datetime,
  FROM
    `{{project_id}}.{{pre_deid_dataset}}.{{observation_table}}`
   WHERE
    observation_id IN (
    SELECT
      observation_id
    FROM
      `{{project_id}}.{{sandbox_dataset}}.{{intermediary_obs_table}}`))
SELECT
  ob.observation_id,
  ob.person_id,
  ob.observation_concept_id,
  coalesce(cs.observation_date,
    ob.observation_date) AS observation_date,
  coalesce(cs.observation_datetime,
    ob.observation_datetime) AS observation_datetime,
  ob.observation_type_concept_id,
  ob.value_as_number,
  ob.value_as_string,
  ob.value_as_concept_id,
  ob.qualifier_concept_id,
  ob.unit_concept_id,
  ob.provider_id,
  ob.visit_occurrence_id,
  ob.visit_detail_id,
  ob.observation_source_value,
  ob.observation_source_concept_id,
  ob.unit_source_value,
  ob.qualifier_source_value,
  ob.value_source_concept_id,
  ob.value_source_value,
  ob.questionnaire_response_id
FROM
  `{{project_id}}.{{dataset_id}}.{{observation_table}}` AS ob
LEFT JOIN
  cope_unshift AS cs
USING
  (observation_id)""")

DATE_UNSHIFT_SC_QUERY = JINJA_ENV.from_string("""
WITH
  cope_unshift AS (
  SELECT
    survey_conduct_id,
    survey_start_date,
    survey_start_datetime,
    survey_end_date,
    survey_end_datetime
  FROM
    `{{project_id}}.{{pre_deid_dataset}}.{{survey_conduct_table}}`
   WHERE
    survey_conduct_id IN (
    SELECT
      survey_conduct_id
    FROM
      `{{project_id}}.{{sandbox_dataset}}.{{intermediary_sc_table}}`))
SELECT
  sc.survey_conduct_id,
  sc.person_id,
  sc.survey_concept_id,
  coalesce(cs.survey_start_date,
    sc.survey_start_date) AS survey_start_date,
  coalesce(cs.survey_start_datetime,
    sc.survey_start_datetime) AS survey_start_datetime,
  coalesce(cs.survey_end_date,
    sc.survey_end_date) AS survey_end_date,
  coalesce(cs.survey_end_datetime,
    sc.survey_end_datetime) AS survey_end_datetime,
  sc.provider_id,
  sc.assisted_concept_id,
  sc.respondent_type_concept_id,
  sc.timing_concept_id,
  sc.collection_method_concept_id,
  sc.assisted_source_value,
  sc.respondent_type_source_value,
  sc.timing_source_value,
  sc.collection_method_source_value,
  sc.survey_source_value,
  sc.survey_source_concept_id,
  sc.survey_source_identifier,
  sc.validated_survey_concept_id,
  sc.validated_survey_source_value,
  sc.survey_version_number,
  sc.visit_occurrence_id,
  sc.response_visit_occurrence_id
FROM
  `{{project_id}}.{{dataset_id}}.{{survey_conduct_table}}` AS sc
LEFT JOIN
  cope_unshift AS cs
USING
  (survey_conduct_id)""")


class DateUnShiftCopeResponses(BaseCleaningRule):
    """
    Reverse Date Shift for COPE Responses...
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
        desc = 'Reverse Date Shift for COPE Responses'
        super().__init__(
            issue_numbers=['DC938', 'DC982', 'DC970', 'DC2438'],
            description=desc,
            affected_datasets=[cdr_consts.REGISTERED_TIER_DEID_BASE],
            affected_tables=[OBSERVATION],
            project_id=project_id,
            dataset_id=dataset_id,
            sandbox_dataset_id=sandbox_dataset_id,
            depends_on=[COPESurveyVersionTask],
            table_namer=table_namer)

    def get_combined_dataset_from_deid_dataset(self, dataset_name):
        """
        Returns a combined dataset name from a de_identified dataset name
        :param dataset_name: name of a de_identified_dataset_name
        :return: combined dataset name as a string
        """

        return f'{dataset_name[1:9].lower()}_combined'

    def setup_rule(self, client=None):
        """
        Load required resources prior to executing cleaning rule queries.

        Method to run data upload options before executing the first cleaning
        rule of a class.  For example, if your class requires loading a static
        table, that load operation should be defined here.  It SHOULD NOT BE
        defined as part of get_query_specs().
        :param client:
        :return:
        """
        pass

    def get_query_specs(self):
        """
        :return:
        """
        sandbox_query = dict()
        sandbox_query[cdr_consts.QUERY] = SANDBOX_COPE_SURVEY_OBS_QUERY.render(
            project_id=self.project_id,
            sandbox_dataset=self.sandbox_dataset_id,
            intermediary_table=self.get_sandbox_tablenames()[0],
            dataset_id=self.dataset_id,
            observation_table=OBSERVATION,
            observation_ext_table=OBSERVATION_EXT)

        update_query = dict()
        update_query[cdr_consts.QUERY] = DATE_UNSHIFT_OBS_QUERY.render(
            project_id=self.project_id,
            pre_deid_dataset=self.get_combined_dataset_from_deid_dataset(
                self.dataset_id),
            dataset_id=self.dataset_id,
            sandbox_dataset=self.sandbox_dataset_id,
            intermediary_table=self.get_sandbox_tablenames()[0],
            observation_table=OBSERVATION)
        update_query[cdr_consts.DESTINATION_TABLE] = OBSERVATION
        update_query[cdr_consts.DISPOSITION] = bq_consts.WRITE_TRUNCATE
        update_query[cdr_consts.DESTINATION_DATASET] = self.dataset_id
        return [sandbox_query, update_query]

    def setup_validation(self, client):
        """
        Run required steps for validation setup
        """
        pass

    def validate_rule(self, client):
        """
        Validates the cleaning rule which deletes or updates the data from the tables
        """
        pass

    def get_sandbox_tablenames(self):
        return [self.sandbox_table_for(table) for table in self.affected_tables]


if __name__ == '__main__':
    import cdr_cleaner.args_parser as parser
    import cdr_cleaner.clean_cdr_engine as clean_engine

    ARGS = parser.parse_args()

    if ARGS.list_queries:
        clean_engine.add_console_logging()
        query_list = clean_engine.get_query_list(ARGS.project_id,
                                                 ARGS.dataset_id,
                                                 ARGS.sandbox_dataset_id,
                                                 [(DateUnShiftCopeResponses,)])
        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(ARGS.project_id, ARGS.dataset_id,
                                   ARGS.sandbox_dataset_id,
                                   [(DateUnShiftCopeResponses,)])
