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
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule
from constants.cdr_cleaner import clean_cdr as cdr_consts
from common import JINJA_ENV, OBSERVATION, SURVEY_CONDUCT
from cdr_cleaner.manual_cleaning_rules.survey_version_info import COPESurveyVersionTask

LOGGER = logging.getLogger(__name__)

OBSERVATION_EXT = f'{OBSERVATION}_ext'

SANDBOX_COPE_SURVEY_OBS_QUERY = JINJA_ENV.from_string("""
CREATE OR REPLACE TABLE `{{project_id}}.{{sandbox_dataset}}.{{intermediary_obs_table}}` AS
SELECT *
FROM `{{project_id}}.{{dataset_id}}.{{observation_table}}`
INNER JOIN `{{project_id}}.{{dataset_id}}.{{observation_ext_table}}`
USING (observation_id)
WHERE survey_version_concept_id IN (2100000002, 2100000003, 2100000004, 2100000005, 2100000006,
  2100000007, 905047, 905055, 765936) 
""")

SANDBOX_COPE_SURVEY_SC_QUERY = JINJA_ENV.from_string("""
CREATE OR REPLACE TABLE `{{project_id}}.{{sandbox_dataset}}.{{intermediary_sc_table}}` AS
SELECT *
FROM `{{project_id}}.{{dataset_id}}.{{survey_conduct_table}}`
WHERE survey_source_concept_id IN (2100000002, 2100000003, 2100000004, 2100000005, 2100000006,
  2100000007, 905047, 905055, 765936) 
""")

DATE_UNSHIFT_OBS_QUERY = JINJA_ENV.from_string("""
UPDATE `{{project_id}}.{{dataset_id}}.{{observation_table}}` ob
SET observation_date = coalesce(po.observation_date, ob.observation_date),
    observation_datetime = coalesce(po.observation_datetime, ob.observation_datetime)
FROM `{{project_id}}.{{pre_deid_dataset}}.{{observation_table}}` po
WHERE ob.observation_id = po.observation_id
AND ob.observation_id IN (
    SELECT observation_id
    FROM `{{project_id}}.{{sandbox_dataset}}.{{intermediary_obs_table}}`)
""")

DATE_UNSHIFT_SC_QUERY = JINJA_ENV.from_string("""
UPDATE `{{project_id}}.{{dataset_id}}.{{survey_conduct_table}}` sc
SET survey_start_date = coalesce(psc.survey_start_date, sc.survey_start_date),
    survey_start_datetime = coalesce(psc.survey_start_datetime, sc.survey_start_datetime),
    survey_end_date = coalesce(psc.survey_end_date, sc.survey_end_date),
    survey_end_datetime = coalesce(psc.survey_end_datetime, sc.survey_end_datetime)
FROM `{{project_id}}.{{pre_deid_dataset}}.{{survey_conduct_table}}` psc
WHERE sc.survey_conduct_id = psc.survey_conduct_id
AND sc.survey_conduct_id IN (
    SELECT survey_conduct_id
    FROM `{{project_id}}.{{sandbox_dataset}}.{{intermediary_sc_table}}`)
""")


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
            issue_numbers=['DC938', 'DC982', 'DC970', 'DC2438', 'DC2717'],
            description=desc,
            affected_datasets=[cdr_consts.REGISTERED_TIER_DEID_BASE],
            affected_tables=[OBSERVATION, SURVEY_CONDUCT],
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
        self.affected_tables = [table.table_id for table in client.list_tables(f'{self.project_id}.{self.dataset_id}')]

    def get_query_specs(self):
        """
        :return:
        """
        sandbox_queries = []
        update_queries = []
        if OBSERVATION in self.affected_tables:
            sandbox_query_obs = dict()
            sandbox_query_obs[cdr_consts.QUERY] = SANDBOX_COPE_SURVEY_OBS_QUERY.render(
                project_id=self.project_id,
                sandbox_dataset=self.sandbox_dataset_id,
                intermediary_obs_table=self.sandbox_table_for(OBSERVATION),
                dataset_id=self.dataset_id,
                observation_table=OBSERVATION,
                observation_ext_table=OBSERVATION_EXT)
            sandbox_queries.append(sandbox_query_obs)

            update_query_obs = dict()
            update_query_obs[cdr_consts.QUERY] = DATE_UNSHIFT_OBS_QUERY.render(
                project_id=self.project_id,
                pre_deid_dataset=self.get_combined_dataset_from_deid_dataset(
                    self.dataset_id),
                dataset_id=self.dataset_id,
                sandbox_dataset=self.sandbox_dataset_id,
                intermediary_obs_table=self.sandbox_table_for(OBSERVATION),
                observation_table=OBSERVATION)
            update_queries.append(update_query_obs)

        if SURVEY_CONDUCT in self.affected_tables:
            sandbox_query_sc = dict()
            sandbox_query_sc[cdr_consts.QUERY] = SANDBOX_COPE_SURVEY_SC_QUERY.render(
                project_id=self.project_id,
                sandbox_dataset=self.sandbox_dataset_id,
                intermediary_sc_table=self.sandbox_table_for(SURVEY_CONDUCT),
                dataset_id=self.dataset_id,
                survey_conduct_table=SURVEY_CONDUCT)
            sandbox_queries.append(sandbox_query_sc)

            update_query_sc = dict()
            update_query_sc[cdr_consts.QUERY] = DATE_UNSHIFT_SC_QUERY.render(
                project_id=self.project_id,
                pre_deid_dataset=self.get_combined_dataset_from_deid_dataset(
                    self.dataset_id),
                dataset_id=self.dataset_id,
                sandbox_dataset=self.sandbox_dataset_id,
                intermediary_sc_table=self.sandbox_table_for(SURVEY_CONDUCT),
                survey_conduct_table=SURVEY_CONDUCT)
            update_queries.append(update_query_sc)
        return sandbox_queries + update_queries

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
