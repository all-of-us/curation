"""
Reverse Date Shift for COPE Responses.

The COPE survey is longitudinal (administered monthly), so researchers will need a way to identify with which survey
monthly iteration data is associated. Because the surveys are known to have been taken in a particular month, these
observations being date shifted could allow all other date shifting for these participants to be broken.

History: In past versions of this cleaning rule, the concept_relationship table was used to gather all concepts related
to the singular OMOP module concept_id(cope - 1333342). Then the data in `pipeline_tables.cope_concepts` was used to
assign individual module ids(AoU_Custom concepts). These data would also have their date fields unshifted.

This cleaning rule now uses the observation_ext table to determine cope data in the observation table, and the
survey_concept_id fields in the survey_conduct table for the same purpose.

Original Issue: DC-938
"""

# Python imports
import logging

# Project Imports
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule
from constants.cdr_cleaner import clean_cdr as cdr_consts
from common import JINJA_ENV, OBSERVATION, SURVEY_CONDUCT, DEID_MAP
from cdr_cleaner.cleaning_rules.deid.survey_version_info import COPESurveyVersionTask
from cdr_cleaner.cleaning_rules.deid.questionnaire_response_id_map import QRIDtoRID

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
OR survey_concept_id IN (2100000002, 2100000003, 2100000004, 2100000005, 2100000006,
  2100000007, 905047, 905055, 765936)
""")

DATE_UNSHIFT_OBS_QUERY = JINJA_ENV.from_string("""
UPDATE `{{project_id}}.{{dataset_id}}.{{observation_table}}` ob
SET observation_date       = unshifted_observation_date,
    observation_datetime   = unshifted_observation_datetime
FROM (SELECT 
        observation_id,
        COALESCE(DATE_ADD(observation_date, INTERVAL shift DAY), observation_date) as unshifted_observation_date,
        COALESCE(DATE_ADD(observation_datetime, INTERVAL shift DAY), observation_datetime) as unshifted_observation_datetime
    FROM `{{project_id}}.{{dataset_id}}.{{observation_table}}` ob
    LEFT JOIN `{{project_id}}.{{mapping_dataset}}.{{_deid_map}}` prm
    ON ob.person_id = prm.research_id  -- reminder: obs has been deid --
    ) AS ref 
WHERE ob.observation_id = ref.observation_id
AND ob.observation_id IN (
    SELECT observation_id
    FROM `{{project_id}}.{{sandbox_dataset}}.{{intermediary_obs_table}}`)
""")

DATE_UNSHIFT_SC_QUERY = JINJA_ENV.from_string("""
UPDATE `{{project_id}}.{{dataset_id}}.{{survey_conduct_table}}` sc
SET survey_start_date = unshifted_start_date,
    survey_start_datetime = unshifted_start_datetime,
    survey_end_date = unshifted_end_date,
    survey_end_datetime = unshifted_end_datetime
FROM (SELECT
        survey_conduct_id,
        COALESCE(DATE_ADD(survey_start_date, INTERVAL shift DAY),survey_start_date) as unshifted_start_date,
        COALESCE(DATE_ADD(survey_start_datetime, INTERVAL shift DAY),survey_start_datetime) as unshifted_start_datetime,
        COALESCE(DATE_ADD(survey_end_date, INTERVAL shift DAY),survey_end_date) as unshifted_end_date,
        COALESCE(DATE_ADD(survey_end_datetime, INTERVAL shift DAY),survey_end_datetime) as unshifted_end_datetime
    FROM `{{project_id}}.{{dataset_id}}.{{survey_conduct_table}}`  sc
    LEFT JOIN `{{project_id}}.{{mapping_dataset}}.{{_deid_map}}`  prm
    ON sc.person_id = prm.research_id) AS ref -- reminder: sc has been deid --
WHERE sc.survey_conduct_id = ref.survey_conduct_id
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
                 mapping_dataset_id,
                 table_namer=None):
        """
        Initialize the class with proper information.

        Set the issue numbers, description and affected datasets. As other tickets may affect
        this SQL, append them to the list of Jira Issues.
        DO NOT REMOVE ORIGINAL JIRA ISSUE NUMBERS!
        """
        desc = 'Reverse Date Shift for COPE Responses'

        self.mapping_dataset_id = mapping_dataset_id

        super().__init__(
            issue_numbers=[
                'DC938', 'DC982', 'DC970', 'DC2438', 'DC2717', 'DC2839'
            ],
            description=desc,
            affected_datasets=[cdr_consts.REGISTERED_TIER_DEID_BASE],
            affected_tables=[OBSERVATION, SURVEY_CONDUCT],
            project_id=project_id,
            dataset_id=dataset_id,
            sandbox_dataset_id=sandbox_dataset_id,
            depends_on=[COPESurveyVersionTask, QRIDtoRID],
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
        self.affected_tables = [
            table.table_id for table in client.list_tables(
                f'{self.project_id}.{self.dataset_id}')
        ]

    def get_query_specs(self):
        """
        :return:
        """
        sandbox_queries = []
        update_queries = []
        if OBSERVATION in self.affected_tables:
            sandbox_query_obs = dict()
            sandbox_query_obs[
                cdr_consts.QUERY] = SANDBOX_COPE_SURVEY_OBS_QUERY.render(
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
                dataset_id=self.dataset_id,
                observation_table=OBSERVATION,
                mapping_dataset=self.mapping_dataset_id,
                _deid_map=DEID_MAP,
                sandbox_dataset=self.sandbox_dataset_id,
                intermediary_obs_table=self.sandbox_table_for(OBSERVATION))
            update_queries.append(update_query_obs)

        if SURVEY_CONDUCT in self.affected_tables:
            sandbox_query_sc = dict()
            sandbox_query_sc[
                cdr_consts.QUERY] = SANDBOX_COPE_SURVEY_SC_QUERY.render(
                    project_id=self.project_id,
                    sandbox_dataset=self.sandbox_dataset_id,
                    intermediary_sc_table=self.sandbox_table_for(
                        SURVEY_CONDUCT),
                    dataset_id=self.dataset_id,
                    survey_conduct_table=SURVEY_CONDUCT)
            sandbox_queries.append(sandbox_query_sc)

            update_query_sc = dict()
            update_query_sc[cdr_consts.QUERY] = DATE_UNSHIFT_SC_QUERY.render(
                project_id=self.project_id,
                dataset_id=self.dataset_id,
                survey_conduct_table=SURVEY_CONDUCT,
                mapping_dataset=self.mapping_dataset_id,
                _deid_map=DEID_MAP,
                sandbox_dataset=self.sandbox_dataset_id,
                intermediary_sc_table=self.sandbox_table_for(SURVEY_CONDUCT))
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

    mapping_dataset_arg = {
        parser.SHORT_ARGUMENT: '-m',
        parser.LONG_ARGUMENT: '--mapping_dataset_id',
        parser.ACTION: 'store',
        parser.DEST: 'mapping_dataset_id',
        parser.HELP: 'Identifies the dataset containing pid-rid map table',
        parser.REQUIRED: True
    }

    ARGS = parser.default_parse_args([mapping_dataset_arg])

    if ARGS.list_queries:
        clean_engine.add_console_logging()
        query_list = clean_engine.get_query_list(
            ARGS.project_id,
            ARGS.dataset_id,
            ARGS.sandbox_dataset_id, [(DateUnShiftCopeResponses,)],
            mapping_dataset_id=ARGS.mapping_dataset_id)
        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(ARGS.project_id,
                                   ARGS.dataset_id,
                                   ARGS.sandbox_dataset_id,
                                   [(DateUnShiftCopeResponses,)],
                                   mapping_dataset_id=ARGS.mapping_dataset_id)
