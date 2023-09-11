"""
Drops records from observation and survey_conduct due to their listed module in survey_conduct. (ex: Wear_consent)


Wear consent responses are associated with the custom concept_ids, 2100000011 and 2100000012, in the survey_conduct
table. Wear consent records need to be suppressed in the CDR. DC-3330

Original Issues: DC-3330
"""
# Python imports
import logging

# Project imports
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule, query_spec_list
from constants.cdr_cleaner.clean_cdr import REGISTERED_TIER_DEID, CONTROLLED_TIER_DEID, QUERY
from common import JINJA_ENV, OBSERVATION, SURVEY_CONDUCT
from cdr_cleaner.cleaning_rules.generate_wear_study_table import GenerateWearStudyTable

LOGGER = logging.getLogger(__name__)

JIRA_ISSUE_NUMBERS = ['DC3330']

DOMAIN_TABLES = [OBSERVATION, SURVEY_CONDUCT]

SANDBOX_OBSERVATION = JINJA_ENV.from_string("""
CREATE OR REPLACE TABLE `{{project_id}}.{{sandbox_dataset_id}}.{{sandbox_table_id}}` AS (
    SELECT o.*
    FROM `{{project_id}}.{{dataset_id}}.observation` o
    LEFT JOIN `{{project_id}}.{{dataset_id}}.survey_conduct` sc
    ON sc.survey_conduct_id = o.questionnaire_response_id 
    WHERE sc.survey_source_concept_id IN (2100000011,2100000012) 
      OR sc.survey_concept_id IN (2100000011,2100000012)
)
""")

SANDBOX_SURVEY_CONDUCT = JINJA_ENV.from_string("""
CREATE OR REPLACE TABLE `{{project_id}}.{{sandbox_dataset_id}}.{{sandbox_table_id}}` AS (
    SELECT *
    FROM `{{project_id}}.{{dataset_id}}.survey_conduct`  sc
    WHERE sc.survey_source_concept_id IN (2100000011,2100000012) 
    OR sc.survey_concept_id IN (2100000011,2100000012)
)
""")

CLEAN_OBSERVATION = JINJA_ENV.from_string("""
DELETE FROM `{{project_id}}.{{dataset_id}}.observation`
WHERE questionnaire_response_id IN (
    SELECT questionnaire_response_id 
    FROM `{{project_id}}.{{sandbox_dataset_id}}.{{sandbox_table_id}}`
    )
""")

CLEAN_SURVEY_CONDUCT = JINJA_ENV.from_string("""
DELETE FROM `{{project_id}}.{{dataset_id}}.survey_conduct`
WHERE survey_conduct_id IN (
    SELECT survey_conduct_id 
    FROM `{{project_id}}.{{sandbox_dataset_id}}.{{sandbox_table_id}}`
    )
""")


class DropViaSurveyConduct(BaseCleaningRule):

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
        desc = (
            "Sandboxes and removes  'observation' and 'survey_conduct' records where associated with defined modules"
            "in the survey_conduct table.")

        super().__init__(
            issue_numbers=JIRA_ISSUE_NUMBERS,
            description=desc,
            affected_datasets=[REGISTERED_TIER_DEID, CONTROLLED_TIER_DEID],
            affected_tables=DOMAIN_TABLES,
            project_id=project_id,
            dataset_id=dataset_id,
            sandbox_dataset_id=sandbox_dataset_id,
            depends_on=[GenerateWearStudyTable],
            table_namer=table_namer)

    def get_query_specs(self, *args, **keyword_args) -> query_spec_list:
        """
        Return a list of dictionary query specifications.

        :return:  A list of dictionaries. Each dictionary contains a single query
            and a specification for how to execute that query. The specifications
            are optional but the query is required.
        """
        queries_list = []

        sandbox_obs_query = dict()
        sandbox_obs_query[QUERY] = SANDBOX_OBSERVATION.render(
            project_id=self.project_id,
            dataset_id=self.dataset_id,
            sandbox_dataset_id=self.sandbox_dataset_id,
            sandbox_table_id=self.sandbox_table_for(OBSERVATION))
        queries_list.append(sandbox_obs_query)

        sandbox_sc_query = dict()
        sandbox_sc_query[QUERY] = SANDBOX_SURVEY_CONDUCT.render(
            project_id=self.project_id,
            dataset_id=self.dataset_id,
            sandbox_dataset_id=self.sandbox_dataset_id,
            sandbox_table_id=self.sandbox_table_for(SURVEY_CONDUCT))
        queries_list.append(sandbox_sc_query)

        clean_obs_query = dict()
        clean_obs_query[QUERY] = CLEAN_OBSERVATION.render(
            project_id=self.project_id,
            dataset_id=self.dataset_id,
            sandbox_dataset_id=self.sandbox_dataset_id,
            sandbox_table_id=self.sandbox_table_for(OBSERVATION))
        queries_list.append(clean_obs_query)

        clean_sc_query = dict()
        clean_sc_query[QUERY] = CLEAN_SURVEY_CONDUCT.render(
            project_id=self.project_id,
            dataset_id=self.dataset_id,
            sandbox_dataset_id=self.sandbox_dataset_id,
            sandbox_table_id=self.sandbox_table_for(SURVEY_CONDUCT))
        queries_list.append(clean_sc_query)

        return queries_list

    def setup_rule(self, client, *args, **keyword_args):
        pass

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
        return [self.sandbox_table_for(table) for table in DOMAIN_TABLES]


if __name__ == '__main__':
    import cdr_cleaner.args_parser as parser
    import cdr_cleaner.clean_cdr_engine as clean_engine

    ARGS = parser.default_parse_args()

    if ARGS.list_queries:
        clean_engine.add_console_logging()
        query_list = clean_engine.get_query_list(ARGS.project_id,
                                                 ARGS.dataset_id,
                                                 ARGS.sandbox_dataset_id,
                                                 [(DropViaSurveyConduct,)])
        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(ARGS.project_id, ARGS.dataset_id,
                                   ARGS.sandbox_dataset_id,
                                   [(DropViaSurveyConduct,)])
