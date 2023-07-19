"""
Drops records from observation and survey_conduct due to their listed, or lack of, module in survey_conduct.


Only data from verified surveys which are NIH and IRB approved(Basics, SDOH, ...) should move to the next stage
of the pipeline. These surveys are given an OMOP concept_id or in some cases an AoU_Custom concept_id.
Data exists in rdr which are not associated with any verified survey, but instead unverified
'surveys' (SNAP, Sitepairing, ...). Any data in the observation or survey_conduct tables that is not
associated with a verified survey, will be sandboxed and removed by this cleaning rule.

Every questionnaire_response_id from the observation toble should join with a survey_conduct_id in the survey_conduct
table. Verified surveys will have an assigned concept_id in survey_concept_id and survey_source_concept_id
while unverified surveys will not.

This cleaning rule assumes that the AoU_Custom concept_ids have been inserted prior to its running as well as
survey_concept_id and survey_source_concept_id both being populated when the survey is valid.

Wear consent responses are associated with a module concept_id but will also be suppressed from
the survey_conduct and observation tables. DC-3330

Original Issues: DC-2775
"""
# Python imports
import logging

# Project imports
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule, query_spec_list
from constants.cdr_cleaner import clean_cdr as cdr_consts
from common import JINJA_ENV, OBSERVATION, SURVEY_CONDUCT

LOGGER = logging.getLogger(__name__)

JIRA_ISSUE_NUMBERS = ['DC2775', 'DC3330']

DOMAIN_TABLES = [OBSERVATION, SURVEY_CONDUCT]

SANDBOX_OBSERVATION = JINJA_ENV.from_string("""
CREATE OR REPLACE TABLE `{{project_id}}.{{sandbox_dataset_id}}.{{sandbox_table_id}}` AS (
    SELECT o.*
    FROM `{{project_id}}.{{dataset_id}}.observation` o
    LEFT JOIN `{{project_id}}.{{dataset_id}}.survey_conduct` sc
    ON sc.survey_conduct_id = o.questionnaire_response_id 
    WHERE sc.survey_source_concept_id IN (0,2100000011,2100000012) 
    OR sc.survey_concept_id IN (0,2100000011,2100000012)
)
""")

SANDBOX_SURVEY_CONDUCT = JINJA_ENV.from_string("""
CREATE OR REPLACE TABLE `{{project_id}}.{{sandbox_dataset_id}}.{{sandbox_table_id}}` AS (
    SELECT *
    FROM `{{project_id}}.{{dataset_id}}.survey_conduct`  sc
    WHERE sc.survey_source_concept_id IN (0,2100000011,2100000012) 
    OR sc.survey_concept_id IN (0,2100000011,2100000012)
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

### Validation Query ####
COUNTS_QUERY = JINJA_ENV.from_string("""
SELECT COUNT(CASE WHEN survey_concept_id = 0 THEN 1 ELSE 0 END) as invalid_surveys,
COUNT(CASE WHEN questionnaire_response_id IS NOT NULL THEN 1 ELSE  0 END) as invalid_obs
FROM `{{project_id}}.{{dataset_id}}.survey_conduct`  sc
LEFT JOIN `{{project_id}}.{{dataset_id}}.observation` o
ON sc.survey_conduct_id = o.questionnaire_response_id 
WHERE sc.survey_source_concept_id = 0 OR sc.survey_concept_id = 0
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
            'Sandboxes and removes erroneous data from observation and survey_conduct.'
        )

        super().__init__(issue_numbers=JIRA_ISSUE_NUMBERS,
                         description=desc,
                         affected_datasets=[cdr_consts.RDR],
                         affected_tables=DOMAIN_TABLES,
                         project_id=project_id,
                         dataset_id=dataset_id,
                         sandbox_dataset_id=sandbox_dataset_id,
                         depends_on=None,
                         table_namer=table_namer)

        self.counts_query = COUNTS_QUERY.render(project_id=self.project_id,
                                                dataset_id=self.dataset_id)

    def get_query_specs(self, *args, **keyword_args) -> query_spec_list:
        """
        Return a list of dictionary query specifications.

        :return:  A list of dictionaries. Each dictionary contains a single query
            and a specification for how to execute that query. The specifications
            are optional but the query is required.
        """
        queries_list = []

        sandbox_obs_query = dict()
        sandbox_obs_query[cdr_consts.QUERY] = SANDBOX_OBSERVATION.render(
            project_id=self.project_id,
            dataset_id=self.dataset_id,
            sandbox_dataset_id=self.sandbox_dataset_id,
            sandbox_table_id=self.sandbox_table_for(OBSERVATION))
        queries_list.append(sandbox_obs_query)

        sandbox_sc_query = dict()
        sandbox_sc_query[cdr_consts.QUERY] = SANDBOX_SURVEY_CONDUCT.render(
            project_id=self.project_id,
            dataset_id=self.dataset_id,
            sandbox_dataset_id=self.sandbox_dataset_id,
            sandbox_table_id=self.sandbox_table_for(SURVEY_CONDUCT))
        queries_list.append(sandbox_sc_query)

        clean_obs_query = dict()
        clean_obs_query[cdr_consts.QUERY] = CLEAN_OBSERVATION.render(
            project_id=self.project_id,
            dataset_id=self.dataset_id,
            sandbox_dataset_id=self.sandbox_dataset_id,
            sandbox_table_id=self.sandbox_table_for(OBSERVATION))
        queries_list.append(clean_obs_query)

        clean_sc_query = dict()
        clean_sc_query[cdr_consts.QUERY] = CLEAN_SURVEY_CONDUCT.render(
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

        init_counts = self._get_counts(client)

        if init_counts.get('invalid_obs') == 0:
            raise RuntimeError('NO DATA EXISTS IN OBSERVATION TABLE')

        if init_counts.get('invalid_survey') == 0:
            raise RuntimeError('NO UNVERIFIED SURVEYS IN SURVEY_CONDUCT')

    def validate_rule(self, client):
        """
        Validates the cleaning rule which deletes or updates the data from the tables
        """
        clean_counts = self._get_counts(client)

        if clean_counts.get('invalid_obs') != 0 or clean_counts.get(
                'invalid_surveys') != 0:
            raise RuntimeError('CLEANING RULE DID NOT FUNCTION PROPERLY')

    def get_sandbox_tablenames(self):
        return [self.sandbox_table_for(table) for table in DOMAIN_TABLES]

    def _get_counts(self, client) -> dict:
        """
        Counts query.
        Used for job validation.

        """
        job = client.query(self.counts_query)
        response = job.result()

        errors = []
        if job.exception():
            errors.append(job.exception())
            LOGGER.error(f"FAILURE:  {job.exception()}\n"
                         f"Problem executing query:\n{self.counts_query}")
        else:
            for item in response:
                invalid_obs = item.get('invalid_obs', 0)
                invalid_survey = item.get('invalid_survey', 0)

        return {'invalid_obs': invalid_obs, 'invalid_survey': invalid_survey}


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
