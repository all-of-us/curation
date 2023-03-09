"""
Updates data representing the version of a survey into survey_conduct table for surveys with custom ids. Ex COPE/Minute

Recurring versions of surveys such as COPE had all of their questions and answers mapped to a single Module in the PPI
vocabulary. The same was the case for the Minute surveys. This will make it seem like there is only one version of each
of these surveys. Researchers need to know the version of the survey the participant took. This CR will use
`cope_survey_semantic_version_map` which is provided to curation from the rdr team, to update the survey version in
survey_source_concept_id and survey_source_value for each observation.

There are multiple cleaning rules that clean the survey conduct table.
This CR will clean survey_concept_id and survey_source_value for the surveys with custom concept ids.
`UpdateSurveySourceConceptId` cleans survey_source_concept_id for all valid records.
Then `DropUnverifiedSurveyData` observations that do not have a valid survey will be dropped in another CR.

Original Issues: DC-3082
"""
# Python imports
import logging

# Project imports
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule, query_spec_list
from constants.cdr_cleaner import clean_cdr as cdr_consts
from common import JINJA_ENV, SURVEY_CONDUCT, COPE_SURVEY_MAP

LOGGER = logging.getLogger(__name__)

JIRA_ISSUE_NUMBERS = ['DC-3082']

DOMAIN_TABLES = [SURVEY_CONDUCT]
REFERENCE_TABLES = [COPE_SURVEY_MAP]

SANDBOX_SURVEY_CONDUCT = JINJA_ENV.from_string("""
CREATE OR REPLACE TABLE `{{project_id}}.{{sandbox_dataset_id}}.{{sandbox_table_id}}` AS (
SELECT * FROM `{{project_id}}.{{dataset_id}}.survey_conduct`
WHERE survey_conduct_id IN (SELECT questionnaire_response_id FROM `{{project_id}}.{{dataset_id}}.{{cope_survey_map}}`)
AND (survey_concept_id NOT IN (2100000006, 2100000007, 2100000002, 2100000005, 2100000004, 2100000003, 905047, 905055, 765936, 1741006)
     OR survey_source_value NOT IN ('AoUDRC_SurveyVersion_CopeDecember2020', 'AoUDRC_SurveyVersion_CopeFebruary2021', 'AoUDRC_SurveyVersion_CopeMay2020', 'AoUDRC_SurveyVersion_CopeNovember2020', 'AoUDRC_SurveyVersion_CopeJuly2020', 'AoUDRC_SurveyVersion_CopeJune2020','cope_vaccine1', 'cope_vaccine2', 'cope_vaccine3', 'cope_vaccine4')
     )
)
""")

CLEAN_SURVEY_CONDUCT = JINJA_ENV.from_string("""
UPDATE `{{project_id}}.{{dataset_id}}.survey_conduct` sc
SET
survey_concept_id = CASE
    WHEN cope_month = 'dec' THEN 2100000006
    WHEN cope_month = 'feb' THEN 2100000007
    WHEN cope_month = 'may' THEN 2100000002
    WHEN cope_month = 'nov' THEN 2100000005
    WHEN cope_month = 'july' THEN 2100000004
    WHEN cope_month = 'june' THEN 2100000003
    WHEN cope_month = 'vaccine1' THEN 905047
    WHEN cope_month = 'vaccine2' THEN 905055
    WHEN cope_month = 'vaccine3' THEN 765936
    WHEN cope_month = 'vaccine4' THEN 1741006
    ELSE sc.survey_concept_id
    END,
survey_source_value = CASE
    WHEN cope_month = 'dec' THEN 'AoUDRC_SurveyVersion_CopeDecember2020'
    WHEN cope_month = 'feb' THEN 'AoUDRC_SurveyVersion_CopeFebruary2021'
    WHEN cope_month = 'may' THEN 'AoUDRC_SurveyVersion_CopeMay2020'
    WHEN cope_month = 'nov' THEN 'AoUDRC_SurveyVersion_CopeNovember2020'
    WHEN cope_month = 'july' THEN 'AoUDRC_SurveyVersion_CopeJuly2020'
    WHEN cope_month = 'june' THEN 'AoUDRC_SurveyVersion_CopeJune2020'
    WHEN cope_month = 'vaccine1' THEN 'cope_vaccine1'
    WHEN cope_month = 'vaccine2' THEN 'cope_vaccine2'
    WHEN cope_month = 'vaccine3' THEN 'cope_vaccine3'
    WHEN cope_month = 'vaccine4' THEN 'cope_vaccine4'
    ELSE sc.survey_source_value
    END
FROM (SELECT *
      FROM `{{project_id}}.{{dataset_id}}.survey_conduct` sc
      JOIN  `{{project_id}}.{{dataset_id}}.{{cope_survey_map}}` m
      ON m.questionnaire_response_id = sc.survey_conduct_id
      WHERE m.questionnaire_response_id IS NOT NULL
      ) sub
WHERE sub.survey_conduct_id = sc.survey_conduct_id 
AND sc.survey_conduct_id IN (SELECT survey_conduct_id FROM `{{project_id}}.{{sandbox_dataset_id}}.{{sandbox_table_id}}`)
""")


class CleanSurveyConductRecurringSurveys(BaseCleaningRule):

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
            'Updates survey_conduct with survey versions for the recurring surveys. (COPE/Minute)'
        )

        super().__init__(issue_numbers=JIRA_ISSUE_NUMBERS,
                         description=desc,
                         affected_datasets=[cdr_consts.RDR],
                         affected_tables=DOMAIN_TABLES,
                         project_id=project_id,
                         dataset_id=dataset_id,
                         sandbox_dataset_id=sandbox_dataset_id,
                         table_namer=table_namer)

    def get_query_specs(self, *args, **keyword_args) -> query_spec_list:
        """
        Return a list of dictionary query specifications.
        :return:  A list of dictionaries. Each dictionary contains a single query
            and a specification for how to execute that query. The specifications
            are optional but the query is required.
        """
        queries_list = []

        sandbox_sc_query = dict()
        sandbox_sc_query[cdr_consts.QUERY] = SANDBOX_SURVEY_CONDUCT.render(
            project_id=self.project_id,
            dataset_id=self.dataset_id,
            cope_survey_map=COPE_SURVEY_MAP,
            sandbox_dataset_id=self.sandbox_dataset_id,
            sandbox_table_id=self.sandbox_table_for(SURVEY_CONDUCT))
        queries_list.append(sandbox_sc_query)

        clean_sc_query = dict()
        clean_sc_query[cdr_consts.QUERY] = CLEAN_SURVEY_CONDUCT.render(
            project_id=self.project_id,
            dataset_id=self.dataset_id,
            cope_survey_map=COPE_SURVEY_MAP,
            sandbox_dataset_id=self.sandbox_dataset_id,
            sandbox_table_id=self.sandbox_table_for(SURVEY_CONDUCT))
        queries_list.append(clean_sc_query)

        return queries_list

    def setup_rule(self, client, *args, **keyword_args):
        """
        Function to run any data upload options before executing a query.
        """
        pass

    def setup_validation(self, client):
        """
        Run required steps for validation setup
        """
        raise NotImplementedError("Please fix me.")

    def validate_rule(self, client):
        """
        Validates the cleaning rule which deletes or updates the data from the tables
        """
        raise NotImplementedError("Please fix me.")

    def get_sandbox_tablenames(self):
        return [self.sandbox_table_for(table) for table in DOMAIN_TABLES]


if __name__ == '__main__':
    import cdr_cleaner.args_parser as parser
    import cdr_cleaner.clean_cdr_engine as clean_engine

    ARGS = parser.default_parse_args()

    if ARGS.list_queries:
        clean_engine.add_console_logging()
        query_list = clean_engine.get_query_list(
            ARGS.project_id, ARGS.dataset_id, ARGS.sandbox_dataset_id,
            [(CleanSurveyConductRecurringSurveys,)])
        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(ARGS.project_id, ARGS.dataset_id,
                                   ARGS.sandbox_dataset_id,
                                   [(CleanSurveyConductRecurringSurveys,)])
