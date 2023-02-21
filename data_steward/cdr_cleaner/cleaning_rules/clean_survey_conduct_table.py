"""
Ensures the survey conduct table is populated as expected.

Survey_conduct table is relatively new at the point of this CR's creation. Because bugs still exist in the creation of
this table.  This CR is needed to ensure all fields are populated as expected.

This CR updates both survey_concept_id and survey_source_concept_id depending on the starting value in survey_concept_id
If survey_concept_id can join to valid ppi or custom concept, survey_source_concept_id will be updated to this concept_id.
If survey_concept_id does not join to a valid ppi or custom concept from the vocabulary, survey_source_concept_id will
be updated to 0.

Dependencies:
The surveys requiring a AOU custom id should have had their fields updated in a previously run CR.
Observations that do not have a valid survey will be dropped in a CR that runs after this one.

Original Issues: DC-3013
"""
# Python imports
import logging
from datetime import datetime

# Project imports
from google.cloud import bigquery
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule, query_spec_list
from cdr_cleaner.cleaning_rules.drop_unverified_survey_data import DropUnverifiedSurveyData
from constants.cdr_cleaner import clean_cdr as cdr_consts
from common import JINJA_ENV, SURVEY_CONDUCT
import resources

LOGGER = logging.getLogger(__name__)

JIRA_ISSUE_NUMBERS = ['DC3013']

DOMAIN_TABLES = [SURVEY_CONDUCT]


SANDBOX_SURVEY_CONDUCT = JINJA_ENV.from_string("""
CREATE OR REPLACE TABLE `{{project_id}}.{{sandbox_dataset_id}}.{{sandbox_table_id}}` AS (
SELECT * FROM `{{project_id}}.{{dataset_id}}.survey_conduct`
WHERE survey_concept_id != survey_source_concept_id
OR survey_concept_id NOT IN (SELECT concept_id FROM `{{project_id}}.{{dataset_id}}.concept` WHERE vocabulary_id IN ('PPI','AoU_Custom','AoU_General')) 
)
""")

CLEAN_SURVEY_CONDUCT = JINJA_ENV.from_string("""
UPDATE `{{project_id}}.{{dataset_id}}.survey_conduct`
SET
survey_source_concept_id = CASE
    WHEN survey_concept_id IN (SELECT concept_id FROM `{{project_id}}.{{dataset_id}}.concept` WHERE vocabulary_id IN ('PPI','AoU_Custom','AoU_General'))  THEN survey_concept_id
    WHEN survey_concept_id NOT IN (SELECT concept_id FROM `{{project_id}}.{{dataset_id}}.concept` WHERE vocabulary_id IN ('PPI','AoU_Custom','AoU_General'))   THEN 0
END
WHERE survey_conduct_id IS NOT NULL

""")

class CleanSurveyConduct(BaseCleaningRule):

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
        desc = ('Updates/Cleans survey_conduct concept_id fields.')

        super().__init__(issue_numbers=JIRA_ISSUE_NUMBERS,
                         description=desc,
                         affected_datasets=[cdr_consts.RDR],
                         affected_tables=DOMAIN_TABLES,
                         project_id=project_id,
                         dataset_id=dataset_id,
                         sandbox_dataset_id=sandbox_dataset_id,
                         depends_on=[DropUnverifiedSurveyData],
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
            sandbox_dataset_id=self.sandbox_dataset_id,
            sandbox_table_id=self.sandbox_table_for(SURVEY_CONDUCT))
        queries_list.append(sandbox_sc_query)

        clean_sc_query = dict()
        clean_sc_query[cdr_consts.QUERY] = CLEAN_SURVEY_CONDUCT.render(
            project_id=self.project_id,
            dataset_id=self.dataset_id,
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
        query_list = clean_engine.get_query_list(ARGS.project_id,
                                                 ARGS.dataset_id,
                                                 ARGS.sandbox_dataset_id,
                                                 [(CleanSurveyConduct,)])
        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(ARGS.project_id, ARGS.dataset_id,
                                   ARGS.sandbox_dataset_id,
                                   [(CleanSurveyConduct,)])
