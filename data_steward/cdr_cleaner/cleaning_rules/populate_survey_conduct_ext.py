"""
Populates survey_conduct_ext table with the language and versioning information
as provided in the questionnaire_response_additional_info table.

Original issue: DC-2627
"""
# Python imports
import logging

# Project imports
import constants.cdr_cleaner.clean_cdr as cdr_consts
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule
from cdr_cleaner.cleaning_rules.generate_ext_tables import GenerateExtTables
from cdr_cleaner.manual_cleaning_rules.survey_version_info import COPESurveyVersionTask
from common import EXT_SUFFIX, JINJA_ENV, SURVEY_CONDUCT

LOGGER = logging.getLogger(__name__)

SANDBOX_SURVEY_CONDUCT_EXT_QUERY = JINJA_ENV.from_string("""
CREATE TABLE `{{project_id}}.{{sandbox_dataset_id}}.{{sandbox_table}}`
AS
    SELECT sce.* FROM `{{project_id}}.{{dataset_id}}.survey_conduct_ext` AS sce
    JOIN `{{project_id}}.{{dataset_id}}.questionnaire_response_additional_info` AS qrai
    ON sce.survey_conduct_id = qrai.questionnaire_response_id
    WHERE (sce.language IS NULL OR sce.language != qrai.value)
    AND qrai.type = 'LANGUAGE'
""")

UPDATE_SURVEY_CONDUCT_EXT_QUERY = JINJA_ENV.from_string("""
UPDATE `{{project_id}}.{{dataset_id}}.survey_conduct_ext` AS sce
SET language = qrai.value
FROM `{{project_id}}.{{dataset_id}}.questionnaire_response_additional_info` AS qrai
WHERE sce.survey_conduct_id = qrai.questionnaire_response_id
AND qrai.type = 'LANGUAGE'
""")


class PopulateSurveyConductExt(BaseCleaningRule):
    """
    Populates survey_conduct_ext table with the language and versioning information.
    """

    def __init__(self,
                 project_id,
                 dataset_id,
                 sandbox_dataset_id,
                 table_namer=None):
        """
        Initialize the class with proper information.
        DO NOT REMOVE ORIGINAL JIRA ISSUE NUMBERS!
        """
        desc = ('Populates survey_conduct_ext table with the language and '
                'versioning information as provided in the '
                'questionnaire_response_additional_info table.')

        super().__init__(issue_numbers=['DC2627'],
                         description=desc,
                         affected_datasets=[
                             cdr_consts.REGISTERED_TIER_DEID,
                             cdr_consts.CONTROLLED_TIER_DEID
                         ],
                         affected_tables=[f"{SURVEY_CONDUCT}{EXT_SUFFIX}"],
                         project_id=project_id,
                         dataset_id=dataset_id,
                         sandbox_dataset_id=sandbox_dataset_id,
                         depends_on=[GenerateExtTables, COPESurveyVersionTask],
                         table_namer=table_namer)

    def get_query_specs(self):
        """
        Runs the query which adds skipped questions to observation table.
        No sandbox table since this is only insert.
        """
        sandbox_query = SANDBOX_SURVEY_CONDUCT_EXT_QUERY.render(
            project_id=self.project_id,
            dataset_id=self.dataset_id,
            sandbox_dataset_id=self.sandbox_dataset_id,
            sandbox_table=self.sandbox_table_for(
                f"{SURVEY_CONDUCT}{EXT_SUFFIX}"))

        insert_query = UPDATE_SURVEY_CONDUCT_EXT_QUERY.render(
            project_id=self.project_id, dataset_id=self.dataset_id)

        sandbox_query_dict = {cdr_consts.QUERY: sandbox_query}
        insert_query_dict = {cdr_consts.QUERY: insert_query}

        return [sandbox_query_dict, insert_query_dict]

    def setup_rule(self, client):
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
                                                 [(PopulateSurveyConductExt,)])
        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(ARGS.project_id, ARGS.dataset_id,
                                   ARGS.sandbox_dataset_id,
                                   [(PopulateSurveyConductExt,)])
