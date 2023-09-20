"""
Populates survey_conduct_ext table with the language information
as provided in the questionnaire_response_additional_info table.

Original issue: DC-2627, DC-2730
"""
# Python imports
import logging

# Project imports
import constants.cdr_cleaner.clean_cdr as cdr_consts
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule
from cdr_cleaner.cleaning_rules.generate_ext_tables import GenerateExtTables
from cdr_cleaner.cleaning_rules.deid.survey_version_info import COPESurveyVersionTask
from common import EXT_SUFFIX, JINJA_ENV, SURVEY_CONDUCT

LOGGER = logging.getLogger(__name__)

UPDATE_SURVEY_CONDUCT_EXT_QUERY = JINJA_ENV.from_string("""
UPDATE `{{project_id}}.{{dataset_id}}.survey_conduct_ext` AS sce
SET language = qrai.value
FROM `{{project_id}}.{{clean_survey_dataset_id}}.questionnaire_response_additional_info` AS qrai
WHERE sce.survey_conduct_id = qrai.questionnaire_response_id
AND UPPER(qrai.type) = 'LANGUAGE'
""")


class PopulateSurveyConductExt(BaseCleaningRule):
    """
    Populates survey_conduct_ext table with the language and versioning information.
    """

    def __init__(self,
                 project_id,
                 dataset_id,
                 sandbox_dataset_id,
                 clean_survey_dataset_id,
                 table_namer=None):
        """
        Initialize the class with proper information.
        DO NOT REMOVE ORIGINAL JIRA ISSUE NUMBERS!
        """
        desc = ('Populates survey_conduct_ext table with the language '
                'information as provided in the '
                'questionnaire_response_additional_info table.')

        super().__init__(issue_numbers=['DC2627', 'DC2730'],
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

        if not clean_survey_dataset_id:
            raise RuntimeError("'clean_survey_dataset_id' must be set")

        self.clean_survey_dataset_id = clean_survey_dataset_id

    def get_query_specs(self):
        """
        Return a list of dictionary query specifications.
        """
        update_query = UPDATE_SURVEY_CONDUCT_EXT_QUERY.render(
            project_id=self.project_id,
            dataset_id=self.dataset_id,
            clean_survey_dataset_id=self.clean_survey_dataset_id)

        update_query_dict = {cdr_consts.QUERY: update_query}

        return [update_query_dict]

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

    ap = parser.get_argument_parser()
    ap.add_argument(
        '--clean_survey_dataset',
        action='store',
        dest='clean_survey_dataset_id',
        help=
        ('Dataset containing the mapping table provided by RDR team.  '
         'These have additional info like language to questionnaire_response_id.'
        ),
        required=True)

    ARGS = ap.parse_args()

    clean_engine.add_console_logging(ARGS.console_log)

    if ARGS.list_queries:
        clean_engine.add_console_logging()
        query_list = clean_engine.get_query_list(
            ARGS.project_id,
            ARGS.dataset_id,
            ARGS.sandbox_dataset_id, [(PopulateSurveyConductExt,)],
            clean_survey_dataset_id=ARGS.clean_survey_dataset_id)
        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(
            ARGS.project_id,
            ARGS.dataset_id,
            ARGS.sandbox_dataset_id, [(PopulateSurveyConductExt,)],
            clean_survey_dataset_id=ARGS.clean_survey_dataset_id)
