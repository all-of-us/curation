"""
Maps questionnaire_response_id to research_response_id.
Questionnaire_response_id exists in the following two tables:
    1. observation table, as column questionnaire_response_id
    2. survey_conduct table, as columns survey_conduct_id(int) and survey_source_identifier(str).

The mapping for questionnaire_response_id and research_response_id is in the
_deid_questionnaire_response_map lookup table.

If no mapping is found for a survey_conduct_id in the mapping table, the row
will be sandboxed and deleted from survey_conduct table, as survey_conduct_id
is the primary key for survey_conduct table and it cannot be mapped to None.

Original Issue: DC-1347, DC-518, DC-2065, DC-2637, DC-2730

The purpose of this cleaning rule is to use the questionnaire mapping lookup
table to remap the questionnaire_response_id to the randomly generated
research_response_id in the _deid_questionnaire_response_map table.

This rule will not sandbox survey_conduct_ext records that do not have a questionnaire_response_id mapping because
1.  The mapping table will be built from the observation.questionnaire_response_id values
2.  A cleaning rule, DC-2735, will ensure records in survey_conduct map to records in observation
    IF there is not a valid mapping from survey_conduct.survey_conduct_id = observation.questionnaire_response_id
    THEN the record will be sandboxed and dropped from survey_conduct.
3.  The clean mapping and extension tables rule will clean the survey_conduct_ext table.
"""

# Python imports
import logging

# Project imports
from utils import pipeline_logging
from common import (DEID_QUESTIONNAIRE_RESPONSE_MAP, EXT_SUFFIX, JINJA_ENV,
                    OBSERVATION, SURVEY_CONDUCT)
from constants.cdr_cleaner import clean_cdr as cdr_consts
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule
from cdr_cleaner.cleaning_rules.generate_ext_tables import GenerateExtTables

LOGGER = logging.getLogger(__name__)

ISSUE_NUMBERS = ['DC1347', 'DC518', 'DC2065', 'DC2637', 'DC2730']

QRID_RID_MAPPING_QUERY = JINJA_ENV.from_string("""
UPDATE `{{project_id}}.{{dataset_id}}.{{table}}` t
SET t.{{qrid_column}} = m.research_response_id
{% if table == 'survey_conduct' %}
   ,t.survey_source_identifier = CAST(m.research_response_id AS STRING)
{% endif %}
FROM `{{project_id}}.{{deid_questionnaire_response_map_dataset_id}}.{{deid_questionnaire_response_map}}` m
WHERE t.{{qrid_column}} = m.questionnaire_response_id
""")


class QRIDtoRID(BaseCleaningRule):
    """
    Remap the QRID (questionnaire_response_id/survey_conduct_id/survey_source_identifier(str))
    to the RID (research_response_id) using mapping lookup table.
    Sandbox and delete survey_conduct entries that cannot be mapped.
    """

    def __init__(self,
                 project_id,
                 dataset_id,
                 sandbox_dataset_id,
                 table_namer=None,
                 deid_questionnaire_response_map_dataset=None):
        """
        Initialize the class with proper info.

        Set the issue numbers, description and affected datasets.  As other
        tickets may affect this SQL, append them to the list of Jira Issues.
        DO NOT REMOVE ORIGINAL JIRA ISSUE NUMBERS!
        """
        desc = (
            'Remap the QID (questionnaire_response_id for observation and '
            'survey_conduct_id(int) and survey_source_identifier(str) for survey_conduct) '
            'to the RID (research_response_id) found in the deid questionnaire '
            'response mapping lookup table.')

        if not deid_questionnaire_response_map_dataset:
            raise TypeError(
                "`deid_questionnaire_response_map_dataset` cannot be empty")

        self.deid_questionnaire_response_map_dataset = deid_questionnaire_response_map_dataset

        super().__init__(issue_numbers=ISSUE_NUMBERS,
                         description=desc,
                         affected_datasets=[
                             cdr_consts.CONTROLLED_TIER_DEID,
                             cdr_consts.REGISTERED_TIER_DEID
                         ],
                         affected_tables=[
                             OBSERVATION, SURVEY_CONDUCT,
                             f'{SURVEY_CONDUCT}{EXT_SUFFIX}'
                         ],
                         project_id=project_id,
                         dataset_id=dataset_id,
                         sandbox_dataset_id=sandbox_dataset_id,
                         depends_on=[GenerateExtTables],
                         table_namer=table_namer,
                         run_for_synthetic=True)

    def get_query_specs(self, *args, **keyword_args):
        """
        Return a list of dictionary query specifications.

        :return:  A list of dictionaries.  Each dictionary contains a
            single query and a specification for how to execute that query.
            The specifications are optional but the query is required.
        """
        table_qrid_mappings = [{
            "table": OBSERVATION,
            "qrid_column": "questionnaire_response_id"
        }, {
            "table": SURVEY_CONDUCT,
            "qrid_column": "survey_conduct_id"
        }, {
            "table": f'{SURVEY_CONDUCT}{EXT_SUFFIX}',
            "qrid_column": "survey_conduct_id"
        }]

        mapping_queries = []
        for table_qrid_mapping in table_qrid_mappings:

            mapping_query = {
                cdr_consts.QUERY:
                    QRID_RID_MAPPING_QUERY.render(
                        project_id=self.project_id,
                        dataset_id=self.dataset_id,
                        table=table_qrid_mapping["table"],
                        qrid_column=table_qrid_mapping["qrid_column"],
                        deid_questionnaire_response_map=
                        DEID_QUESTIONNAIRE_RESPONSE_MAP,
                        deid_questionnaire_response_map_dataset_id=self.
                        deid_questionnaire_response_map_dataset)
            }

            mapping_queries.append(mapping_query)

        return mapping_queries

    def setup_rule(self, client, *args, **keyword_args):
        """
        Function to run any data upload options before executing a query.
        """
        pass

    def get_sandbox_tablenames(self):
        return [self.sandbox_table_for(table) for table in self.affected_tables]

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


if __name__ == '__main__':
    import cdr_cleaner.args_parser as parser
    import cdr_cleaner.clean_cdr_engine as clean_engine

    ext_parser = parser.get_argument_parser()
    ext_parser.add_argument(
        '-q',
        '--deid_questionnaire_response_map_dataset',
        action='store',
        dest='deid_questionnaire_response_map_dataset',
        help=
        'Identifies the dataset containing the _deid_questionnaire_response_map lookup table',
        required=True)
    ARGS = ext_parser.parse_args()

    pipeline_logging.configure(level=logging.DEBUG, add_console_handler=True)

    if ARGS.list_queries:
        clean_engine.add_console_logging()
        query_list = clean_engine.get_query_list(
            ARGS.project_id,
            ARGS.dataset_id,
            ARGS.sandbox_dataset_id, [(QRIDtoRID,)],
            deid_questionnaire_response_map_dataset=ARGS.
            deid_questionnaire_response_map_dataset)
        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(ARGS.project_id,
                                   ARGS.dataset_id,
                                   ARGS.sandbox_dataset_id, [(QRIDtoRID,)],
                                   deid_questionnaire_response_map_dataset=ARGS.
                                   deid_questionnaire_response_map_dataset)
