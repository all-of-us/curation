# coding=utf-8
"""
Moves NLP records from note_nlp to domain tables.
Currently only applies to the condition_occurrence table.
Applied at every CDR pipeline stage from combined, wherever condition rows are filtered

Jira issues = DC-3864
"""
# Python imports
import logging

# Project imports
from gcloud.bq import BigQueryClient
import constants.cdr_cleaner.clean_cdr as cdr_consts
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule, query_spec_list
from common import JINJA_ENV, NOTE_NLP

LOGGER = logging.getLogger(__name__)

JIRA_ISSUE_NUMBERS = ['DC3864']

INSERT_NLP_COND_EXT = JINJA_ENV.from_string("""
INSERT INTO `{{project_id}}.{{dataset_id}}.condition_occurrence_ext` (
    condition_occurrence_id,
    src_id,
    note_nlp_id)
SELECT
    ROW_NUMBER() OVER() + (SELECT COALESCE(MAX(condition_occurrence_id), 0)
        FROM `{{project_id}}.{{dataset_id}}.condition_occurrence_ext`) AS condition_occurrence_id,
    n.src_id,
    l.note_nlp_id,
FROM `{{project_id}}.{{dataset_id}}.note_nlp` l
JOIN `{{project_id}}.{{dataset_id}}.note_nlp_ext` n USING (note_nlp_id)
""")

INSERT_NLP_COND = JINJA_ENV.from_string("""
INSERT INTO `{{project_id}}.{{dataset_id}}.condition_occurrence` (
    condition_occurrence_id,
    person_id,
    condition_concept_id,
    condition_start_date,
    condition_start_datetime,
    condition_end_date,
    condition_end_datetime,
    condition_type_concept_id,
    condition_status_concept_id,
    stop_reason,
    provider_id,
    visit_occurrence_id,
    visit_detail_id,
    condition_source_value,
    condition_source_concept_id,
    condition_status_source_value)
SELECT
    e.condition_occurrence_id,
    n.person_id,
    l.note_nlp_concept_id AS condition_concept_id,
    n.note_date AS condition_start_date,
    n.note_datetime AS condition_start_datetime,
    NULL AS condition_end_date,
    NULL AS condition_end_datetime,
    n.note_type_concept_id AS condition_type_concept_id,
    NULL AS condition_status_concept_id,
    NULL AS stop_reason,
    n.provider_id,
    n.visit_occurrence_id,
    n.visit_detail_id AS visit_detail_id,
    'note_nlp' AS condition_source_value,
    l.note_nlp_source_concept_id AS condition_source_concept_id,
    NULL AS condition_status_source_value
FROM `{{project_id}}.{{dataset_id}}.note_nlp` l
JOIN `{{project_id}}.{{dataset_id}}.note` n USING (note_id)
JOIN `{{project_id}}.{{dataset_id}}.condition_occurrence_ext` e ON l.note_nlp_id = e.note_nlp_id
""")


class MoveNLPtoDomains(BaseCleaningRule):

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
        desc = 'Nulls string fields in the note table.'

        super().__init__(issue_numbers=JIRA_ISSUE_NUMBERS,
                         description=desc,
                         affected_datasets=[cdr_consts.COMBINED,
                                            cdr_consts.CONTROLLED_TIER_DEID,
                                            cdr_consts.REGISTERED_TIER_PRE_DEID,
                                            cdr_consts.REGISTERED_TIER_DEID_BASE,
                                            cdr_consts.CONTROLLED_TIER_DEID_BASE,
                                            cdr_consts.REGISTERED_TIER_DEID_CLEAN,
                                            cdr_consts.CONTROLLED_TIER_DEID_CLEAN
                                            ],
                         affected_tables=[NOTE_NLP],
                         project_id=project_id,
                         dataset_id=dataset_id,
                         sandbox_dataset_id=sandbox_dataset_id,
                         table_namer=table_namer)

    def get_sandbox_tablenames(self) -> list:
        return [self.sandbox_table_for(table) for table in self.affected_tables]

    def setup_rule(self, client: BigQueryClient, *args, **keyword_args) -> None:
        """
        Load the lookup table values into the sandbox.

        The following queries will use the lookup table as part of the execution.
        Loads the operational pii fields from resource_files/_operational_pii_fields.csv
        into project_id.sandbox_dataset_id.operational_pii_fields in BQ
        """
        pass

    def get_query_specs(self, *args, **keyword_args) -> query_spec_list:
        """
        Return a list of dictionary query specifications.

        :return:  A list of dictionaries. Each dictionary contains a single query
            and a specification for how to execute that query. The specifications
            are optional but the query is required.
        """
        queries_list = []

        insert_ext_query = dict()
        insert_ext_query[cdr_consts.QUERY] = INSERT_NLP_COND_EXT.render(
            project_id=self.project_id, dataset_id=self.dataset_id)
        queries_list.append(insert_ext_query)

        insert_query = dict()
        insert_query[cdr_consts.QUERY] = INSERT_NLP_COND.render(
            project_id=self.project_id, dataset_id=self.dataset_id)
        queries_list.append(insert_query)

        return queries_list

    def setup_validation(self, client: BigQueryClient) -> None:
        """
        Run required steps for validation setup
        """
        pass

    def validate_rule(self, client: BigQueryClient) -> None:
        """
        Validates the cleaning rule which deletes or updates the data from the tables
        """
        pass


if __name__ == '__main__':
    import cdr_cleaner.args_parser as parser
    import cdr_cleaner.clean_cdr_engine as clean_engine

    ARGS = parser.parse_args()

    if ARGS.list_queries:
        clean_engine.add_console_logging()
        query_list = clean_engine.get_query_list(ARGS.project_id,
                                                 ARGS.dataset_id,
                                                 ARGS.sandbox_dataset_id,
                                                 [(MoveNLPtoDomains,)])
        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(ARGS.project_id, ARGS.dataset_id,
                                   ARGS.sandbox_dataset_id,
                                   [(MoveNLPtoDomains,)])
