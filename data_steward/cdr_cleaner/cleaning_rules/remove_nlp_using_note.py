# coding=utf-8
"""
Removes Note records that do not have any note_nlp

Jira issues = DC-3867
"""
# Python imports
import logging

# Project imports
from gcloud.bq import BigQueryClient
import constants.cdr_cleaner.clean_cdr as cdr_consts
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule, query_spec_list
from common import JINJA_ENV, NOTE, NOTE_NLP

LOGGER = logging.getLogger(__name__)

JIRA_ISSUE_NUMBERS = ['DC3867']

SANDBOX_NOTE_TABLE = JINJA_ENV.from_string("""
CREATE OR REPLACE TABLE `{{project_id}}.{{sandbox_dataset_id}}.{{sandbox_table}}` AS (
   SELECT * FROM `{{project_id}}.{{dataset_id}}.note`
   WHERE note_id NOT IN (SELECT note_id FROM `{{project_id}}.{{dataset_id}}.note_nlp`)
 )
""")

SANDBOX_NOTE_NLP_TABLE = JINJA_ENV.from_string("""
CREATE OR REPLACE TABLE `{{project_id}}.{{sandbox_dataset_id}}.{{sandbox_table}}` AS (
   SELECT * FROM `{{project_id}}.{{dataset_id}}.note_nlp`
   WHERE note_id NOT IN (SELECT note_id FROM `{{project_id}}.{{dataset_id}}.note`)
 )
""")

SUPPRESS_NOTE_QUERY = JINJA_ENV.from_string("""
DELETE FROM `{{project_id}}.{{dataset_id}}.note`
 WHERE note_id NOT IN (SELECT note_id FROM `{{project_id}}.{{dataset_id}}.note_nlp`)
""")

SUPPRESS_NOTE_NLP_QUERY = JINJA_ENV.from_string("""
DELETE FROM `{{project_id}}.{{dataset_id}}.note_nlp`
 WHERE note_id NOT IN (SELECT note_id FROM `{{project_id}}.{{dataset_id}}.note`)
""")


class RemoveNoteUsingNLP(BaseCleaningRule):

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
                         affected_datasets=[
                             cdr_consts.COMBINED,
                             cdr_consts.REGISTERED_TIER_DEID,
                             cdr_consts.REGISTERED_TIER_DEID_BASE,
                             cdr_consts.REGISTERED_TIER_DEID_CLEAN,
                             cdr_consts.CONTROLLED_TIER_DEID,
                             cdr_consts.CONTROLLED_TIER_DEID_BASE,
                             cdr_consts.CONTROLLED_TIER_DEID_CLEAN
                         ],
                         affected_tables=[NOTE, NOTE_NLP],
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

        sandbox_note_query = dict()
        sandbox_note_query[cdr_consts.QUERY] = SANDBOX_NOTE_TABLE.render(
            project_id=self.project_id,
            dataset_id=self.dataset_id,
            sandbox_dataset_id=self.sandbox_dataset_id,
            sandbox_table=self.get_sandbox_tablenames()[0])
        queries_list.append(sandbox_note_query)

        sandbox_note_nlp_query = dict()
        sandbox_note_nlp_query[
            cdr_consts.QUERY] = SANDBOX_NOTE_NLP_TABLE.render(
                project_id=self.project_id,
                dataset_id=self.dataset_id,
                sandbox_dataset_id=self.sandbox_dataset_id,
                sandbox_table=self.get_sandbox_tablenames()[1])
        queries_list.append(sandbox_note_nlp_query)

        suppress_note_query = dict()
        suppress_note_query[cdr_consts.QUERY] = SUPPRESS_NOTE_QUERY.render(
            project_id=self.project_id,
            dataset_id=self.dataset_id,
            sandbox_dataset_id=self.sandbox_dataset_id,
            sandbox_table=self.get_sandbox_tablenames()[0])
        queries_list.append(suppress_note_query)

        suppress_note_nlp_query = dict()
        suppress_note_nlp_query[
            cdr_consts.QUERY] = SUPPRESS_NOTE_NLP_QUERY.render(
                project_id=self.project_id,
                dataset_id=self.dataset_id,
                sandbox_dataset_id=self.sandbox_dataset_id,
                sandbox_table=self.get_sandbox_tablenames()[1])
        queries_list.append(suppress_note_nlp_query)

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
                                                 [(RemoveNoteUsingNLP,)])
        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(ARGS.project_id, ARGS.dataset_id,
                                   ARGS.sandbox_dataset_id,
                                   [(RemoveNoteUsingNLP,)])
