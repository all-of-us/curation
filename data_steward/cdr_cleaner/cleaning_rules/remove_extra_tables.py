"""
Remove any tables that are not OMOP, OMOP extension (created by curation), or Vocabulary tables.

Sandbox any tables that are removed.
Should be final cleaning rule.

Original Issues: DC-1441
"""

# Python imports
import logging

# Third party imports
from google.cloud import bigquery

# Project imports
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule
from constants.cdr_cleaner import clean_cdr as cdr_consts
from common import JINJA_ENV, AOU_CUSTOM_TABLES, WEAR_STUDY
from resources import cdm_schemas, has_domain_table_id
from utils import pipeline_logging
from utils.bq import list_tables

LOGGER = logging.getLogger(__name__)

SANDBOX_TABLES_QUERY = JINJA_ENV.from_string("""
{% for sandboxed_extra_table in sandboxed_extra_tables %}
CREATE OR REPLACE TABLE `{{project_id}}.{{sandbox_id}}.{{sandboxed_extra_table}}` AS (
SELECT
    *
FROM `{{project_id}}.{{dataset_id}}.{{extra_tables[loop.index0]}}`
);
{% endfor %}
""")

DROP_TABLES_QUERY = JINJA_ENV.from_string("""
{% for extra_table in extra_tables %}
DROP TABLE IF EXISTS `{{project_id}}.{{dataset_id}}.{{extra_table}}`;
{% endfor %}
""")


class RemoveExtraTables(BaseCleaningRule):
    extra_tables = []

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
        desc = 'Remove any tables that are not OMOP, OMOP extension, Vocabulary or AOU custom tables.'
        # Use custom cdr_metadata instead of metadata
        cdm_achilles_vocab_tables = list(
            set(
                cdm_schemas(
                    include_achilles=True,
                    include_vocabulary=True).keys())) + ['_cdr_metadata']
        # Use person_src_hpos_ext instead of person_ext
        extension_tables = list({
            f'{table}_ext' for table in cdm_schemas().keys()
            if has_domain_table_id(table)
        }) + ['person_src_hpos_ext']
        # To Keep AOU_DEATH table
        custom_tables = AOU_CUSTOM_TABLES + [WEAR_STUDY]
        affected_tables = cdm_achilles_vocab_tables + extension_tables + custom_tables
        super().__init__(issue_numbers=['DC1441'],
                         description=desc,
                         affected_datasets=[cdr_consts.CONTROLLED_TIER_DEID],
                         affected_tables=affected_tables,
                         project_id=project_id,
                         dataset_id=dataset_id,
                         sandbox_dataset_id=sandbox_dataset_id,
                         table_namer=table_namer)

    def get_query_specs(self, *args, **keyword_args):
        """
        Interface to return a list of query dictionaries.

        :returns:  a list of query dictionaries.  Each dictionary specifies
            the query to execute and how to execute.  The dictionaries are
            stored in list order and returned in list order to maintain
            an ordering.
        """

        sandbox_queries = []
        drop_queries = []

        if self.extra_tables:
            sandbox_queries = [{
                cdr_consts.QUERY: sandbox_query.strip()
            } for sandbox_query in SANDBOX_TABLES_QUERY.render(
                project_id=self.project_id,
                dataset_id=self.dataset_id,
                sandbox_id=self.sandbox_dataset_id,
                extra_tables=self.extra_tables,
                sandboxed_extra_tables=[
                    self.sandbox_table_for(table) for table in self.extra_tables
                ]).split(';')[:-1]]

            drop_queries = [{
                cdr_consts.QUERY: drop_query.strip()
            } for drop_query in DROP_TABLES_QUERY.render(
                project_id=self.project_id,
                dataset_id=self.dataset_id,
                extra_tables=self.extra_tables).split(';')[:-1]]

        return sandbox_queries + drop_queries

    def get_sandbox_tablenames(self):
        return [self.sandbox_table_for(table) for table in self.extra_tables]

    def setup_validation(self, client, *args, **keyword_args):
        """
        Run required steps for validation setup

        Method to run to setup validation on cleaning rules that will be updating or deleting the values.
        For example:
        if your class updates all the datetime fields you should be implementing the
        logic to get the initial list of values which adhere to a condition we are looking for.

        if your class deletes a subset of rows in the tables you should be implementing
        the logic to get the row counts of the tables prior to applying cleaning rule

        """
        raise NotImplementedError("Please fix me.")

    def validate_rule(self, client, *args, **keyword_args):
        """
        Validates the cleaning rule which deletes or updates the data from the tables

        Method to run validation on cleaning rules that will be updating the values.
        For example:
        if your class updates all the datetime fields you should be implementing the
        validation that checks if the date time values that needs to be updated no
        longer exists in the table.

        if your class deletes a subset of rows in the tables you should be implementing
        the validation that checks if the count of final final row counts + deleted rows
        should equals to initial row counts of the affected tables.

        Raises RunTimeError if the validation fails.
        """

        dataset_ref = bigquery.DatasetReference(client.project, self.dataset_id)
        current_tables = list_tables(client, dataset_ref)
        current_tables = [table.table_id for table in current_tables]
        extra_tables = list(set(current_tables) - set(self.affected_tables))

        if extra_tables:
            raise RuntimeError(
                f'Some extra tables remain in the dataset: {extra_tables}')

    def setup_rule(self, client, *args, **keyword_args):
        """
        Load required resources prior to executing cleaning rule queries.

        Method to run data upload options before executing the first cleaning
        rule of a class.  For example, if your class requires loading a static
        table, that load operation should be defined here.  It SHOULD NOT BE
        defined as part of get_query_specs().
        """

        dataset_ref = bigquery.DatasetReference(client.project, self.dataset_id)
        current_tables = client.list_tables(dataset_ref)
        current_tables = [table.table_id for table in current_tables]
        self.extra_tables = list(
            set(current_tables) - set(self.affected_tables))


if __name__ == '__main__':
    import cdr_cleaner.args_parser as parser
    import cdr_cleaner.clean_cdr_engine as clean_engine

    ARGS = parser.parse_args()
    pipeline_logging.configure(level=logging.DEBUG, add_console_handler=True)

    if ARGS.list_queries:
        clean_engine.add_console_logging()
        query_list = clean_engine.get_query_list(ARGS.project_id,
                                                 ARGS.dataset_id,
                                                 ARGS.sandbox_dataset_id,
                                                 [(RemoveExtraTables,)])

        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(ARGS.project_id, ARGS.dataset_id,
                                   ARGS.sandbox_dataset_id,
                                   [(RemoveExtraTables,)])
