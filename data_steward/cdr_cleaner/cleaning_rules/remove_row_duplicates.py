"""
All columns except ID column in each domain should be unique
"""
import logging

# Project imports
import cdm
from utils.bq import fields_for
from constants.cdr_cleaner import clean_cdr as cdr_consts
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule, query_spec_list
from cdr_cleaner.clean_cdr_utils import get_tables_in_dataset
from common import JINJA_ENV

LOGGER = logging.getLogger(__name__)

JIRA_ISSUE_NUMBERS = ['DC1732']

DE_DUP_SANDBOX_QUERY_TEMPLATE = JINJA_ENV.from_string("""
CREATE OR REPLACE TABLE `{{project_id}}.{{sandbox_dataset_id}}.{{sandbox_table_name}}` AS
SELECT
    t.* EXCEPT (row_num)
FROM (
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY {{cols}}) AS row_num
    FROM `{{project_id}}.{{dataset_id}}.{{table_name}}`
) AS t
WHERE row_num > 1
""")

DE_DUP_QUERY_TEMPLATE = JINJA_ENV.from_string("""
DELETE
FROM `{{project_id}}.{{dataset_id}}.{{table_name}}`
WHERE table_id IN
(SELECT {{table_name}}_id
FROM `{{project_id}}.{{sandbox_dataset_id}}.{{sandbox_table_name}}`)
""")


class DeduplicateExceptIdColumn(BaseCleaningRule):

    def __init__(self, project_id, dataset_id, sandbox_dataset_id):
        """
        Initialize the class with proper information.

        Set the issue numbers, description and affected datasets. As other tickets may affect
        this SQL, append them to the list of Jira Issues.
        DO NOT REMOVE ORIGINAL JIRA ISSUE NUMBERS!
        """
        desc = (
            'Remove the duplicate columns excluding ID from OMOP tables in a given dataset'
        )

        super().__init__(issue_numbers=JIRA_ISSUE_NUMBERS,
                         description=desc,
                         affected_datasets=[cdr_consts.UNIONED],
                         affected_tables=cdm.tables_to_map(),
                         project_id=project_id,
                         dataset_id=dataset_id,
                         sandbox_dataset_id=sandbox_dataset_id)

    def get_query_specs(self, *args, **keyword_args) -> query_spec_list:

        sandbox_queries = []
        # iterate through the list of CDM tables with an id column
        for table_name in self.affected_tables:
            schema = fields_for(table_name)
            cols = [
                column.get('name')
                for column in schema
                if column.get('name') != f'{table_name}_id'
            ]
            sandbox_queries.append({
                cdr_consts.QUERY:
                    DE_DUP_SANDBOX_QUERY_TEMPLATE.render(
                        project_id=self.project_id,
                        dataset_id=self.dataset_id,
                        sandbox_dataset_id=self.sandbox_dataset_id,
                        table_name=table_name,
                        sandbox_table_name=self.sandbox_table_for(table_name),
                        cols=',\n'.join(cols))
            })

        queries = []
        # iterate through the list of CDM tables with an id column
        for table_name in self.affected_tables:
            queries.append({
                cdr_consts.QUERY:
                    DE_DUP_QUERY_TEMPLATE.render(
                        project_id=self.project_id,
                        dataset_id=self.dataset_id,
                        sandbox_dataset_id=self.sandbox_dataset_id,
                        table_name=table_name,
                        sandbox_table_name=self.sandbox_table_for(table_name))
            })

        return sandbox_queries + queries

    def get_sandbox_tablenames(self):
        return [
            self.sandbox_table_for(affected_table)
            for affected_table in self.affected_tables
        ]

    def setup_rule(self, client, *args, **keyword_args):
        self.affected_tables = get_tables_in_dataset(client, self.project_id,
                                                     self.dataset_id,
                                                     self.affected_tables)

    def setup_validation(self, client, *args, **keyword_args):
        pass

    def validate_rule(self, client, *args, **keyword_args):
        pass


if __name__ == '__main__':
    import cdr_cleaner.args_parser as parser
    import cdr_cleaner.clean_cdr_engine as clean_engine

    ARGS = parser.default_parse_args()

    if ARGS.list_queries:
        clean_engine.add_console_logging()
        query_list = clean_engine.get_query_list(ARGS.project_id,
                                                 ARGS.dataset_id,
                                                 ARGS.sandbox_dataset_id,
                                                 [(DeduplicateExceptIdColumn,)])
        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(ARGS.project_id, ARGS.dataset_id,
                                   ARGS.sandbox_dataset_id,
                                   [(DeduplicateExceptIdColumn,)])
