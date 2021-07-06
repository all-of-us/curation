"""
Rule: 4
ID columns in each domain should be unique
"""
import logging

# Project imports
import cdm
from constants.cdr_cleaner import clean_cdr as cdr_consts
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule, query_spec_list
from cdr_cleaner.clean_cdr_utils import get_tables_in_dataset
from common import JINJA_ENV
from constants.bq_utils import WRITE_TRUNCATE

LOGGER = logging.getLogger(__name__)

JIRA_ISSUE_NUMBERS = ['DC392', 'DC810']

# Since an row_number() function is run on the {table_name}_id column, the order of the row
# numbers is not deterministic in case of duplicate IDs so it might be a good idea to sandbox
# all records associated with the same id column value
ID_DE_DUP_SANDBOX_QUERY_TEMPLATE = JINJA_ENV.from_string("""
WITH id_ranks AS 
(
    SELECT 
        ROW_NUMBER() OVER (PARTITION BY t.{{table_name}}_id) AS row_num,
        t.*
    FROM `{{project_id}}.{{dataset_id}}.{{table_name}}` AS t
),
id_with_multiple_ranks AS (
    SELECT
        i.{{table_name}}_id
    FROM id_ranks AS i
    GROUP BY i.{{table_name}}_id
    HAVING COUNT(*) > 1
)
SELECT 
    ir.*
FROM id_with_multiple_ranks AS im
JOIN id_ranks AS ir
    ON ir.{{table_name}}_id = im.{{table_name}}_id
""")

ID_DE_DUP_QUERY_TEMPLATE = JINJA_ENV.from_string("""
SELECT 
    t.* EXCEPT (row_num)
FROM (
    SELECT 
        t.*,
        ROW_NUMBER() OVER (PARTITION BY t.{{table_name}}_id) AS row_num
    FROM `{{project_id}}.{{dataset_id}}.{{table_name}}` AS t
) AS t
WHERE row_num = 1
""")


class DeduplicateIdColumn(BaseCleaningRule):

    def __init__(self, project_id, dataset_id, sandbox_dataset_id):
        """
        Initialize the class with proper information.

        Set the issue numbers, description and affected datasets. As other tickets may affect
        this SQL, append them to the list of Jira Issues.
        DO NOT REMOVE ORIGINAL JIRA ISSUE NUMBERS!
        """
        desc = (
            'Remove the duplicate id columns from OMOP tables that have an ID column '
            'in a given dataset')

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
            sandbox_queries.append({
                cdr_consts.QUERY:
                    ID_DE_DUP_SANDBOX_QUERY_TEMPLATE.render(
                        project_id=self.project_id,
                        dataset_id=self.dataset_id,
                        table_name=table_name),
                cdr_consts.DESTINATION_TABLE:
                    self.sandbox_table_for(table_name),
                cdr_consts.DISPOSITION:
                    WRITE_TRUNCATE,
                cdr_consts.DESTINATION_DATASET:
                    self.sandbox_dataset_id
            })

        queries = []
        # iterate through the list of CDM tables with an id column
        for table_name in self.affected_tables:
            queries.append({
                cdr_consts.QUERY:
                    ID_DE_DUP_QUERY_TEMPLATE.render(project_id=self.project_id,
                                                    dataset_id=self.dataset_id,
                                                    table_name=table_name),
                cdr_consts.DESTINATION_TABLE:
                    table_name,
                cdr_consts.DISPOSITION:
                    WRITE_TRUNCATE,
                cdr_consts.DESTINATION_DATASET:
                    self.dataset_id
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
                                                 [(DeduplicateIdColumn,)])
        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(ARGS.project_id, ARGS.dataset_id,
                                   ARGS.sandbox_dataset_id,
                                   [(DeduplicateIdColumn,)])
