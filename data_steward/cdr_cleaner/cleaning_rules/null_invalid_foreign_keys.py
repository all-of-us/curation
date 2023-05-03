"""
Cleaning Rule 1 : Foreign key references (i.e. visit_occurrence_id in the condition table) should be valid.

(Existing Achilles rule - validating for foreign keys include provider_id,
care_site_id, location_id, person_id, visit_occurrence_id)

Original Issues: DC-807, DC-388

The intent of this cleaning rule is to null out any invalid foreign keys while keeping
the remaining rules unchanged. A valid foreign key means that an existing foreign key already exists
in the table it references. An invalid foreign key means there is NOT an existing foreign key
in the table it references.
"""

# Python imports
import logging

# Project Imports
import resources
from common import AOU_DEATH, JINJA_ENV, MAPPING_PREFIX
from constants import bq_utils as bq_consts
from constants.cdr_cleaner import clean_cdr as cdr_consts
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule

LOGGER = logging.getLogger(__name__)

JIRA_ISSUE_NUMBERS = ['DC388', 'DC807']

FOREIGN_KEYS_FIELDS = [
    'person_id', 'visit_occurrence_id', 'location_id', 'care_site_id',
    'provider_id'
]

INVALID_FOREIGN_KEY_QUERY = JINJA_ENV.from_string("""
    SELECT {{cols}}
    FROM `{{project_id}}.{{dataset_id}}.{{table_name}}` t
    {{join_expr}}""")

LEFT_JOIN = JINJA_ENV.from_string("""
    LEFT JOIN `{{dataset_id}}.{{table}}` {{prefix}} 
    ON t.{{field}} = {{prefix}}.{{field}}""")

SANDBOX_QUERY = JINJA_ENV.from_string("""
    CREATE OR REPLACE TABLE 
    `{{project_id}}.{{sandbox_dataset_id}}.{{intermediary_table}}` AS (
        SELECT t.* FROM `{{project_id}}.{{dataset_id}}.{{table_name}}` AS t 
        WHERE {{sandbox_expr}})""")

SANDBOX_EXPRESSION = JINJA_ENV.from_string("""
     ({{field}} NOT IN (
        SELECT {{field}} 
        FROM `{{dataset_id}}.{{table}}` AS {{prefix}})
        AND {{field}} IS NOT NULL)
""")


class NullInvalidForeignKeys(BaseCleaningRule):
    """
    Ensure invalid foreign keys are null while the remainder of the rows persist
    """

    def __init__(self, project_id, dataset_id, sandbox_dataset_id):
        """
        Initialize the class with proper information.

        Set the issue numbers, description and affected datasets. As other tickets may affect this SQL,
        append them to the list of Jira Issues.
        DO NOT REMOVE ORIGINAL JIRA ISSUE NUMBERS!
        """
        desc = (
            'Ensures that invalid foreign keys are null while the remainder of the rows persist'
        )
        super().__init__(issue_numbers=JIRA_ISSUE_NUMBERS,
                         description=desc,
                         affected_datasets=[cdr_consts.COMBINED],
                         project_id=project_id,
                         dataset_id=dataset_id,
                         sandbox_dataset_id=sandbox_dataset_id,
                         affected_tables=self.get_affected_tables())

    def get_affected_tables(self):
        """
        This method gets all the tables that are affected by this cleaning rule
        
        :return: list of affected tables
        """
        return resources.CDM_TABLES + [AOU_DEATH]

    def get_mapping_table(self, domain_table):
        """
        Get name of mapping table generated for a domain table

        :param domain_table: one of the domain tables (e.g. 'visit_occurrence',
            'condition_occurrence')
        :return: mapping table name
        """
        return f'{MAPPING_PREFIX}{domain_table}'

    def get_field_names(self, table):
        """
        This method gets the list of field names in a table affected by the cleaning rule

        :param table: single table in the list of affected tables
        :return: list of field names in a single affected table
        """
        field_names = [field['name'] for field in resources.fields_for(table)]
        return field_names

    def get_foreign_keys(self, table):
        """
        This method gets the list of foreign keys determined from the list of field names

        :param table: single table in the list of affected tables
        :return: list of foreign keys
        """
        foreign_keys_flags = []
        for field_name in self.get_field_names(table):
            if field_name in FOREIGN_KEYS_FIELDS and field_name != table + '_id':
                foreign_keys_flags.append(field_name)
        return foreign_keys_flags

    def has_foreign_key(self, table):
        """
        This method determines if a table contains has a foreign key

        :param table: single table in the list of affected tables
        :return: true, if the table contains a foreign key, false if the table does not contain a foreign key
        """
        return len(self.get_foreign_keys(table)) > 0

    def get_col_expression(self, table):
        """
        This method formats the column name depending of if it is a foreign key or not. If the column is a foreign key
        it will be prefixed with the first part of the table name. If the column is not a foreign key, there will be
        no changes to the column name.

        :param table: single table in the list of affected tables
        :return: all the formatted column names for the table
        """
        col_exprs = []
        foreign_keys = self.get_foreign_keys(table)
        for field in self.get_field_names(table):
            if field in foreign_keys:
                col_expr = f'{field[:3]}.{field}'
            else:
                col_expr = field
            col_exprs.append(col_expr)
        return ', '.join(col_exprs)

    def get_join_expression(self, table):
        """
        This method generates the LEFT_JOIN query. Only columns that are foreign keys will be
        used in the query generation.

        :param table: single table in the list of affected tables
        :return: LEFT_JOIN query expression
        """
        join_expression = []
        for key in self.get_foreign_keys(table):
            if key in FOREIGN_KEYS_FIELDS:
                if key == 'person_id':
                    table_alias = cdr_consts.PERSON_TABLE_NAME
                else:
                    table_alias = self.get_mapping_table(
                        '{x}'.format(x=key)[:-3])
                join_expression.append(
                    LEFT_JOIN.render(dataset_id=self.dataset_id,
                                     prefix=key[:3],
                                     field=key,
                                     table=table_alias))
        return ' '.join(join_expression)

    def get_sandbox_expression(self, table):
        """
        This method generates the SANDBOX_EXPRESSION query. Only columns that are foreign keys will be
        used in the query generation.

        :param table: single table in the list of affected tables
        :return: SANDBOX_EXPRESSION query
        """
        sandbox_expression = []
        for key in self.get_foreign_keys(table):
            if key in FOREIGN_KEYS_FIELDS:
                if key == 'person_id':
                    table_alias = cdr_consts.PERSON_TABLE_NAME
                else:
                    table_alias = self.get_mapping_table(
                        '{x}'.format(x=key)[:-3])
                sandbox_expression.append(
                    SANDBOX_EXPRESSION.render(field=key,
                                              dataset_id=self.dataset_id,
                                              table=table_alias,
                                              prefix=key[:3]))
        return ' OR '.join(sandbox_expression)

    def get_query_specs(self):
        """
        This method gets the queries required to make invalid foreign keys null

        :return: a list of queries
        """
        queries_list = []
        sandbox_queries_list = []

        for table in self.get_affected_tables():
            if self.has_foreign_key(table):
                cols = self.get_col_expression(table)
                join_expression = self.get_join_expression(table)
                sandbox_expression = self.get_sandbox_expression(table)

                sandbox_query = {
                    cdr_consts.QUERY:
                        SANDBOX_QUERY.render(
                            project_id=self.project_id,
                            sandbox_dataset_id=self.sandbox_dataset_id,
                            intermediary_table=self.sandbox_table_for(table),
                            dataset_id=self.dataset_id,
                            table_name=table,
                            sandbox_expr=sandbox_expression),
                }

                sandbox_queries_list.append(sandbox_query)

                invalid_foreign_key_query = {
                    cdr_consts.QUERY:
                        INVALID_FOREIGN_KEY_QUERY.render(
                            cols=cols,
                            table_name=table,
                            dataset_id=self.dataset_id,
                            project_id=self.project_id,
                            join_expr=join_expression),
                    cdr_consts.DESTINATION_TABLE:
                        table,
                    cdr_consts.DESTINATION_DATASET:
                        self.dataset_id,
                    cdr_consts.DISPOSITION:
                        bq_consts.WRITE_TRUNCATE
                }

                queries_list.append(invalid_foreign_key_query)

        return sandbox_queries_list + queries_list

    def setup_rule(self, client):
        """
        Function to run any data upload options before executing a query.
        """
        pass

    def get_sandbox_tablenames(self):
        """
        Get the sandbox dataset id for this class instance
        """
        return [
            self.sandbox_table_for(affected_table)
            for affected_table in self.affected_tables
        ]

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


if __name__ == '__main__':
    import cdr_cleaner.args_parser as parser
    import cdr_cleaner.clean_cdr_engine as clean_engine

    ARGS = parser.parse_args()

    if ARGS.list_queries:
        clean_engine.add_console_logging()
        query_list = clean_engine.get_query_list(ARGS.project_id,
                                                 ARGS.dataset_id,
                                                 ARGS.sandbox_dataset_id,
                                                 [(NullInvalidForeignKeys,)])
        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(ARGS.project_id, ARGS.dataset_id,
                                   ARGS.sandbox_dataset_id,
                                   [(NullInvalidForeignKeys,)])
