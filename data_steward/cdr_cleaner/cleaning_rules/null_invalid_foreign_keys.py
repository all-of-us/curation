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
from common import JINJA_ENV
from constants import bq_utils as bq_consts
from constants.cdr_cleaner import clean_cdr as cdr_consts
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule

LOGGER = logging.getLogger(__name__)

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
    SELECT {{cols}}
    FROM `{{project_id}}.{{dataset_id}}.{{table_name}}` t
    {{join_exp}})""")


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
        super().__init__(issue_numbers=['DC-388', 'DC-807'],
                         description=desc,
                         affected_datasets=[cdr_consts.COMBINED],
                         project_id=project_id,
                         dataset_id=dataset_id,
                         sandbox_dataset_id=sandbox_dataset_id,
                         affected_tables=resources.CDM_TABLES)

    def get_mapping_tables(self, domain_table):
        """
        Get name of mapping table generated for a domain table

        :param domain_table: one of the domain tables (e.g. 'visit_occurrence',
            'condition_occurrence')
        :return: mapping table name
        """
        return '_mapping_' + domain_table

    def get_query_specs(self):
        """
        This method gets the queries required to make invalid foreign keys null

        :param project_id: Project associated with the input and output datasets
        :param dataset_id: Dataset where cleaning rules are to be applied
        :param sandbox_dataset_id: Identifies the sandbox dataset to store rows
        :return: a list of queries
        """
        queries_list = []
        sandbox_queries_list = []

        for table in self.affected_tables:
            field_names = [
                field['name'] for field in resources.fields_for(table)
            ]
            foreign_keys_flags = []
            fields_to_join = []

            for field_name in field_names:
                if field_name in FOREIGN_KEYS_FIELDS and field_name != table + '_id':
                    fields_to_join.append(field_name)
                    foreign_keys_flags.append(field_name)

            if fields_to_join:
                col_exprs = []
                for field in field_names:
                    if field in fields_to_join:
                        if field in foreign_keys_flags:
                            col_expr = '{x}.'.format(x=field[:3]) + field
                    else:
                        col_expr = field
                    col_exprs.append(col_expr)
                cols = ', '.join(col_exprs)

                join_expression = []
                for key in FOREIGN_KEYS_FIELDS:
                    if key in foreign_keys_flags:
                        if key == 'person_id':
                            table_alias = cdr_consts.PERSON_TABLE_NAME
                        else:
                            table_alias = self.get_mapping_tables(
                                '{x}'.format(x=key)[:-3])
                        join_expression.append(
                            LEFT_JOIN.render(dataset_id=self.dataset_id,
                                             prefix=key[:3],
                                             field=key,
                                             table=table_alias))

                full_join_expression = " ".join(join_expression)

                invalid_foreign_key_query = {
                    cdr_consts.QUERY:
                        INVALID_FOREIGN_KEY_QUERY.render(
                            cols=cols,
                            table_name=table,
                            dataset_id=self.dataset_id,
                            project_id=self.project_id,
                            join_expr=full_join_expression),
                    cdr_consts.DESTINATION_TABLE:
                        table,
                    cdr_consts.DESTINATION_DATASET:
                        self.dataset_id,
                    cdr_consts.DISPOSITION:
                        bq_consts.WRITE_TRUNCATE
                }

                queries_list.append(invalid_foreign_key_query)

                sandbox_query = {
                    cdr_consts.QUERY:
                        SANDBOX_QUERY.render(
                            project_id=self.project_id,
                            sandbox_dataset_id=self.sandbox_dataset_id,
                            intermediary_table=self.get_sandbox_tablenames(),
                            cols=cols,
                            dataset_id=self.dataset_id,
                            table_name=table,
                            join_expr=full_join_expression),
                    cdr_consts.DESTINATION_DATASET:
                        self.dataset_id,
                    cdr_consts.DESTINATION_TABLE:
                        table,
                    cdr_consts.DISPOSITION:
                        bq_consts.WRITE_TRUNCATE
                }

                sandbox_queries_list.append(sandbox_query)

                print(f'cleaning rule queries list: {queries_list}')

        return queries_list + sandbox_queries_list

    def setup_rule(self, client):
        """
        Function to run any data upload options before executing a query.
        """
        pass

    def get_sandbox_tablenames(self):
        """
        Get the sandbox dataset id for this class instance.
        """
        return f'{self._issue_numbers[0].lower()}_{self._affected_tables[0]}'

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
