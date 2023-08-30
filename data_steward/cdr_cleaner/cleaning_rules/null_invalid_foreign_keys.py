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
from typing import Dict

# Project Imports
import resources
from common import AOU_DEATH, JINJA_ENV
from constants.cdr_cleaner.clean_cdr import COMBINED, QUERY
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule

LOGGER = logging.getLogger(__name__)

JIRA_ISSUE_NUMBERS = ['DC388', 'DC807', 'DC3230']

FOREIGN_KEYS_FIELDS = [
    'person_id', 'visit_occurrence_id', 'location_id', 'care_site_id',
    'provider_id'
]

SANDBOX_QUERY = JINJA_ENV.from_string("""
CREATE OR REPLACE TABLE `{{project_id}}.{{sandbox_dataset_id}}.{{sandbox_table}}` AS (
    SELECT * FROM `{{project_id}}.{{dataset_id}}.{{table_name}}`
    WHERE {% for key in foreign_keys %}
        (
            {{key}} NOT IN (
                SELECT {{key}} FROM `{{dataset_id}}.{{key[:-3]}}`
            )
            AND {{key}} IS NOT NULL
        ){% if not loop.last -%} OR {% endif %}
    {% endfor %}
)""")

DELETE_QUERY = JINJA_ENV.from_string("""
DELETE FROM `{{project_id}}.{{dataset_id}}.{{table_name}}`
WHERE {{key}} NOT IN (
    SELECT {{key}} FROM `{{project_id}}.{{dataset_id}}.{{key[:-3]}}`
)""")

UPDATE_QUERY = JINJA_ENV.from_string("""
UPDATE `{{project_id}}.{{dataset_id}}.{{table_name}}`
SET {{key}} = NULL
WHERE {{key}} NOT IN (
    SELECT {{key}} FROM `{{dataset_id}}.{{key[:-3]}}`
) AND {{key}} IS NOT NULL
""")


class NullInvalidForeignKeys(BaseCleaningRule):
    """
    Ensure invalid foreign keys are null while the remainder of the rows persist
    """

    def __init__(self,
                 project_id,
                 dataset_id,
                 sandbox_dataset_id,
                 table_namer=None):
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
                         affected_datasets=[COMBINED],
                         project_id=project_id,
                         dataset_id=dataset_id,
                         sandbox_dataset_id=sandbox_dataset_id,
                         affected_tables=self.get_affected_tables(),
                         table_namer=table_namer)

    def get_affected_tables(self):
        """
        This method gets all the tables that are affected by this cleaning rule
        
        :return: list of affected tables
        """
        return [
            table for table in resources.CDM_TABLES + [AOU_DEATH]
            if self.has_foreign_key(table)
        ]

    def get_column_mode_dict(self, table) -> Dict[str, str]:
        """
        This method gets the dict of field names as keys and nullable/required as values.
        :param table: single table in the list of affected tables
        :return: dict. Table's column names as keys and the column's mode (nullable/required) as values.
        """
        return {
            field['name']: field['mode']
            for field in resources.fields_for(table)
        }

    def get_foreign_keys(self, table):
        """
        This method gets the list of foreign keys determined from the list of field names

        :param table: single table in the list of affected tables
        :return: list of foreign keys
        """
        foreign_keys_flags = []
        for field_name in self.get_column_mode_dict(table).keys():
            if field_name in FOREIGN_KEYS_FIELDS and field_name != f'{table}_id':
                foreign_keys_flags.append(field_name)
        return foreign_keys_flags

    def has_foreign_key(self, table):
        """
        This method determines if a table contains has a foreign key

        :param table: single table in the list of affected tables
        :return: true, if the table contains a foreign key, false if the table does not contain a foreign key
        """
        return len(self.get_foreign_keys(table)) > 0

    def get_query_specs(self):
        """
        This method gets the queries required to make invalid foreign keys null

        :return: a list of queries
        """
        queries, sandbox_queries = [], []

        for table in self.get_affected_tables():

            sandbox_query = {
                QUERY:
                    SANDBOX_QUERY.render(
                        project_id=self.project_id,
                        sandbox_dataset_id=self.sandbox_dataset_id,
                        sandbox_table=self.sandbox_table_for(table),
                        dataset_id=self.dataset_id,
                        table_name=table,
                        foreign_keys=self.get_foreign_keys(table))
            }
            sandbox_queries.append(sandbox_query)

            for key in self.get_foreign_keys(table):
                if self.get_column_mode_dict(table)[key] == 'required':
                    query = {
                        QUERY:
                            DELETE_QUERY.render(project_id=self.project_id,
                                                dataset_id=self.dataset_id,
                                                table_name=table,
                                                key=key)
                    }

                else:
                    query = {
                        QUERY:
                            UPDATE_QUERY.render(project_id=self.project_id,
                                                dataset_id=self.dataset_id,
                                                table_name=table,
                                                key=key)
                    }
                queries.append(query)

        return sandbox_queries + queries

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
