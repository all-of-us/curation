"""
Background

To ensure participant privacy, curation will null the data in identifying fields.  These fields will be
nulled/de-identified regardless of the table the column exists in.

For all OMOP common data model tables, null or otherwise de-identify the following fields, if they exist in the table.

Fields:
month_of_birth
day_of_birth
location_id
provider_id
care_site_id

person_source_value, value_source_value, and value_as_string: these fields will be caught by the rule implemented for
DC-1369

For NULLABLE fields, use the NULL value.
For REQUIRED fields:
if numeric, use zero, 0.
if varchar/character/string, use empty string , ''.
If using a DML statement, sandboxing is not required.

Should be added to list of CONTROLLED_TIER_DEID_CLEANING_CLASSES in data_steward/cdr_cleaner/clean_cdr.py
Should occur after data remapping rules.

Should not be applied to mapping tables or other non-OMOP tables.
"""
import logging

# Project imports
from constants import bq_utils as bq_consts
from constants.cdr_cleaner import clean_cdr as cdr_consts
from common import JINJA_ENV, NPH_TABLES
from utils import pipeline_logging
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule, query_spec_list
from cdr_cleaner.cleaning_rules.table_suppression import TableSuppression
from resources import fields_for

LOGGER = logging.getLogger(__name__)

JIRA_ISSUE_NUMBERS = ['DC1372']

# identifying fields to be suppressed
fields = [
    'month_of_birth', 'day_of_birth', 'location_id', 'provider_id',
    'care_site_id'
]

# negative age at recorded time in table
ID_FIELD_SUPPRESSION_QUERY = JINJA_ENV.from_string("""
SELECT
  *
  REPLACE({{replace_statement}})
FROM
  `{{project_id}}.{{dataset_id}}.{{table}}`
""")

# replace string to use in query
REPLACE_STRING = JINJA_ENV.from_string("""
{{suppression_statement}} AS {{field}}
""")


class IDFieldSuppression(BaseCleaningRule):

    def __init__(self,
                 project_id,
                 dataset_id,
                 sandbox_dataset_id=None,
                 table_namer=None):
        """
        Initialize the class with proper information.

        Set the issue numbers, description and affected datasets. As other tickets may affect
        this SQL, append them to the list of Jira Issues.
        DO NOT REMOVE ORIGINAL JIRA ISSUE NUMBERS!
        """
        desc = (
            'Returns queries to null the data in identifying fields for all OMOP common data model tables.'
        )

        super().__init__(
            issue_numbers=JIRA_ISSUE_NUMBERS,
            description=desc,
            affected_datasets=[cdr_consts.COMBINED],
            affected_tables=NPH_TABLES,
            project_id=project_id,
            dataset_id=dataset_id,
            sandbox_dataset_id=sandbox_dataset_id,
            depends_on=[TableSuppression],
            table_namer=table_namer
        )  # table_suppression.py module will handle identifying fields in provider, care_site, location

    def get_query_specs(self, *args, **keyword_args) -> query_spec_list:
        """
        Return a list of dictionary query specifications.

        :return:  A list of dictionaries. Each dictionary contains a single query
            and a specification for how to execute that query. The specifications
            are optional but the query is required.
        """
        queries = []
        for table in self.affected_tables:
            schema = fields_for(table)
            statements = []
            for item in schema:
                if item.get('name') in fields:
                    if item.get('mode').lower() == 'nullable':
                        value = 'NULL'
                    elif item.get('type').lower() == 'integer':
                        value = 0
                    elif item.get('type').lower() == 'string':
                        value = ''
                    else:
                        raise RuntimeError(
                            f"Required field {item.get('name')} needs to be integer or string type to be replaced"
                        )
                    suppression_statement = REPLACE_STRING.render(
                        suppression_statement=value, field=item.get('name'))
                    statements.append(suppression_statement)
            if statements:
                suppression_statement = ', '.join(statements)
                query = dict()
                query[cdr_consts.QUERY] = ID_FIELD_SUPPRESSION_QUERY.render(
                    project_id=self.project_id,
                    dataset_id=self.dataset_id,
                    table=table,
                    replace_statement=suppression_statement)
                query[cdr_consts.DESTINATION_TABLE] = table
                query[cdr_consts.DISPOSITION] = bq_consts.WRITE_TRUNCATE
                query[cdr_consts.DESTINATION_DATASET] = self.dataset_id
                queries.append(query)

        return queries

    def setup_rule(self, client, *args, **keyword_args):
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

    ARGS = parser.parse_args()
    pipeline_logging.configure(level=logging.DEBUG, add_console_handler=True)

    if ARGS.list_queries:
        clean_engine.add_console_logging()
        query_list = clean_engine.get_query_list(ARGS.project_id,
                                                 ARGS.dataset_id,
                                                 ARGS.sandbox_dataset_id,
                                                 [(IDFieldSuppression,)])
        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(ARGS.project_id, ARGS.dataset_id,
                                   ARGS.sandbox_dataset_id,
                                   [(IDFieldSuppression,)])
