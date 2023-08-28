"""
Create extension tables

Original Issues: DC-1640

The intent of this cleaning rule is generate the extension tables with the proper fields and populate each with the
    correct <table>_id and src_id data from the site_masking table.
"""

# Python imports
import logging

# Third party imports
from google.cloud.bigquery import DatasetReference
from google.cloud.exceptions import NotFound

# Project imports
from common import EXT_SUFFIX, JINJA_ENV, MAPPING_PREFIX, PIPELINE_TABLES, SITE_MASKING_TABLE_ID
from utils import pipeline_logging
from resources import fields_for, MAPPING_TABLES
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule
import constants.cdr_cleaner.clean_cdr as cdr_consts

LOGGER = logging.getLogger(__name__)

ISSUE_NUMBERS = ['DC-1351', 'DC-1500', 'DC-1640', 'DC-2628']
EXT_FIELD_TEMPLATE = [{
    "type": "integer",
    "name": "{table}_id",
    "mode": "nullable",
    "description": "The {table}_id used in the {table} table."
}, {
    "type": "string",
    "name": "src_id",
    "mode": "nullable",
    "description": "The provenance of the data associated with the {table}_id."
}]

REPLACE_SRC_QUERY = JINJA_ENV.from_string("""
CREATE OR REPLACE TABLE `{{project_id}}.{{dataset_id}}.{{ext_table}}` ({{ext_table_fields}})
AS (
SELECT 
    m.{{cdm_table_id}}_id 
    , s.src_id
    {% for field in additional_fields %}
        {% if field is in mapping_fields %}
          , m.{{field}}
        {% else %}
          , null as {{field}}
        {% endif %}
    {% endfor %}
FROM `{{project_id}}.{{mapping_dataset_id}}.{{mapping_table_id}}` m
JOIN `{{project_id}}.{{shared_sandbox_id}}.{{site_maskings_table_id}}` s
ON m.src_hpo_id = s.hpo_id
)
""")


class GenerateExtTables(BaseCleaningRule):
    """
    Generates extension tables and populates with the site maskings stored in the site_maskings table
    """

    def __init__(self,
                 project_id,
                 dataset_id,
                 sandbox_dataset_id,
                 mapping_dataset_id,
                 table_namer=None):
        """
        Initialize the class with proper information.

        Set the issue numbers, description and affected datasets. As other tickets may affect
        this SQL, append them to the list of Jira Issues.
        DO NOT REMOVE ORIGINAL JIRA ISSUE NUMBERS!

        :param project_id: project identifier
        :param dataset_id: dataset identifier.  extension tables will be created
            in this dataset
        :param sandbox_dataset_id:  dataset identifier.  this dataset will hold
            helper/logging/lookup tables
        :param mapping_dataset_id: dataset identifier.  identifies a dataset that
            contains mapping tables to convert to extension tables.
        """
        desc = 'Generate extension tables and populate with masking data from the site_maskings table'
        super().__init__(issue_numbers=ISSUE_NUMBERS,
                         description=desc,
                         affected_datasets=[
                             cdr_consts.REGISTERED_TIER_DEID,
                             cdr_consts.CONTROLLED_TIER_DEID
                         ],
                         affected_tables=[],
                         project_id=project_id,
                         dataset_id=dataset_id,
                         sandbox_dataset_id=sandbox_dataset_id,
                         table_namer=table_namer,
                         run_for_synthetic=True)

        self._mapping_dataset_id = mapping_dataset_id
        self.mapping_table_ids = []

    def get_table_fields_str(self, table, ext_table_id):
        """
        Generates fields for ext tables for the provided cdm table

        :param table: cdm table to generate ext fields for
        :param ext_table_id: cdm table extension name.  used to load schema
            definition if it exists as a json file
        :return: tuple that contains the following:
            first item: Fields for the ext table in str format.
                        You can use this string for the DDL "CREATE TABLE xyz (_HERE_)"
            second item: Fields for the ext table in dict format
        """
        table_fields = []

        try:
            table_fields = fields_for(ext_table_id)
            LOGGER.info(
                f"using json schema file definition for table: {ext_table_id}")
        except RuntimeError:
            for field in EXT_FIELD_TEMPLATE:
                table_field = dict()
                for key in field:
                    table_field[key] = field[key].format(table=table)
                table_fields.append(table_field)
            LOGGER.info(
                f"using dynamic extension table schema for table: {ext_table_id}"
            )

        table_field_str = self.get_bq_fields_sql(table_fields)

        return (table_field_str, table_fields)

    def get_mapping_table_ids(self, client):
        """
        returns all the mapping table ids found in the dataset
        :param project_id: project_id containing the dataset
        :param mapping_dataset_id: dataset_id containing mapping tables
        :return: returns mapping table ids
        """
        dataset_ref = DatasetReference(self.project_id,
                                       self._mapping_dataset_id)
        table_objs = client.list_tables(dataset_ref)
        mapping_table_ids = [
            table_obj.table_id
            for table_obj in table_objs
            if table_obj.table_id in MAPPING_TABLES
        ]
        return mapping_table_ids

    def get_query_specs(self):
        """
        Return a list of dictionary query specifications.

        :return:  A list of dictionaries.  Each dictionary contains a
            single query and a specification for how to execute that query.
            The specifications are optional but the query is required.
        """
        queries = []

        for mapping_table_id in self.mapping_table_ids:
            # get cdm table name
            cdm_table_id = mapping_table_id[len(MAPPING_PREFIX):]
            ext_table_id = f"{cdm_table_id}{EXT_SUFFIX}"
            ext_table_fields_str, table_fields = self.get_table_fields_str(
                cdm_table_id, ext_table_id)

            query = dict()

            mapping_field_names = fields_for(mapping_table_id)
            mapping_field_names = [
                field.get('name') for field in mapping_field_names
            ]

            additional_field_names = [
                field.get('name')
                for field in table_fields
                if field.get('name') not in ['src_id', f'{cdm_table_id}_id']
            ]

            query[cdr_consts.QUERY] = REPLACE_SRC_QUERY.render(
                project_id=self.project_id,
                dataset_id=self.dataset_id,
                ext_table=ext_table_id,
                ext_table_fields=ext_table_fields_str,
                additional_fields=additional_field_names,
                cdm_table_id=cdm_table_id,
                mapping_dataset_id=self._mapping_dataset_id,
                mapping_table_id=mapping_table_id,
                shared_sandbox_id=self.sandbox_dataset_id,
                site_maskings_table_id=SITE_MASKING_TABLE_ID)
            queries.append(query)

        return queries

    def setup_rule(self, client, *args, **keyword_args):
        """
        Function to run any data upload options before executing a query.
        """
        try:
            client.get_table(
                f'{self.project_id}.{self.sandbox_dataset_id}.{SITE_MASKING_TABLE_ID}'
            )
        except NotFound:
            job = client.copy_table(
                f'{self.project_id}.{PIPELINE_TABLES}.{SITE_MASKING_TABLE_ID}',
                f'{self.project_id}.{self.sandbox_dataset_id}.{SITE_MASKING_TABLE_ID}'
            )
            job.result()
            LOGGER.info(f'Copied {PIPELINE_TABLES}.{SITE_MASKING_TABLE_ID} to '
                        f'{self.sandbox_dataset_id}.{SITE_MASKING_TABLE_ID}')

        self.mapping_table_ids = self.get_mapping_table_ids(client)

    def get_sandbox_tablenames(self):
        """
        Get the sandbox table ids for this class instance
        """
        return [SITE_MASKING_TABLE_ID]

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

    mapping_dataset_arg = {
        parser.SHORT_ARGUMENT: '-m',
        parser.LONG_ARGUMENT: '--mapping_dataset_id',
        parser.ACTION: 'store',
        parser.DEST: 'mapping_dataset_id',
        parser.HELP: 'Identifies the dataset containing the mapping tables',
        parser.REQUIRED: True
    }

    ARGS = parser.default_parse_args([mapping_dataset_arg])
    pipeline_logging.configure(level=logging.DEBUG, add_console_handler=True)

    if ARGS.list_queries:
        clean_engine.add_console_logging()
        query_list = clean_engine.get_query_list(
            ARGS.project_id,
            ARGS.dataset_id,
            ARGS.sandbox_dataset_id, [(GenerateExtTables,)],
            mapping_dataset_id=ARGS.mapping_dataset_id)
        for query_info in query_list:
            LOGGER.info(query_info)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(ARGS.project_id,
                                   ARGS.dataset_id,
                                   ARGS.sandbox_dataset_id,
                                   [(GenerateExtTables,)],
                                   mapping_dataset_id=ARGS.mapping_dataset_id)
