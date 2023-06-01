"""
Generic clean up rule to ensure each mapping table contains only the records for
domain tables existing after the dataset has been fully cleaned.

Original Issue: DC-715

The intent is to ensure the mapping table continues to represent a true record of the
cleaned domain table by sandboxing the mapping table records and rows dropped
when the records of the row references have been dropped by a cleaning rule.
"""

import logging

from common import EXT, EXT_SUFFIX, JINJA_ENV, MAPPING, MAPPING_PREFIX, UNIONED_EHR
import constants.bq_utils as bq_consts
import constants.cdr_cleaner.clean_cdr as cdr_consts
import resources
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule
import constants.global_variables
from utils import bq

LOGGER = logging.getLogger(__name__)

RECORDS_QUERY = JINJA_ENV.from_string("""
{{query_stmt}}
FROM `{{project}}.{{dataset}}.{{table}}`
WHERE {{table_id}} NOT IN 
(SELECT {{table_id}} FROM `{{project}}.{{dataset}}.{{cdm_table}}`)
""")

GET_TABLES_QUERY = JINJA_ENV.from_string("""
SELECT DISTINCT table_name
FROM `{{project}}.{{dataset}}.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name LIKE '%{{table_type}}%'
""")

TABLE_NAME = 'table_name'

ISSUE_NUMBERS = ['DC-715', 'DC-1513', 'DC-2629']


def get_mapping_tables():
    """
    Returns list of mapping tables in fields path

    Uses json table defintion files to identify mapping tables and create
    a list of extension tables.

    :returns: a list of mapping and extension tables based on mapping
        table names
    """
    mapping_tables = resources.MAPPING_TABLES
    ext_tables = []
    for table in mapping_tables:
        table_name = table.replace(MAPPING_PREFIX, '')
        table_name = f"{table_name}{EXT_SUFFIX}"
        ext_tables.append(table_name)
    return mapping_tables + ext_tables


class CleanMappingExtTables(BaseCleaningRule):
    """
    Ensures each domain mapping table only contains records for domain tables
    that exist after the dataset has been fully cleaned.
    """

    def __init__(self,
                 project_id,
                 dataset_id,
                 sandbox_dataset_id,
                 table_namer=None):
        """
        Initialize the class with proper info.

        Set the issue numbers, description and affected datasets.  As other
        tickets may affect this SQL, append them to the list of Jira Issues.
        DO NOT REMOVE ORIGINAL JIRA ISSUE NUMBERS!
        """
        desc = ('Sandboxes the mapping table records and rows dropped '
                'when the record of the row reference has been dropped '
                'by a cleaning rule')
        super().__init__(issue_numbers=ISSUE_NUMBERS,
                         description=desc,
                         affected_datasets=[
                             cdr_consts.EHR, cdr_consts.UNIONED, cdr_consts.RDR,
                             cdr_consts.COMBINED,
                             cdr_consts.REGISTERED_TIER_DEID,
                             cdr_consts.REGISTERED_TIER_DEID_BASE,
                             cdr_consts.REGISTERED_TIER_DEID_CLEAN,
                             cdr_consts.CONTROLLED_TIER_DEID,
                             cdr_consts.CONTROLLED_TIER_DEID_BASE,
                             cdr_consts.CONTROLLED_TIER_DEID_CLEAN
                         ],
                         project_id=project_id,
                         dataset_id=dataset_id,
                         sandbox_dataset_id=sandbox_dataset_id,
                         affected_tables=get_mapping_tables(),
                         table_namer=table_namer)
        # setting default values for these variables based on table schema
        # definition files and table naming conventions.  These values will be
        # reset when setup_rule is executed.
        tables = get_mapping_tables()
        self.mapping_tables = [
            table for table in tables if table.startswith(MAPPING_PREFIX)
        ]
        self.ext_tables = [
            table for table in tables if table.endswith(EXT_SUFFIX)
        ]

    @staticmethod
    def get_cdm_table(table, table_type):
        """
        Returns the cdm_table that the mapping/ext table references

        :param table: mapping/ext table
        :param table_type: can take values 'mapping' or 'ext'
        :return: cdm_table for the mapping/ext table
        """
        if table_type == MAPPING:
            cdm_table = table.replace(MAPPING_PREFIX, '')
        else:
            cdm_table = table.replace(EXT_SUFFIX, '')

        return cdm_table

    def get_tables(self, table_type):
        """
        Retrieves mapping/ext tables in dataset

        :param table_type: can take values 'mapping' or 'ext', identifies
            tables in the dataset with the given type

        :return: list of tables in the dataset which are mapping or ext tables of cdm_tables
        """
        tables_query = GET_TABLES_QUERY.render(project=self.project_id,
                                               dataset=self.dataset_id,
                                               table_type=table_type)
        tables = bq.query(tables_query).get(TABLE_NAME).to_list()
        cdm_tables = set(resources.CDM_TABLES)
        tables = [
            table for table in tables
            if self.get_cdm_table(table, table_type) in cdm_tables
        ]
        return tables

    @staticmethod
    def is_ehr_dataset(dataset_id):
        """
        identifies ehr datasets

        :param dataset_id: identifies the dataset
        :return: Boolean identifying if the dataset is an ehr dataset
        """
        return 'ehr' in dataset_id and 'unioned' not in dataset_id

    def get_clean_queries(self, table_list, table_type):
        """
        Collect queries for sandboxing and cleaning either mapping or ext tables

        :param table_list: list of tables to create cleaning queries for
        :param table_type: can take values 'mapping' or 'ext', generates
            queries targeting the respective tables

        :return: list of query dicts
        """
        queries = []

        is_ehr = self.is_ehr_dataset(self.dataset_id)

        for table in table_list:
            cdm_table = self.get_cdm_table(table, table_type)
            table_id = f"{cdm_table}_id"

            if is_ehr:
                cdm_table = f"{UNIONED_EHR}_{cdm_table}"

            if not constants.global_variables.DISABLE_SANDBOX:
                sandbox_query = dict()
                sandbox_query[cdr_consts.QUERY] = RECORDS_QUERY.render(
                    query_stmt='SELECT *',
                    project=self.project_id,
                    dataset=self.dataset_id,
                    table=table,
                    cdm_table=cdm_table,
                    table_id=table_id)
                sandbox_query[
                    cdr_consts.DESTINATION_DATASET] = self.sandbox_dataset_id
                sandbox_query[cdr_consts.
                              DESTINATION_TABLE] = self.sandbox_table_for(table)
                sandbox_query[cdr_consts.DISPOSITION] = bq_consts.WRITE_APPEND
                queries.append(sandbox_query)

            query = dict()
            query[cdr_consts.QUERY] = RECORDS_QUERY.render(
                query_stmt='DELETE',
                project=self.project_id,
                dataset=self.dataset_id,
                table=table,
                cdm_table=cdm_table,
                table_id=table_id)
            queries.append(query)
        return queries

    def get_query_specs(self):
        """
        Collect queries for cleaning mapping and ext tables

        :return: list of query dicts
        """
        mapping_clean_queries = self.get_clean_queries(self.mapping_tables,
                                                       table_type=MAPPING)
        ext_clean_queries = self.get_clean_queries(self.ext_tables,
                                                   table_type=EXT)
        return mapping_clean_queries + ext_clean_queries

    def setup_rule(self, client):
        """
        Function to run any data upload options before executing a query.

        Should also be used to setup the class with any calls required to
        instantiate the class properly.
        """
        self.mapping_tables = self.get_tables(MAPPING)
        self.ext_tables = self.get_tables(EXT)

    def get_sandbox_tablenames(self):
        """
        Returns a list of sandbox table names. 
        """
        return [self.sandbox_table_for(table) for table in self.affected_tables]

    def setup_validation(self, client):
        """
        Run required steps for validation setup

        This abstract method was added to the base class after this rule was authored.
        This rule needs to implement logic to setup validation on cleaning rules that
        will be updating or deleting the values.
        Until done no issue exists for this yet.
        """
        raise NotImplementedError("Please fix me.")

    def validate_rule(self, client):
        """
        Validates the cleaning rule which deletes or updates the data from the tables

        This abstract method was added to the base class after this rule was authored.
        This rule needs to implement logic to run validation on cleaning rules that will
        be updating or deleting the values.
        Until done no issue exists for this yet.
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
                                                 [(CleanMappingExtTables,)])
        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(ARGS.project_id, ARGS.dataset_id,
                                   ARGS.sandbox_dataset_id,
                                   [(CleanMappingExtTables,)])
