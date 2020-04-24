"""
Generic clean up rule to ensure each mapping table contains only the records for
domain tables existing after the dataset has been fully cleaned.

Original Issue: DC-715

The intent is to ensure the mapping table continues to represent a true record of the
cleaned domain table by sandboxing the mapping table records and rows dropped
when the records of the row references have been dropped by a cleaning rule.
"""

import logging

import constants.bq_utils as bq_consts
import constants.cdr_cleaner.clean_cdr as cdr_consts
import resources
from utils import bq
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule

LOGGER = logging.getLogger(__name__)

SELECT_RECORDS_QUERY = """
SELECT m.*
FROM `{project}.{dataset}.{table}` m
LEFT JOIN `{project}.{dataset}.{cdm_table}` c
USING ({table_id})
WHERE c.{table_id} IS {value}
"""

GET_TABLES_QUERY = """
SELECT DISTINCT table_name
FROM `{project}.{dataset}.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name LIKE '%{table_type}%'
"""

UNIONED_PREFIX = 'unioned_ehr_'

TABLE_NAME = 'table_name'
MAPPING = 'mapping'
MAPPING_PREFIX = '_{}_'.format(MAPPING)
EXT = 'ext'
EXT_SUFFIX = '_{}'.format(EXT)

NULL = 'NULL'
NOT_NULL = 'NOT NULL'

ISSUE_NUMBER = 'DC-715'


class CleanMappingExtTables(BaseCleaningRule):
    """
    Ensures each domain mapping table only contains records for domain tables
    that exist after the dataset has been fully cleaned.
    """

    def __init__(self, project_id, dataset_id, sandbox_dataset_id):
        """
        Initialize the class with proper info.

        Set the issue numbers, description and affected datasets.  As other
        tickets may affect this SQL, append them to the list of Jira Issues.
        DO NOT REMOVE ORIGINAL JIRA ISSUE NUMBERS!
        """
        desc = (
            'Remove records from the rdr dataset where '
            'observation_source_concept_id in (43530490, 43528818, 43530333)')
        super().__init__(issue_numbers=[ISSUE_NUMBER],
                         description=desc,
                         affected_datasets=[cdr_consts.RDR],
                         project_id=project_id,
                         dataset_id=dataset_id,
                         sandbox_dataset_id=sandbox_dataset_id)

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
            return cdm_table
        cdm_table = table.replace(EXT_SUFFIX, '')
        return cdm_table

    def get_tables(self, project_id, dataset_id, table_type):
        """
        Retrieves mapping/ext tables in dataset

        :param project_id: identifies the project
        :param dataset_id: identifies the dataset
        :param table_type: can take values 'mapping' or 'ext', generates queries targeting the respective tables
        :return: list of tables in the dataset which are mapping or ext tables of cdm_tables
        """
        tables_query = GET_TABLES_QUERY.format(project=project_id,
                                               dataset=dataset_id,
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
        identifies unioned datasets

        :param dataset_id: identifies the dataset
        :return: Boolean identifying if the dataset is a unioned dataset
        """
        return 'ehr' in dataset_id and 'unioned' not in dataset_id

    def get_clean_queries(self, project_id, dataset_id, sandbox_dataset_id,
                          table_type):
        """
        Collect queries for sandboxing and cleaning either mapping or ext tables

        :param project_id: identifies the project
        :param dataset_id: identifies the dataset
        :param sandbox_dataset_id: identifies the sandbox dataset
        :param table_type: can take values 'mapping' or 'ext', generates queries targeting the respective tables

        :return: list of query dicts
        """
        queries = []

        tables = self.get_tables(project_id, dataset_id, table_type)

        # TODO modify based on new naming convention
        is_ehr = self.is_ehr_dataset(dataset_id)

        for table in tables:
            cdm_table = self.get_cdm_table(
                table, table_type
            ) if not is_ehr else UNIONED_PREFIX + self.get_cdm_table(
                table, table_type)
            table_id = cdm_table + '_id'

            sandbox_query = dict()
            sandbox_query[cdr_consts.QUERY] = SELECT_RECORDS_QUERY.format(
                project=project_id,
                dataset=dataset_id,
                table=table,
                cdm_table=cdm_table,
                table_id=table_id,
                value=NULL)
            sandbox_query[cdr_consts.DESTINATION_DATASET] = sandbox_dataset_id
            sandbox_query[cdr_consts.DESTINATION_TABLE] = table
            sandbox_query[cdr_consts.DISPOSITION] = bq_consts.WRITE_APPEND
            queries.append(sandbox_query)

            query = dict()
            query[cdr_consts.QUERY] = SELECT_RECORDS_QUERY.format(
                project=project_id,
                dataset=dataset_id,
                table=table,
                cdm_table=cdm_table,
                table_id=table_id,
                value=NOT_NULL)
            query[cdr_consts.DESTINATION_DATASET] = dataset_id
            query[cdr_consts.DESTINATION_TABLE] = table
            query[cdr_consts.DISPOSITION] = bq_consts.WRITE_TRUNCATE
            queries.append(query)
        return queries

    def get_query_specs(self):
        """
        Collect queries for cleaning mapping and ext tables

        :return: list of query dicts
        """
        mapping_clean_queries = self.get_clean_queries(
            project_id=self.get_project_id(),
            dataset_id=self.get_dataset_id(),
            sandbox_dataset_id=self.get_sandbox_dataset_id(),
            table_type=MAPPING)
        ext_clean_queries = self.get_clean_queries(
            project_id=self.get_project_id(),
            dataset_id=self.get_dataset_id(),
            sandbox_dataset_id=self.get_sandbox_dataset_id(),
            table_type=EXT)
        return mapping_clean_queries + ext_clean_queries

    def setup_rule(self):
        """
        Function to run any data upload options before executing a query.
        """
        pass

    def get_sandbox_tablenames(self):
        """
        Returns a list of sandbox table names.

        This abstract method was added to the base class after this rule was
        authored.  This rule needs to implement returning a list of sandbox
        table names.  Until done, it is raising an error.  No issue exists for
        this yet.
        """
        raise NotImplementedError("Please fix me")


if __name__ == '__main__':
    import cdr_cleaner.args_parser as parser
    import cdr_cleaner.clean_cdr_engine as clean_engine

    ARGS = parser.parse_args()
    clean_engine.add_console_logging(ARGS.console_log)
    cleaner = CleanMappingExtTables(ARGS.project_id, ARGS.dataset_id,
                                    ARGS.sandbox_dataset_id)
    query_list = cleaner.get_query_specs()
    if ARGS.list_queries:
        cleaner.log_queries()
    else:
        clean_engine.clean_dataset(ARGS.project_id, query_list)
