import constants.bq_utils as bq_consts
import constants.cdr_cleaner.clean_cdr as cdr_consts
import resources
from utils import bq

SELECT_RECORDS_QUERY = """
SELECT m.*
FROM `{project}.{dataset}.{table}` m
LEFT JOIN `{project}.{dataset}.{cdm_table}` c
USING ({table_id})
WHERE c.{table_id} IS NOT NULL
"""

GET_TABLES_QUERY = """
SELECT DISTINCT table_name
FROM `{project}.{dataset}.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name LIKE '%{table_type}%'
"""

TABLE_NAME = 'table_name'
MAPPING = 'mapping'
MAPPING_PREFIX = '_{}_'.format(MAPPING)
EXT = 'ext'
EXT_SUFFIX = '_{}'.format(EXT)


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


def get_tables(project_id, dataset_id, table_type):
    """

    :param project_id: identifies the project
    :param dataset_id: identifies the dataset
    :param table_type: can take values 'mapping' or 'ext', generates queries targeting the respective tables
    :return: list of tables in the dataset which are mapping or ext tables of cdm_tables
    """
    tables_query = GET_TABLES_QUERY.format(project=project_id,
                                           dataset=dataset_id)
    tables = bq.query(tables_query).get(TABLE_NAME).to_list()
    cdm_tables = set(resources.CDM_TABLES)
    tables = [
        table for table in tables
        if get_cdm_table(table, table_type) in cdm_tables
    ]
    return tables


def get_clean_queries(project_id, dataset_id, table_type):
    """
    Collect queries for cleaning either mapping or ext tables

    :param project_id: identifies the project
    :param dataset_id: identifies the dataset
    :param table_type: can take values 'mapping' or 'ext', generates queries targeting the respective tables

    :return: list of query dicts
    """
    query_list = []

    tables = get_tables(project_id, dataset_id, table_type)

    for table in tables:
        cdm_table = get_cdm_table(table, table_type)
        query = dict()
        table_id = cdm_table + '_id'
        query[cdr_consts.QUERY] = SELECT_RECORDS_QUERY.format(
            project=project_id,
            dataset=dataset_id,
            table=table,
            cdm_table=cdm_table,
            table_id=table_id)
        query[cdr_consts.DESTINATION_DATASET] = dataset_id
        query[cdr_consts.DESTINATION_TABLE] = table
        query[cdr_consts.DISPOSITION] = bq_consts.WRITE_TRUNCATE
        query_list.append(query)
    return query_list


def get_clean_mapping_queries(project_id, dataset_id):
    """
    Collect queries for cleaning mapping and ext tables

    :param project_id: identifies the project
    :param dataset_id: identifies the dataset
    :return: list of query dicts
    """
    mapping_clean_queries = get_clean_queries(project_id, dataset_id, MAPPING)
    ext_clean_queries = get_clean_queries(project_id, dataset_id, EXT)
    return mapping_clean_queries + ext_clean_queries


if __name__ == '__main__':
    import cdr_cleaner.args_parser as parser
    import cdr_cleaner.clean_cdr_engine as clean_engine

    ARGS = parser.parse_args()
    clean_engine.add_console_logging(ARGS.console_log)
    query_list = get_clean_mapping_queries(ARGS.project_id, ARGS.dataset_id)
    clean_engine.clean_dataset(ARGS.project_id, query_list)
