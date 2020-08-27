"""
Rule: 4
ID columns in each domain should be unique
"""
import logging

# Project imports
import cdm
from constants import bq_utils as bq_consts
from constants.cdr_cleaner import clean_cdr as cdr_consts
import resources

LOGGER = logging.getLogger(__name__)

ID_DE_DUP_QUERY = (
    'select {columns} '
    'from (select m.*, '
    'ROW_NUMBER() OVER (PARTITION BY m.{domain_table}_id) AS row_num '
    'from `{project_id}.{dataset_id}.{table_name}` as m) as t '
    'where row_num = 1 ')


def get_id_deduplicate_queries(project_id, dataset_id):
    """
    This function gets the queries required to remove the duplicate id columns from a dataset

    :param project_id: Project name
    :param dataset_id: Name of the dataset where a rule should be applied
    :return: a list of queries.
    """
    queries = []
    tables_with_primary_key = cdm.tables_to_map()
    for table in tables_with_primary_key:
        table_name = table
        fields = resources.fields_for(table)
        # Generate column expressions for select
        col_exprs = [field['name'] for field in fields]
        cols = ', '.join(col_exprs)
        query = dict()
        query[cdr_consts.QUERY] = ID_DE_DUP_QUERY.format(columns=cols,
                                                         project_id=project_id,
                                                         dataset_id=dataset_id,
                                                         domain_table=table,
                                                         table_name=table_name)

        query[cdr_consts.DESTINATION_TABLE] = table
        query[cdr_consts.DISPOSITION] = bq_consts.WRITE_TRUNCATE
        query[cdr_consts.DESTINATION_DATASET] = dataset_id
        queries.append(query)
    return queries


if __name__ == '__main__':
    import cdr_cleaner.args_parser as parser
    import cdr_cleaner.clean_cdr_engine as clean_engine

    ARGS = parser.parse_args()

    if ARGS.list_queries:
        clean_engine.add_console_logging()
        query_list = clean_engine.get_query_list(
            ARGS.project_id, ARGS.dataset_id, [(get_id_deduplicate_queries,)])
        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(ARGS.project_id, ARGS.dataset_id,
                                   [(get_id_deduplicate_queries,)])
