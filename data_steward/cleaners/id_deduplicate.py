"""
Rule: 4
ID columns in each domain should be unique
"""

# Project imports
import bq_utils
import cdm
import constants.bq_utils as bq_consts
import constants.cleaners.clean_cdr as cdr_consts
import resources

ID_DE_DUP_QUERY = (
    'select {columns} '
    'from (select m.*, '
    'ROW_NUMBER() OVER (PARTITION BY m.{domain_table}_id) AS row_num '
    'from `{project_id}.{dataset_id}.{table_name}` as m) as t '
    'where row_num = 1 '
)


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
        if bq_utils.table_exists(table, dataset_id):
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
    import args_parser as parser

    query_list = get_id_deduplicate_queries(parser.args.project_id, parser.args.dataset_id)
    parser.clean_engine.clean_dataset(parser.args.project_id, parser.args.dataset_id, query_list)
