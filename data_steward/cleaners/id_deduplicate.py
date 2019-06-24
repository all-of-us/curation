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
        if 'unioned' in dataset_id:
            table_name = 'unioned_ehr_{table}'.format(table=table)
        else:
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
    import argparse
    import clean_cdr_engine

    parser = argparse.ArgumentParser(
        description='Parse project_id and dataset_id',
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('-p', '--project_id',
                        action='store', dest='project_id',
                        help='Project associated with the input and output datasets')
    parser.add_argument('-d', '--dataset_id',
                        action='store', dest='dataset_id',
                        help='Dataset where cleaning rules are to be applied')
    args = parser.parse_args()
    if args.dataset_id:
        query_list = get_id_deduplicate_queries(args.project_id, args.dataset_id)
        clean_cdr_engine.clean_dataset(args.project_id, args.dataset_id, query_list)
