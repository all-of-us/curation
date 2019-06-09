"""
Rule: 4
ID columns in each domain should be unique
"""

import bq_utils
import cdm
import clean_cdr_engine
import resources
import argparse

rule_4_query = """
select {columns}
from (select m.*,
ROW_NUMBER() OVER (PARTITION BY m.{domain_table}_id) AS row_num
from `{project_id}.{dataset_id}.{table_name}` as m) as t
where row_num = 1
"""


def run_clean_rule_4(project_id, dataset_id):
    tables_with_primary_key = cdm.tables_to_map()
    for table in tables_with_primary_key:
        if 'unioned' in dataset_id:
            table_name = 'unioned_ehr_{table}'.format(table=table)
        else:
            table_name = table
        if bq_utils.table_exists(table_name, dataset_id):
            fields = resources.fields_for(table)
            # Generate column expressions for select
            col_exprs = [field['name'] for field in fields]
            cols = ',\n        '.join(col_exprs)
            query = rule_4_query.format(columns=cols,
                                        project_id=project_id,
                                        dataset_id=dataset_id,
                                        domain_table=table,
                                        table_name=table_name)
            clean_cdr_engine.clean_dataset_2(project_id, dataset_id, query)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Parse project_id and dataset_id',
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('project_id',
                        help='Project associated with the input and output datasets')
    parser.add_argument('dataset_id',
                        help='Dataset where cleaning rules are to be applied')
    args = parser.parse_args()
    if args.dataset_id:
        run_clean_rule_4(args.project_id, args.dataset_id)
