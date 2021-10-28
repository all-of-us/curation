import argparse

import resources
from bq_utils import list_all_table_ids, query, wait_on_jobs, BigQueryJobWaitError
from utils import bq
from tools import snapshot_by_query

MODIFIED_FIELD_NAMES = {
    # Modified field names from 5.2 to 5.3.1
    'modifier_source_value': {
        'old_name': 'qualifier_source_value',
        'new_name': 'modifier_source_value'
    },
    'npi': {
        'old_name': 'NPI',
        'new_name': 'npi'
    },
    'dea': {
        'old_name': 'DEA',
        'new_name': 'dea'
    },
    'revenue_code_source_value': {
        'old_name': 'reveue_code_source_value',
        'new_name': 'revenue_code_source_value'
    }
}


def get_field_cast_expr_with_schema_change(dest_field, source_fields):
    """
    generates cast expression based on data_type for the field and modified column names

    :param dest_field: field dictionary object
    :param source_fields: list of field names in source table
    :return:  col string
    """

    dest_field_name = dest_field['name']
    dest_field_mode = dest_field['mode']
    dest_field_type = dest_field['type']
    if dest_field_name not in source_fields:
        if dest_field_name in MODIFIED_FIELD_NAMES.keys():
            # Case when the field is one of the modified fields from 5.2 to 5.3
            col = f'CAST({MODIFIED_FIELD_NAMES[dest_field_name]["old_name"]} AS {snapshot_by_query.BIGQUERY_DATA_TYPES[dest_field_type.lower()]}) AS {dest_field_name}'
        elif dest_field_mode == 'required':
            raise RuntimeError(
                f'Unable to load the field "{dest_field_name}" which is required in the destination table \
                and missing from the source table')
        elif dest_field_mode == 'nullable':
            col = f'CAST(NULL AS {snapshot_by_query.BIGQUERY_DATA_TYPES[dest_field_type.lower()]}) AS {dest_field_name}'
        else:
            raise RuntimeError(
                f'Unable to determine the mode for "{dest_field_name}".')
    else:
        col = f'CAST({dest_field_name} AS {snapshot_by_query.BIGQUERY_DATA_TYPES[dest_field_type.lower()]}) AS {dest_field_name}'
    return col


def get_copy_table_query(project_id, dataset_id, table_id, client):

    try:
        source_table = f'{project_id}.{dataset_id}.{table_id}'
        source_fields = snapshot_by_query.get_source_fields(
            client, source_table)
        dst_fields = resources.fields_for(table_id)
        col_cast_exprs = [
            get_field_cast_expr_with_schema_change(field, source_fields)
            for field in dst_fields
        ]
        col_expr = ', '.join(col_cast_exprs)
    except (OSError, IOError, RuntimeError):
        # default to select *
        col_expr = '*'
    select_all_query = 'SELECT {col_expr} FROM `{project_id}.{dataset_id}.{table_id}`'
    return select_all_query.format(col_expr=col_expr,
                                   project_id=project_id,
                                   dataset_id=dataset_id,
                                   table_id=table_id)


def schema_upgrade_cdm52_to_cdm531(project_id,
                                   dataset_id,
                                   snapshot_dataset_id,
                                   overwrite_existing=True):
    """
       :param project_id:
       :param dataset_id:
       :param snapshot_dataset_id:
       :param overwrite_existing: Default is True, False if a dataset is already created.
       :return:
       """
    if overwrite_existing:
        snapshot_by_query.create_empty_dataset(project_id, dataset_id,
                                               snapshot_dataset_id)

    snapshot_by_query.create_empty_cdm_tables(snapshot_dataset_id)

    copy_table_job_ids = []
    client = bq.get_client(project_id)
    for table_id in list_all_table_ids(dataset_id):
        q = get_copy_table_query(project_id, dataset_id, table_id, client)
        results = query(q,
                        use_legacy_sql=False,
                        destination_table_id=table_id,
                        destination_dataset_id=snapshot_dataset_id,
                        batch=True)
        copy_table_job_ids.append(results['jobReference']['jobId'])
    incomplete_jobs = wait_on_jobs(copy_table_job_ids)
    if len(incomplete_jobs) > 0:
        raise BigQueryJobWaitError(incomplete_jobs)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Parse project_id and dataset_id',
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        '-p',
        '--project_id',
        action='store',
        dest='project_id',
        help='Project associated with the input and output datasets',
        required=True)
    parser.add_argument('-d',
                        '--dataset_id',
                        action='store',
                        dest='dataset_id',
                        help='Dataset where cleaning rules are to be applied',
                        required=True)
    parser.add_argument('-n',
                        '--snapshot_dataset_id',
                        action='store',
                        dest='snapshot_dataset_id',
                        help='Name of the new dataset that needs to be created',
                        required=True)
    args = parser.parse_args()

    schema_upgrade_cdm52_to_cdm531(args.project_id, args.dataset_id,
                                   args.snapshot_dataset_id)
