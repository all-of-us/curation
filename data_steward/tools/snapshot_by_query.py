import argparse

import cdm
import resources
from bq_utils import create_dataset, list_all_table_ids, query, wait_on_jobs, BigQueryJobWaitError, \
    create_standard_table
from utils import bq

BIGQUERY_DATA_TYPES = {
    'integer': 'INT64',
    'float': 'FLOAT64',
    'string': 'STRING',
    'date': 'DATE',
    'timestamp': 'TIMESTAMP',
    'bool': 'BOOLEAN'
}


def create_empty_dataset(project_id, dataset_id, snapshot_dataset_id):
    """
    Create the empty tables in the new snapshot dataset
    :param project_id:
    :param dataset_id:
    :param snapshot_dataset_id:
    :return:
    """
    create_dataset(
        project_id=project_id,
        dataset_id=snapshot_dataset_id,
        description='Snapshot of {dataset_id}'.format(dataset_id=dataset_id),
        overwrite_existing=True)


def create_empty_cdm_tables(snapshot_dataset_id, hpo_id=None):
    """
    Copy the table content from the current dataset to the snapshot dataset
    :param snapshot_dataset_id:
    :param hpo_id: Identifies the hpo_id of the site table
    :return:
    """
    for table in resources.CDM_TABLES:
        table_id = resources.get_table_id(table, hpo_id)
        table_name = table
        create_standard_table(table_name,
                              table_id,
                              drop_existing=True,
                              dataset_id=snapshot_dataset_id)
    if not hpo_id:
        cdm.create_vocabulary_tables(snapshot_dataset_id)


def get_field_cast_expr(dest_field, source_fields):
    """
    generates cast expression based on data_type for the field

    :param dest_field: field dictionary object
    :param source_fields: list of field names in source table
    :return:  col string
    """

    dest_field_name = dest_field['name']
    dest_field_mode = dest_field['mode']
    dest_field_type = dest_field['type']
    if dest_field_name in source_fields:
        col = f'CAST({dest_field_name} AS {BIGQUERY_DATA_TYPES[dest_field_type.lower()]}) AS {dest_field_name}'
    else:
        if dest_field_mode == 'required':
            raise RuntimeError(
                f'Unable to load the field "{dest_field_name}" which is required in the destination table \
                and missing from the source table')
        elif dest_field_mode == 'nullable':
            col = f'CAST(NULL AS {BIGQUERY_DATA_TYPES[dest_field_type.lower()]}) AS {dest_field_name}'
        else:
            raise RuntimeError(
                f'Unable to determine the mode for "{dest_field_name}".')
    return col


def get_source_fields(client, source_table):
    """
    Gets column names of a table in bigquery

    :param client: BigQuery client
    :param source_table: fully qualified table name.

    returns as a list of column names.
    """
    return [f'{field.name}' for field in client.get_table(source_table).schema]


def get_copy_table_query(project_id, dataset_id, table_id, client):

    try:
        source_table = f'{project_id}.{dataset_id}.{table_id}'
        source_fields = get_source_fields(client, source_table)
        dst_fields = resources.fields_for(table_id)
        col_cast_exprs = [
            get_field_cast_expr(field, source_fields) for field in dst_fields
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


def copy_tables_to_new_dataset(project_id, dataset_id, snapshot_dataset_id):
    """
    lists the tables in the dataset and copies each table to a new dataset.
    :param dataset_id:
    :param project_id:
    :param snapshot_dataset_id:
    :return:
    """
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


def create_schemaed_snapshot_dataset(project_id,
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
        create_empty_dataset(project_id, dataset_id, snapshot_dataset_id)

    create_empty_cdm_tables(snapshot_dataset_id)

    copy_tables_to_new_dataset(project_id, dataset_id, snapshot_dataset_id)


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

    create_schemaed_snapshot_dataset(args.project_id, args.dataset_id,
                                     args.snapshot_dataset_id)
