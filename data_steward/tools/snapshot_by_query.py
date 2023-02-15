# Python imports
import argparse
import logging

# Project imports
import cdm
import resources
from bq_utils import query, wait_on_jobs, BigQueryJobWaitError, \
    create_standard_table
from gcloud.bq import BigQueryClient
from utils.pipeline_logging import configure

LOGGER = logging.getLogger(__name__)

BIGQUERY_DATA_TYPES = {
    'integer': 'INT64',
    'float': 'FLOAT64',
    'string': 'STRING',
    'date': 'DATE',
    'timestamp': 'TIMESTAMP',
    'bool': 'BOOLEAN',
    'datetime': 'DATETIME',
    'record': 'RECORD',
    'numeric': 'NUMERIC'
}


def create_empty_dataset(client, dataset_id, snapshot_dataset_id):
    """
    Create the empty tables in the new snapshot dataset

    :param client: a BigQueryClient
    :param dataset_id: identifies the source dataset
    :param snapshot_dataset_id: identifies the new dataset
    :return:
    """
    client.delete_dataset(snapshot_dataset_id,
                          delete_contents=True,
                          not_found_ok=True)
    dataset_result = client.create_dataset(snapshot_dataset_id)
    dataset_result.description = 'Snapshot of {dataset_id}'.format(
        dataset_id=dataset_id)


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
        LOGGER.info(f'Creating table {snapshot_dataset_id}.{table}...')
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

    :param client: a BigQueryClient
    :param source_table: fully qualified table name.

    returns as a list of column names.
    """
    return [f'{field.name}' for field in client.get_table(source_table).schema]


def get_copy_table_query(client, dataset_id, table_id):
    try:
        source_table = f'{client.project}.{dataset_id}.{table_id}'
        source_fields = get_source_fields(client, source_table)
        dst_fields = resources.fields_for(table_id)
        if dst_fields:
            col_cast_exprs = [
                get_field_cast_expr(field, source_fields)
                for field in dst_fields
            ]
            col_expr = ', '.join(col_cast_exprs)
        else:
            col_expr = '*'
    except (OSError, IOError, RuntimeError):
        # default to select *
        col_expr = '*'
    select_all_query = 'SELECT {col_expr} FROM `{project_id}.{dataset_id}.{table_id}`'
    return select_all_query.format(col_expr=col_expr,
                                   project_id=client.project,
                                   dataset_id=dataset_id,
                                   table_id=table_id)


def copy_tables_to_new_dataset(client, dataset_id, snapshot_dataset_id):
    """
    lists the tables in the dataset and copies each table to a new dataset.

    :param client: a BigQueryClient
    :param dataset_id: identifies the source dataset
    :param snapshot_dataset_id: identifies the destination dataset
    :return:
    """
    copy_table_job_ids = []
    destination_tables = [
        table.table_id for table in client.list_tables(snapshot_dataset_id)
    ]

    for table in client.list_tables(dataset_id):
        LOGGER.info(
            f" Copying {dataset_id}.{table.table_id} to {snapshot_dataset_id}.{table.table_id}"
        )
        if table.table_id not in destination_tables:
            try:
                fields = resources.fields_for(table.table_id)
                client.create_tables([
                    f'{client.project}.{snapshot_dataset_id}.{table.table_id}'
                ], False, [fields])
            except RuntimeError:
                LOGGER.info(f'Unable to find schema for {table.table_id}')
        q = get_copy_table_query(client, dataset_id, table.table_id)
        results = query(q,
                        use_legacy_sql=False,
                        destination_table_id=table.table_id,
                        destination_dataset_id=snapshot_dataset_id,
                        batch=True)
        copy_table_job_ids.append(results['jobReference']['jobId'])
    incomplete_jobs = wait_on_jobs(copy_table_job_ids)
    if len(incomplete_jobs) > 0:
        raise BigQueryJobWaitError(incomplete_jobs)


def create_schemaed_snapshot_dataset(client,
                                     dataset_id,
                                     snapshot_dataset_id,
                                     overwrite_existing=True):
    """
    :param client: a BigQueryClient
    :param dataset_id: identifies the source dataset
    :param snapshot_dataset_id: identifies the destination dataset
    :param overwrite_existing: Default is True, False if a dataset is already created.
    :return:
    """
    if overwrite_existing:
        create_empty_dataset(client, dataset_id, snapshot_dataset_id)

    create_empty_cdm_tables(snapshot_dataset_id)

    copy_tables_to_new_dataset(client, dataset_id, snapshot_dataset_id)


if __name__ == '__main__':
    configure(add_console_handler=True)
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
    bq_client = BigQueryClient(args.project_id)
    create_schemaed_snapshot_dataset(bq_client, args.dataset_id,
                                     args.snapshot_dataset_id)
