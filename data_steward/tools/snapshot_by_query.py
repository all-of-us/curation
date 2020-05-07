import argparse

import cdm
import resources
from bq_utils import create_dataset, list_all_table_ids, query, wait_on_jobs, BigQueryJobWaitError, \
    create_standard_table

PERSON = 'person'
PRE_DEID = 'pre_deid'
POST_DEID = 'post_deid'


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


def create_empty_cdm_tables(snapshot_dataset_id, data_stage):
    """
    Copy the table content from the current dataset to the snapshot dataset
    :param snapshot_dataset_id:
    :return:
    """
    for table in resources.CDM_TABLES:
        if table == PERSON and data_stage == PRE_DEID:
            table_id = table
            table_name = 'post_deid_person'
        else:
            table_id = table
            table_name = table
        create_standard_table(table_name,
                              table_id,
                              drop_existing=True,
                              dataset_id=snapshot_dataset_id)
    cdm.create_vocabulary_tables(snapshot_dataset_id)


def get_field_cast_expr(field, data_type):
    """
    generates cast expression based on data_type for the field

    :param field: field name
    :param data_type: data type of the field
    :return:  col string
    """
    bigquery_int_float = {'integer': 'INT64', 'float': 'FLOAT64'}

    if data_type not in ['integer', 'float']:
        col = f'CAST({field} AS {data_type.upper()}) AS {field}'
    else:
        col = f'CAST({field} AS {bigquery_int_float[data_type]}) AS {field}'

    return col


def get_copy_table_query(project_id, dataset_id, table_id, data_stage):
    try:
        if table_id == PERSON and data_stage == POST_DEID:
            table_name = 'post_deid_person'
        else:
            table_name = table_id
        fields = resources.fields_for(table_name)
        fields_with_datatypes = [
            get_field_cast_expr(field['name'], field['type'])
            for field in fields
        ]
        col_expr = ', '.join(fields_with_datatypes)
    except (OSError, IOError):
        # default to select *
        col_expr = '*'
    select_all_query = 'SELECT {col_expr} FROM `{project_id}.{dataset_id}.{table_id}`'
    return select_all_query.format(col_expr=col_expr,
                                   project_id=project_id,
                                   dataset_id=dataset_id,
                                   table_id=table_id)


def copy_tables_to_new_dataset(project_id, dataset_id, snapshot_dataset_id,
                               data_stage):
    """
    lists the tables in the dataset and copies each table to a new dataset.
    :param dataset_id:
    :param project_id:
    :param snapshot_dataset_id:
    :return:
    """
    copy_table_job_ids = []
    for table_id in list_all_table_ids(dataset_id):
        q = get_copy_table_query(project_id, dataset_id, table_id, data_stage)
        results = query(q,
                        use_legacy_sql=False,
                        destination_table_id=table_id,
                        destination_dataset_id=snapshot_dataset_id,
                        batch=True)
        copy_table_job_ids.append(results['jobReference']['jobId'])
    incomplete_jobs = wait_on_jobs(copy_table_job_ids)
    if len(incomplete_jobs) > 0:
        raise BigQueryJobWaitError(incomplete_jobs)


def create_snapshot_dataset(project_id, dataset_id, snapshot_dataset_id,
                            data_stage):
    """
    :param project_id:
    :param dataset_id:
    :param snapshot_dataset_id:
    :return:
    """
    create_empty_dataset(project_id, dataset_id, snapshot_dataset_id)

    create_empty_cdm_tables(snapshot_dataset_id, data_stage)

    copy_tables_to_new_dataset(project_id, dataset_id, snapshot_dataset_id,
                               data_stage)


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
    parser.add_argument('-s',
                        '--data_stage',
                        action='store',
                        dest='data_stage',
                        help='Stage of the cdr generation',
                        choices=['pre_deid', 'post_deid'],
                        required=True)
    args = parser.parse_args()

    create_snapshot_dataset(args.project_id, args.dataset_id,
                            args.snapshot_dataset_id, args.data_stage)
