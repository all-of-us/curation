import argparse

from google.cloud.bigquery import QueryJobConfig

from common import PII_TABLES
import resources
from utils import bq
from tools import snapshot_by_query as sq

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
"""Modified field names from 5.2 to 5.3.1"""


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
    if dest_field_name in source_fields:
        col = f'CAST({dest_field_name} AS {sq.BIGQUERY_DATA_TYPES[dest_field_type.lower()]}) AS {dest_field_name}'
        # TODO handle possible data type difference?
    else:
        if dest_field_mode == 'nullable':
            col = f'CAST(NULL AS {sq.BIGQUERY_DATA_TYPES[dest_field_type.lower()]}) AS {dest_field_name}'
            if dest_field_name in MODIFIED_FIELD_NAMES.keys():
                old_name = MODIFIED_FIELD_NAMES[dest_field_name]["old_name"]
                if old_name in source_fields:
                    # Case when the field is one of the modified fields from 5.2 to 5.3
                    col = f'CAST({old_name} AS {sq.BIGQUERY_DATA_TYPES[dest_field_type.lower()]}) AS {dest_field_name}'
        elif dest_field_mode == 'required':
            raise RuntimeError(
                f'Unable to load the field "{dest_field_name}" which is required in the destination table \
                and missing from the source table')
        else:
            raise RuntimeError(
                f'Mode for "{dest_field_name}" is set to unexpected value "{dest_field_mode}".'
            )
    return col


def get_upgrade_table_query(client, dataset_id, table_id, hpo_id=None):
    """
    Generate query for specified tables

    :param client: BQ Client
    :param dataset_id: Source dataset
    :param table_id: Source table
    :param hpo_id: 
    :return: 
    """
    try:
        source_table = f'{client.project}.{dataset_id}.{table_id}'
        source_fields = sq.get_source_fields(client, source_table)
        dst_fields = resources.fields_for(
            resources.get_base_table_name(table_id, hpo_id))
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
                                   project_id=client.project,
                                   dataset_id=dataset_id,
                                   table_id=table_id)


def schema_upgrade_cdm52_to_cdm531(project_id,
                                   dataset_id,
                                   snapshot_dataset_id,
                                   hpo_id=None):
    """
   :param project_id:
   :param dataset_id: Dataset to convert
   :param snapshot_dataset_id: Dataset with converted tables. Overwritten if tables already exist
   :param hpo_id: Identifies the hpo_id of the site
   :return:
    """
    # Create dataset if not exists
    client = bq.get_client(project_id)
    client.create_dataset(snapshot_dataset_id, exists_ok=True)

    sq.create_empty_cdm_tables(snapshot_dataset_id, hpo_id)

    copy_table_job_ids = []
    tables = [table.table_id for table in list(client.list_tables(dataset_id))]
    if hpo_id:
        hpo_tables = [
            resources.get_table_id(table, hpo_id)
            for table in resources.CDM_TABLES + PII_TABLES
        ]
        # Filter tables that do not exist
        tables = [table for table in hpo_tables if table in tables]
    for table_id in tables:
        q = get_upgrade_table_query(client, dataset_id, table_id, hpo_id)
        job_config = QueryJobConfig()
        job_config.destination = f'{client.project}.{snapshot_dataset_id}.{table_id}'
        job_config.use_legacy_sql = False
        job = client.query(q, job_config)
        copy_table_job_ids.append(job.job_id)
        job.result()
    return copy_table_job_ids


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
                        help='Dataset where CDM conversion is to be applied',
                        required=True)
    parser.add_argument(
        '-n',
        '--snapshot_dataset_id',
        action='store',
        dest='snapshot_dataset_id',
        help='Name of the dataset to store v531 converted tables',
        required=True)
    parser.add_argument('-s',
                        '--hpo_id',
                        action='store',
                        dest='hpo_id',
                        help="Specify hpo_id if running on a site's submission",
                        required=False)
    args = parser.parse_args()

    schema_upgrade_cdm52_to_cdm531(args.project_id, args.dataset_id,
                                   args.snapshot_dataset_id, args.hpo_id)
