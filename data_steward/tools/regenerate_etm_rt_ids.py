"""
Regenerate Primary Key IDs in a Registered Tier Dataset for ETM Tables.

This script regenerates primary key IDs (sitting_id) in a Registered Tier (RT) dataset
for ETM tables (delaydiscounting, emorecog, flanker, gradcpt).

1) Creates empty output tables with proper schema, clustering, etc.

2) Creates mapping tables:
   - For ETM tables: sitting_id -> new sitting_id (sequential)
   - Uses existing person mapping for person_id

3) Loads data into output tables with new IDs.
"""
import argparse
import logging
from google.cloud.bigquery import Table
import bq_utils

from common import (MAPPING_PREFIX)

from gcloud.bq import BigQueryClient
from utils import pipeline_logging

LOGGER = logging.getLogger(__name__)

ETM_TABLES = ['delaydiscounting', 'emorecog', 'flanker', 'gradcpt']


def output_table_for(table_id):
    """
    Get the name of the output table.
    """
    return table_id


def mapping_table_for(domain_table):
    """
    Get the name of the mapping table for a given domain table.
    """
    return f'{MAPPING_PREFIX}{domain_table}'


def mapping_query(table_name, input_dataset_id, project_id):
    """
    Get the query used to generate new sequential IDs for an ETM table.
    """
    return f'''
    SELECT
        '{input_dataset_id}.{table_name}' AS src_table_id,
        sitting_id AS src_sitting_id,
        ROW_NUMBER() OVER (ORDER BY sitting_id) AS sitting_id
    FROM `{project_id}.{input_dataset_id}.{table_name}`
    '''


def mapping(domain_table, input_dataset_id, mapping_dataset_id,
            mapping_project_id, project_id):
    """
    Create and load a table that assigns unique sequential ids to records
    """
    q = mapping_query(domain_table, input_dataset_id, project_id)
    mapping_table = mapping_table_for(domain_table)
    LOGGER.info(f'Query for {mapping_table} is {q}')
    bq_utils.query(q,
                   destination_dataset_id=mapping_dataset_id,
                   destination_table_id=mapping_table,
                   write_disposition='WRITE_TRUNCATE',
                   destination_project_id=mapping_project_id)


def table_query(client, table_name, input_dataset_id, mapping_project_id,
                mapping_dataset_id, project_id, pipeline_project_id,
                pipeline_dataset_id, rt_ids_view):
    """
    Returns a query to retrieve all records from an input table with new IDs.
    """
    # Fetch schema dynamically from BigQuery
    table_ref = f'{project_id}.{input_dataset_id}.{table_name}'
    table = client.get_table(table_ref)
    schema = table.schema

    col_exprs = []
    for field in schema:
        field_name = field.name

        if field_name == 'sitting_id':
            col_expr = 'm.sitting_id'
        elif field_name == 'person_id':
            col_expr = 'mp.registered_tier_id AS person_id'
        else:
            col_expr = f't.{field_name}'

        col_exprs.append(col_expr)

    cols = ',\n        '.join(col_exprs)
    mapping_table = mapping_table_for(table_name)

    return f'''
    SELECT
        {cols}
    FROM `{project_id}.{input_dataset_id}.{table_name}` t
    JOIN `{mapping_project_id}.{mapping_dataset_id}.{mapping_table}` m
        ON t.sitting_id = m.src_sitting_id
        AND m.src_table_id = '{input_dataset_id}.{table_name}'
    JOIN `{pipeline_project_id}.{pipeline_dataset_id}.{rt_ids_view}` mp
        ON t.person_id = mp.research_id
    '''


def load(client, table_name, input_dataset_id, output_dataset_id, project_id,
         pipeline_project_id, pipeline_dataset_id, rt_ids_view,
         mapping_project_id, mapping_dataset_id):
    """
    Loads a single ETM table into the output dataset with new IDs.
    """
    output_table = output_table_for(table_name)
    LOGGER.info(
        f'Loading {table_name} from {input_dataset_id} into {output_table}')

    # Check if the source table exists before proceeding
    if not client.table_exists(table_name, input_dataset_id):
        LOGGER.info(
            f'Table {table_name} not found in source dataset {input_dataset_id}. Skipping.'
        )
        return None

    q = table_query(client, table_name, input_dataset_id, mapping_project_id,
                    mapping_dataset_id, project_id, pipeline_project_id,
                    pipeline_dataset_id, rt_ids_view)
    query_result = bq_utils.query(q,
                                  destination_table_id=output_table,
                                  destination_dataset_id=output_dataset_id)
    query_job_id = query_result['jobReference']['jobId']
    bq_utils.wait_on_jobs([query_job_id])
    LOGGER.info(f'Job {query_job_id} completed for {table_name}')
    return query_result


def main(input_dataset_id, output_dataset_id, project_id, pipeline_project_id,
         pipeline_dataset_id, rt_ids_view, mapping_project_id,
         mapping_dataset_id):
    """
    Create a new CDM dataset with regenerated IDs for ETM tables
    """
    bq_client = BigQueryClient(project_id)  # for input/output datasets

    LOGGER.info('ETM RT ID regeneration started')

    # Create output and mapping datasets if they don't exist
    try:
        bq_client.get_dataset(output_dataset_id)
        LOGGER.info(f'Dataset {output_dataset_id} already exists')
    except Exception:
        LOGGER.info(f'Creating output dataset {output_dataset_id}')
        bq_client.create_dataset(output_dataset_id, exists_ok=True)

    # Use a separate client for the mapping project if it's different
    mapping_bq_client = BigQueryClient(
        mapping_project_id) if mapping_project_id != project_id else bq_client
    try:
        mapping_bq_client.get_dataset(mapping_dataset_id)
    except Exception:
        LOGGER.info(f'Creating mapping dataset {mapping_dataset_id}')
        mapping_bq_client.create_dataset(mapping_dataset_id, exists_ok=True)

    # Process ETM tables
    for table in ETM_TABLES:
        if not bq_client.table_exists(table, input_dataset_id):
            LOGGER.info(f'Table {table} not found in input dataset, skipping')
            continue

        result_table = output_table_for(table)
        LOGGER.info(f'Creating {output_dataset_id}.{result_table}...')

        # Get schema for the table
        table_ref = f'{project_id}.{input_dataset_id}.{table}'
        source_table = bq_client.get_table(table_ref)
        schema_list = source_table.schema

        # Create table with proper schema and clustering
        fq_table_name = f'{project_id}.{output_dataset_id}.{result_table}'

        clustering_fields = None
        # Add clustering for tables with person_id
        if any(field.name == 'person_id' for field in schema_list):
            clustering_fields = ['person_id']

        table_to_create = Table(fq_table_name, schema=schema_list)
        if clustering_fields:
            table_to_create.clustering_fields = clustering_fields

        bq_client.delete_table(fq_table_name, not_found_ok=True)
        bq_client.create_table(table_to_create)

        # Create mapping table
        LOGGER.info(f'Creating mapping for {table}...')
        mapping(table, input_dataset_id, mapping_dataset_id, mapping_project_id,
                project_id)

        # Load table
        LOGGER.info(f'Loading table {table}...')
        load(bq_client, table, input_dataset_id, output_dataset_id, project_id,
             pipeline_project_id, pipeline_dataset_id, rt_ids_view,
             mapping_project_id, mapping_dataset_id)

    LOGGER.info('ETM RT ID regeneration complete')


if __name__ == '__main__':
    pipeline_logging.configure(logging.INFO, add_console_handler=True)
    parser = argparse.ArgumentParser(
        description='Regenerate IDs in Registered tier dataset for ETM tables',
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--project_id',
                        dest='project_id',
                        required=True,
                        help='Project associated with the datasets')
    parser.add_argument('--input_dataset_id',
                        dest='input_dataset_id',
                        required=True,
                        help='Dataset containing source data')
    parser.add_argument('--output_dataset_id',
                        dest='output_dataset_id',
                        required=True,
                        help='Dataset where results should be stored')
    parser.add_argument(
        '--pipeline_project_id',
        dest='pipeline_project_id',
        required=True,
        help='Project associated with the pipeline tables dataset')
    parser.add_argument(
        '--pipeline_dataset_id',
        dest='pipeline_dataset_id',
        required=True,
        help=
        'Dataset containing rdr_participant_research_ids_view for person mapping'
    )
    parser.add_argument('--rt_ids_view',
                        dest='rt_ids_view',
                        required=True,
                        help='View containing person_id to rt_id mapping')
    parser.add_argument('--mapping_project_id',
                        dest='mapping_project_id',
                        required=True,
                        help='Project that the mapping dataset resides in.')
    parser.add_argument(
        '--mapping_dataset_id',
        dest='mapping_dataset_id',
        required=True,
        help='Dataset to store/lookup mapping tables. Can be a sandbox dataset.'
    )

    args = parser.parse_args()
    main(args.input_dataset_id, args.output_dataset_id, args.project_id,
         args.pipeline_project_id, args.pipeline_dataset_id, args.rt_ids_view,
         args.mapping_project_id, args.mapping_dataset_id)
