"""
Regenerate Primary Key IDs in a Registered Tier Dataset.

This script regenerates all primary key IDs in a Registered Tier (RT) dataset using
the same logic as ehr_union.py. Unlike EHR union which combines multiple HPO
tables, this script:

1) Creates empty output tables with proper schema, clustering, etc.

2) For all tables (INCLUDING person and survey_conduct, EXCEPT death) that have 
   numeric primary key columns, creates mapping tables used to assign new IDs in output.

   Special handling for person:
   - Uses existing mapping from pipeline_tables.rdr_participant_research_ids_view
   - research_id (existing person_id) -> registered_tier_id (new person_id)

   Special handling for survey_conduct:
   - survey_conduct_id is regenerated sequentially
   - survey_source_identifier is updated to match survey_conduct_id

   Special handling for aou_death:
   - aou_death_id (GUID) is preserved (not regenerated)
   - person_id foreign key is updated with new person_id from mapping

   The structure of a mapping table follows this convention:

   _mapping_<cdm_table> (
     src_table_id:       STRING, -- identifies input table whose ids are to be mapped
     src_<cdm_table>_id: INTEGER, -- original value of unique identifier in source table
     <cdm_table>_id:     INTEGER  -- new unique identifier which will be used in output
   )

   The mapping tables are loaded with queries that generate sequential IDs:

   SELECT
     'input_dataset.measurement' AS src_table_id,
     measurement_id AS src_measurement_id,
     ROW_NUMBER() OVER (ORDER BY measurement_id) AS measurement_id
   FROM `project_id.input_dataset.measurement`

3) For all tables, composes a query to fetch records from the input dataset and save
   the results in output with new IDs where applicable.

## Notes
Required Environment variables:
 * GOOGLE_APPLICATION_CREDENTIALS: path to service account key json file
 * APPLICATION_ID: GCP project ID
 * BIGQUERY_DATASET_ID: Input/output dataset_id

Usage:
  python regenerate_rt_ids.py \
    --project_id <project> \
    --input_dataset_id <input_dataset> \
    --output_dataset_id <output_dataset> \
    --pipeline_dataset_id <pipeline_tables_dataset> \
    --rt_ids_view <rdr_participant_research_ids_view>
"""
import argparse
import logging
from google.cloud.bigquery import Table
import bq_utils

from common import (AOU_DEATH, DEATH, MAPPING_PREFIX, PERSON,
                    SURVEY_CONDUCT, VISIT_DETAIL, VISIT_OCCURRENCE, CARE_SITE,
                    LOCATION, NOTE, NOTE_NLP)

from gcloud.bq import BigQueryClient
from resources import fields_for, has_primary_key, CDM_TABLES
from utils import pipeline_logging

LOGGER = logging.getLogger(__name__)

# Tables that should not have IDs regenerated
SKIP_ID_REGEN_TABLES = [DEATH]

# Extension tables that need ID updates
EXT_TABLES = [
    'person_ext', 'visit_occurrence_ext', 'condition_occurrence_ext',
    'device_exposure_ext', 'drug_exposure_ext', 'measurement_ext', 'note_ext',
    'note_nlp_ext', 'observation_ext', 'observation_period_ext',
    'procedure_occurrence_ext', 'survey_conduct_ext', 'condition_era_ext',
    'drug_era_ext'
]


def output_table_for(table_id):
    """
    Get the name of the output table. For RT regeneration, it's the same as input.

    :param table_id: name of a CDM table
    :return: name of the table in output dataset
    """
    return table_id


def mapping_table_for(domain_table):
    """
    Get the name of the mapping table for a given domain table.

    :param domain_table: one of the domain tables (e.g. 'visit_occurrence', 'condition_occurrence')
    :return: mapping table name
    """
    return f'{MAPPING_PREFIX}{domain_table}'


def mapping_query(table_name, input_dataset_id, project_id,
                  pipeline_dataset_id, rt_ids_view):
    """
    Get the query used to generate new sequential IDs for a CDM table.

    :param table_name: name of CDM table
    :param input_dataset_id: identifies the BQ dataset containing the input table
    :param project_id: identifies the GCP project containing the dataset
    :param pipeline_dataset_id: identifies the pipeline_tables dataset (for person mapping)
    :param rt_ids_view: view containing person_id to rt_id mapping
    :return: the query
    """
    # Special handling for person table - use existing mapping from pipeline_tables
    if table_name == PERSON and pipeline_dataset_id:
        return f'''
        SELECT
            '{pipeline_dataset_id}.{rt_ids_view}' AS src_table_id,
            research_id AS src_person_id,
            registered_tier_id AS person_id
        FROM `{project_id}.{pipeline_dataset_id}.{rt_ids_view}`
        '''

    # For all other tables, generate sequential IDs
    return f'''
    SELECT
        '{input_dataset_id}.{table_name}' AS src_table_id,
        {table_name}_id AS src_{table_name}_id,
        ROW_NUMBER() OVER (ORDER BY {table_name}_id) AS {table_name}_id
    FROM `{project_id}.{input_dataset_id}.{table_name}`
    '''


def mapping(domain_table,
            input_dataset_id,
            output_dataset_id,
            project_id,
            pipeline_dataset_id,
            rt_ids_view):
    """
    Create and load a table that assigns unique sequential ids to records in domain tables

    :param domain_table: name of the CDM table
    :param input_dataset_id: identifies dataset with source data
    :param output_dataset_id: identifies dataset where mapping table should be output
    :param project_id: identifies GCP project that contains the datasets
    :param pipeline_dataset_id: identifies pipeline_tables dataset (for person mapping)
    :param rt_ids_view: view containing person_id to rt_id mapping
    :return:
    """
    q = mapping_query(domain_table, input_dataset_id, project_id,
                      pipeline_dataset_id, rt_ids_view)
    mapping_table = mapping_table_for(domain_table)
    LOGGER.info(f'Query for {mapping_table} is {q}')
    bq_utils.query(q,
                   destination_dataset_id=output_dataset_id,
                   destination_table_id=mapping_table,
                   write_disposition='WRITE_TRUNCATE')


def table_query(table_name, input_dataset_id, output_dataset_id, project_id, pipeline_dataset_id, rt_ids_view):
    """
    Returns a query to retrieve all records from an input table with new IDs.

    This function constructs a query to select data from the source table and join it
    with the necessary mapping tables to replace primary and foreign keys with newly
    generated IDs. It handles special cases for tables like aou_death, survey_conduct,
    and tables without primary keys.

    :param table_name: one of the domain tables (e.g. 'visit_occurrence', 'condition_occurrence')
    :param input_dataset_id: identifies dataset containing source data
    :param output_dataset_id: identifies dataset where mapping tables are stored
    :param project_id: identifies the GCP project
    :param pipeline_dataset_id: identifies the pipeline_tables dataset (for person mapping)
    :param rt_ids_view: view containing person_id to rt_id mapping
    :return: the query
    """
    # Special handling for aou_death - keep aou_death_id but update person_id
    if table_name == AOU_DEATH:
        fields = fields_for(table_name)
        col_exprs = []
        for field in fields:
            field_name = field['name']
            if field_name == 'person_id':
                col_exprs.append('mp.person_id')
            else:
                col_exprs.append(f't.{field_name}')
        cols = ',\n        '.join(col_exprs)
        mapping_person = mapping_table_for(PERSON)
        return f'''
    SELECT {cols}
    FROM `{project_id}.{input_dataset_id}.{table_name}` t
    JOIN `{project_id}.{output_dataset_id}.{mapping_person}` mp
        ON t.person_id = mp.src_person_id
        AND mp.src_table_id = '{pipeline_dataset_id}.{rt_ids_view}'
    '''

    # Special handling for survey_conduct - update both survey_conduct_id and survey_source_identifier
    if table_name == SURVEY_CONDUCT:
        fields = fields_for(table_name)
        col_exprs = []
        for field in fields:
            field_name = field['name']
            if field_name == 'survey_conduct_id':
                col_exprs.append('m.survey_conduct_id')
            elif field_name == 'survey_source_identifier':
                # survey_source_identifier should match survey_conduct_id
                col_exprs.append(
                    'CAST(m.survey_conduct_id AS STRING) AS survey_source_identifier'
                )
            elif field_name == 'person_id':
                col_exprs.append('mp.person_id')
            else:
                col_exprs.append(f't.{field_name}')
        cols = ',\n        '.join(col_exprs)
        mapping_table = mapping_table_for(table_name)
        mapping_person = mapping_table_for(PERSON)
        return f'''
    SELECT
        {cols}
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY nm.survey_conduct_id) AS row_num
        FROM
            `{project_id}.{input_dataset_id}.{table_name}` AS nm) AS t
    JOIN
        `{project_id}.{output_dataset_id}.{mapping_table}` AS m
    ON
        t.survey_conduct_id = m.src_survey_conduct_id
    AND m.src_table_id = '{input_dataset_id}.{table_name}'
    JOIN
        `{project_id}.{output_dataset_id}.{mapping_person}` AS mp
    ON
        t.person_id = mp.src_person_id
    AND mp.src_table_id = '{pipeline_dataset_id}.{rt_ids_view}'
    WHERE
        row_num = 1
    '''

    # Tables that don't need ID remapping (death only)
    if table_name in SKIP_ID_REGEN_TABLES:
        fields = fields_for(table_name)
        col_exprs = [field['name'] for field in fields]
        cols = ',\n        '.join(col_exprs)
        return f'''
    SELECT {cols} 
    FROM `{project_id}.{input_dataset_id}.{table_name}`'''

    # Tables without primary keys - copy as-is but update person_id if present
    if not has_primary_key(table_name):
        fields = fields_for(table_name)
        col_exprs = []
        has_person_id = False

        for field in fields:
            field_name = field['name']
            if field_name == 'person_id':
                col_exprs.append('mp.person_id')
                has_person_id = True
            else:
                col_exprs.append(f't.{field_name}')

        cols = ',\n        '.join(col_exprs)

        if has_person_id:
            # Join with person mapping if table has person_id
            mapping_person = mapping_table_for(PERSON)
            return f'''
    SELECT {cols}
    FROM `{project_id}.{input_dataset_id}.{table_name}` t
    JOIN `{project_id}.{output_dataset_id}.{mapping_person}` mp
        ON t.person_id = mp.src_person_id
        AND mp.src_table_id = '{pipeline_dataset_id}.{rt_ids_view}'
    '''
        else:
            # No person_id, just copy as-is
            return f'''
    SELECT {cols}
    FROM `{project_id}.{input_dataset_id}.{table_name}`'''

    # Tables that need ID remapping
    mapping_table = mapping_table_for(table_name)
    fields = fields_for(table_name)
    id_col = f'{table_name}_id'
    col_exprs = []

    # Track which foreign keys are present
    has_visit_occurrence_id = False
    has_preceding_visit_occurrence_id = False
    has_visit_detail_id = False
    has_preceding_visit_detail_id = False
    has_visit_detail_parent_id = False
    has_care_site_id = False
    has_location_id = False
    has_note_id = False

    # Track if table has person_id
    has_person_id = False

    for field in fields:
        field_name = field['name']

        if field_name == id_col:
            # Use mapping for record ID column
            col_expr = f'm.{field_name}'
        elif field_name == 'person_id' and table_name != PERSON:
            # Update person_id foreign key with new person_id
            col_expr = f'mp.person_id'
            has_person_id = True
        elif field_name == 'visit_occurrence_id' and table_name != VISIT_OCCURRENCE:
            col_expr = f'mvo.visit_occurrence_id'
            has_visit_occurrence_id = True
        elif field_name == 'preceding_visit_occurrence_id':
            col_expr = f'pvo.visit_occurrence_id AS preceding_visit_occurrence_id'
            has_preceding_visit_occurrence_id = True
        elif field_name == 'visit_detail_id' and table_name != VISIT_DETAIL:
            col_expr = f'mvd.visit_detail_id'
            has_visit_detail_id = True
        elif field_name == 'preceding_visit_detail_id':
            col_expr = f'pvd.visit_detail_id AS preceding_visit_detail_id'
            has_preceding_visit_detail_id = True
        elif field_name == 'visit_detail_parent_id':
            col_expr = f'ppvd.visit_detail_id AS visit_detail_parent_id'
            has_visit_detail_parent_id = True
        elif field_name == 'care_site_id' and table_name != CARE_SITE:
            col_expr = f'mcs.care_site_id'
            has_care_site_id = True
        elif field_name == 'location_id' and table_name != LOCATION:
            col_expr = f'loc.location_id'
            has_location_id = True
        elif field_name == 'note_id' and table_name != NOTE:
            col_expr = f'ni.note_id'
            has_note_id = True
        elif field_name in ('snippet', 'offset') and table_name == NOTE_NLP:
            col_expr = f'CAST({field_name} AS STRING) AS {field_name}'
        else:
            col_expr = field_name

        col_exprs.append(col_expr)

    cols = ',\n        '.join(col_exprs)

    def _get_join_expression(join_table,
                             join_alias,
                             on_field,
                             src_table_alias='t'):
        """Helper to create a LEFT JOIN expression for a mapping table."""
        mapping_tbl = mapping_table_for(join_table)
        src_field = f'src_{join_table}_id'
        return f'''
        LEFT JOIN `{project_id}.{output_dataset_id}.{mapping_tbl}` {join_alias}
          ON {src_table_alias}.{on_field} = {join_alias}.{src_field}
         AND {join_alias}.src_table_id = '{input_dataset_id}.{join_table}'
        '''

    # Build JOIN expressions for foreign keys
    person_join_expr = ''
    visit_occurrence_join_expr = ''
    preceding_visit_occurrence_join_expr = ''
    visit_detail_join_expr = ''
    preceding_visit_detail_join_expr = ''
    visit_detail_parent_join_expr = ''
    location_join_expr = ''
    care_site_join_expr = ''
    note_join_expr = ''
    visit_detail_filter_expr = ''

    if has_person_id:
        mapping_person = mapping_table_for(PERSON)
        person_join_expr = f'''
        JOIN `{project_id}.{output_dataset_id}.{mapping_person}` mp
            ON t.person_id = mp.src_person_id
            AND mp.src_table_id = '{pipeline_dataset_id}.{rt_ids_view}'
        '''

    if has_visit_occurrence_id:
        visit_occurrence_join_expr = _get_join_expression(
            VISIT_OCCURRENCE, 'mvo', 'visit_occurrence_id')

    if has_preceding_visit_occurrence_id:
        preceding_visit_occurrence_join_expr = _get_join_expression(
            VISIT_OCCURRENCE, 'pvo', 'preceding_visit_occurrence_id')

    if has_visit_detail_id:
        visit_detail_join_expr = _get_join_expression(VISIT_DETAIL, 'mvd',
                                                      'visit_detail_id')

    if has_preceding_visit_detail_id:
        preceding_visit_detail_join_expr = _get_join_expression(
            VISIT_DETAIL, 'pvd', 'preceding_visit_detail_id')

    if has_visit_detail_parent_id:
        visit_detail_parent_join_expr = _get_join_expression(
            VISIT_DETAIL, 'ppvd', 'visit_detail_parent_id')

    if has_care_site_id:
        care_site_join_expr = _get_join_expression(CARE_SITE, 'mcs', 'care_site_id')

    if has_location_id:
        location_join_expr = _get_join_expression(LOCATION, 'loc', 'location_id')

    if has_note_id:
        note_join_expr = _get_join_expression(NOTE, 'ni', 'note_id')

    if table_name == PERSON:
        return f'''
        SELECT
            {cols}
        FROM (
            SELECT
                *,
                ROW_NUMBER() OVER (PARTITION BY nm.person_id) AS row_num
            FROM
                `{project_id}.{input_dataset_id}.{table_name}` AS nm) AS t
        JOIN
            `{project_id}.{output_dataset_id}.{mapping_table}` AS m
        ON
            t.person_id = m.src_person_id
        AND m.src_table_id = '{input_dataset_id}.{table_name}'
        {location_join_expr}
        {care_site_join_expr}
        WHERE
            row_num = 1
        '''

    if table_name == VISIT_DETAIL:
        visit_detail_filter_expr = f'''
        AND mvo.visit_occurrence_id IS NOT NULL
        '''

    # For tables with primary keys that need remapping
    return f'''
    SELECT
        {cols}
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY nm.{table_name}_id) AS row_num
        FROM
            `{project_id}.{input_dataset_id}.{table_name}` AS nm) AS t
    JOIN
        `{project_id}.{output_dataset_id}.{mapping_table}` AS m
    ON
        t.{table_name}_id = m.src_{table_name}_id
    AND m.src_table_id = '{input_dataset_id}.{table_name}'
    {person_join_expr}
    {visit_occurrence_join_expr}
    {visit_detail_join_expr}
    {care_site_join_expr}
    {preceding_visit_occurrence_join_expr}
    {preceding_visit_detail_join_expr}
    {visit_detail_parent_join_expr}
    {location_join_expr}
    {note_join_expr}
    WHERE
        row_num = 1
    {visit_detail_filter_expr}
    '''


def load(client, cdm_table, input_dataset_id, output_dataset_id, project_id, pipeline_dataset_id):
    """
    Loads a single domain table into the output dataset with new IDs.

    :param client: BigQueryClient
    :param cdm_table: name of the CDM table (e.g. 'person', 'visit_occurrence')
    :param input_dataset_id: identifies dataset containing input data
    :param output_dataset_id: identifies dataset where result should be output
    :param project_id: identifies the GCP project
    :return:
    """
    output_table = output_table_for(cdm_table)
    LOGGER.info(
        f'Loading {cdm_table} from {input_dataset_id} into {output_table}')

    # Check if the source table exists before proceeding
    try:
        client.get_table(f'{project_id}.{input_dataset_id}.{cdm_table}')
    except Exception:
        LOGGER.info(
            f'Table {cdm_table} not found in source dataset {input_dataset_id}. Skipping.'
        )
        return None

    q = table_query(cdm_table, input_dataset_id, output_dataset_id, project_id, pipeline_dataset_id)
    query_result = bq_utils.query(q,
                                  destination_table_id=output_table,
                                  destination_dataset_id=output_dataset_id)
    query_job_id = query_result['jobReference']['jobId']
    bq_utils.wait_on_jobs([query_job_id])
    LOGGER.info(f'Job {query_job_id} completed for {cdm_table}')
    return query_result


def update_ext_table(ext_table_name, input_dataset_id, output_dataset_id,
                     project_id):
    """
    Update an extension table with new IDs from its corresponding mapping table.

    :param ext_table_name: name of the extension table (e.g. 'person_ext')
    :param input_dataset_id: identifies dataset containing source data
    :param output_dataset_id: identifies dataset where result should be output
    :param project_id: identifies the GCP project
    :return: query result
    """
    # Extract base table name (e.g. 'person' from 'person_ext')
    base_table = ext_table_name.replace('_ext', '')
    id_field = f'{base_table}_id'
    mapping_table = mapping_table_for(base_table)

    # Check if ext table exists
    try:
        client = BigQueryClient(project_id)  # Re-instantiate to be safe
        client.get_table(f'{input_dataset_id}.{ext_table_name}')
    except Exception:
        LOGGER.info(
            f'Extension table {ext_table_name} does not exist, skipping')
        return None

    LOGGER.info(f'Updating extension table {ext_table_name}')

    q = f'''
    SELECT
        m.{id_field},
        e.* EXCEPT({id_field})
    FROM `{project_id}.{input_dataset_id}.{ext_table_name}` e
    JOIN `{project_id}.{output_dataset_id}.{mapping_table}` m
        ON e.{id_field} = m.src_{id_field}
        AND m.src_table_id = '{input_dataset_id}.{base_table}'
    '''

    return bq_utils.query(q,
                          destination_table_id=ext_table_name,
                          destination_dataset_id=output_dataset_id,
                          write_disposition='WRITE_TRUNCATE')


def main(input_dataset_id, output_dataset_id, project_id,
         pipeline_dataset_id, rt_ids_view):
    """
    Create a new CDM dataset with regenerated IDs

    :param input_dataset_id: identifies the source dataset
    :param output_dataset_id: identifies the dataset to store the new CDM in
    :param project_id: project containing the datasets
    :param pipeline_dataset_id: dataset containing rdr_participant_research_ids_view for person mapping
    :param rt_ids_view: view containing person_id to rt_id mapping
    :returns: list of tables generated successfully
    """
    bq_client = BigQueryClient(project_id)

    LOGGER.info('RT ID regeneration started')

    # Create output dataset if it doesn't exist
    try:
        bq_client.get_dataset(output_dataset_id)
        LOGGER.info(f'Dataset {output_dataset_id} already exists')
    except Exception:
        LOGGER.info(f'Creating dataset {output_dataset_id}')
        bq_client.create_dataset(output_dataset_id, exists_ok=True)

    # Create empty output tables to ensure proper schema, clustering, etc.
    for table in CDM_TABLES:
        result_table = output_table_for(table)
        LOGGER.info(f'Creating {output_dataset_id}.{result_table}...')

        # Get schema for the table
        schema_list = bq_client.get_table_schema(table)

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

    # Create mapping tables for tables that need ID regeneration
    # NOTE: Now includes PERSON and SURVEY_CONDUCT tables
    for domain_table in CDM_TABLES:
        if domain_table in SKIP_ID_REGEN_TABLES:
            continue
        if not has_primary_key(domain_table):
            continue

        LOGGER.info(f'Creating mapping for {domain_table}...')
        mapping(domain_table, input_dataset_id, output_dataset_id, project_id,
                pipeline_dataset_id, rt_ids_view)

    # Load all tables with new IDs
    for table_name in CDM_TABLES:
        if table_name == DEATH:
            LOGGER.info(f'Skipping {table_name} load.')
            continue
        LOGGER.info(f'Loading table {table_name}...')
        load(bq_client, table_name, input_dataset_id, output_dataset_id,
             project_id, pipeline_dataset_id)

    # Update extension tables with new IDs
    LOGGER.info('Updating extension tables...')
    for ext_table in EXT_TABLES:
        update_ext_table(ext_table, input_dataset_id, output_dataset_id,
                         project_id)

    LOGGER.info('RT ID regeneration complete')


if __name__ == '__main__':
    pipeline_logging.configure(logging.INFO, add_console_handler=True)
    parser = argparse.ArgumentParser(
        description='Regenerate IDs in Registered tier dataset',
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
    parser.add_argument('--pipeline_dataset_id',
                        dest='pipeline_dataset_id',
                        required=True,
                        help='Dataset containing rdr_participant_research_ids_view for person mapping')
    parser.add_argument('--rt_ids_view',
                        dest='rt_ids_view',
                        required=True,
                        help='View containing person_id to rt_id mapping')

    args = parser.parse_args()
    main(args.input_dataset_id, args.output_dataset_id, args.project_id,
         args.pipeline_dataset_id, args.rt_ids_view)
