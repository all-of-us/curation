"""
Create a new CDM dataset which is the union of all EHR datasets submitted by HPOs
NOTE We DO NOT create unioned_ehr_death in this script. We create unioned_ehr_aou_death instead.
     We use aou_death here, not death, because we want to keep the src info and assign unique 
     keys to the death records. 

 1) Create empty output tables to ensure proper schema, clustering, etc.

 2) For all tables -EXCEPT person, death, and aou_death- that have numeric primary key columns, 
    create mapping tables used to assign new IDs in output.

    The structure of the mapping table follows this convention:

    _mapping_<cdm_table> (         -- table name is derived from the CDM table it maps
      src_table_id:       STRING,  -- identifies input table whose ids are to be mapped
      src_<cdm_table>_id: INTEGER, -- original value of unique identifier in source table
      <cdm_table>_id:     INTEGER, -- new unique identifier which will be used in output
      hpo_id:             STRING   -- identifier of the hpo site
    )

    For example, this table is eventually used to load the output visit_occurrence table:

    _mapping_measurement_id (
      src_table_id:       STRING,
      src_measurement_id: INTEGER,
      measurement_id:     INTEGER,
      hpo_id:             STRING
    )

    The table _mapping_measurement_id is loaded with a query which looks like this:

    WITH all_measurement AS (
      (SELECT
        'chs_measurement'  AS src_table_id,
        measurement_id     AS src_measurement_id
       FROM `project_id.dataset_id.chs_measurement`)

      UNION ALL

      (SELECT
        'pitt_measurement' AS src_table_id,
        measurement_id     AS src_measurement_id
       FROM `project_id.dataset_id.pitt_measurement`)

      -- ...  <subqueries for each hpo_id>

      UNION ALL

      (SELECT
        'nyc_measurement' AS src_table_id,
        measurement_id    AS src_measurement_id
       FROM `project_id.dataset_id.nyc_measurement`)
    )

    SELECT
      src_table_id,
      src_measurement_id,
      ROW_NUMBER() OVER () AS measurement_id
      SUBSTR(src_table_id, 1, STRPOS(src_table_id, "_measurement")-1) AS src_hpo_id
    FROM all_measurement

 3) For all tables -EXCEPT death and aou_death-, compose a query to fetch the union of 
    records submitted by all HPOs and save the results in output.
   * Use new primary keys in output where applicable
   * Use new visit_occurrence_id where applicable

 4) Create and load aou_death. death is not created in this process.

## Notes
Currently the following environment variables must be set:
 * GOOGLE_APPLICATION_CREDENTIALS: path to service account key json file (e.g. /path/to/all-of-us-ehr-dev-abc123.json)
 * APPLICATION_ID: GCP project ID (e.g. all-of-us-ehr-dev)
 * BIGQUERY_DATASET_ID: input dataset where submissions are stored
 * UNIONED_DATASET_ID: output dataset where unioned results should be stored

TODO
 * Besides `visit_occurrence` also handle mapping of other foreign key fields (e.g. `location_id`)
 
TODO: Refactor query generation logic in mapping_query and table_union_query 
      so that both use either
      A) a single jinja template or 
      B) dynamic SQL (i.e. EXECUTE IMMEDIATE)
"""
# Python imports
import argparse
import logging
from datetime import datetime

# Third party imports
import google.cloud.bigquery as bq

# Project imports
import app_identity
import bq_utils
import cdm
import cdr_cleaner.clean_cdr_engine as clean_engine
from cdr_cleaner.cleaning_rules.drop_race_ethnicity_gender_observation import DropRaceEthnicityGenderObservation
from common import (AOU_DEATH, CARE_SITE, DEATH, FACT_RELATIONSHIP,
                    ID_CONSTANT_FACTOR, JINJA_ENV, LOCATION, MAPPING_PREFIX,
                    MEASUREMENT_DOMAIN_CONCEPT_ID, OBSERVATION, PERSON,
                    PERSON_DOMAIN_CONCEPT_ID, SURVEY_CONDUCT, UNIONED_EHR,
                    VISIT_DETAIL, VISIT_OCCURRENCE, BIGQUERY_DATASET_ID)
from constants.validation import ehr_union as eu_constants
from utils import pipeline_logging
from gcloud.bq import BigQueryClient
from resources import (fields_for, get_table_id, has_primary_key,
                       validate_date_string, CDM_TABLES)

UNION_ALL = '''

        UNION ALL

'''

LOAD_AOU_DEATH = JINJA_ENV.from_string("""
INSERT INTO `{{project}}.{{output_dataset}}.{{aou_death}}`
(
    aou_death_id,
    person_id,
    death_date,
    death_datetime,
    death_type_concept_id,
    cause_concept_id,
    cause_source_value,
    cause_source_concept_id,
    src_id,
    primary_death_record
)
WITH union_aou_death AS (
    {% for hpo_id in hpo_ids %}
    SELECT
        person_id,
        death_date,
        death_datetime,
        death_type_concept_id,
        cause_concept_id,
        cause_source_value,
        cause_source_concept_id,
        '{{hpo_id}}' AS src_id
    FROM `{{project}}.{{input_dataset}}.{{hpo_id}}_{{death}}`
    {% if not loop.last -%} UNION ALL {% endif %}
    {% endfor %}
)
SELECT
    GENERATE_UUID() AS aou_death_id, -- NOTE this is STR, not INT --
    person_id,
    death_date,
    death_datetime,
    death_type_concept_id,
    cause_concept_id,
    cause_source_value,
    cause_source_concept_id,
    src_id,
    FALSE AS primary_death_record -- this value is re-calculated at CalculatePrimaryDeathRecord --
FROM union_aou_death
""")


def get_hpo_offsets(hpo_ids):
    """
    For each HPO, get a numeric constant to add to record ids

    :param hpo_ids: list of HPO identifiers
    :return: a dictionary mapping hpo_id => numeric offset
    """
    result = dict()
    i = eu_constants.EHR_ID_MULTIPLIER_START
    for hpo_id in hpo_ids:
        result[hpo_id] = i * ID_CONSTANT_FACTOR
        i += 1
    return result


def output_table_for(table_id):
    """
    Get the name of the table where results of the union will be stored

    :param table_id: name of a CDM table
    :return: name of the table where results of the union will be stored
    """
    return f'{UNIONED_EHR}_{table_id}'


def _mapping_subqueries(client, table_name, hpo_ids, dataset_id, project_id):
    """
    Get list of subqueries (one for each HPO table found in the source) that comprise the ID mapping query

    :param client: a BigQueryClient
    :param table_name: name of a CDM table whose ID field must be remapped
    :param hpo_ids: list of HPOs to process
    :param dataset_id: identifies the source dataset
    :param project_id: identifies the GCP project
    :return: list of subqueries
    """
    # Until dynamic queries are refactored to use either a single template or dynamic SQL,
    # defining template locally (rather than top of module) so it is closer to code
    # that references it below
    hpo_subquery_tpl = JINJA_ENV.from_string('''
    (SELECT '{{table_id}}' AS src_table_id,
      {{table_name}}_id AS src_{{table_name}}_id,
      -- offset is added to the destination key only if add_hpo_offset == True --
      {{table_name}}_id 
        {%- if add_hpo_offset %} + {{hpo_offset}} {%- endif %} AS {{table_name}}_id
      FROM `{{project_id}}.{{dataset_id}}.{{table_id}}`)
    ''')
    result = []
    hpo_unique_identifiers = get_hpo_offsets(hpo_ids)

    # Exclude subqueries that reference tables that are missing from source dataset
    all_table_ids = [table.table_id for table in client.list_tables(dataset_id)]
    for hpo_id in hpo_ids:
        table_id = get_table_id(table_name, hpo_id=hpo_id)
        hpo_offset = hpo_unique_identifiers[hpo_id]
        if table_id in all_table_ids:
            add_hpo_offset = table_name != PERSON
            subquery = hpo_subquery_tpl.render(table_id=table_id,
                                               table_name=table_name,
                                               add_hpo_offset=add_hpo_offset,
                                               hpo_offset=hpo_offset,
                                               project_id=project_id,
                                               dataset_id=dataset_id)
            result.append(subquery)
        else:
            logging.info(
                f'Excluding table {table_id} from mapping query because it does not exist'
            )
    return result


def mapping_query(client,
                  table_name,
                  hpo_ids,
                  dataset_id=None,
                  project_id=None):
    """
    Get query used to generate new ids for a CDM table

    :param client: a BigQueryClient
    :param table_name: name of CDM table
    :param hpo_ids: identifies the HPOs
    :param dataset_id: identifies the BQ dataset containing the input table
    :param project_id: identifies the GCP project containing the dataset
    :return: the query
    """
    if dataset_id is None:
        dataset_id = BIGQUERY_DATASET_ID
    if project_id is None:
        project_id = app_identity.get_application_id()
    subqueries = _mapping_subqueries(client, table_name, hpo_ids, dataset_id,
                                     project_id)
    union_all_query = UNION_ALL.join(subqueries)
    return f'''
    WITH all_{table_name} AS (
    {union_all_query}
    )
    SELECT DISTINCT
        src_table_id,
        src_{table_name}_id,
        {table_name}_id,
        SUBSTR(src_table_id, 1, STRPOS(src_table_id, "_{table_name}")-1) AS src_hpo_id,
        '{dataset_id}' as src_dataset_id
    FROM all_{table_name}
    '''


def mapping_table_for(domain_table):
    """
    Get name of mapping table generated for a domain table

    :param domain_table: one of the domain tables (e.g. 'visit_occurrence', 'condition_occurrence')
    :return:
    """
    return f'{MAPPING_PREFIX}{domain_table}'


def mapping(domain_table, hpo_ids, input_dataset_id, output_dataset_id,
            project_id, client):
    """
    Create and load a table that assigns unique ids to records in domain tables
    Note: Overwrites destination table if it already exists

    :param domain_table:
    :param hpo_ids: identifies which HPOs' data to include in union
    :param input_dataset_id: identifies dataset with multiple CDMs, each from an HPO submission
    :param output_dataset_id: identifies dataset where mapping table should be output
    :param project_id: identifies GCP project that contain the datasets
    :param client: a BigQueryClient
    :return:
    """
    q = mapping_query(client, domain_table, hpo_ids, input_dataset_id,
                      project_id)
    mapping_table = mapping_table_for(domain_table)
    logging.info(f'Query for {mapping_table} is {q}')
    fq_mapping_table = f'{project_id}.{output_dataset_id}.{mapping_table}'
    schema = fields_for(mapping_table)
    table = bq.Table(fq_mapping_table, schema=schema)
    table = client.create_table(table, exists_ok=True)
    query(q, mapping_table, output_dataset_id, 'WRITE_TRUNCATE')


def query(q, dst_table_id, dst_dataset_id, write_disposition='WRITE_APPEND'):
    """
    Run query and save results to a table

    :param q: SQL statement
    :param dst_table_id: save results in a table with the specified id
    :param dst_dataset_id: identifies output dataset
    :param write_disposition: WRITE_TRUNCATE, WRITE_EMPTY, or WRITE_APPEND (default, to preserve schema)
    :return: query result
    """
    query_job_result = bq_utils.query(q,
                                      destination_table_id=dst_table_id,
                                      destination_dataset_id=dst_dataset_id,
                                      write_disposition=write_disposition)
    query_job_id = query_job_result['jobReference']['jobId']
    logging.info(f'Job {query_job_id} started for table {dst_table_id}')
    job_status = query_job_result['status']
    error_result = job_status.get('errorResult')
    if error_result is not None:
        msg = f'Job {query_job_id} failed because: {error_result}'
        raise bq_utils.InvalidOperationError(msg)
    incomplete_jobs = bq_utils.wait_on_jobs([query_job_id])
    if len(incomplete_jobs) > 0:
        raise bq_utils.BigQueryJobWaitError(incomplete_jobs)
    return query_job_result


def fact_relationship_hpo_subquery(hpo_id, input_dataset_id, output_dataset_id):
    """
    Get query for all fact_relationship records with mapped fact_id

    :param hpo_id: identifies the HPO
    :param input_dataset_id: identifies dataset containing HPO submission
    :param output_dataset_id: identifies dataset where output is saved
    :return: the query
    """
    table_id = get_table_id(FACT_RELATIONSHIP, hpo_id=hpo_id)
    fact_query = f'''SELECT F.domain_concept_id_1,
        CASE
            WHEN F.domain_concept_id_1= {MEASUREMENT_DOMAIN_CONCEPT_ID} THEN M1.measurement_id
            WHEN F.domain_concept_id_1= {PERSON_DOMAIN_CONCEPT_ID} THEN fact_id_1
            ELSE 0
        END AS fact_id_1,
        F.domain_concept_id_2,
        CASE
            WHEN F.domain_concept_id_2= {MEASUREMENT_DOMAIN_CONCEPT_ID} THEN M2.measurement_id
            WHEN F.domain_concept_id_2= {PERSON_DOMAIN_CONCEPT_ID} THEN fact_id_2
            ELSE 0
        END AS fact_id_2,
        relationship_concept_id
        FROM
        `{input_dataset_id}.{table_id}` AS F
        LEFT JOIN
            `{output_dataset_id}._mapping_measurement` AS M1
        ON
            M1.src_measurement_id = F.fact_id_1
            AND (F.domain_concept_id_1 = {MEASUREMENT_DOMAIN_CONCEPT_ID}) AND (M1.src_hpo_id = '{hpo_id}')
        LEFT JOIN
            `{output_dataset_id}._mapping_measurement` AS M2
        ON
            M2.src_measurement_id = F.fact_id_2
            AND (F.domain_concept_id_2 = {MEASUREMENT_DOMAIN_CONCEPT_ID}) AND (M2.src_hpo_id = '{hpo_id}')'''
    return fact_query


def table_hpo_subquery(table_name, hpo_id, input_dataset_id, output_dataset_id):
    """
    Returns query used to retrieve all records in a submitted table

    :param table_name: one of the domain tables (e.g. 'visit_occurrence', 'condition_occurrence')
    :param hpo_id: identifies the HPO
    :param input_dataset_id: identifies dataset containing HPO submission
    :param output_dataset_id:
    :return:
    """
    tables_to_ref = []
    for table in CDM_TABLES:
        if has_primary_key(table):
            tables_to_ref.append(table)

    is_id_mapped = table_name in tables_to_ref
    fields = fields_for(table_name)
    table_id = get_table_id(table_name, hpo_id=hpo_id)

    # Generate column expressions for select
    if not is_id_mapped:
        # e.g. death
        col_exprs = [field['name'] for field in fields]
        cols = ',\n        '.join(col_exprs)
        return f'''
    SELECT {cols} 
    FROM `{input_dataset_id}.{table_id}`'''
    else:
        # Ensure that we
        #  1) populate primary key from the mapping table and
        #  2) populate any foreign key fields from the mapping visit table
        # NOTE: Assumes that besides person_id foreign keys exist only for visit_occurrence, location, care_site, visit_detail
        # NOTE: visit_occurrence and visit_detail have self-reference foreign keys (DC-2398)
        mapping_table = mapping_table_for(table_name) if is_id_mapped else None
        has_visit_occurrence_id = False
        has_preceding_visit_occurrence_id = False
        has_visit_detail_id = False
        has_preceding_visit_detail_id = False
        has_visit_detail_parent_id = False
        has_care_site_id = False
        has_location_id = False
        id_col = f'{table_name}_id'
        col_exprs = []

        for field in fields:
            field_name = field['name']
            if field_name == id_col:
                # Use mapping for record ID column
                # m is an alias that should resolve to the associated mapping table
                if field_name == eu_constants.PERSON_ID:
                    col_expr = f'{field_name}'
                else:
                    col_expr = f'm.{field_name}'
            elif field_name == eu_constants.VISIT_OCCURRENCE_ID:
                # Replace with mapped visit_occurrence_id
                # mvo is an alias that should resolve to the mapping visit_occurrence table
                # Note: This is only reached when table_name != visit_occurrence
                col_expr = f'mvo.{eu_constants.VISIT_OCCURRENCE_ID}'
                has_visit_occurrence_id = True
            elif field_name == eu_constants.PRECEDING_VISIT_OCCURRENCE_ID:
                # Replace with mapped visit_occurrence_id
                # pvo is an alias that should resolve to the mapping visit_occurrence table
                col_expr = f'pvo.{eu_constants.VISIT_OCCURRENCE_ID} {eu_constants.PRECEDING_VISIT_OCCURRENCE_ID}'
                has_preceding_visit_occurrence_id = True
            elif field_name == eu_constants.VISIT_DETAIL_ID:
                # Replace with mapped visit_detail_id
                # mvd is an alias that should resolve to the mapping visit_detail table
                # Note: This is only reached when table_name != visit_detail
                col_expr = f'mvd.{eu_constants.VISIT_DETAIL_ID}'
                has_visit_detail_id = True
            elif field_name == eu_constants.PRECEDING_VISIT_DETAIL_ID:
                # Replace with mapped visit_detail_id
                # pvd is an alias that should resolve to the mapping visit_detail table
                col_expr = f'pvd.{eu_constants.VISIT_DETAIL_ID} {eu_constants.PRECEDING_VISIT_DETAIL_ID}'
                has_preceding_visit_detail_id = True
            elif field_name == eu_constants.VISIT_DETAIL_PARENT_ID:
                # Replace with mapped visit_detail_id
                # ppvd is an alias that should resolve to the mapping visit_detail table
                col_expr = f'ppvd.{eu_constants.VISIT_DETAIL_ID} {eu_constants.VISIT_DETAIL_PARENT_ID}'
                has_visit_detail_parent_id = True
            elif field_name == eu_constants.CARE_SITE_ID:
                # Replace with mapped care_site_id
                # cs is an alias that should resolve to the mapping care_site table
                # Note: This is only reached when table_name != care_site
                col_expr = f'mcs.{eu_constants.CARE_SITE_ID}'
                has_care_site_id = True
            elif field_name == eu_constants.LOCATION_ID:
                # Replace with mapped location_id
                # lc is an alias that should resolve to the mapping visit table
                # Note: This is only reached when table_name != location
                col_expr = f'loc.{eu_constants.LOCATION_ID}'
                has_location_id = True
            else:
                col_expr = field_name
            col_exprs.append(col_expr)
        cols = ',\n        '.join(col_exprs)

        visit_occurrence_join_expr = ''
        preceding_visit_occurrence_join_expr = ''
        visit_detail_join_expr = ''
        preceding_visit_detail_join_expr = ''
        visit_detail_parent_join_expr = ''
        location_join_expr = ''
        care_site_join_expr = ''
        visit_detail_filter_expr = ''

        if has_visit_occurrence_id:
            # Include a join to mapping visit occurrence table
            # Note: Using left join in order to keep records that aren't mapped to visits
            mvo = mapping_table_for(VISIT_OCCURRENCE)
            src_visit_occurrence_table_id = get_table_id(VISIT_OCCURRENCE,
                                                         hpo_id=hpo_id)
            visit_occurrence_join_expr = f'''
            LEFT JOIN `{output_dataset_id}.{mvo}` mvo 
              ON t.visit_occurrence_id = mvo.src_visit_occurrence_id 
             AND mvo.src_table_id = '{src_visit_occurrence_table_id}'
            '''

        if has_preceding_visit_occurrence_id:
            # Include a join to mapping visit occurrence table for preceding visit occurrence
            # Note: Using left join in order to keep records that aren't mapped to visits
            pvo = mapping_table_for(VISIT_OCCURRENCE)
            src_visit_occurrence_table_id = get_table_id(VISIT_OCCURRENCE,
                                                         hpo_id=hpo_id)
            preceding_visit_occurrence_join_expr = f'''
            LEFT JOIN `{output_dataset_id}.{pvo}` pvo 
              ON t.preceding_visit_occurrence_id = pvo.src_visit_occurrence_id 
             AND pvo.src_table_id = '{src_visit_occurrence_table_id}'
            '''

        if has_visit_detail_id:
            # Include a join to mapping visit detail table
            # Note: Using left join in order to keep records that aren't mapped to visits
            mvd = mapping_table_for(VISIT_DETAIL)
            src_visit_detail_table_id = get_table_id(VISIT_DETAIL,
                                                     hpo_id=hpo_id)
            visit_detail_join_expr = f'''
            LEFT JOIN `{output_dataset_id}.{mvd}` mvd 
              ON t.visit_detail_id = mvd.src_visit_detail_id 
             AND mvd.src_table_id = '{src_visit_detail_table_id}'
            '''

        if has_preceding_visit_detail_id:
            # Include a join to mapping visit detail table for preceding visit detail
            # Note: Using left join in order to keep records that aren't mapped to visits
            pvd = mapping_table_for(VISIT_DETAIL)
            src_visit_detail_table_id = get_table_id(VISIT_DETAIL,
                                                     hpo_id=hpo_id)
            preceding_visit_detail_join_expr = f'''
            LEFT JOIN `{output_dataset_id}.{pvd}` pvd 
              ON t.preceding_visit_detail_id = pvd.src_visit_detail_id
             AND pvd.src_table_id = '{src_visit_detail_table_id}'
            '''

        if has_visit_detail_parent_id:
            # Include a join to mapping visit detail table for parent visit detail
            # Note: Using left join in order to keep records that aren't mapped to visits
            ppvd = mapping_table_for(VISIT_DETAIL)
            src_visit_detail_table_id = get_table_id(VISIT_DETAIL,
                                                     hpo_id=hpo_id)
            visit_detail_parent_join_expr = f'''
            LEFT JOIN `{output_dataset_id}.{ppvd}` ppvd 
              ON t.visit_detail_parent_id = ppvd.src_visit_detail_id
             AND ppvd.src_table_id = '{src_visit_detail_table_id}'
            '''

        if has_care_site_id:
            # Include a join to mapping visit table
            # Note: Using left join in order to keep records that aren't mapped to visits
            cs = mapping_table_for(CARE_SITE)
            src_care_site_table_id = get_table_id(CARE_SITE, hpo_id=hpo_id)
            care_site_join_expr = f'''
            LEFT JOIN `{output_dataset_id}.{cs}` mcs 
                ON t.care_site_id = mcs.src_care_site_id 
                AND mcs.src_table_id = '{src_care_site_table_id}'
            '''

        if has_location_id:
            # Include a join to mapping visit table
            # Note: Using left join in order to keep records that aren't mapped to visits
            lc = mapping_table_for(LOCATION)
            src_location_table_id = get_table_id(LOCATION, hpo_id=hpo_id)
            location_join_expr = f'''
            LEFT JOIN `{output_dataset_id}.{lc}` loc 
                ON t.location_id = loc.src_location_id 
                AND loc.src_table_id = '{src_location_table_id}'
            '''

        if table_name == PERSON:
            return f'''
                    SELECT {cols} 
                    FROM `{input_dataset_id}.{table_id}` t
                       {location_join_expr}
                       {care_site_join_expr} 
                    '''

        if table_name == VISIT_DETAIL:
            visit_detail_filter_expr = f'''
            AND mvo.{eu_constants.VISIT_OCCURRENCE_ID} IS NOT NULL
            '''

        # NOTE The order of xyz_join_expr should align with the order of the columns in SELECT clause.
        # Otherwise, integration test needs some adjustment. (visit_detail falls into that)
        # TODO Optimize the process and the tests.
        return f'''
        SELECT
            {cols}
        FROM (
            SELECT
                *,
                ROW_NUMBER() OVER (PARTITION BY nm.{table_name}_id) AS row_num
            FROM
                `{input_dataset_id}.{table_id}` AS nm) AS t
        JOIN
            `{output_dataset_id}.{mapping_table}` AS m
        ON
            t.{table_name}_id = m.src_{table_name}_id
        AND m.src_table_id = '{table_id}'
        {visit_occurrence_join_expr}
        {visit_detail_join_expr}
        {care_site_join_expr}
        {preceding_visit_occurrence_join_expr}
        {preceding_visit_detail_join_expr}
        {visit_detail_parent_join_expr}
        {location_join_expr}
        WHERE
            row_num = 1
        {visit_detail_filter_expr}
        '''


def _union_subqueries(client, table_name, hpo_ids, input_dataset_id,
                      output_dataset_id):
    """
    Get list of subqueries (one for each HPO table found in the source) that comprise the load query

    :param client: BigQueryClient
    :param table_name: name of a CDM table to load
    :param hpo_ids: list of HPOs to process
    :param input_dataset_id: identifies the source dataset
    :param output_dataset_id: identifies the output dataset
    :return: list of subqueries
    """
    result = []
    # Exclude subqueries that reference tables that are missing from source dataset
    all_table_ids = [
        table.table_id for table in client.list_tables(input_dataset_id)
    ]
    for hpo_id in hpo_ids:
        table_id = get_table_id(table_name, hpo_id=hpo_id)
        if table_id in all_table_ids:
            if table_name == FACT_RELATIONSHIP:
                subquery = fact_relationship_hpo_subquery(
                    hpo_id, input_dataset_id, output_dataset_id)
                result.append(subquery)
            else:
                subquery = table_hpo_subquery(table_name, hpo_id,
                                              input_dataset_id,
                                              output_dataset_id)
                result.append(subquery)
        else:
            logging.info(
                f'Excluding table {table_id} from mapping query because it does not exist'
            )
    return result


def table_union_query(client, table_name, hpo_ids, input_dataset_id,
                      output_dataset_id):
    """
    For a CDM table returns a query which aggregates all records from each HPO's submission for that table

    :param client: BigQuerClient
    :param table_name: name of a CDM table loaded by the resulting query
    :param hpo_ids: list of HPOs to process
    :param input_dataset_id: identifies the source dataset
    :param output_dataset_id: identifies the output dataset
    :return: query used to load the table in the output dataset
    """
    subqueries = _union_subqueries(client, table_name, hpo_ids,
                                   input_dataset_id, output_dataset_id)
    return UNION_ALL.join(subqueries)


def fact_table_union_query(client, cdm_table, hpo_ids, input_dataset_id,
                           output_dataset_id):
    """
    :param client: BigQueryClient
    :param cdm_table: name of the CDM table (e.g. 'person', 'visit_occurrence', 'death')
    :param hpo_ids: identifies which HPOs to include in union
    :param input_dataset_id: identifies dataset containing input data
    :param output_dataset_id: identifies dataset where result of union should be output
    :return:
    """
    union_query = table_union_query(client, cdm_table, hpo_ids,
                                    input_dataset_id, output_dataset_id)

    return f'''
    SELECT domain_concept_id_1,
     fact_id_1,
     domain_concept_id_2,
     fact_id_2,
     relationship_concept_id 
        from ({union_query})
    WHERE  fact_id_1 is NOT NULL and fact_id_2 is NOT NULL
    '''


def load(client, cdm_table, hpo_ids, input_dataset_id, output_dataset_id):
    """
    Create and load a single domain table with union of all HPO domain tables

    :param client: BigQueryClient
    :param cdm_table: name of the CDM table (e.g. 'person', 'visit_occurrence', 'death')
    :param hpo_ids: identifies which HPOs to include in union
    :param input_dataset_id: identifies dataset containing input data
    :param output_dataset_id: identifies dataset where result of union should be output
    :return:
    """
    output_table = output_table_for(cdm_table)
    logging.info(
        f'Loading union of {cdm_table} tables from {hpo_ids} into {output_table}'
    )

    if cdm_table == FACT_RELATIONSHIP:
        q = fact_table_union_query(client, cdm_table, hpo_ids, input_dataset_id,
                                   output_dataset_id)
    else:
        q = table_union_query(client, cdm_table, hpo_ids, input_dataset_id,
                              output_dataset_id)
    query_result = query(q, output_table, output_dataset_id)
    query_job_id = query_result['jobReference']['jobId']
    logging.info(
        f'Job {query_job_id} completed for union of {cdm_table} tables from {hpo_ids}'
    )
    return query_result


def get_person_to_observation_query(dataset_id, ehr_cutoff_date=None):
    # Set ehr_cutoff_date if doesn't exist
    if not ehr_cutoff_date:
        ehr_cutoff_date = datetime.now().strftime('%Y-%m-%d')

    q = f"""
        --Race
        SELECT
            person_id,
            4013886 as observation_concept_id,
            38000280 as observation_type_concept_id,
            TIMESTAMP('{ehr_cutoff_date}') observation_datetime,
            race_concept_id as value_as_concept_id,
            NULL as value_as_string,
            race_source_value as observation_source_value,
            race_source_concept_id as observation_source_concept_id
        FROM `{dataset_id}.unioned_ehr_person`

        UNION ALL

        --Ethnicity
        SELECT
            person_id,
            4271761 as observation_concept_id,
            38000280 as observation_type_concept_id,
            TIMESTAMP('{ehr_cutoff_date}') observation_datetime,
            ethnicity_concept_id as value_as_concept_id,
            NULL as value_as_string,
            ethnicity_source_value as observation_source_value,
            ethnicity_source_concept_id as observation_source_concept_id
        FROM `{dataset_id}.unioned_ehr_person`

        UNION ALL

        --Gender
        SELECT
            person_id,
            4135376 as observation_concept_id,
            38000280 as observation_type_concept_id,
            TIMESTAMP('{ehr_cutoff_date}') observation_datetime,
            gender_concept_id as value_as_concept_id,
            NULL as value_as_string,
            gender_source_value as observation_source_value,
            gender_source_concept_id as observation_source_concept_id
        FROM `{dataset_id}.unioned_ehr_person`

        UNION ALL

        --DOB
        SELECT
            person_id,
            4083587 as observation_concept_id,
            38000280 as observation_type_concept_id,
            TIMESTAMP('{ehr_cutoff_date}') observation_datetime,
            NULL as value_as_concept_id,
            birth_datetime as value_as_string,
            NULL as observation_source_value,
            NULL as observation_source_concept_id
        FROM `{dataset_id}.unioned_ehr_person`
        """
    return q


def move_ehr_person_to_observation(output_dataset_id, ehr_cutoff_date=None):
    """
    This function moves the demographics from the EHR person table to
    the observation table in the combined data set
    :return:
    """

    q = '''
        SELECT
            CASE observation_concept_id
                WHEN {gender_concept_id} THEN pto.person_id + {pto_offset} + {gender_offset} + {hpo_offset} * hpo.Display_Order
                WHEN {race_concept_id} THEN pto.person_id + {pto_offset} + {race_offset} + {hpo_offset} * hpo.Display_Order
                WHEN {dob_concept_id} THEN pto.person_id + {pto_offset} + {dob_offset} + {hpo_offset} * hpo.Display_Order 
                WHEN {ethnicity_concept_id} THEN pto.person_id + {pto_offset} + {ethnicity_offset} + {hpo_offset} * hpo.Display_Order
            END AS observation_id,
            pto.person_id,
            observation_concept_id,
            EXTRACT(DATE FROM observation_datetime) as observation_date,
            observation_type_concept_id,
            observation_datetime,
            CAST(NULL AS FLOAT64) as value_as_number,
            value_as_concept_id,
            CAST(value_as_string AS STRING) as value_as_string,
            observation_source_value,
            observation_source_concept_id,
            NULL as qualifier_concept_id,
            NULL as unit_concept_id,
            NULL as provider_id,
            NULL as visit_occurrence_id,
            CAST(NULL AS STRING) as unit_source_value,
            CAST(NULL AS STRING) as qualifier_source_value,
            NULL as value_source_concept_id,
            CAST(NULL AS STRING) as value_source_value,
            NULL as questionnaire_response_id
        FROM
            ({person_to_obs_query}
            ORDER BY person_id) AS pto
            JOIN
            `{output_dataset_id}._mapping_person` AS mp
            ON pto.person_id = mp.src_person_id
            JOIN
            `lookup_tables.hpo_site_id_mappings` AS hpo
            ON LOWER(hpo.HPO_ID) = mp.src_hpo_id
        '''.format(output_dataset_id=output_dataset_id,
                   pto_offset=eu_constants.EHR_PERSON_TO_OBS_CONSTANT,
                   gender_concept_id=eu_constants.GENDER_CONCEPT_ID,
                   gender_offset=eu_constants.GENDER_CONSTANT_FACTOR,
                   race_concept_id=eu_constants.RACE_CONCEPT_ID,
                   race_offset=eu_constants.RACE_CONSTANT_FACTOR,
                   dob_concept_id=eu_constants.DOB_CONCEPT_ID,
                   dob_offset=eu_constants.DOB_CONSTANT_FACTOR,
                   ethnicity_concept_id=eu_constants.ETHNICITY_CONCEPT_ID,
                   ethnicity_offset=eu_constants.ETHNICITY_CONSTANT_FACTOR,
                   hpo_offset=eu_constants.HPO_CONSTANT_FACTOR,
                   person_to_obs_query=get_person_to_observation_query(
                       output_dataset_id, ehr_cutoff_date=ehr_cutoff_date))
    logging.info(
        f'Copying EHR person table from {BIGQUERY_DATASET_ID} to unioned dataset. Query is `{q}`'
    )
    dst_table_id = output_table_for(OBSERVATION)
    dst_dataset_id = output_dataset_id
    query(q, dst_table_id, dst_dataset_id, write_disposition='WRITE_APPEND')


def map_ehr_person_to_observation(output_dataset_id, ehr_cutoff_date=None):
    """
    Maps the newly created observation records from person into the observation mapping table
    :param input_dataset_id:
    :param output_dataset_id:
    :param hpo_id:
    """
    table_name = OBSERVATION

    q = '''
        SELECT
            mp.src_table_id AS src_table_id,
            CASE observation_concept_id
                WHEN {gender_concept_id} THEN pto.person_id + {pto_offset} + {gender_offset} + {hpo_offset} * hpo.Display_Order
                WHEN {race_concept_id} THEN pto.person_id + {pto_offset} + {race_offset} + {hpo_offset} * hpo.Display_Order
                WHEN {dob_concept_id} THEN pto.person_id + {pto_offset} + {dob_offset} + {hpo_offset} * hpo.Display_Order 
                WHEN {ethnicity_concept_id} THEN pto.person_id + {pto_offset} + {ethnicity_offset} + {hpo_offset} * hpo.Display_Order
            END AS observation_id,
            pto.person_id AS src_observation_id,
            mp.src_hpo_id AS src_hpo_id
        FROM
            ({person_to_obs_query}) AS pto
            JOIN
            `{output_dataset_id}._mapping_person` AS mp
            ON pto.person_id = mp.src_person_id
            JOIN
            `lookup_tables.hpo_site_id_mappings` AS hpo
            ON LOWER(hpo.HPO_ID) = mp.src_hpo_id            
        '''.format(output_dataset_id=output_dataset_id,
                   pto_offset=eu_constants.EHR_PERSON_TO_OBS_CONSTANT,
                   gender_concept_id=eu_constants.GENDER_CONCEPT_ID,
                   gender_offset=eu_constants.GENDER_CONSTANT_FACTOR,
                   race_concept_id=eu_constants.RACE_CONCEPT_ID,
                   race_offset=eu_constants.RACE_CONSTANT_FACTOR,
                   dob_concept_id=eu_constants.DOB_CONCEPT_ID,
                   dob_offset=eu_constants.DOB_CONSTANT_FACTOR,
                   ethnicity_concept_id=eu_constants.ETHNICITY_CONCEPT_ID,
                   ethnicity_offset=eu_constants.ETHNICITY_CONSTANT_FACTOR,
                   hpo_offset=eu_constants.HPO_CONSTANT_FACTOR,
                   person_to_obs_query=get_person_to_observation_query(
                       output_dataset_id, ehr_cutoff_date=ehr_cutoff_date))
    dst_dataset_id = output_dataset_id
    dst_table_id = mapping_table_for(table_name)
    logging.info(
        f'Mapping EHR person table from {BIGQUERY_DATASET_ID} to unioned dataset. Query is `{q}`'
    )
    query(q, dst_table_id, dst_dataset_id, write_disposition='WRITE_APPEND')


def create_load_aou_death(bq_client, project_id, input_dataset_id,
                          output_dataset_id, hpo_ids) -> None:
    """Create and load AOU_DEATH table.
    :param project_id: project containing the datasets
    :param input_dataset_id identifies a dataset containing multiple CDMs, one for each HPO submission
    :param output_dataset_id identifies the dataset to store the new CDM in
    :param hpo_ids: HPO site IDs. Note some sites may not have a DEATH table if they have not submitted anything yet.
    NOTE: `primary_death_record` is all `False` at this point. The CR
        `CalculatePrimaryDeathRecord` updates the table at the end of the
        Unioned EHR data tier creation.
    """
    # EHR Union runs every night so the table needs to be deleted first if exists.
    bq_client.delete_table(f'{output_dataset_id}.{UNIONED_EHR}_{AOU_DEATH}',
                           not_found_ok=True)

    table_name = f'{project_id}.{output_dataset_id}.{UNIONED_EHR}_{AOU_DEATH}'
    schema_list = bq_client.get_table_schema(AOU_DEATH)
    table_obj = bq.Table(table_name, schema=schema_list)
    table_obj.clustering_fields = 'person_id'
    table_obj.time_partitioning = bq.table.TimePartitioning(type_='DAY')
    bq_client.create_table(table_obj)

    # Filter out HPO sites without death data submission.
    hpo_ids_with_death = [
        hpo_id for hpo_id in hpo_ids
        if bq_client.table_exists(f'{hpo_id}_{DEATH}', input_dataset_id)
    ]

    query = LOAD_AOU_DEATH.render(project=project_id,
                                  input_dataset=input_dataset_id,
                                  output_dataset=output_dataset_id,
                                  aou_death=f'{UNIONED_EHR}_{AOU_DEATH}',
                                  death=DEATH,
                                  hpo_ids=hpo_ids_with_death)
    job = bq_client.query(query)
    _ = job.result()


def main(input_dataset_id,
         output_dataset_id,
         project_id,
         hpo_ids_ex=None,
         ehr_cutoff_date=None):
    """
    Create a new CDM which is the union of all EHR datasets submitted by HPOs

    :param input_dataset_id identifies a dataset containing multiple CDMs, one for each HPO submission
    :param output_dataset_id identifies the dataset to store the new CDM in
    :param project_id: project containing the datasets
    :param hpo_ids_ex: (optional) list that identifies HPOs not to process, by default process all
    :param ehr_cutoff_date: (optional) cutoff date for ehr data(same as CDR cutoff date)
    :returns: list of tables generated successfully
    """
    bq_client = BigQueryClient(project_id)

    logging.info('EHR union started')
    # NOTE hpo_ids here includes HPO sites without any submissions. Those may not
    # have OMOP tables (hpo_dummy_observation, etc) in the EHR dataset.
    hpo_ids = [item['hpo_id'] for item in bq_utils.get_hpo_info()]
    if hpo_ids_ex:
        hpo_ids = [hpo_id for hpo_id in hpo_ids if hpo_id not in hpo_ids_ex]

    # Create empty output tables to ensure proper schema, clustering, etc.
    # AOU_DEATH and DEATH are not created here.
    for table in CDM_TABLES:
        result_table = output_table_for(table)
        if table == DEATH:
            logging.info(f'Skipping {result_table} creation. '
                         f'{UNIONED_EHR}_{AOU_DEATH} will be created instead.')
            continue
        logging.info(f'Creating {output_dataset_id}.{result_table}...')
        bq_utils.create_standard_table(table,
                                       result_table,
                                       drop_existing=True,
                                       dataset_id=output_dataset_id)

    # Create mapping tables. AOU_DEATH and DEATH are not included here.
    for domain_table in cdm.tables_to_map():
        if domain_table == SURVEY_CONDUCT:
            continue
        logging.info(f'Mapping {domain_table}...')
        mapping(domain_table, hpo_ids, input_dataset_id, output_dataset_id,
                project_id, bq_client)

    # Load all tables with union of submitted tables
    # AOU_DEATH and DEATH are not loaded here.
    for table_name in CDM_TABLES:
        if table_name in [DEATH, SURVEY_CONDUCT]:
            continue
        logging.info(f'Creating union of table {table_name}...')
        load(bq_client, table_name, hpo_ids, input_dataset_id,
             output_dataset_id)

    # AOU_DEATH is created and loaded here.
    logging.info(f'Creating and loading {UNIONED_EHR}_{AOU_DEATH}...')
    create_load_aou_death(bq_client, project_id, input_dataset_id,
                          output_dataset_id, hpo_ids)
    logging.info(f'Completed {UNIONED_EHR}_{AOU_DEATH} load.')

    logging.info('Creation of Unioned EHR complete')

    # create person mapping table
    domain_table = PERSON
    logging.info(f'Mapping {domain_table}...')
    mapping(domain_table, hpo_ids, input_dataset_id, output_dataset_id,
            project_id, bq_client)

    logging.info(
        'Dropping race/ethnicity/gender records from unioned_ehr_observation')
    clean_engine.clean_dataset(project_id, output_dataset_id, output_dataset_id,
                               [(DropRaceEthnicityGenderObservation,)])
    logging.info(
        'Completed dropping race/ethnicity/gender records from unioned_ehr_observation'
    )

    logging.info('Starting process for Person to Observation')
    # Map and move EHR person records into four rows in observation, one each for race, ethnicity, dob and gender
    map_ehr_person_to_observation(output_dataset_id,
                                  ehr_cutoff_date=ehr_cutoff_date)
    move_ehr_person_to_observation(output_dataset_id,
                                   ehr_cutoff_date=ehr_cutoff_date)

    logging.info('Completed Person to Observation')


if __name__ == '__main__':
    pipeline_logging.configure(logging.INFO, add_console_handler=True)
    parser = argparse.ArgumentParser(
        description=
        'Create a new CDM dataset which is the union of all EHR datasets submitted by HPOs',
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        '--project_id',
        dest='project_id',
        help='Project associated with the input and output datasets')
    parser.add_argument('--input_dataset_id',
                        dest='input_dataset_id',
                        help='Dataset where HPO submissions are stored')
    parser.add_argument('--output_dataset_id',
                        dest='output_dataset_id',
                        help='Dataset where the results should be stored')
    parser.add_argument(
        '--hpo_id_ex',
        nargs='*',
        help='List of HPOs to exclude from processing (none by default)')
    parser.add_argument(
        '--ehr_cutoff_date',
        dest='ehr_cutoff_date',
        help=
        "Date to set for observation table rows transferred from person table",
        type=validate_date_string)

    # HPOs to exclude. If nothing given, exclude nothing.
    args = parser.parse_args()
    if args.input_dataset_id:
        main(args.input_dataset_id,
             args.output_dataset_id,
             args.project_id,
             hpo_ids_ex=args.hpo_id_ex,
             ehr_cutoff_date=args.ehr_cutoff_date)
