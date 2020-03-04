"""
Create a new CDM dataset which is the union of all EHR datasets submitted by HPOs

 1) Create empty output tables to ensure proper schema, clustering, etc.

 2) For all tables -EXCEPT person- that have numeric primary key columns, create mapping tables used to assign new
    IDs in output.

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

 3) For all tables, compose a query to fetch the union of records submitted by all HPOs and save the results in output.
   * Use new primary keys in output where applicable
   * Use new visit_occurrence_id where applicable

## Notes
Currently the following environment variables must be set:
 * GOOGLE_APPLICATION_CREDENTIALS: path to service account key json file (e.g. /path/to/all-of-us-ehr-dev-abc123.json)
 * APPLICATION_ID: GCP project ID (e.g. all-of-us-ehr-dev)
 * BIGQUERY_DATASET_ID: input dataset where submissions are stored
 * UNIONED_DATASET_ID: output dataset where unioned results should be stored

TODO
 * Besides `visit_occurrence` also handle mapping of other foreign key fields (e.g. `location_id`)
"""
import argparse
import logging

import app_identity

import bq_utils
import cdm
import common
from constants.validation import ehr_union as eu_constants
import resources
from constants.tools.combine_ehr_rdr import PERSON_TABLE, OBSERVATION_TABLE

UNION_ALL = '''

        UNION ALL
        
'''


def get_hpo_offsets(hpo_ids):
    """
    For each HPO, get a numeric constant to add to record ids

    :param hpo_ids: list of HPO identifiers
    :return: a dictionary mapping hpo_id => numeric offset
    """
    result = dict()
    i = eu_constants.EHR_ID_MULTIPLIER_START
    for hpo_id in hpo_ids:
        result[hpo_id] = i * common.ID_CONSTANT_FACTOR
        i += 1
    return result


def output_table_for(table_id):
    """
    Get the name of the table where results of the union will be stored

    :param table_id: name of a CDM table
    :return: name of the table where results of the union will be stored
    """
    return 'unioned_ehr_' + table_id


def _mapping_subqueries(table_name, hpo_ids, dataset_id, project_id):
    """
    Get list of subqueries (one for each HPO table found in the source) that comprise the ID mapping query

    :param table_name: name of a CDM table whose ID field must be remapped
    :param hpo_ids: list of HPOs to process
    :param dataset_id: identifies the source dataset
    :param project_id: identifies the GCP project
    :return: list of subqueries
    """
    result = []
    hpo_unique_identifiers = get_hpo_offsets(hpo_ids)

    # Exclude subqueries that reference tables that are missing from source dataset
    all_table_ids = bq_utils.list_all_table_ids(dataset_id)
    for hpo_id in hpo_ids:
        table_id = bq_utils.get_table_id(hpo_id, table_name)
        if table_id in all_table_ids:
            subquery = '''
                (SELECT '{table_id}' AS src_table_id,
                  {table_name}_id AS src_{table_name}_id,
                  {table_name}_id + {hpo_unique_num} as {table_name}_id
                  FROM `{project_id}.{dataset_id}.{table_id}`)
                '''.format(table_id=table_id,
                           table_name=table_name,
                           project_id=project_id,
                           dataset_id=dataset_id,
                           hpo_unique_num=hpo_unique_identifiers[hpo_id])
            result.append(subquery)
        else:
            logging.info(
                'Excluding table {table_id} from mapping query because it does not exist'
                .format(table_id=table_id))
    return result


def mapping_query(table_name, hpo_ids, dataset_id=None, project_id=None):
    """
    Get query used to generate new ids for a CDM table

    :param table_name: name of CDM table
    :param hpo_ids: identifies the HPOs
    :param dataset_id: identifies the BQ dataset containing the input table
    :param project_id: identifies the GCP project containing the dataset
    :return: the query
    """
    if dataset_id is None:
        dataset_id = bq_utils.get_dataset_id()
    if project_id is None:
        project_id = app_identity.get_application_id()
    subqueries = _mapping_subqueries(table_name, hpo_ids, dataset_id,
                                     project_id)
    union_all_query = UNION_ALL.join(subqueries)
    return '''
    WITH all_{table_name} AS (
      {union_all_query}
    )
    SELECT DISTINCT
        src_table_id,
        src_{table_name}_id,
        {table_name}_id,
        SUBSTR(src_table_id, 1, STRPOS(src_table_id, "_{table_name}")-1) AS src_hpo_id
    FROM all_{table_name}
    '''.format(union_all_query=union_all_query, table_name=table_name)


def mapping_table_for(domain_table):
    """
    Get name of mapping table generated for a domain table

    :param domain_table: one of the domain tables (e.g. 'visit_occurrence', 'condition_occurrence')
    :return:
    """
    return '_mapping_' + domain_table


def mapping(domain_table, hpo_ids, input_dataset_id, output_dataset_id,
            project_id):
    """
    Create and load a table that assigns unique ids to records in domain tables
    Note: Overwrites destination table if it already exists

    :param domain_table:
    :param hpo_ids: identifies which HPOs' data to include in union
    :param input_dataset_id: identifies dataset with multiple CDMs, each from an HPO submission
    :param output_dataset_id: identifies dataset where mapping table should be output
    :param project_id: identifies GCP project that contain the datasets
    :return:
    """
    q = mapping_query(domain_table, hpo_ids, input_dataset_id, project_id)
    mapping_table = mapping_table_for(domain_table)
    logging.info('Query for {mapping_table} is {q}'.format(
        mapping_table=mapping_table, q=q))
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
    job_status = query_job_result['status']
    error_result = job_status.get('errorResult')
    if error_result is not None:
        msg = 'Job {job_id} failed because: {error_result}'.format(
            job_id=query_job_id, error_result=error_result)
        raise bq_utils.InvalidOperationError(msg)
    incomplete_jobs = bq_utils.wait_on_jobs([query_job_id])
    if len(incomplete_jobs) > 0:
        raise bq_utils.BigQueryJobWaitError(incomplete_jobs)


def fact_relationship_hpo_subquery(hpo_id, input_dataset_id, output_dataset_id):
    """
    Get query for all fact_relationship records with mapped fact_id

    :param hpo_id: identifies the HPO
    :param input_dataset_id: identifies dataset containing HPO submission
    :param output_dataset_id: identifies dataset where output is saved
    :return: the query
    """
    table_id = bq_utils.get_table_id(hpo_id, eu_constants.FACT_RELATIONSHIP)
    fact_query = '''SELECT F.domain_concept_id_1,
        CASE
            WHEN F.domain_concept_id_1= {measurement_domain_concept_id} THEN M1.measurement_id
            WHEN F.domain_concept_id_1= {person_domain_concept_id} THEN fact_id_1
            ELSE 0
        END AS fact_id_1,
        F.domain_concept_id_2,
        CASE
            WHEN F.domain_concept_id_2= {measurement_domain_concept_id} THEN M2.measurement_id
            WHEN F.domain_concept_id_2= {person_domain_concept_id} THEN fact_id_2
            ELSE 0
        END AS fact_id_2,
        relationship_concept_id
        FROM
        `{input_dataset}.{table_id}` AS F
        LEFT JOIN
            `{dataset_id}._mapping_measurement` AS M1
        ON
            M1.src_measurement_id = F.fact_id_1
            AND (F.domain_concept_id_1 = {measurement_domain_concept_id}) AND (M1.src_hpo_id = '{hpo_id}')
        LEFT JOIN
            `{dataset_id}._mapping_measurement` AS M2
        ON
            M2.src_measurement_id = F.fact_id_2
            AND (F.domain_concept_id_2 = {measurement_domain_concept_id}) AND (M2.src_hpo_id = '{hpo_id}')'''.format(
        table_id=table_id,
        input_dataset=input_dataset_id,
        hpo_id=hpo_id,
        dataset_id=output_dataset_id,
        measurement_domain_concept_id=common.MEASUREMENT_DOMAIN_CONCEPT_ID,
        person_domain_concept_id=common.PERSON_DOMAIN_CONCEPT_ID)
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
    for table in resources.CDM_TABLES:
        if bq_utils.has_primary_key(table):
            tables_to_ref.append(table)

    is_id_mapped = table_name in tables_to_ref
    fields = resources.fields_for(table_name)
    table_id = bq_utils.get_table_id(hpo_id, table_name)

    # Generate column expressions for select
    if not is_id_mapped:
        # e.g. death
        col_exprs = [field['name'] for field in fields]
        cols = ',\n        '.join(col_exprs)
        return '''
    SELECT {cols} 
    FROM {input_dataset_id}.{table_id}'''.format(
            cols=cols, table_id=table_id, input_dataset_id=input_dataset_id)
    else:
        # Ensure that we
        #  1) populate primary key from the mapping table and
        #  2) populate any foreign key fields from the mapping visit table
        # NOTE: Assumes that besides person_id foreign keys exist only for visit_occurrence, location, care_site
        mapping_table = mapping_table_for(table_name) if is_id_mapped else None
        has_visit_occurrence_id = False
        has_care_site_id = False
        has_location_id = False
        id_col = '{table_name}_id'.format(table_name=table_name)
        col_exprs = []

        for field in fields:
            field_name = field['name']
            if field_name == id_col:
                # Use mapping for record ID column
                # m is an alias that should resolve to the associated mapping table
                if field_name == eu_constants.PERSON_ID:
                    col_expr = '{field_name}'.format(field_name=field_name)
                else:
                    col_expr = 'm.{field_name}'.format(field_name=field_name)
            elif field_name == eu_constants.VISIT_OCCURRENCE_ID:
                # Replace with mapped visit_occurrence_id
                # mv is an alias that should resolve to the mapping visit table
                # Note: This is only reached when table_name != visit_occurrence
                col_expr = 'mv.' + eu_constants.VISIT_OCCURRENCE_ID
                has_visit_occurrence_id = True
            elif field_name == eu_constants.CARE_SITE_ID:
                # Replace with mapped care_site_id
                # cs is an alias that should resolve to the mapping care_site table
                # Note: This is only reached when table_name != care_site
                col_expr = 'mcs.' + eu_constants.CARE_SITE_ID
                has_care_site_id = True
            elif field_name == eu_constants.LOCATION_ID:
                # Replace with mapped location_id
                # lc is an alias that should resolve to the mapping visit table
                # Note: This is only reached when table_name != location
                col_expr = 'loc.' + eu_constants.LOCATION_ID
                has_location_id = True
            else:
                col_expr = field_name
            col_exprs.append(col_expr)
        cols = ',\n        '.join(col_exprs)

        visit_join_expr = ''
        location_join_expr = ''
        care_site_join_expr = ''

        if has_visit_occurrence_id:
            # Include a join to mapping visit table
            # Note: Using left join in order to keep records that aren't mapped to visits
            mv = mapping_table_for(eu_constants.VISIT_OCCURRENCE)
            src_visit_table_id = bq_utils.get_table_id(
                hpo_id, eu_constants.VISIT_OCCURRENCE)
            visit_join_expr = '''
            LEFT JOIN {output_dataset_id}.{mapping_visit_occurrence} mv 
              ON t.visit_occurrence_id = mv.src_visit_occurrence_id 
             AND mv.src_table_id = '{src_visit_table_id}'
            '''.format(output_dataset_id=output_dataset_id,
                       mapping_visit_occurrence=mv,
                       src_visit_table_id=src_visit_table_id)

        if has_care_site_id:
            # Include a join to mapping visit table
            # Note: Using left join in order to keep records that aren't mapped to visits
            cs = mapping_table_for(eu_constants.CARE_SITE)
            src_care_site_table_id = bq_utils.get_table_id(
                hpo_id, eu_constants.CARE_SITE)
            care_site_join_expr = '''
                        LEFT JOIN {output_dataset_id}.{mapping_care_site} mcs 
                          ON t.care_site_id = mcs.src_care_site_id 
                         AND mcs.src_table_id = '{src_care_table_id}'
                        '''.format(output_dataset_id=output_dataset_id,
                                   mapping_care_site=cs,
                                   src_care_table_id=src_care_site_table_id)

        if has_location_id:
            # Include a join to mapping visit table
            # Note: Using left join in order to keep records that aren't mapped to visits
            lc = mapping_table_for(eu_constants.LOCATION)
            src_location_table_id = bq_utils.get_table_id(
                hpo_id, eu_constants.LOCATION)
            location_join_expr = '''
                        LEFT JOIN {output_dataset_id}.{mapping_location} loc 
                          ON t.location_id = loc.src_location_id 
                         AND loc.src_table_id = '{src_location_id}'
                        '''.format(output_dataset_id=output_dataset_id,
                                   mapping_location=lc,
                                   src_location_id=src_location_table_id)

        if table_name == eu_constants.PERSON:
            return '''
                    SELECT {cols} 
                    FROM {ehr_dataset_id}.{table_id} t
                       {location_join_expr}
                       {care_site_join_expr} 
                    '''.format(cols=cols,
                               table_id=table_id,
                               ehr_dataset_id=input_dataset_id,
                               visit_join_expr=visit_join_expr,
                               care_site_join_expr=care_site_join_expr,
                               location_join_expr=location_join_expr,
                               hpo_id=hpo_id)

        else:
            return '''
            SELECT
                {cols}
            FROM (
                SELECT
                    *,
                    ROW_NUMBER() OVER (PARTITION BY nm.{table_name}_id) AS row_num
                FROM
                    {ehr_dataset_id}.{table_id} AS nm) AS t
            JOIN
                {output_dataset_id}.{mapping_table} AS m
            ON
                t.{table_name}_id = m.src_{table_name}_id
            AND m.src_table_id = '{table_id}'
            {visit_join_expr}
            {care_site_join_expr}
            {location_join_expr}
            WHERE
                row_num = 1
                '''.format(cols=cols,
                           table_id=table_id,
                           ehr_dataset_id=input_dataset_id,
                           output_dataset_id=output_dataset_id,
                           mapping_table=mapping_table,
                           visit_join_expr=visit_join_expr,
                           care_site_join_expr=care_site_join_expr,
                           location_join_expr=location_join_expr,
                           table_name=table_name)


def _union_subqueries(table_name, hpo_ids, input_dataset_id, output_dataset_id):
    """
    Get list of subqueries (one for each HPO table found in the source) that comprise the load query

    :param table_name: name of a CDM table to load
    :param hpo_ids: list of HPOs to process
    :param input_dataset_id: identifies the source dataset
    :param output_dataset_id: identifies the output dataset
    :return: list of subqueries
    """
    result = []
    # Exclude subqueries that reference tables that are missing from source dataset
    all_table_ids = bq_utils.list_all_table_ids(input_dataset_id)
    for hpo_id in hpo_ids:
        table_id = bq_utils.get_table_id(hpo_id, table_name)
        if table_id in all_table_ids:
            if table_name == eu_constants.FACT_RELATIONSHIP:
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
                'Excluding table {table_id} from mapping query because it does not exist'
                .format(table_id=table_id))
    return result


def table_union_query(table_name, hpo_ids, input_dataset_id, output_dataset_id):
    """
    For a CDM table returns a query which aggregates all records from each HPO's submission for that table

    :param table_name: name of a CDM table loaded by the resulting query
    :param hpo_ids: list of HPOs to process
    :param input_dataset_id: identifies the source dataset
    :param output_dataset_id: identifies the output dataset
    :return: query used to load the table in the output dataset
    """
    subqueries = _union_subqueries(table_name, hpo_ids, input_dataset_id,
                                   output_dataset_id)
    return UNION_ALL.join(subqueries)


def fact_table_union_query(cdm_table, hpo_ids, input_dataset_id,
                           output_dataset_id):
    union_query = table_union_query(cdm_table, hpo_ids, input_dataset_id,
                                    output_dataset_id)

    null_condition_query = '''
    SELECT domain_concept_id_1,
     fact_id_1,
     domain_concept_id_2,
     fact_id_2,
     relationship_concept_id 
        from ({union_q})
    WHERE  fact_id_1 is NOT NULL and fact_id_2 is NOT NULL
    '''

    return null_condition_query.format(union_q=union_query)


def load(cdm_table, hpo_ids, input_dataset_id, output_dataset_id):
    """
    Create and load a single domain table with union of all HPO domain tables

    :param cdm_table: name of the CDM table (e.g. 'person', 'visit_occurrence', 'death')
    :param hpo_ids: identifies which HPOs to include in union
    :param input_dataset_id: identifies dataset containing input data
    :param output_dataset_id: identifies dataset where result of union should be output
    :return:
    """
    output_table = output_table_for(cdm_table)
    logging.info(
        'Loading union of {domain_table} tables from {hpo_ids} into {output_table}'
        .format(domain_table=cdm_table,
                hpo_ids=hpo_ids,
                output_table=output_table))

    if cdm_table == eu_constants.FACT_RELATIONSHIP:
        q = fact_table_union_query(cdm_table, hpo_ids, input_dataset_id,
                                   output_dataset_id)
    else:
        q = table_union_query(cdm_table, hpo_ids, input_dataset_id,
                              output_dataset_id)
    logging.info(
        'Query for union of {domain_table} tables from {hpo_ids} is {q}'.format(
            domain_table=cdm_table, hpo_ids=hpo_ids, q=q))
    query_result = query(q, output_table, output_dataset_id)
    return query_result


def get_person_to_observation_query(dataset_id):
    q = """
        --Race
        SELECT
            person_id,
            4013886 as observation_concept_id,
            38000280 as observation_type_concept_id,
            CASE
                WHEN birth_datetime IS NULL THEN TIMESTAMP(CONCAT(CAST(year_of_birth AS STRING),'-',
                CAST(month_of_birth AS STRING),'-',CAST(day_of_birth AS STRING)))
                ELSE birth_datetime
            END AS observation_datetime,
            race_concept_id as value_as_concept_id,
            NULL as value_as_string,
            race_source_value as observation_source_value,
            race_source_concept_id as observation_source_concept_id
        FROM {dataset_id}.unioned_ehr_person
        WHERE birth_datetime IS NOT NULL OR (month_of_birth IS NOT NULL AND day_of_birth IS NOT NULL)

        UNION ALL

        --Ethnicity
        SELECT
            person_id,
            4271761 as observation_concept_id,
            38000280 as observation_type_concept_id,
            CASE
                WHEN birth_datetime IS NULL THEN TIMESTAMP(CONCAT(CAST(year_of_birth AS STRING),'-',
                CAST(month_of_birth AS STRING),'-',CAST(day_of_birth AS STRING)))
                ELSE birth_datetime
            END AS observation_datetime,
            ethnicity_concept_id as value_as_concept_id,
            NULL as value_as_string,
            ethnicity_source_value as observation_source_value,
            ethnicity_source_concept_id as observation_source_concept_id
        FROM {dataset_id}.unioned_ehr_person
        WHERE birth_datetime IS NOT NULL OR (month_of_birth IS NOT NULL AND day_of_birth IS NOT NULL)

        UNION ALL

        --Gender
        SELECT
            person_id,
            4135376 as observation_concept_id,
            38000280 as observation_type_concept_id,
            CASE
                WHEN birth_datetime IS NULL THEN TIMESTAMP(CONCAT(CAST(year_of_birth AS STRING),'-',
                CAST(month_of_birth AS STRING),'-',CAST(day_of_birth AS STRING)))
                ELSE birth_datetime
            END AS observation_datetime,
            gender_concept_id as value_as_concept_id,
            NULL as value_as_string,
            gender_source_value as observation_source_value,
            gender_source_concept_id as observation_source_concept_id
        FROM {dataset_id}.unioned_ehr_person
        WHERE birth_datetime IS NOT NULL OR (month_of_birth IS NOT NULL AND day_of_birth IS NOT NULL)

        UNION ALL

        --DOB
        SELECT
            person_id,
            4083587 as observation_concept_id,
            38000280 as observation_type_concept_id,
            CASE
                WHEN birth_datetime IS NULL THEN TIMESTAMP(CONCAT(CAST(year_of_birth AS STRING),'-',
                CAST(month_of_birth AS STRING),'-',CAST(day_of_birth AS STRING)))
                ELSE birth_datetime
            END AS observation_datetime,
            NULL as value_as_concept_id,
            birth_datetime as value_as_string,
            NULL as observation_source_value,
            NULL as observation_source_concept_id
        FROM {dataset_id}.unioned_ehr_person
        WHERE birth_datetime IS NOT NULL OR (month_of_birth IS NOT NULL AND day_of_birth IS NOT NULL)
        """.format(dataset_id=dataset_id)
    return q


def move_ehr_person_to_observation(output_dataset_id):
    """
    This function moves the demographics from the EHR person table to
    the observation table in the combined data set
    :return:
    """

    q = '''
        SELECT
            CASE observation_concept_id
                WHEN {gender_concept_id} THEN pto.person_id + {pto_offset} + {gender_offset}
                WHEN {race_concept_id} THEN pto.person_id + {pto_offset} + {race_offset}
                WHEN {dob_concept_id} THEN pto.person_id + {pto_offset} + {dob_offset}
                WHEN {ethnicity_concept_id} THEN pto.person_id + {pto_offset} + {ethnicity_offset}
            END AS observation_id,
            person_id,
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
        '''.format(
        output_dataset_id=output_dataset_id,
        pto_offset=eu_constants.EHR_PERSON_TO_OBS_CONSTANT,
        gender_concept_id=eu_constants.GENDER_CONCEPT_ID,
        gender_offset=eu_constants.GENDER_CONSTANT_FACTOR,
        race_concept_id=eu_constants.RACE_CONCEPT_ID,
        race_offset=eu_constants.RACE_CONSTANT_FACTOR,
        dob_concept_id=eu_constants.DOB_CONCEPT_ID,
        dob_offset=eu_constants.DOB_CONSTANT_FACTOR,
        ethnicity_concept_id=eu_constants.ETHNICITY_CONCEPT_ID,
        ethnicity_offset=eu_constants.ETHNICITY_CONSTANT_FACTOR,
        person_to_obs_query=get_person_to_observation_query(output_dataset_id))
    logging.info(
        'Copying EHR person table from {ehr_dataset_id} to unioned dataset. Query is `{q}`'
        .format(ehr_dataset_id=bq_utils.get_dataset_id(), q=q))
    dst_table_id = output_table_for(OBSERVATION_TABLE)
    dst_dataset_id = output_dataset_id
    query(q, dst_table_id, dst_dataset_id, write_disposition='WRITE_APPEND')


def map_ehr_person_to_observation(output_dataset_id):
    """
    Maps the newly created observation records from person into the observation mapping table
    :param input_dataset_id:
    :param output_dataset_id:
    :param hpo_id:
    """
    table_name = OBSERVATION_TABLE

    q = '''
        SELECT
            mp.src_table_id AS src_table_id,
            CASE observation_concept_id
                WHEN {gender_concept_id} THEN pto.person_id + {pto_offset} + {gender_offset}
                WHEN {race_concept_id} THEN pto.person_id + {pto_offset} + {race_offset}
                WHEN {dob_concept_id} THEN pto.person_id + {pto_offset} + {dob_offset}
                WHEN {ethnicity_concept_id} THEN pto.person_id + {pto_offset} + {ethnicity_offset}
            END AS observation_id,
            pto.person_id AS src_observation_id,
            mp.src_hpo_id AS src_hpo_id
        FROM
            ({person_to_obs_query}) AS pto
            JOIN
            {output_dataset_id}._mapping_person AS mp
            ON pto.person_id = mp.src_person_id
        '''.format(
        output_dataset_id=output_dataset_id,
        pto_offset=eu_constants.EHR_PERSON_TO_OBS_CONSTANT,
        gender_concept_id=eu_constants.GENDER_CONCEPT_ID,
        gender_offset=eu_constants.GENDER_CONSTANT_FACTOR,
        race_concept_id=eu_constants.RACE_CONCEPT_ID,
        race_offset=eu_constants.RACE_CONSTANT_FACTOR,
        dob_concept_id=eu_constants.DOB_CONCEPT_ID,
        dob_offset=eu_constants.DOB_CONSTANT_FACTOR,
        ethnicity_concept_id=eu_constants.ETHNICITY_CONCEPT_ID,
        ethnicity_offset=eu_constants.ETHNICITY_CONSTANT_FACTOR,
        person_to_obs_query=get_person_to_observation_query(output_dataset_id))
    dst_dataset_id = output_dataset_id
    dst_table_id = mapping_table_for(table_name)
    logging.info(
        'Mapping EHR person table from {ehr_dataset_id} to unioned dataset. Query is `{q}`'
        .format(ehr_dataset_id=bq_utils.get_dataset_id(), q=q))
    query(q, dst_table_id, dst_dataset_id, write_disposition='WRITE_APPEND')


def main(input_dataset_id, output_dataset_id, project_id, hpo_ids=None):
    """
    Create a new CDM which is the union of all EHR datasets submitted by HPOs

    :param input_dataset_id identifies a dataset containing multiple CDMs, one for each HPO submission
    :param output_dataset_id identifies the dataset to store the new CDM in
    :param project_id: project containing the datasets
    :param hpo_ids: (optional) identifies HPOs to process, by default process all
    :returns: list of tables generated successfully
    """
    logging.info('EHR union started')
    if hpo_ids is None:
        hpo_ids = [item['hpo_id'] for item in bq_utils.get_hpo_info()]

    # Create empty output tables to ensure proper schema, clustering, etc.
    for table in resources.CDM_TABLES:
        result_table = output_table_for(table)
        logging.info('Creating {dataset_id}.{table_id}...'.format(
            dataset_id=output_dataset_id, table_id=result_table))
        bq_utils.create_standard_table(table,
                                       result_table,
                                       drop_existing=True,
                                       dataset_id=output_dataset_id)

    # Create mapping tables
    for domain_table in cdm.tables_to_map():
        logging.info(
            'Mapping {domain_table}...'.format(domain_table=domain_table))
        mapping(domain_table, hpo_ids, input_dataset_id, output_dataset_id,
                project_id)

    # Load all tables with union of submitted tables
    for table_name in resources.CDM_TABLES:
        logging.info(
            'Creating union of table {table}...'.format(table=table_name))
        load(table_name, hpo_ids, input_dataset_id, output_dataset_id)

    logging.info('Creation of Unioned EHR complete')

    # create person mapping table
    domain_table = PERSON_TABLE
    logging.info('Mapping {domain_table}...'.format(domain_table=domain_table))
    mapping(domain_table, hpo_ids, input_dataset_id, output_dataset_id,
            project_id)

    logging.info('Starting process for Person to Observation')
    # Map and move EHR person records into four rows in observation, one each for race, ethnicity, dob and gender
    map_ehr_person_to_observation(output_dataset_id)
    move_ehr_person_to_observation(output_dataset_id)

    logging.info('Completed Person to Observation')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description=
        'Create a new CDM dataset which is the union of all EHR datasets submitted by HPOs',
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        'project_id',
        help='Project associated with the input and output datasets')
    parser.add_argument('input_dataset_id',
                        help='Dataset where HPO submissions are stored')
    parser.add_argument('output_dataset_id',
                        help='Dataset where the results should be stored')
    parser.add_argument('-hpo_id',
                        nargs='+',
                        help='HPOs to process (all by default)')
    args = parser.parse_args()
    if args.input_dataset_id:
        main(args.input_dataset_id, args.output_dataset_id, args.project_id)
