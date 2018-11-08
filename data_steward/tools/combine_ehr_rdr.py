"""
Combine data sets `ehr` and `rdr` to form another data set `combined`

 * Create all the CDM tables in `combined`

 * Load `person_id` of those who have consented to share EHR data in `combined.ehr_consent`

 * Copy all `rdr.person` records to `combined.person`

 * Copy `ehr.death` records that link to `combined.ehr_consent`

 * Load `combined.visit_mapping(dst_visit_occurrence_id, src_dataset, src_visit_occurrence_id)`
   with UNION ALL of:
     1) all `rdr.visit_occurrence_id`s and
     2) `ehr.visit_occurrence_id`s that link to `combined.ehr_consent`

 * Load tables `combined.{visit_occurrence, condition_occurrence, procedure_occurrence}` etc. from UNION ALL
   of `ehr` and `rdr` records that link to `combined.person`. Use `combined.visit_mapping.dest_visit_occurrence_id`
   for records that have a (valid) `visit_occurrence_id`.

## Notes

Currently the following environment variables must be set:
 * BIGQUERY_DATASET_ID: BQ dataset where unioned EHR data is stored
 * RDR_DATASET_ID: BQ dataset where the RDR is stored
 * EHR_RDR_DATASET_ID: BQ dataset where the combined result will be stored
 * APPLICATION_ID: GCP project ID (e.g. all-of-us-ehr-dev)
 * GOOGLE_APPLICATION_CREDENTIALS: location of service account key json file (e.g. /path/to/all-of-us-ehr-dev-abc123.json)

Assumptions made:
 * The tables are NOT prefixed in the dataset referred to by BIGQUERY_DATASET_ID (use `table_copy.sh` as needed)
 * The RDR dataset exists. It must be loaded from the GCS bucket where RDR dumps are placed (i.e. using `import_rdr_omop.sh`).

Caveats:
 * Generating the curation report with `run_achilles_and_export.py` requires you to create a copy of the output
   tables with prefixes (use `table_copy.sh`)

TODO
 * Load fact relationship records from RDR
   - measurement: concept_id 21
   - observation: concept_id 27
 * Load `combined.<hpo>_observation` with records derived from values in `ehr.<hpo>_person`
 * Communicate to data steward EHR records not matched with RDR
"""
import logging

import bq_utils
import resources
import common

logger = logging.getLogger(__name__)

SOURCE_VALUE_EHR_CONSENT = 'EHRConsentPII_ConsentPermission'
CONCEPT_ID_CONSENT_PERMISSION_YES = 1586100  # ConsentPermission_Yes
EHR_CONSENT_TABLE_ID = '_ehr_consent'
PERSON_TABLE = 'person'
OBSERVATION_TABLE = 'observation'
VISIT_OCCURRENCE = 'visit_occurrence'
VISIT_OCCURRENCE_ID = 'visit_occurrence_id'
RDR_TABLES_TO_COPY = ['person', 'location', 'care_site']
EHR_TABLES_TO_COPY = ['death']
DOMAIN_TABLES = ['visit_occurrence', 'condition_occurrence', 'drug_exposure', 'measurement', 'procedure_occurrence',
                 'observation', 'device_exposure']
TABLES_TO_PROCESS = RDR_TABLES_TO_COPY + EHR_TABLES_TO_COPY + DOMAIN_TABLES
MEASUREMENT_DOMAIN_CONCEPT_ID = 21
OBSERVATION_DOMAIN_CONCEPT_ID = 27
OBSERVATION_TO_MEASUREMENT_CONCEPT_ID = 581410
MEASUREMENT_TO_OBSERVATION_CONCEPT_ID = 581411
PARENT_TO_CHILD_MEASUREMENT_CONCEPT_ID = 581436
CHILD_TO_PARENT_MEASUREMENT_CONCEPT_ID = 581437
DIASTOLIC_TO_SYSTOLIC_CONCEPT_ID = 46233682
SYSTOLIC_TO_DIASTOLIC_CONCEPT_ID = 46233683


def query(q, dst_table_id, write_disposition='WRITE_APPEND'):
    """
    Run query and block until job is done
    :param q: SQL statement
    :param dst_table_id: if set, output is saved in a table with the specified id
    :param write_disposition: WRITE_TRUNCATE, WRITE_EMPTY, or WRITE_APPEND (default, to preserve schema)
    """
    dst_dataset_id = bq_utils.get_ehr_rdr_dataset_id()
    query_job_result = bq_utils.query(q, destination_table_id=dst_table_id, write_disposition=write_disposition,
                                      destination_dataset_id=dst_dataset_id)
    query_job_id = query_job_result['jobReference']['jobId']
    incomplete_jobs = bq_utils.wait_on_jobs([query_job_id])
    if len(incomplete_jobs) > 0:
        raise bq_utils.BigQueryJobWaitError(incomplete_jobs)


def ehr_consent_query():
    """
    Returns query used to get the `person_id` of only those participants who have consented to share EHR data

    :return:
    """
    # Consenting are strictly those whose *most recent* (based on observation_datetime) consent record is YES
    # If the most recent record is NO or NULL, they are NOT consenting
    return '''
    WITH ordered_response AS
     (SELECT
        person_id, 
        value_source_concept_id,
        observation_datetime,
        ROW_NUMBER() OVER(PARTITION BY person_id ORDER BY observation_datetime DESC, value_source_concept_id ASC) AS rn
      FROM {dataset_id}.observation
      WHERE observation_source_value = '{source_value_ehr_consent}')
    
     SELECT person_id 
     FROM ordered_response
     WHERE rn = 1 
       AND value_source_concept_id = {concept_id_consent_permission_yes}
    '''.format(dataset_id=bq_utils.get_rdr_dataset_id(),
               source_value_ehr_consent=SOURCE_VALUE_EHR_CONSENT,
               concept_id_consent_permission_yes=CONCEPT_ID_CONSENT_PERMISSION_YES)


def assert_tables_in(dataset_id):
    """
    Raise assertion error if any CDM tables missing from a dataset
    :param dataset_id: dataset to check for tables in
    """
    tables = bq_utils.list_dataset_contents(dataset_id)
    logger.debug('Dataset {dataset_id} has tables: {tables}'.format(dataset_id=dataset_id, tables=tables))
    for table in TABLES_TO_PROCESS:
        if table not in tables:
            raise RuntimeError(
                'Dataset {dataset} is missing table {table}. Aborting.'.format(dataset=dataset_id, table=table))


def assert_ehr_and_rdr_tables():
    """
    Raise assertion error if any CDM tables missing from EHR or RDR dataset
    """
    ehr_dataset_id = bq_utils.get_dataset_id()
    assert_tables_in(ehr_dataset_id)
    rdr_dataset_id = bq_utils.get_rdr_dataset_id()
    assert_tables_in(rdr_dataset_id)


def create_cdm_tables():
    """
    Create all CDM tables

    Note: Recreates any existing tables
    """
    ehr_rdr_dataset_id = bq_utils.get_ehr_rdr_dataset_id()
    for table in common.CDM_TABLES:
        logger.debug('Creating table {dataset}.{table}...'.format(table=table, dataset=ehr_rdr_dataset_id))
        bq_utils.create_standard_table(table, table, drop_existing=True, dataset_id=ehr_rdr_dataset_id)


def ehr_consent():
    """
    Create and load ehr consent table in combined dataset

    :return:
    """
    q = ehr_consent_query()
    logger.debug('Query for {ehr_consent_table_id} is {q}'.format(ehr_consent_table_id=EHR_CONSENT_TABLE_ID, q=q))
    query(q, EHR_CONSENT_TABLE_ID)


def copy_rdr_table(table):
    """
    Copy table from the RDR dataset to the combined dataset

    Note: Overwrites if a table already exists
    """
    q = '''SELECT * FROM {rdr_dataset_id}.{table}'''.format(rdr_dataset_id=bq_utils.get_rdr_dataset_id(), table=table)
    logger.debug('Query for {table} is `{q}`'.format(table=table, q=q))
    query(q, table)


def move_ehr_person_to_observation():
    """
    This function moves the demographics from the EHR person table to
    the observation table in the combined data set
    :return:
    """

    q_max_ehr_obs_id = '''select max(observation_id) from {}.observation '''.format(bq_utils.get_dataset_id())
    q_max_rdr_obs_id = '''select max(observation_id) from {}.observation '''.format(bq_utils.get_rdr_dataset_id())
    max_ehr_obs_id = int(bq_utils.query(q_max_ehr_obs_id)['rows'][0]['f'][0]['v'])
    max_rdr_obs_id = int(bq_utils.query(q_max_rdr_obs_id)['rows'][0]['f'][0]['v'])
    q = ''' --Race
            SELECT
                ROW_NUMBER() OVER() + {offset} AS observation_id,
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
            (
              SELECT person_id, 4013886 as observation_concept_id, 38000280 as observation_type_concept_id, 
              birth_datetime as observation_datetime,
              race_concept_id as value_as_concept_id,
              NULL as value_as_string,
              race_source_value as observation_source_value, 
              race_source_concept_id as observation_source_concept_id
              FROM {ehr_dataset_id}.person

              UNION ALL

              --Ethnicity
              SELECT person_id, 4271761 as observation_concept_id, 38000280 as observation_type_concept_id, 
              birth_datetime as observation_datetime,
              ethnicity_concept_id as value_as_concept_id,
              NULL as value_as_string,
              ethnicity_source_value as observation_source_value, 
              ethnicity_source_concept_id as observation_source_concept_id
              FROM {ehr_dataset_id}.person

              UNION ALL

              --Gender
              SELECT person_id, 4135376 as observation_concept_id, 38000280 as observation_type_concept_id, 
              birth_datetime as observation_datetime,
              gender_concept_id as value_as_concept_id,
              NULL as value_as_string,
              gender_source_value as observation_source_value, 
              gender_source_concept_id as observation_source_concept_id
              FROM {ehr_dataset_id}.person

              UNION ALL

              --DOB
              SELECT person_id, 4083587 as observation_concept_id, 38000280 as observation_type_concept_id, 
              birth_datetime as observation_datetime,
              NULL as value_as_concept_id,
              birth_datetime as value_as_string,
              NULL as observation_source_value,
              NULL as observation_source_concept_id
              FROM {ehr_dataset_id}.person
            )
    '''.format(ehr_dataset_id=bq_utils.get_dataset_id(),
               offset = max_ehr_obs_id + max_rdr_obs_id)
    logger.debug('Copying EHR person table from {ehr_dataset_id} to combined dataset. Query is `{q}`'.format(ehr_dataset_id=bq_utils.get_dataset_id(), q=q))
    query(q, dst_table_id=OBSERVATION_TABLE, write_disposition='WRITE_APPEND')


def copy_ehr_table(table):
    """
    Copy table from EHR (consenting participants only) to the combined dataset without regenerating ids

    Note: Overwrites if a table already exists
    """
    fields = resources.fields_for(table)
    field_names = [field['name'] for field in fields]
    if 'person_id' not in field_names:
        raise RuntimeError('Cannot copy EHR table {table}. It is missing columns needed for consent filter'.format(
            table=table))
    q = '''
      SELECT * FROM {ehr_dataset_id}.{table} t
      WHERE EXISTS
           (SELECT 1 FROM {ehr_rdr_dataset_id}.{ehr_consent_table_id} c 
            WHERE t.person_id = c.person_id)
    '''.format(ehr_dataset_id=bq_utils.get_dataset_id(),
               table=table,
               ehr_consent_table_id=EHR_CONSENT_TABLE_ID,
               ehr_rdr_dataset_id=bq_utils.get_ehr_rdr_dataset_id())
    logger.debug('Query for {table} is `{q}`'.format(table=table, q=q))
    query(q, table)


def mapping_query(domain_table):
    """
    Returns query used to get mapping of all records from RDR combined with EHR records of consented participants

    :param domain_table: one of the domain tables (e.g. 'visit_occurrence', 'condition_occurrence')
    :return:
    """
    q = '''select MAX({domain_table}_id) as constant from {rdr_dataset_id}.{domain_table}'''.format(rdr_dataset_id=bq_utils.get_rdr_dataset_id(), domain_table=domain_table)
    mapping_constant_query_result = bq_utils.query(q)
    rows = bq_utils.response2rows(mapping_constant_query_result)
    mapping_constant = rows[0]['constant']

    return '''
    WITH all_records AS
    (
        SELECT
          '{rdr_dataset_id}'  AS src_dataset_id,
          {domain_table}_id AS src_{domain_table}_id, 
          'rdr' as src_hpo_id,
          {domain_table}_id AS {domain_table}_id
        FROM {rdr_dataset_id}.{domain_table}

        UNION ALL

        SELECT
          '{ehr_dataset_id}'  AS src_dataset_id, 
          t.{domain_table}_id AS src_{domain_table}_id
          v.src_hpo_id AS src_hpo_id,
          t.{domain_table}_id + mapping_constant AS {domain_table}_id          
        FROM {ehr_dataset_id}.{domain_table} t
        JOIN {ehr_dataset_id}._mapping_{domain_table}  v on t.{domain_table}_id = v.{domain_table}_id 
        WHERE EXISTS
           (SELECT 1 FROM {ehr_rdr_dataset_id}.{ehr_consent_table_id} c 
            WHERE t.person_id = c.person_id)
    )
    FROM all_records
    '''.format(rdr_dataset_id=bq_utils.get_rdr_dataset_id(),
               ehr_dataset_id=bq_utils.get_dataset_id(),
               ehr_rdr_dataset_id=bq_utils.get_ehr_rdr_dataset_id(),
               domain_table=domain_table,
               mapping_constant=mapping_constant,
               ehr_consent_table_id=EHR_CONSENT_TABLE_ID)


def mapping_table_for(domain_table):
    """
    Get name of mapping table generated for a domain table

    :param domain_table: one of the domain tables (e.g. 'visit_occurrence', 'condition_occurrence')
    :return:
    """
    return '_mapping_' + domain_table


def mapping(domain_table):
    """
    Create and load a mapping of all records from RDR combined with EHR records of consented participants

    :param domain_table:
    :return:
    """
    q = mapping_query(domain_table)
    mapping_table = mapping_table_for(domain_table)
    logger.debug('Query for {mapping_table} is {q}'.format(mapping_table=mapping_table, q=q))
    query(q, mapping_table)


def load_query(domain_table):
    """
    Returns query used to load a domain table

    :param domain_table: one of the domain tables (e.g. 'visit_occurrence', 'condition_occurrence')
    :return:
    """
    rdr_dataset_id = bq_utils.get_rdr_dataset_id()
    ehr_dataset_id = bq_utils.get_dataset_id()
    ehr_rdr_dataset_id = bq_utils.get_ehr_rdr_dataset_id()
    mapping_table = mapping_table_for(domain_table)
    has_visit_occurrence_id = False
    id_col = '{domain_table}_id'.format(domain_table=domain_table)
    fields = resources.fields_for(domain_table)

    # Generate column expressions for select, ensuring that
    #  1) we get the record IDs from the mapping table and
    #  2) if there is a reference to `visit_occurrence` we get `visit_occurrence_id` from the mapping visit table
    col_exprs = []
    for field in fields:
        field_name = field['name']
        if field_name == id_col:
            # Use mapping for record ID column
            # m is an alias that should resolve to the associated mapping table
            col_expr = 'm.{field_name} '.format(field_name=field_name)
        elif field_name == VISIT_OCCURRENCE_ID:
            # Replace with mapped visit_occurrence_id
            # mv is an alias that should resolve to the mapping visit table
            # Note: This is only reached when domain_table != visit_occurrence
            col_expr = 'mv.' + VISIT_OCCURRENCE_ID
            has_visit_occurrence_id = True
        else:
            col_expr = field_name
        col_exprs.append(col_expr)
    cols = ',\n  '.join(col_exprs)

    visit_join_expr = ''
    if has_visit_occurrence_id:
        # Include a join to mapping visit table
        # Note: Using left join in order to keep records that aren't mapped to visits
        mv = mapping_table_for(VISIT_OCCURRENCE)
        visit_join_expr = '''
        LEFT JOIN {ehr_rdr_dataset_id}.{mapping_visit_occurrence} mv 
          ON t.visit_occurrence_id = mv.src_visit_occurrence_id
         AND m.src_dataset_id = mv.src_dataset_id'''.format(ehr_rdr_dataset_id=ehr_rdr_dataset_id,
                                                            mapping_visit_occurrence=mv)

    return '''
    SELECT {cols} 
    FROM {rdr_dataset_id}.{domain_table} t 
      JOIN {ehr_rdr_dataset_id}.{mapping_table} m
        ON t.{domain_table}_id = m.src_{domain_table}_id {visit_join_expr}
    WHERE m.src_dataset_id = '{rdr_dataset_id}'
    
    UNION ALL
    
    SELECT {cols} 
    FROM {ehr_dataset_id}.{domain_table} t 
      JOIN {ehr_rdr_dataset_id}.{mapping_table} m
        ON t.{domain_table}_id = m.src_{domain_table}_id {visit_join_expr}
    WHERE m.src_dataset_id = '{ehr_dataset_id}'
    '''.format(cols=cols,
               domain_table=domain_table,
               rdr_dataset_id=rdr_dataset_id,
               ehr_dataset_id=ehr_dataset_id,
               mapping_table=mapping_table,
               visit_join_expr=visit_join_expr,
               ehr_rdr_dataset_id=ehr_rdr_dataset_id)


def load(domain_table):
    """
    Load a domain table

    :param domain_table: one of the domain tables (e.g. 'visit_occurrence', 'condition_occurrence')
    """
    q = load_query(domain_table)
    logger.debug('Query for {domain_table} is {q}'.format(domain_table=domain_table, q=q))
    query(q, domain_table)


def fact_relationship_query():
    """
    Load fact_relationship, using mapped IDs based on domain concept in fact 1 and fact 2
    :return:
    """
    return '''
    SELECT 
      fr.domain_concept_id_1 AS domain_concept_id_1,
      CASE
          WHEN domain_concept_id_1 = 21 
            THEN m1.measurement_id 
          WHEN domain_concept_id_1 = 27
            THEN o1.observation_id
      END AS fact_id_1,
      fr.domain_concept_id_2,
      CASE 
          WHEN domain_concept_id_2 = 21 
            THEN m2.measurement_id
          WHEN domain_concept_id_2 = 27
            THEN o2.observation_id
      END AS fact_id_2,
      fr.relationship_concept_id AS relationship_concept_id
    FROM {rdr_dataset_id}.fact_relationship fr
      LEFT JOIN {combined_dataset_id}.{mapping_measurement} m1
        ON m1.src_measurement_id = fr.fact_id_1 AND fr.domain_concept_id_1=21
      LEFT JOIN {combined_dataset_id}.{mapping_observation} o1
        ON o1.src_observation_id = fr.fact_id_1 AND fr.domain_concept_id_1=27
      LEFT JOIN {combined_dataset_id}.{mapping_measurement} m2
        ON m2.src_measurement_id = fr.fact_id_2 AND fr.domain_concept_id_2=21
      LEFT JOIN {combined_dataset_id}.{mapping_observation} o2
        ON o2.src_observation_id = fr.fact_id_2 AND fr.domain_concept_id_2=27
    '''.format(rdr_dataset_id=bq_utils.get_rdr_dataset_id(),
               combined_dataset_id=bq_utils.get_ehr_rdr_dataset_id(),
               mapping_measurement=mapping_table_for('measurement'),
               mapping_observation=mapping_table_for('observation'))


def load_fact_relationship():
    """
    Load fact_relationship table
    """
    q = fact_relationship_query()
    logger.debug('Query for fact_relationship is {q}'.format(q=q))
    query(q, 'fact_relationship')


def main():
    logger.info('EHR + RDR combine started')
    logger.info('Verifying all CDM tables in EHR and RDR datasets...')
    assert_ehr_and_rdr_tables()
    logger.info('Creating destination CDM tables...')
    create_cdm_tables()
    ehr_consent()
    for table in RDR_TABLES_TO_COPY:
        logger.info('Copying {table} table from RDR...'.format(table=table))
        copy_rdr_table(table)
    logger.info('Translating {table} table from EHR...'.format(table=PERSON_TABLE))
    move_ehr_person_to_observation()
    for table in EHR_TABLES_TO_COPY:
        logger.info('Copying {table} table from EHR...'.format(table=table))
        copy_ehr_table(table)
    logger.info('Loading {ehr_consent_table_id}...'.format(ehr_consent_table_id=EHR_CONSENT_TABLE_ID))
    for domain_table in DOMAIN_TABLES:
        logger.info('Mapping {domain_table}...'.format(domain_table=domain_table))
        mapping(domain_table)
    for domain_table in DOMAIN_TABLES:
        logger.info('Loading {domain_table}...'.format(domain_table=domain_table))
        load(domain_table)
    logger.info('Loading fact_relationship...')
    load_fact_relationship()
    logger.info('EHR + RDR combine completed')


if __name__ == '__main__':
    main()
