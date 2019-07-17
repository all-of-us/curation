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
 * GOOGLE_APPLICATION_CREDENTIALS: location of service account key json file (e.g.
 /path/to/all-of-us-ehr-dev-abc123.json)

Assumptions made:
 * The tables are NOT prefixed in the dataset referred to by BIGQUERY_DATASET_ID (use `table_copy.sh` as needed)
 * The RDR dataset exists. It must be loaded from the GCS bucket where RDR dumps are placed (i.e. using
 `import_rdr_omop.sh`).

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
import common
import constants.tools.combine_ehr_rdr as combine_consts
import resources

logger = logging.getLogger(__name__)


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
    return (' WITH ordered_response AS\n'
            ' (SELECT'
            ' person_id,'
            ' value_source_concept_id,'
            ' observation_datetime,'
            ' ROW_NUMBER() OVER(PARTITION BY person_id ORDER BY observation_datetime DESC, '
            ' value_source_concept_id ASC) AS rn'
            ' FROM {dataset_id}.observation'
            ' WHERE observation_source_value = \'{source_value_ehr_consent}\')'
            ' SELECT person_id'
            ' FROM ordered_response'
            ' WHERE rn = 1'
            ' AND value_source_concept_id = {concept_id_consent_permission_yes}'
            ).format(dataset_id=bq_utils.get_rdr_dataset_id(),
                     source_value_ehr_consent=combine_consts.SOURCE_VALUE_EHR_CONSENT,
                     concept_id_consent_permission_yes=combine_consts.CONCEPT_ID_CONSENT_PERMISSION_YES)


def assert_tables_in(dataset_id):
    """
    Raise assertion error if any CDM tables missing from a dataset
    :param dataset_id: dataset to check for tables in
    """
    tables = bq_utils.list_dataset_contents(dataset_id)
    logger.debug('Dataset {dataset_id} has tables: {tables}'.format(dataset_id=dataset_id, tables=tables))
    for table in combine_consts.TABLES_TO_PROCESS:
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
    for table in resources.CDM_TABLES:
        logger.debug('Creating table {dataset}.{table}...'.format(table=table, dataset=ehr_rdr_dataset_id))
        bq_utils.create_standard_table(table, table, drop_existing=True, dataset_id=ehr_rdr_dataset_id)


def ehr_consent():
    """
    Create and load ehr consent table in combined dataset

    :return:
    """
    q = ehr_consent_query()
    logger.debug(
        'Query for {ehr_consent_table_id} is {q}'.format(ehr_consent_table_id=combine_consts.EHR_CONSENT_TABLE_ID, q=q))
    query(q, combine_consts.EHR_CONSENT_TABLE_ID)


def copy_rdr_table(table):
    """
    Copy table from the RDR dataset to the combined dataset

    Note: Overwrites if a table already exists
    """
    q = 'SELECT * FROM {rdr_dataset_id}.{table}'.format(rdr_dataset_id=bq_utils.get_rdr_dataset_id(), table=table)
    logger.debug('Query for {table} is `{q}`'.format(table=table, q=q))
    query(q, table)


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
    q = ('      SELECT * FROM {ehr_dataset_id}.{table} t'
         '      WHERE EXISTS'
         '           (SELECT 1 FROM {ehr_rdr_dataset_id}.{ehr_consent_table_id} c'
         '            WHERE t.person_id = c.person_id)').format(ehr_dataset_id=bq_utils.get_dataset_id(),
                                                                table=table,
                                                                ehr_consent_table_id=combine_consts.EHR_CONSENT_TABLE_ID,
                                                                ehr_rdr_dataset_id=bq_utils.get_ehr_rdr_dataset_id())
    logger.debug('Query for {table} is `{q}`'.format(table=table, q=q))
    query(q, table)


def mapping_query(domain_table):
    """
    Returns query used to get mapping of all records from RDR combined with EHR records of consented participants

    :param domain_table: one of the domain tables (e.g. 'visit_occurrence', 'condition_occurrence')
    :return:
    """

    if combine_consts.PERSON_ID in [field['name'] for field in resources.fields_for(domain_table)]:
        return ('SELECT DISTINCT'
                ' \'{rdr_dataset_id}\'  AS src_dataset_id,'
                '  {domain_table}_id  AS src_{domain_table}_id,'
                '  \'rdr\' as src_hpo_id,'
                '  {domain_table}_id + {mapping_constant}  AS {domain_table}_id'
                '  FROM {rdr_dataset_id}.{domain_table}'
                ''
                '  UNION ALL'
                ''
                '  SELECT DISTINCT'
                '  \'{ehr_dataset_id}\'  AS src_dataset_id,'
                '  t.{domain_table}_id AS src_{domain_table}_id,'
                '  v.src_hpo_id AS src_hpo_id,'
                '  t.{domain_table}_id  AS {domain_table}_id'
                '  FROM {ehr_dataset_id}.{domain_table} t'
                '  JOIN {ehr_dataset_id}._mapping_{domain_table}  v on t.{domain_table}_id = v.{domain_table}_id'
                '  WHERE EXISTS'
                '  (SELECT 1 FROM {ehr_rdr_dataset_id}.{ehr_consent_table_id} c'
                '  WHERE t.person_id = c.person_id)').format(rdr_dataset_id=bq_utils.get_rdr_dataset_id(),
                                                             ehr_dataset_id=bq_utils.get_dataset_id(),
                                                             ehr_rdr_dataset_id=bq_utils.get_ehr_rdr_dataset_id(),
                                                             domain_table=domain_table,
                                                             mapping_constant=common.RDR_ID_CONSTANT,
                                                             ehr_consent_table_id=combine_consts.EHR_CONSENT_TABLE_ID)
    else:
        return ('SELECT DISTINCT'
                ' \'{rdr_dataset_id}\'  AS src_dataset_id,'
                '  {domain_table}_id  AS src_{domain_table}_id,'
                '  \'rdr\' as src_hpo_id,'
                '  {domain_table}_id + {mapping_constant}  AS {domain_table}_id'
                '  FROM {rdr_dataset_id}.{domain_table}'
                ''
                '  UNION ALL'
                ''
                '  SELECT DISTINCT'
                '  \'{ehr_dataset_id}\'  AS src_dataset_id,'
                '  t.{domain_table}_id AS src_{domain_table}_id,'
                '  v.src_hpo_id AS src_hpo_id,'
                '  t.{domain_table}_id  AS {domain_table}_id'
                '  FROM {ehr_dataset_id}.{domain_table} t'
                '  JOIN {ehr_dataset_id}._mapping_{domain_table}  v on t.{domain_table}_id = v.{domain_table}_id'
                ).format(rdr_dataset_id=bq_utils.get_rdr_dataset_id(),
                         ehr_dataset_id=bq_utils.get_dataset_id(),
                         ehr_rdr_dataset_id=bq_utils.get_ehr_rdr_dataset_id(),
                         domain_table=domain_table,
                         mapping_constant=common.RDR_ID_CONSTANT
                         )


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
    if domain_table in combine_consts.DOMAIN_TABLES:
        q = mapping_query(domain_table)
        mapping_table = mapping_table_for(domain_table)
        logger.debug('Query for {mapping_table} is {q}'.format(mapping_table=mapping_table, q=q))
        query(q, mapping_table)
    else:
        logging.info(
            'Excluding table {table_id} from mapping query because it does not exist'.format(table_id=domain_table))


def join_expression_generator(domain_table, ehr_rdr_dataset_id):
    """
    adds table aliases as references to columns and generates join expression

    :param domain_table: Name of the cdm table
    :param ehr_rdr_dataset_id: name of the datqset where the tables are present
    :return: returns cols and join expression strings.
    """
    field_names = [field['name'] for field in resources.fields_for(domain_table)]
    fields_to_join = []
    primary_key = []
    join_expression = []
    col_exprs = []
    cols = ''
    for field_name in field_names:
        if field_name == domain_table + '_id' and field_name != 'person_id':
            primary_key.append(field_name)
        elif field_name in combine_consts.FOREIGN_KEYS_FIELDS:
            fields_to_join.append(field_name)
        if field_name in fields_to_join:
            col_expr = '{x}.'.format(x=field_name[:3]) + field_name
        elif field_name in primary_key:
            col_expr = 'm.' + field_name
        else:
            col_expr = field_name
        col_exprs.append(col_expr)
        cols = ', '.join(col_exprs)

    for key in combine_consts.FOREIGN_KEYS_FIELDS:
        if key in fields_to_join:
            if domain_table == combine_consts.PERSON_TABLE:
                table_alias = mapping_table_for('{x}'.format(x=key)[:-3])
                join_expression.append(
                    combine_consts.LEFT_JOIN_PERSON.format(dataset_id=ehr_rdr_dataset_id,
                                                           prefix=key[:3],
                                                           field=key,
                                                           table=table_alias
                                                           )
                )
            else:
                table_alias = mapping_table_for('{x}'.format(x=key)[:-3])
                join_expression.append(
                    combine_consts.LEFT_JOIN.format(dataset_id=ehr_rdr_dataset_id,
                                                    prefix=key[:3],
                                                    field=key,
                                                    table=table_alias
                                                    )
                )
    full_join_expression = " ".join(join_expression)
    return cols, full_join_expression


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
    cols, join_expression = join_expression_generator(domain_table, ehr_rdr_dataset_id)

    return ('    SELECT {cols}'
            '    FROM {rdr_dataset_id}.{domain_table} t'
            '    JOIN'
            '    ('
            '        SELECT *'
            '        FROM ('
            '          SELECT'
            '              *,'
            '              row_number() OVER (PARTITION BY m.src_{domain_table}_id, m.src_hpo_id ) AS row_num'
            '          FROM {ehr_rdr_dataset_id}.{mapping_table} as m'
            '        )'
            '        WHERE row_num = 1'
            '    ) m        ON t.{domain_table}_id = m.src_{domain_table}_id'
            '       {join_expr}'
            '    WHERE m.src_dataset_id = \'{rdr_dataset_id}\''
            ''
            '    UNION ALL'
            ''
            '    SELECT {cols}'
            '    FROM'
            '    ('
            '        SELECT *'
            '        FROM ('
            '          SELECT'
            '              *,'
            '              row_number() OVER (PARTITION BY m.{domain_table}_id) AS row_num'
            '          FROM {ehr_dataset_id}.{domain_table} as m'
            '        )'
            '        WHERE row_num = 1'
            '    ) t'
            '    JOIN'
            '    ('
            '        SELECT *'
            '        FROM ('
            '          SELECT'
            '              *,'
            '              row_number() OVER (PARTITION BY m.src_{domain_table}_id, m.src_hpo_id) AS row_num'
            '          FROM {ehr_rdr_dataset_id}.{mapping_table} as m'
            '        )'
            '        WHERE row_num = 1'
            '    ) m'
            '        ON t.{domain_table}_id = m.src_{domain_table}_id '
            '       {join_expr}'
            '    WHERE m.src_dataset_id = \'{ehr_dataset_id}\''
            '    ').format(cols=cols,
                           domain_table=domain_table,
                           rdr_dataset_id=rdr_dataset_id,
                           ehr_dataset_id=ehr_dataset_id,
                           mapping_table=mapping_table,
                           join_expr=join_expression,
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
    return ('\n'
            '  SELECT'
            '    fr.domain_concept_id_1 AS domain_concept_id_1,'
            '    CASE'
            '        WHEN domain_concept_id_1 = {measurement_domain_concept_id}'
            '          THEN m1.measurement_id'
            '        WHEN domain_concept_id_1 = {observation_domain_concept_id}'
            '          THEN o1.observation_id'
            '    END AS fact_id_1,'
            '    fr.domain_concept_id_2,'
            '    CASE'
            '        WHEN domain_concept_id_2 = {measurement_domain_concept_id}'
            '          THEN m2.measurement_id'
            '        WHEN domain_concept_id_2 = {observation_domain_concept_id}'
            '          THEN o2.observation_id'
            '    END AS fact_id_2,'
            '    fr.relationship_concept_id AS relationship_concept_id'
            '  FROM {rdr_dataset_id}.fact_relationship fr'
            '    LEFT JOIN {combined_dataset_id}.{mapping_measurement} m1'
            '      ON m1.src_measurement_id = fr.fact_id_1 AND fr.domain_concept_id_1={'
            'measurement_domain_concept_id}'
            '    LEFT JOIN {combined_dataset_id}.{mapping_observation} o1'
            '      ON o1.src_observation_id = fr.fact_id_1 AND fr.domain_concept_id_1={'
            'observation_domain_concept_id}'
            '    LEFT JOIN {combined_dataset_id}.{mapping_measurement} m2'
            '      ON m2.src_measurement_id = fr.fact_id_2 AND fr.domain_concept_id_2={measurement_domain_concept_id}'
            '    LEFT JOIN {combined_dataset_id}.{mapping_observation} o2'
            '      ON o2.src_observation_id = fr.fact_id_2 AND fr.domain_concept_id_2={observation_domain_concept_id}'
            '    ').format(rdr_dataset_id=bq_utils.get_rdr_dataset_id(),
                           combined_dataset_id=bq_utils.get_ehr_rdr_dataset_id(),
                           mapping_measurement=mapping_table_for('measurement'),
                           mapping_observation=mapping_table_for('observation'),
                           measurement_domain_concept_id=common.MEASUREMENT_DOMAIN_CONCEPT_ID,
                           observation_domain_concept_id=common.OBSERVATION_DOMAIN_CONCEPT_ID)


def load_fact_relationship():
    """
    Load fact_relationship table
    """
    q = fact_relationship_query()
    logger.debug('Query for fact_relationship is {q}'.format(q=q))
    query(q, 'fact_relationship')


def person_query(table_name):
    """
    Maps location and care_Site id in person table

    :return: query
    """
    ehr_rdr_dataset_id = bq_utils.get_ehr_rdr_dataset_id()
    columns, join_expression = join_expression_generator(table_name, ehr_rdr_dataset_id)
    mapped_person_query = ('select {cols} '
                           'from {dataset}.{table} as t '
                           '{join_expr}').format(cols=columns,
                                                 dataset=ehr_rdr_dataset_id,
                                                 table=table_name,
                                                 join_expr=join_expression)
    return mapped_person_query


def load_mapped_person():
    q = person_query(combine_consts.PERSON_TABLE)
    logger.debug('Query for Person table is {q}'.format(q=q))
    query(q, 'person', write_disposition='WRITE_TRUNCATE')


def main():
    logger.info('EHR + RDR combine started')
    logger.info('Verifying all CDM tables in EHR and RDR datasets...')
    assert_ehr_and_rdr_tables()
    logger.info('Creating destination CDM tables...')
    create_cdm_tables()
    ehr_consent()
    for table in combine_consts.RDR_TABLES_TO_COPY:
        logger.info('Copying {table} table from RDR...'.format(table=table))
        copy_rdr_table(table)
    logger.info('Translating {table} table from EHR...'.format(table=combine_consts.PERSON_TABLE))
    for table in combine_consts.EHR_TABLES_TO_COPY:
        logger.info('Copying {table} table from EHR...'.format(table=table))
        copy_ehr_table(table)
    logger.info('Loading {ehr_consent_table_id}...'.format(ehr_consent_table_id=combine_consts.EHR_CONSENT_TABLE_ID))
    for domain_table in combine_consts.DOMAIN_TABLES:
        logger.info('Mapping {domain_table}...'.format(domain_table=domain_table))
        mapping(domain_table)
    for domain_table in combine_consts.DOMAIN_TABLES:
        logger.info('Loading {domain_table}...'.format(domain_table=domain_table))
        load(domain_table)
    logger.info('Loading fact_relationship...')
    load_fact_relationship()
    logger.info('Loading foreign key Mapped Person table...')
    load_mapped_person()
    logger.info('EHR + RDR combine completed')


if __name__ == '__main__':
    main()
