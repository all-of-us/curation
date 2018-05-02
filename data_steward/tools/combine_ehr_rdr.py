"""
Combine data sets `ehr` and `rdr` to form another data set `combined`

 * Find the `person_id`s of those who have consented to share EHR data

 * Copy `rdr.person` table as-is to `combined.person`

 * Create table `combined.<hpo>_visit_mapping(dest_visit_occurrence_id, source_table_name, source_visit_occurrence_id)`
   and populate it with UNION ALL of `visit_occurrence_id`s from ehr and rdr records that link to `combined.person`

 * Create tables `combined.<hpo>_{visit_occurrence, condition_occurrence, procedure_occurrence}` etc. from UNION ALL of
   `ehr` and `rdr` records that link to `combined.person`. Use `combined.<hpo>_visit_mapping.dest_visit_occurrence_id`
   for records that have a (valid) `visit_occurrence_id`.

 * Load `combined.<hpo>_observation` with records derived from values in `ehr.<hpo>_person`

## Notes
Currently the following environment variables must be set:
 * BIGQUERY_DATASET_ID: BQ dataset where combined result is stored (e.g. test_join_ehr_rdr)
 * APPLICATION_ID: GCP project ID (e.g. all-of-us-ehr-dev)
 * GOOGLE_APPLICATION_CREDENTIALS: location of service account key json file (e.g. /path/to/all-of-us-ehr-dev-abc123.json)
"""
import argparse
import json
import os
import logging

import bq_utils
from resources import fields_path
from google.appengine.api import app_identity

BQ_WAIT_TIME = 2
SOURCE_VALUE_EHR_CONSENT = 'EHRConsentPII_ConsentPermission'
CONCEPT_ID_CONSENT_PERMISSION_YES = 1586100  # ConsentPermission_Yes
CONCEPT_ID_CONSENT_PERMISSION_NO = 1586101   # ConsentPermission_No
ONE_BILLION = 1000000000
MAPPING_TABLE_ID = 'ehr_rdr_id_mapping'
ID_MAPPING_QUERY = '''
SELECT 
  rdr.row_id AS cdr_id
 ,rdr.person_id AS rdr_person_id
 ,ehr.person_id AS ehr_person_id
FROM 
(SELECT 
   ROW_NUMBER() OVER (ORDER BY person_id) AS row_id
  ,person_id
FROM `%(rdr_project)s.%(rdr_dataset)s.person`) AS rdr
JOIN
(SELECT 
   ROW_NUMBER() OVER (ORDER BY person_id) AS row_id
  ,person_id
FROM `%(ehr_project)s.%(ehr_dataset)s.person`) AS ehr
ON rdr.row_id = ehr.row_id
'''
TABLE_NAMES = ['person', 'visit_occurrence', 'condition_occurrence', 'procedure_occurrence', 'drug_exposure',
               'device_exposure', 'measurement', 'observation', 'death']


def construct_mapping_query(table_name, source, project_id, dataset_id, id_offset=None):
    """
    Get select query for CDM table with proper qualifiers and using cdr_id for person_id
    :param table_name: name of the CDM table
    :param source: 'ehr' or 'rdr'
    :param project_id: ID of the source table
    :param dataset_id: source dataset name
    :param id_offset: constant to add to *_id fields
    :return: the query
    """
    assert(source in ['ehr', 'rdr'])
    source_person_id_field = source + '_person_id'
    json_path = os.path.join(fields_path, table_name + '.json')
    with open(json_path, 'r') as fp:
        fields = json.load(fp)
        col_exprs = []
        for field in fields:
            field_name = field['name']
            field_type = field['type']
            if field_name == 'person_id':
                col_expr = 'cdr_id as person_id'
            elif id_offset and field_name.endswith('_id') and not field_name.endswith('concept_id') and field_type == 'integer':
                col_expr = '%(field_name)s + %(id_offset)s as %(field_name)s' % locals()
            else:
                col_expr = field_name
            col_exprs.append(col_expr)
        q = 'SELECT\n  '
        q += ',\n  '.join(col_exprs)
        q += '\nFROM `%(project_id)s.%(dataset_id)s.%(table_name)s` t' % locals()
        q += '\nJOIN %s m ON t.person_id = m.%s' % (MAPPING_TABLE_ID, source_person_id_field)
        return q


def consented_person_id_query(dataset_id, project_id=app_identity.get_application_id()):
    """
    Returns query used to get only those participants who have consented to share EHR data

    :param dataset_id: Data set with RDR data in OMOP
    :param project_id: Project with the dataset. By default env var APPLICATION_ID.
    :return:
    """
    return '''
    WITH yes_consent AS (
     SELECT 
       person_id, 
       observation_datetime
     FROM `{project_id}.{dataset_id}.observation`
     WHERE observation_source_value = '{source_value_ehr_consent}'
     AND value_source_concept_id = {concept_id_consent_permission_yes}
    ),
    no_consent AS (
     SELECT
       person_id,
       observation_datetime
     FROM `{project_id}.{dataset_id}.observation`
     WHERE observation_source_value = '{source_value_ehr_consent}'
     AND value_source_concept_id = {concept_id_consent_permission_no}
    )
    SELECT DISTINCT person_id 
    FROM yes_consent y
    WHERE NOT EXISTS (
     SELECT 1 FROM no_consent n
     WHERE n.person_id = y.person_id
     AND n.observation_datetime >= y.observation_datetime
    )
    '''.format(
        project_id=project_id,
        dataset_id=dataset_id,
        source_value_ehr_consent=SOURCE_VALUE_EHR_CONSENT,
        concept_id_consent_permission_yes=CONCEPT_ID_CONSENT_PERMISSION_YES,
        concept_id_consent_permission_no=CONCEPT_ID_CONSENT_PERMISSION_NO)


def query(q, destination_table_id, write_disposition):
    """
    Run query and block until job is done
    :param q: SQL statement
    :param destination_table_id: if set, output is saved in a table with the specified id
    :param write_disposition: WRITE_TRUNCATE, WRITE_APPEND or WRITE_EMPTY (default)
    """
    query_job_result = bq_utils.query(q, destination_table_id=destination_table_id, write_disposition=write_disposition)
    query_job_id = query_job_result['jobReference']['jobId']
    incomplete_jobs = bq_utils.wait_on_jobs([query_job_id])
    if len(incomplete_jobs) > 0:
        raise bq_utils.BigQueryJobWaitError(incomplete_jobs)


def copy_rdr_person():
    """
    Copy person table from the RDR dataset to the combined dataset

    Note: Overwrites if a person table already exists
    """
    src_dataset_id = bq_utils.get_rdr_dataset_id()
    dst_dataset_id = bq_utils.get_ehr_rdr_dataset_id()
    bq_utils.copy_table('person', 'person', src_dataset_id=src_dataset_id, dst_dataset_id=dst_dataset_id)


def main(args):
    mapping_query = ID_MAPPING_QUERY % args.__dict__
    logging.log(logging.INFO, 'Loading ' + MAPPING_TABLE_ID)

    query(mapping_query, destination_table_id=MAPPING_TABLE_ID, write_disposition='WRITE_TRUNCATE')

    for table_name in TABLE_NAMES:
        q = construct_mapping_query(table_name, 'ehr', args.ehr_project, args.ehr_dataset)
        logging.log(logging.INFO, 'Loading EHR table: ' + table_name)
        query(q, destination_table_id=table_name, write_disposition='WRITE_TRUNCATE')

    for table_name in [table_name for table_name in TABLE_NAMES if table_name != 'person']:
        q = construct_mapping_query(table_name, 'rdr', args.rdr_project, args.rdr_dataset, ONE_BILLION)
        logging.log(logging.INFO, 'Loading RDR table: ' + table_name)
        query(q, destination_table_id=table_name, write_disposition='WRITE_APPEND')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--ehr_project',
                        default='aou-res-curation-test',
                        help='Project containing the EHR dataset')
    parser.add_argument('--ehr_dataset',
                        default='synthetic_derivative_test_load',
                        help='Dataset containing EHR data in OMOP format')
    parser.add_argument('--rdr_project',
                        default='all-of-us-rdr-sandbox',
                        help='Project containing the RDR dataset')
    parser.add_argument('--rdr_dataset',
                        default='test_etl_6',
                        help='Dataset containing RDR data in OMOP format')
    main(parser.parse_args())
