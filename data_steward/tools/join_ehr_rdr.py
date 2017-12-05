"""
Combine synthetic EHR and RDR data sets to form another data set

 * Create a mapping table which arbitrarily maps EHR person_id to RDR person_id and assigns a cdr_id
 * For each CDM table, load the EHR data and append RDR data (ignore RDR person table)
 * RDR entity IDs (e.g. visit_occurrence_id, measurement_id) start at 1B

## Notes
Currently the following environment variables must be set:
 * BIGQUERY_DATASET_ID: BQ dataset where combined result is stored (e.g. test_join_ehr_rdr)
 * APPLICATION_ID: GCP project ID (e.g. all-of-us-ehr-dev)
 * GOOGLE_APPLICATION_CREDENTIALS: location of service account key json file (e.g. /path/to/all-of-us-ehr-dev-abc123.json)
"""
import argparse
import json
import os
import time

import bq_utils
from resources import fields_path

BQ_WAIT_TIME = 2
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


def construct_query(table_name, source, project_id, dataset_id, id_offset=None):
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


def query(q, destination_table_id, write_disposition):
    """
    Run query, write to stdout any errors encountered
    :param q: SQL statement
    :param destination_table_id: if set, output is saved in a table with the specified id
    :param write_disposition: WRITE_TRUNCATE, WRITE_APPEND or WRITE_EMPTY (default)
    :return: query result
    """
    qr = bq_utils.query(q, destination_table_id=destination_table_id, write_disposition=write_disposition)
    if 'errors' in qr['status']:
        print '== ERROR =='
        print qr
        print '\n'
    return qr


def main(args):
    mapping_query = ID_MAPPING_QUERY % args.__dict__
    print 'Loading ' + MAPPING_TABLE_ID
    query(mapping_query, destination_table_id=MAPPING_TABLE_ID, write_disposition='WRITE_TRUNCATE')
    time.sleep(BQ_WAIT_TIME)

    for table_name in TABLE_NAMES:
        q = construct_query(table_name, 'ehr', args.ehr_project, args.ehr_dataset)
        print 'Loading EHR table: ' + table_name
        query(q, destination_table_id=table_name, write_disposition='WRITE_TRUNCATE')

    incomplete_jobs = bq_utils.wait_on_jobs(jobs_to_wait_on, retry_count=10) 
    if len(incomplete_jobs) == 0:
        print " ---- EHR loading done succesful! ---- "
    else:
        print " ---- EHR LOAD TAKES TOO LONG! ---- "
        raise TimeoutError
    
    jobs_to_wait_on = []
    for table_name in [table_name for table_name in TABLE_NAMES if table_name not in ['person','observation']]:
        q = construct_query(table_name, 'rdr', args.rdr_project, args.rdr_dataset, ONE_BILLION)
        print 'Loading RDR table: ' + table_name
        query(q, destination_table_id=table_name, write_disposition='WRITE_APPEND')


    observation_rdr_table_name = 'observation_rdr'
    table_name = 'observation'
    observation_rdr_json_path = os.path.join(fields_path, observation_rdr_table_name + '.json')
    bq_utils.update_table_schema(table_name, observation_rdr_json_path)

    print " ---- UPDATED {} ---- ".format(table_name)
    q = construct_query(observation_rdr_table_name, 'rdr', args.rdr_project, args.rdr_dataset, ONE_BILLION)
    q = q.replace(observation_rdr_table_name, table_name)
    print 'Loading RDR table: ' + observation_rdr_table_name
    query_result = query(q, destination_table_id=table_name, write_disposition='WRITE_APPEND')
    query_job_id = query_result['jobReference']['jobId']
    incomplete_jobs = bq_utils.wait_on_jobs([query_job_id], retry_count=10) 
    if len(incomplete_jobs) == 0:
        print 'Loading RDR table: ' + observation_rdr_table_name, ' done!'
    else:
        print 'Loading RDR table: ' + observation_rdr_table_name, ' TAKING TOO LONG!'



if __name__ == '__main__':
    parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--ehr_project',
                        default='pmi-drc-api-test',
                        help='Project containing the EHR dataset')
    parser.add_argument('--ehr_dataset',
                        default='synthetic_derivative_test_load',
                        help='Dataset containing a CDM from synthetic EHR')
    parser.add_argument('--rdr_project',
                        default='all-of-us-rdr-sandbox',
                        help='Project containing the RDR dataset')
    parser.add_argument('--rdr_dataset',
                        default='test_etl_6',
                        help='Dataset containing a CDM from RDR ETL')
    main(parser.parse_args())
