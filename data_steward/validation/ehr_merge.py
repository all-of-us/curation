"""
Combine EHR datasets to form full data set

 * Create a mapping table which arbitrarily maps EHR person_id to RDR person_id and assigns a cdr_id
 * For each CDM table, load the EHR data and append RDR data (ignore RDR person table)
 * RDR entity IDs (e.g. visit_occurrence_id, measurement_id) start at 1B

## Notes
Currently the following environment variables must be set:
 * BIGQUERY_DATASET_ID: BQ dataset where combined result is stored (e.g. test_join_ehr_rdr)
 * APPLICATION_ID: GCP project ID (e.g. all-of-us-ehr-dev)
 * GOOGLE_APPLICATION_CREDENTIALS: path to service account key json file (e.g. /path/to/all-of-us-ehr-dev-abc123.json)
"""
import argparse
import json
import os
import logging

import bq_utils
import resources
import common
from googleapiclient.errors import HttpError
from resources import fields_path

BQ_WAIT_TIME = 10
PERSON_ID_MAPPING_TABLE = 'person_id_mapping_table'
VISIT_ID_MAPPING_TABLE = 'visit_id_mapping_table'

PERSON_ID_MAPPING_QUERY_SKELETON = '''
SELECT
    ROW_NUMBER() OVER() as global_person_id
    ,hpo
    ,person_id as mapping_person_id
FROM
(%(union_all_blocks)s);
'''

PERSON_ID_HPO_BLOCK = '''
(SELECT
   person_id
  ,"%(hpo)s" as hpo
FROM `%(project_id)s.%(dataset_id)s.%(hpo)s_person`)
'''

VIST_ID_MAPPING_QUERY_SKELETON = '''
SELECT
    ROW_NUMBER() OVER() as global_visit_id
    ,hpo
    ,visit_occurrence_id as mapping_visit_id
FROM
(%(union_all_blocks)s);
'''

VISIT_ID_HPO_BLOCK = '''
(SELECT
  visit_occurrence_id
  ,"%(hpo)s" as hpo
FROM `%(project_id)s.%(dataset_id)s.%(hpo)s_visit_occurrence`)
'''
PERSON_ID_MAPPING_TABLE_SUBQUERY = '''
( SELECT global_person_id, hpo, mapping_person_id
FROM
`%(project_id)s.%(dataset_id)s.%(person_id_mapping_table)s`
)
person_id_map ON t.person_id = person_id_map.mapping_person_id
AND
person_id_map.hpo = '%(hpo)s' '''

VISIT_ID_MAPPING_TABLE_SUBQUERY = '''
( SELECT global_visit_id, hpo, mapping_visit_id
FROM
`%(project_id)s.%(dataset_id)s.%(visit_id_mapping_table)s`
)
visit_id_map ON t.visit_occurrence_id = visit_id_map.mapping_visit_id
AND
visit_id_map.hpo = '%(hpo)s' '''

TABLE_NAMES = ['person', 'visit_occurrence', 'condition_occurrence', 'procedure_occurrence', 'drug_exposure',
               'device_exposure', 'measurement', 'observation', 'death']


def table_exists(project_id, dataset_id, table_id):
    """
    Determine whether a bigquery table exists
    :param table_id: id of the table
    :return: `True` if the table exists, `False` otherwise
    """
    bq_service = bq_utils.create_service()
    try:
        bq_service.tables().get(
            projectId=project_id,
            datasetId=dataset_id,
            tableId=table_id).execute()
        return True
    except HttpError, err:
        if err.resp.status != 404:
            raise
        return False


def construct_query(table_name, hpos_to_merge, hpos_with_visit, project_id, dataset_id):
    """
    Get select query for CDM table with proper qualifiers and using cdr_id for person_id
    :param table_name: name of the CDM table
    :param source_hpo: hpo names
    :param project_id: ID of the source table
    :param dataset_id: source dataset name
    :param id_offset: constant to add to *_id fields
    :param no_source: no source tables flag
    :return: the query
    """
    person_id_mapping_table = PERSON_ID_MAPPING_TABLE
    visit_id_mapping_table = VISIT_ID_MAPPING_TABLE
    source_person_id_field = 'person_id'
    json_path = os.path.join(fields_path, table_name + '.json')
    with open(json_path, 'r') as fp:
        visit_id_flag = False
        person_id_flag = False
        fields = json.load(fp)
        col_exprs = []
        for field in fields:
            field_name = field['name']
            # field_type = field['type']
            if field_name == 'person_id':
                person_id_flag = True
                col_expr = 'global_person_id as person_id'
            elif field_name == 'visit_occurrence_id':
                visit_id_flag = True
                col_expr = 'global_visit_id as visit_occurrence_id'
            # elif field_name.endswith('_id') and not field_name.endswith('concept_id') and field_type == 'integer':
            # not using this because we refer to other ids in some tables which will get overwritten
            elif field_name == table_name + '_id':
                col_expr = 'ROW_NUMBER() OVER() as %(field_name)s ' % locals()
            else:
                col_expr = field_name
            col_exprs.append(col_expr)
        col_expr_str = ',\n  '.join(col_exprs)
        q = 'SELECT\n  '
        q += ',\n  '.join(col_exprs)
        q += '\nFROM'
        q += '\n ('
        q_blocks = []
        for hpo in hpos_to_merge:
            if not table_exists(project_id, dataset_id, hpo + '_' + table_name):
                continue
            q_dum = ' ( SELECT * FROM `%(project_id)s.%(dataset_id)s.%(hpo)s_%(table_name)s` t' % locals()
            if person_id_flag:
                q_dum += '\n LEFT JOIN '
                q_dum += PERSON_ID_MAPPING_TABLE_SUBQUERY % locals()
            if visit_id_flag and hpo in hpos_with_visit:
                q_dum += '\n LEFT JOIN  '
                q_dum += VISIT_ID_MAPPING_TABLE_SUBQUERY % locals()
            q_dum += ')'
            q_blocks.append(q_dum)
        if len(q_blocks) == 0:
            return ""
        q += "\n UNION ALL \n".join(q_blocks)
        q += ')'
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


def merge(dataset_id, project_id):
    # list of hpos with person table and creating person id mapping table queries
    os.environ['BIGQUERY_DATASET_ID'] = dataset_id
    # establlishing locals()
    existing_tables = bq_utils.list_dataset_contents(dataset_id)

    hpos_to_merge = []
    for item in resources.hpo_csv() + [{'hpo_id': 'fake', 'name': 'FAKE'}]:
        hpo_id = item['hpo_id']
        if hpo_id + '_person' in existing_tables:
            hpos_to_merge.append(hpo_id)

    hpos_with_visit = []
    for item in resources.hpo_csv() + [{'hpo_id': 'fake', 'name': 'FAKE'}]:
        hpo_id = item['hpo_id']
        if hpo_id + '_visit_occurrence' in existing_tables:
            hpos_with_visit.append(hpo_id)

    hpo_queries = []
    logging.info('merging hpos: ' + ','.join(hpos_to_merge))
    for hpo in hpos_to_merge:
        hpo_queries.append(PERSON_ID_HPO_BLOCK % locals())
    union_all_blocks = 'UNION ALL'.join(hpo_queries)
    person_mapping_query = PERSON_ID_MAPPING_QUERY_SKELETON % locals()

    logging.info('Loading ' + PERSON_ID_MAPPING_TABLE)
    query_result = query(person_mapping_query,
                         destination_table_id=PERSON_ID_MAPPING_TABLE,
                         write_disposition='WRITE_TRUNCATE')
    person_mapping_query_job_id = query_result['jobReference']['jobId']
    incomplete_jobs = bq_utils.wait_on_jobs([person_mapping_query_job_id], retry_count=10)
    if len(incomplete_jobs) == 0:
        query_result = bq_utils.get_job_details(person_mapping_query_job_id)
        if 'errors' in query_result['status']:
            logging.error('{} load failed!'.format(PERSON_ID_MAPPING_TABLE))
            return "visit mapping table failed"
    else:
        logging.error('{} load taking too long!'.format(PERSON_ID_MAPPING_TABLE))
        return "visit mapping table taking too long"

    # # list of hpos with visit table and creating visit id mapping table queries
    visit_hpo_queries = []
    for hpo in hpos_with_visit:
        visit_hpo_queries.append(VISIT_ID_HPO_BLOCK % locals())
    union_all_blocks = '\n UNION ALL'.join(visit_hpo_queries)
    visit_mapping_query = VIST_ID_MAPPING_QUERY_SKELETON % locals()

    logging.info('Loading ' + VISIT_ID_MAPPING_TABLE)
    query_result = query(visit_mapping_query,
                         destination_table_id=VISIT_ID_MAPPING_TABLE,
                         write_disposition='WRITE_TRUNCATE')
    visit_mapping_query_job_id = query_result['jobReference']['jobId']
    incomplete_jobs = bq_utils.wait_on_jobs([], retry_count=10)
    if len(incomplete_jobs) == 0:
        query_result = bq_utils.get_job_details(visit_mapping_query_job_id)
        if 'errors' in query_result['status']:
            logging.error('{} load failed!'.format(VISIT_ID_MAPPING_TABLE))
            return "visit mapping table failed"
    else:
        logging.error('{} load taking too long!'.format(VISIT_ID_MAPPING_TABLE))
        return "visit mapping table failed"

    jobs_to_wait_on = []
    for table_name in common.CDM_TABLES:
        q = construct_query(table_name, hpos_to_merge, hpos_with_visit, project_id, dataset_id)
        logging.info('Merging table: ' + table_name)
        query_result = query(q, destination_table_id='merged_'+table_name, write_disposition='WRITE_TRUNCATE')
        query_job_id = query_result['jobReference']['jobId']
        jobs_to_wait_on.append(query_job_id)

    incomplete_jobs = bq_utils.wait_on_jobs(jobs_to_wait_on, retry_count=10)
    required_tables_flag = True
    if len(incomplete_jobs) == 0:
        table_errors = []
        for job_id in jobs_to_wait_on:
            query_result = bq_utils.get_job_details(job_id)
            if 'errors' in query_result['status']:
                table_errors.append(table_name)
                if table_name in common.REQUIRED_TABLES:
                    required_tables_flag = False
        if len(table_errors) == 0:
            logging.info(" ---- Merge succesful! ---- ")
            return "success: " + ','.join(hpos_to_merge)
        else:
            logging.info(" ---- Following tables fail --- " + ",".join(table_errors))
            if not required_tables_flag:
                return "required-not-done"
            return "required-done"
    else:
        raise RuntimeError("---- Merge takes too long! ---- ")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--project_id',
                        default='aou-res-curation-test',
                        help='Project containing the EHR dataset')
    parser.add_argument('--dataset_id',
                        default='test_temp',
                        help='Dataset containing a CDM from all EHR')
    args = parser.parse_args()
    project_id = args.project_id
    dataset_id = args.dataset_id
    merge(dataset_id, project_id)
