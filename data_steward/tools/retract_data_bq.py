# Python imports
import argparse
import re
import logging

# Third party imports

# Project imports
import common
import bq_utils


UNIONED_REGEX = re.compile('unioned_ehr_?\d{6}(_base|_clean)?')
COMBINED_REGEX = re.compile('combined\d{6}(_base|_clean)?')
DEID_REGEX = re.compile('.*deid.*')
EHR_REGEX = re.compile('ehr_?\d{6}')
RELEASE_REGEX = re.compile('R\d{4}Q\dR\d')

UNIONED_EHR = 'unioned_ehr_'
QUERY = 'QUERY'
DEST_TABLE = 'DEST_TABLE'
DEST_DATASET = 'DEST_DATASET'
WRITE_TRUNCATE = 'WRITE_TRUNCATE'

PERSON_DOMAIN = 56

NON_PID_TABLES = [common.CARE_SITE, common.LOCATION, common.FACT_RELATIONSHIP, common.PROVIDER]
NON_EHR_TABLES = [common.PERSON]
TABLES_WITH_PID = set(common.PII_TABLES + common.AOU_REQUIRED) - set(NON_PID_TABLES + NON_EHR_TABLES)

RETRACT_DATA_SITE_QUERY = (
    ' SELECT *'
    ' FROM `{project}.{dataset}.{hpo_id}_{table}`'
    ' WHERE person_id NOT IN {pids}'
)

RETRACT_MAPPING_DATA_UNIONED_QUERY = (
    ' SELECT *'
    ' FROM `{project}.{dataset}.{mapping_table}`'
    ' WHERE {table_id} NOT IN'
    ' (SELECT {table_id}'
    ' FROM `{project}.{dataset}.{table}`'
    ' WHERE person_id IN {pids})'
)

RETRACT_DATA_UNIONED_QUERY = (
    ' SELECT *'
    ' FROM `{project}.{dataset}.{table}`'
    ' WHERE person_id NOT IN {pids}'
)

RETRACT_MAPPING_DATA_COMBINED_QUERY = (
    ' SELECT *'
    ' FROM `{project}.{dataset}.{mapping_table}`'
    ' WHERE {table_id} NOT IN'
    ' (SELECT {table_id}'
    ' FROM `{project}.{dataset}.{table}`'
    ' WHERE person_id IN {pids}'
    ' AND {table_id} >= {CONSTANT_FACTOR})'
)

RETRACT_DATA_COMBINED_QUERY = (
    ' SELECT *'
    ' FROM `{project}.{dataset}.{table}`'
    ' WHERE person_id NOT IN {pids}'
    ' AND {table_id} >= {CONSTANT_FACTOR}'
    ' UNION ALL'
    ' SELECT *'
    ' FROM `{project}.{dataset}.{table}`'
    ' WHERE {table_id} < {CONSTANT_FACTOR}'
)

RETRACT_DATA_FACT_RELATIONSHIP = (
    ' SELECT *'
    ' FROM `{project}.{dataset}.{fact_relationship_table}`'
    ' WHERE NOT ((domain_concept_id_1 = {PERSON_DOMAIN}'
    ' AND fact_id_1 IN {pids})'
    ' OR (domain_concept_id_2 = {PERSON_DOMAIN}'
    ' AND fact_id_2 IN {pids}))'
)


def get_table_id(table):
    if table == common.DEATH:
        return common.PERSON + '_id'
    else:
        return table + '_id'


def mapping_table_for(table):
    return '_mapping_' + table


def queries_to_retract_from_ehr_dataset(project_id, dataset_id, hpo_id, pids):
    tables = list_dataset_contents(project_id, dataset_id)
    site_queries = []
    unioned_mapping_queries = []
    unioned_mapping_legacy_queries = []
    unioned_queries = []
    for table in tables:
        if table in TABLES_WITH_PID:

            q_site = dict()
            q_site[QUERY] = RETRACT_DATA_SITE_QUERY.format(
                                            project=project_id,
                                            dataset=dataset_id,
                                            hpo_id=hpo_id,
                                            pids=pids,
                                            table=table)
            q_site[DEST_TABLE] = table
            q_site[DEST_DATASET] = dataset_id
            site_queries.append(q_site)

            q_unioned_mapping = dict()
            q_unioned_mapping[QUERY] = RETRACT_MAPPING_DATA_UNIONED_QUERY.format(
                                            project=project_id,
                                            dataset=dataset_id,
                                            mapping_table=mapping_table_for(table),
                                            table_id=get_table_id(table),
                                            table=UNIONED_EHR + table,
                                            pids=pids)
            q_unioned_mapping[DEST_TABLE] = UNIONED_EHR + table
            q_unioned_mapping[DEST_DATASET] = dataset_id
            unioned_mapping_queries.append(q_unioned_mapping)

            q_unioned_mapping_legacy = dict()
            q_unioned_mapping_legacy[QUERY] = RETRACT_MAPPING_DATA_UNIONED_QUERY.format(
                                            project=project_id,
                                            dataset=dataset_id,
                                            mapping_table=UNIONED_EHR + mapping_table_for(table),
                                            table_id=get_table_id(table),
                                            table=UNIONED_EHR + table,
                                            pids=pids)
            q_unioned_mapping_legacy[DEST_TABLE] = UNIONED_EHR + table
            q_unioned_mapping_legacy[DEST_DATASET] = dataset_id
            unioned_mapping_legacy_queries.append(q_unioned_mapping_legacy)

            q_unioned = dict()
            q_unioned[QUERY] = RETRACT_DATA_UNIONED_QUERY.format(
                                            project=project_id,
                                            dataset=dataset_id,
                                            table=UNIONED_EHR + table,
                                            pids=pids)
            q_unioned[DEST_TABLE] = UNIONED_EHR + table
            q_unioned[DEST_DATASET] = dataset_id
            unioned_queries.append(q_unioned)

    q_site_fact_relationship = dict()
    q_site_fact_relationship[QUERY] = RETRACT_DATA_FACT_RELATIONSHIP.format(
                                            project=project_id,
                                            dataset=dataset_id,
                                            table=hpo_id + '_' + common.FACT_RELATIONSHIP,
                                            PERSON_DOMAIN=PERSON_DOMAIN,
                                            pids=pids)
    q_site_fact_relationship[DEST_TABLE] = hpo_id + '_' + common.FACT_RELATIONSHIP
    q_site_fact_relationship[DEST_DATASET] = dataset_id
    site_queries.append(q_site_fact_relationship)

    q_unioned_fact_relationship = dict()
    q_unioned_fact_relationship[QUERY] = RETRACT_DATA_FACT_RELATIONSHIP.format(
                                            project=project_id,
                                            dataset=dataset_id,
                                            table=UNIONED_EHR + common.FACT_RELATIONSHIP,
                                            PERSON_DOMAIN=PERSON_DOMAIN,
                                            pids=pids)
    q_unioned_fact_relationship[DEST_TABLE] = UNIONED_EHR + common.FACT_RELATIONSHIP
    q_unioned_fact_relationship[DEST_DATASET] = dataset_id
    unioned_queries.append(q_unioned_fact_relationship)

    all_ehr_queries = unioned_mapping_legacy_queries + unioned_mapping_queries + unioned_queries + site_queries
    return all_ehr_queries


def queries_to_retract_from_unioned_dataset(project_id, dataset_id, pids):
    tables = list_dataset_contents(project_id, dataset_id)
    unioned_mapping_queries = []
    unioned_queries = []
    for table in tables:
        if table in TABLES_WITH_PID:

            q_unioned_mapping = dict()
            q_unioned_mapping[QUERY] = RETRACT_MAPPING_DATA_UNIONED_QUERY.format(
                                            project=project_id,
                                            dataset=dataset_id,
                                            mapping_table=mapping_table_for(table),
                                            table_id=get_table_id(table),
                                            table=table,
                                            pids=pids)
            q_unioned_mapping[DEST_TABLE] = table
            q_unioned_mapping[DEST_DATASET] = dataset_id
            unioned_mapping_queries.append(q_unioned_mapping)

            q_unioned = dict()
            q_unioned[QUERY] = RETRACT_DATA_UNIONED_QUERY.format(
                                            project=project_id,
                                            dataset=dataset_id,
                                            table=table,
                                            pids=pids)
            q_unioned[DEST_TABLE] = table
            q_unioned[DEST_DATASET] = dataset_id
            unioned_queries.append(q_unioned)

    q_unioned_fact_relationship = dict()
    q_unioned_fact_relationship[QUERY] = RETRACT_DATA_FACT_RELATIONSHIP.format(
                                            project=project_id,
                                            dataset=dataset_id,
                                            table=common.FACT_RELATIONSHIP,
                                            PERSON_DOMAIN=PERSON_DOMAIN,
                                            pids=pids)
    q_unioned_fact_relationship[DEST_TABLE] = common.FACT_RELATIONSHIP
    q_unioned_fact_relationship[DEST_DATASET] = dataset_id
    unioned_queries.append(q_unioned_fact_relationship)

    all_unioned_queries = unioned_mapping_queries + unioned_queries
    return all_unioned_queries


def queries_to_retract_from_combined_dataset(project_id, dataset_id, pids):
    tables = list_dataset_contents(project_id, dataset_id)
    combined_mapping_queries = []
    combined_queries = []
    for table in tables:
        if table in TABLES_WITH_PID:

            q_combined_mapping = dict()
            q_combined_mapping[QUERY] = RETRACT_MAPPING_DATA_COMBINED_QUERY.format(
                                            project=project_id,
                                            dataset=dataset_id,
                                            mapping_table=mapping_table_for(table),
                                            table_id=get_table_id(table),
                                            table=table,
                                            pids=pids,
                                            CONSTANT_FACTOR=common.RDR_ID_CONSTANT
                                            + common.ID_CONSTANT_FACTOR)
            q_combined_mapping[DEST_TABLE] = table
            q_combined_mapping[DEST_DATASET] = dataset_id
            combined_mapping_queries.append(q_combined_mapping)

            q_combined = dict()
            q_combined[QUERY] = RETRACT_DATA_COMBINED_QUERY.format(
                                            project=project_id,
                                            dataset=dataset_id,
                                            table=table,
                                            pids=pids,
                                            CONSTANT_FACTOR=common.RDR_ID_CONSTANT
                                            + common.ID_CONSTANT_FACTOR)
            q_combined[DEST_TABLE] = table
            q_combined[DEST_DATASET] = dataset_id
            combined_queries.append(q_combined)

    q_combined_fact_relationship = dict()
    q_combined_fact_relationship[QUERY] = RETRACT_DATA_FACT_RELATIONSHIP.format(
                                            project=project_id,
                                            dataset=dataset_id,
                                            table=common.FACT_RELATIONSHIP,
                                            PERSON_DOMAIN=PERSON_DOMAIN,
                                            pids=pids)
    q_combined_fact_relationship[DEST_TABLE] = common.FACT_RELATIONSHIP
    q_combined_fact_relationship[DEST_DATASET] = dataset_id
    combined_queries.append(q_combined_fact_relationship)

    all_combined_queries = combined_mapping_queries + combined_queries
    return all_combined_queries


def queries_to_retract_from_deid_dataset(project_id, dataset_id, research_ids):
    tables = list_dataset_contents(project_id, dataset_id)
    deid_mapping_queries = []
    deid_queries = []
    for table in tables:
        if table in TABLES_WITH_PID:
            q_deid_mapping = dict()
            q_deid_mapping[QUERY] = RETRACT_MAPPING_DATA_COMBINED_QUERY.format(
                                            project=project_id,
                                            dataset=dataset_id,
                                            mapping_table=mapping_table_for(table),
                                            table_id=get_table_id(table),
                                            table=table,
                                            pids=research_ids,
                                            CONSTANT_FACTOR=common.RDR_ID_CONSTANT
                                            + common.ID_CONSTANT_FACTOR)
            q_deid_mapping[DEST_TABLE] = table
            q_deid_mapping[DEST_DATASET] = dataset_id
            deid_mapping_queries.append(q_deid_mapping)

            q_deid = dict()
            q_deid[QUERY] = RETRACT_DATA_COMBINED_QUERY.format(
                                            project=project_id,
                                            dataset=dataset_id,
                                            table=table,
                                            pids=research_ids,
                                            CONSTANT_FACTOR=common.RDR_ID_CONSTANT
                                            + common.ID_CONSTANT_FACTOR)
            q_deid[DEST_TABLE] = table
            q_deid[DEST_DATASET] = dataset_id
            deid_queries.append(q_deid)

    q_deid_fact_relationship = dict()
    q_deid_fact_relationship[QUERY] = RETRACT_DATA_FACT_RELATIONSHIP.format(
                                            project=project_id,
                                            dataset=dataset_id,
                                            table=common.FACT_RELATIONSHIP,
                                            PERSON_DOMAIN=PERSON_DOMAIN,
                                            pids=research_ids)
    q_deid_fact_relationship[DEST_TABLE] = common.FACT_RELATIONSHIP
    q_deid_fact_relationship[DEST_DATASET] = dataset_id
    deid_queries.append(q_deid_fact_relationship)

    all_deid_queries = deid_mapping_queries + deid_queries
    return all_deid_queries


def retraction_query_runner(queries):
    failures = 0
    for query_dict in queries:
        logging.debug('Retracting from %s.%s using query %s'
                      % (query_dict[DEST_DATASET], query_dict[DEST_TABLE], query_dict[QUERY]))
        job_results = bq_utils.query(
                            q=query_dict[QUERY],
                            destination_table_id=query_dict[DEST_TABLE],
                            write_disposition=WRITE_TRUNCATE,
                            destination_dataset_id=query_dict[DEST_DATASET])
        query_job_id = job_results['jobReference']['jobId']
        incomplete_jobs = bq_utils.wait_on_jobs([query_job_id])
        if not incomplete_jobs:
            failures += 1
            raise bq_utils.BigQueryJobWaitError(incomplete_jobs)

    if failures > 0:
        logging.debug("%d queries failed".format(failures))


def run_retraction(project_id, pids, deid_flag, hpo_id):
    datasets = list_project_contents(project_id)
    deid_datasets = []
    combined_datasets = []
    unioned_datasets = []
    ehr_datasets = []
    release_datasets = []
    for dataset in datasets:
        if re.match(UNIONED_REGEX, dataset):
            unioned_datasets.append(dataset)
        elif re.match(COMBINED_REGEX, dataset):
            combined_datasets.append(dataset)
        elif re.match(DEID_REGEX, dataset):
            deid_datasets.append(dataset)
        elif re.match(EHR_REGEX, dataset) or dataset == bq_utils.get_dataset_id():
            ehr_datasets.append(dataset)
        elif re.match(RELEASE_REGEX, dataset):
            release_datasets.append(dataset)

    if not deid_flag:
        logging.debug('Retracting from EHR datasets')
        for dataset in ehr_datasets:
            ehr_queries = queries_to_retract_from_ehr_dataset(project_id, dataset, hpo_id, pids)
            retraction_query_runner(ehr_queries)
        logging.debug('Finished retracting from EHR datasets')

        logging.debug('Retracting from UNIONED datasets')
        for dataset in unioned_datasets:
            unioned_queries = queries_to_retract_from_unioned_dataset(project_id, dataset, pids)
            retraction_query_runner(unioned_queries)
        logging.debug('Finished retracting from UNIONED datasets')

        logging.debug('Retracting from COMBINED datasets')
        for dataset in combined_datasets:
            combined_queries = queries_to_retract_from_combined_dataset(project_id, dataset, pids)
            retraction_query_runner(combined_queries)
        logging.debug('Finished retracting from COMBINED datasets')
    else:
        logging.debug('Retracting from DEID datasets')
        for dataset in deid_datasets:
            deid_queries = queries_to_retract_from_deid_dataset(project_id, dataset, pids)
            retraction_query_runner(deid_queries)
        logging.debug('Finished retracting from DEID datasets')


def list_dataset_contents(project_id, dataset_id):
    service = bq_utils.create_service()
    req = service.tables().list(projectId=project_id, datasetId=dataset_id)
    all_tables = []
    while req:
        resp = req.execute()
        items = [item['id'].split('.')[-1] for item in resp.get('tables', [])]
        all_tables.extend(items or [])
        req = service.tables().list_next(req, resp)
    return all_tables


def list_project_contents(project_id):
    service = bq_utils.create_service()
    req = service.datasets().list(projectId=project_id)
    all_datasets = []
    while req:
        resp = req.execute()
        items = [item['id'].split(':')[-1] for item in resp.get('datasets', [])]
        all_datasets.extend(items or [])
        req = service.datasets().list_next(req, resp)
    return all_datasets


def extract_pids_from_file(pid_file_name):
    pids_to_retract = []
    with open(pid_file_name) as f:
        for line in f:
            pid = line.strip()
            if pid != '':
                pids_to_retract.append(pid)
    pids_for_bq = '(' + ', '.join(pids_to_retract) + ')'
    return pids_for_bq


if __name__ == '__main__':
    parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('-p', '--project_id',
                        action='store', dest='project_id',
                        help='Identifies the project to retract data from',
                        required=True)
    parser.add_argument('-s', '--hpo_id',
                        action='store', dest='hpo_id',
                        help='Identifies the site to retract data from',
                        required=True)
    parser.add_argument('-f', '--pid_file',
                        action='store', dest='pid_file',
                        help='File containing the pids in the same directory (tools)',
                        required=True)
    parser.add_argument('-r', '--research_id', dest='deid_flag', action='store_true',
                        help='Indicates pids supplied are research ids')
    args = parser.parse_args()

    pids = extract_pids_from_file(args.pid_file)
    run_retraction(args.project_id, pids, args.deid_flag, args.hpo_id)
    logging.debug('Retraction complete')



