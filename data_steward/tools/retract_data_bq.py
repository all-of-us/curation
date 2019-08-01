# Python imports
import argparse
import re
import logging

# Third party imports

# Project imports
import common
import bq_utils
from validation import ehr_union

logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger('Data retraction logger')
# logger.setLevel(logging.DEBUG)

UNIONED_REGEX = re.compile('unioned_ehr_?\d{6}')
COMBINED_REGEX = re.compile('combined\d{6}')
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
    ' FROM `{project}.{dataset}.{table}`'
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
    ' FROM `{project}.{dataset}.{table}`'
    ' WHERE NOT ((domain_concept_id_1 = {PERSON_DOMAIN}'
    ' AND fact_id_1 IN {pids})'
    ' OR (domain_concept_id_2 = {PERSON_DOMAIN}'
    ' AND fact_id_2 IN {pids}))'
)


def get_site_table(hpo_id, table):
    return hpo_id + '_' + table


def get_table_id(table):
    if table == common.DEATH:
        return common.PERSON + '_id'
    else:
        return table + '_id'


def list_existing_tables(project_id, dataset_id):
    existing_tables = []
    all_table_objs = bq_utils.list_tables(project_id=project_id, dataset_id=dataset_id)
    for table_obj in all_table_objs:
        table_id = bq_utils.get_table_id_from_obj(table_obj)
        existing_tables.append(table_id)
    return existing_tables


def queries_to_retract_from_ehr_dataset(project_id, dataset_id, hpo_id, ids):
    """
    Get list of queries to remove all records in all tables associated with supplied ids

    :param project_id: identifies associated project
    :param dataset_id: identifies associated dataset
    :param hpo_id: identifies the HPO site
    :param pids: list of ids
    :return: list of dict with keys query, dataset, table
    """
    logger.debug('Checking existing tables for %s.%s' % (project_id, dataset_id))
    pids = int_list_to_bq(ids)
    existing_tables = list_existing_tables(project_id, dataset_id)
    site_queries = []
    unioned_mapping_queries = []
    unioned_mapping_legacy_queries = []
    unioned_queries = []
    for table in TABLES_WITH_PID:
        q_site = dict()
        q_site[DEST_DATASET] = dataset_id
        q_site[DEST_TABLE] = get_site_table(hpo_id, table)
        if q_site[DEST_TABLE] in existing_tables:
            q_site[QUERY] = RETRACT_DATA_SITE_QUERY.format(
                                            project=project_id,
                                            dataset=q_site[DEST_DATASET],
                                            table=q_site[DEST_TABLE],
                                            pids=pids)
            site_queries.append(q_site)

        q_unioned_mapping = dict()
        q_unioned_mapping[DEST_DATASET] = dataset_id
        q_unioned_mapping[DEST_TABLE] = ehr_union.mapping_table_for(table)
        if q_unioned_mapping[DEST_TABLE] in existing_tables:
            q_unioned_mapping[QUERY] = RETRACT_MAPPING_DATA_UNIONED_QUERY.format(
                                            project=project_id,
                                            dataset=q_unioned_mapping[DEST_DATASET],
                                            mapping_table=q_unioned_mapping[DEST_TABLE],
                                            table_id=get_table_id(table),
                                            table=UNIONED_EHR + table,
                                            pids=pids)
            unioned_mapping_queries.append(q_unioned_mapping)

        q_unioned_mapping_legacy = dict()
        q_unioned_mapping_legacy[DEST_DATASET] = dataset_id
        q_unioned_mapping_legacy[DEST_TABLE] = UNIONED_EHR + ehr_union.mapping_table_for(table)
        if q_unioned_mapping_legacy[DEST_TABLE] in existing_tables:
            q_unioned_mapping_legacy[QUERY] = RETRACT_MAPPING_DATA_UNIONED_QUERY.format(
                                            project=project_id,
                                            dataset=q_unioned_mapping_legacy[DEST_DATASET],
                                            mapping_table=q_unioned_mapping_legacy[DEST_TABLE],
                                            table_id=get_table_id(table),
                                            table=UNIONED_EHR + table,
                                            pids=pids)
            unioned_mapping_legacy_queries.append(q_unioned_mapping_legacy)

        q_unioned = dict()
        q_unioned[DEST_DATASET] = dataset_id
        q_unioned[DEST_TABLE] = UNIONED_EHR + table
        if q_unioned[DEST_TABLE] in existing_tables:
            q_unioned[QUERY] = RETRACT_DATA_UNIONED_QUERY.format(
                                            project=project_id,
                                            dataset=q_unioned[DEST_DATASET],
                                            table=q_unioned[DEST_TABLE],
                                            pids=pids)
            unioned_queries.append(q_unioned)

    # Remove fact_relationship records referencing retracted person_ids
    q_site_fact_relationship = dict()
    q_site_fact_relationship[DEST_DATASET] = dataset_id
    q_site_fact_relationship[DEST_TABLE] = get_site_table(hpo_id, common.FACT_RELATIONSHIP)
    if q_site_fact_relationship[DEST_TABLE] in existing_tables:
        q_site_fact_relationship[QUERY] = RETRACT_DATA_FACT_RELATIONSHIP.format(
                                                project=project_id,
                                                dataset=q_site_fact_relationship[DEST_DATASET],
                                                table=q_site_fact_relationship[DEST_TABLE],
                                                PERSON_DOMAIN=PERSON_DOMAIN,
                                                pids=pids)
        site_queries.append(q_site_fact_relationship)

    q_unioned_fact_relationship = dict()
    q_unioned_fact_relationship[DEST_DATASET] = dataset_id
    q_unioned_fact_relationship[DEST_TABLE] = UNIONED_EHR + common.FACT_RELATIONSHIP
    if q_unioned_fact_relationship[DEST_TABLE] in existing_tables:
        q_unioned_fact_relationship[QUERY] = RETRACT_DATA_FACT_RELATIONSHIP.format(
                                                project=project_id,
                                                dataset=q_unioned_fact_relationship[DEST_DATASET],
                                                table=q_unioned_fact_relationship[DEST_TABLE],
                                                PERSON_DOMAIN=PERSON_DOMAIN,
                                                pids=pids)
        unioned_queries.append(q_unioned_fact_relationship)

    all_ehr_queries = unioned_mapping_legacy_queries + unioned_mapping_queries + unioned_queries + site_queries
    return all_ehr_queries


def queries_to_retract_from_unioned_dataset(project_id, dataset_id, ids):
    # TODO include person for ehr and exclude after combine
    logger.debug('Checking existing tables for %s.%s' % (project_id, dataset_id))
    pids = int_list_to_bq(ids)
    existing_tables = list_existing_tables(project_id, dataset_id)
    unioned_mapping_queries = []
    unioned_queries = []
    for table in TABLES_WITH_PID:
        q_unioned_mapping = dict()
        q_unioned_mapping[DEST_DATASET] = dataset_id
        q_unioned_mapping[DEST_TABLE] = ehr_union.mapping_table_for(table)
        if q_unioned_mapping[DEST_TABLE] in existing_tables:
            q_unioned_mapping[QUERY] = RETRACT_MAPPING_DATA_UNIONED_QUERY.format(
                                            project=project_id,
                                            dataset=q_unioned_mapping[DEST_DATASET],
                                            mapping_table=q_unioned_mapping[DEST_TABLE],
                                            table_id=get_table_id(table),
                                            table=table,
                                            pids=pids)
            unioned_mapping_queries.append(q_unioned_mapping)

        q_unioned = dict()
        q_unioned[DEST_DATASET] = dataset_id
        q_unioned[DEST_TABLE] = table
        if q_unioned[DEST_TABLE] in existing_tables:
            q_unioned[QUERY] = RETRACT_DATA_UNIONED_QUERY.format(
                                            project=project_id,
                                            dataset=q_unioned[DEST_DATASET],
                                            table=q_unioned[DEST_TABLE],
                                            pids=pids)
            unioned_queries.append(q_unioned)

    q_unioned_fact_relationship = dict()
    q_unioned_fact_relationship[DEST_DATASET] = dataset_id
    q_unioned_fact_relationship[DEST_TABLE] = common.FACT_RELATIONSHIP
    if q_unioned_fact_relationship[DEST_TABLE] in existing_tables:
        q_unioned_fact_relationship[QUERY] = RETRACT_DATA_FACT_RELATIONSHIP.format(
                                                project=project_id,
                                                dataset=q_unioned_fact_relationship[DEST_DATASET],
                                                table=q_unioned_fact_relationship[DEST_TABLE],
                                                PERSON_DOMAIN=PERSON_DOMAIN,
                                                pids=pids)
        unioned_queries.append(q_unioned_fact_relationship)

    all_unioned_queries = unioned_mapping_queries + unioned_queries
    return all_unioned_queries


def queries_to_retract_from_combined_or_deid_dataset(project_id, dataset_id, ids):
    pids = int_list_to_bq(ids)
    logger.debug('Checking existing tables for %s.%s' % (project_id, dataset_id))
    existing_tables = list_existing_tables(project_id, dataset_id)
    combined_mapping_queries = []
    combined_queries = []
    for table in TABLES_WITH_PID:
        q_combined_mapping = dict()
        q_combined_mapping[DEST_DATASET] = dataset_id
        q_combined_mapping[DEST_TABLE] = ehr_union.mapping_table_for(table)
        if q_combined_mapping[DEST_TABLE] in existing_tables:
            q_combined_mapping[QUERY] = RETRACT_MAPPING_DATA_COMBINED_QUERY.format(
                                            project=project_id,
                                            dataset=q_combined_mapping[DEST_DATASET],
                                            mapping_table=q_combined_mapping[DEST_TABLE],
                                            table_id=get_table_id(table),
                                            table=table,
                                            pids=pids,
                                            CONSTANT_FACTOR=common.RDR_ID_CONSTANT
                                            + common.ID_CONSTANT_FACTOR)
            combined_mapping_queries.append(q_combined_mapping)

        q_combined = dict()
        q_combined[DEST_DATASET] = dataset_id
        q_combined[DEST_TABLE] = table
        if q_combined[DEST_TABLE] in existing_tables:
            q_combined[QUERY] = RETRACT_DATA_COMBINED_QUERY.format(
                                            project=project_id,
                                            dataset=q_combined[DEST_DATASET],
                                            table=q_combined[DEST_TABLE],
                                            pids=pids,
                                            table_id=get_table_id(table),
                                            CONSTANT_FACTOR=common.RDR_ID_CONSTANT
                                            + common.ID_CONSTANT_FACTOR)
            combined_queries.append(q_combined)

    q_combined_fact_relationship = dict()
    q_combined_fact_relationship[DEST_DATASET] = dataset_id
    q_combined_fact_relationship[DEST_TABLE] = common.FACT_RELATIONSHIP
    if q_combined_fact_relationship[DEST_TABLE] in existing_tables:
        q_combined_fact_relationship[QUERY] = RETRACT_DATA_FACT_RELATIONSHIP.format(
                                                project=project_id,
                                                dataset=q_combined_fact_relationship[DEST_DATASET],
                                                table=q_combined_fact_relationship[DEST_TABLE],
                                                PERSON_DOMAIN=PERSON_DOMAIN,
                                                pids=pids)
        combined_queries.append(q_combined_fact_relationship)

    all_combined_queries = combined_mapping_queries + combined_queries
    return all_combined_queries


def retraction_query_runner(queries):
    failures = 0
    for query_dict in queries:
        logger.debug('Retracting from %s.%s using query %s'
                     % (query_dict[DEST_DATASET], query_dict[DEST_TABLE], query_dict[QUERY]))
        job_results = bq_utils.query(
                            q=query_dict[QUERY],
                            destination_table_id=query_dict[DEST_TABLE],
                            write_disposition=WRITE_TRUNCATE,
                            destination_dataset_id=query_dict[DEST_DATASET])
        query_job_id = job_results['jobReference']['jobId']
        incomplete_jobs = bq_utils.wait_on_jobs([query_job_id])
        if incomplete_jobs:
            failures += 1
            raise bq_utils.BigQueryJobWaitError(incomplete_jobs)

    if failures > 0:
        logger.debug("%d queries failed" % failures)


def is_deid_dataset(dataset_id):
    return bool(re.match(DEID_REGEX, dataset_id))


def is_combined_dataset(dataset_id):
    if is_deid_dataset(dataset_id):
        return False
    return bool(re.match(COMBINED_REGEX, dataset_id))


def is_unioned_dataset(dataset_id):
    return bool(re.match(UNIONED_REGEX, dataset_id))


def is_ehr_dataset(dataset_id):
    return bool(re.match(EHR_REGEX, dataset_id)) or dataset_id == bq_utils.get_dataset_id()


def int_list_to_bq(l):
    str_l = map(str, l)
    return "(%s)" % ', '.join(str_l)


def run_retraction(project_id, pids, deid_flag, hpo_id):
    dataset_objs = bq_utils.list_datasets(project_id)
    dataset_ids = []
    for dataset_obj in dataset_objs:
        dataset = bq_utils.get_dataset_id_from_obj(dataset_obj)
        dataset_ids.append(dataset)
    logger.debug('Found datasets to retract from: %s' % ', '.join(dataset_ids))
    deid_datasets = []
    combined_datasets = []
    unioned_datasets = []
    ehr_datasets = []
    for dataset in dataset_ids:
        if is_deid_dataset(dataset):
            deid_datasets.append(dataset)
        elif is_combined_dataset(dataset):
            combined_datasets.append(dataset)
        elif is_unioned_dataset(dataset):
            unioned_datasets.append(dataset)
        elif is_ehr_dataset(dataset):
            ehr_datasets.append(dataset)

    if deid_flag:
        logger.debug('Retracting from DEID datasets: %s' % ', '.join(deid_datasets))
        for dataset in deid_datasets:
            deid_queries = queries_to_retract_from_combined_or_deid_dataset(project_id, dataset, pids)
            retraction_query_runner(deid_queries)
        logger.debug('Finished retracting from DEID datasets')
    else:
        logger.debug('Retracting from EHR datasets: %s' % ', '.join(ehr_datasets))
        for dataset in ehr_datasets:
            ehr_queries = queries_to_retract_from_ehr_dataset(project_id, dataset, hpo_id, pids)
            retraction_query_runner(ehr_queries)
        logger.debug('Finished retracting from EHR datasets')

        logger.debug('Retracting from UNIONED datasets: %s' % ', '.join(unioned_datasets))
        for dataset in unioned_datasets:
            unioned_queries = queries_to_retract_from_unioned_dataset(project_id, dataset, pids)
            retraction_query_runner(unioned_queries)
        logger.debug('Finished retracting from UNIONED datasets')

        logger.debug('Retracting from COMBINED datasets: %s' % ', '.join(combined_datasets))
        for dataset in combined_datasets:
            combined_queries = queries_to_retract_from_combined_or_deid_dataset(project_id, dataset, pids)
            retraction_query_runner(combined_queries)
        logger.debug('Finished retracting from COMBINED datasets')


def to_int(val, default=None):
    """
    Convert numeric string value to int and return default value if invalid int

    :param val: the numeric string value
    :param default: the default value
    :return: int if conversion successful, None otherwise
    """
    try:
        return int(val)
    except (ValueError, TypeError):
        return default


def extract_pids_from_file(pid_file_name):
    """
    Read specified text file and return list of person_id

    :param pid_file_name: name of the file
    :return: list of int (person_id)
    """
    pids_to_retract = []
    with open(pid_file_name) as f:
        for line in f:
            pid = to_int(line.strip())
            if pid is None:
                logger.warning('Found invalid person_id "%s", skipping.' % line)
            else:
                pids_to_retract.append(pid)
    return pids_to_retract


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
                        help='Text file containing the pids on separate lines',
                        required=True)
    parser.add_argument('-r', '--research_id', dest='deid_flag', action='store_true',
                        help='Indicates pids supplied are research ids')
    args = parser.parse_args()

    pids = extract_pids_from_file(args.pid_file)
    logger.debug('Found the following pids to retract: %s' % pids)
    run_retraction(args.project_id, pids, args.deid_flag, args.hpo_id)
    logger.debug('Retraction complete')

