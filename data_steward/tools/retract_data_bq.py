# Python imports
import argparse
import os
import re
import logging

# Third party imports

# Project imports
import common
import bq_utils
import resources
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
DELETE_FLAG = 'DELETE_FLAG'

THRESHOLD_FOR_DML = 50
PERSON_DOMAIN = 56

NON_PID_TABLES = [common.CARE_SITE, common.LOCATION, common.FACT_RELATIONSHIP, common.PROVIDER]
# person from RDR should not be removed, but person from EHR must be
NON_EHR_TABLES = [common.PERSON]
TABLES_FOR_RETRACTION = set(common.PII_TABLES + common.AOU_REQUIRED) - set(NON_PID_TABLES + NON_EHR_TABLES)

SELECT_RETRACT_DATA_SITE_QUERY = (
    ' SELECT *'
    ' FROM `{project}.{dataset}.{table}`'
    ' WHERE person_id NOT IN {pids}'
)

DELETE_RETRACT_DATA_SITE_QUERY = (
    ' DELETE'
    ' FROM `{project}.{dataset}.{table}`'
    ' WHERE person_id IN {pids}'
)

SELECT_RETRACT_MAPPING_DATA_UNIONED_QUERY = (
    ' SELECT *'
    ' FROM `{project}.{dataset}.{mapping_table}`'
    ' WHERE {table_id} NOT IN'
    ' (SELECT {table_id}'
    ' FROM `{project}.{dataset}.{table}`'
    ' WHERE person_id IN {pids})'
)

DELETE_RETRACT_MAPPING_DATA_UNIONED_QUERY = (
    ' DELETE'
    ' FROM `{project}.{dataset}.{mapping_table}`'
    ' WHERE {table_id} IN'
    ' (SELECT {table_id}'
    ' FROM `{project}.{dataset}.{table}`'
    ' WHERE person_id IN {pids})'
)

SELECT_RETRACT_DATA_UNIONED_QUERY = (
    ' SELECT *'
    ' FROM `{project}.{dataset}.{table}`'
    ' WHERE person_id NOT IN {pids}'
)

DELETE_RETRACT_DATA_UNIONED_QUERY = (
    ' DELETE'
    ' FROM `{project}.{dataset}.{table}`'
    ' WHERE person_id IN {pids}'
)

SELECT_RETRACT_MAPPING_DATA_COMBINED_QUERY = (
    ' SELECT *'
    ' FROM `{project}.{dataset}.{mapping_table}`'
    ' WHERE {table_id} NOT IN'
    ' (SELECT {table_id}'
    ' FROM `{project}.{dataset}.{table}`'
    ' WHERE person_id IN {pids}'
    ' AND {table_id} >= {CONSTANT_FACTOR})'
)

DELETE_RETRACT_MAPPING_DATA_COMBINED_QUERY = (
    ' DELETE'
    ' FROM `{project}.{dataset}.{mapping_table}`'
    ' WHERE {table_id} IN'
    ' (SELECT {table_id}'
    ' FROM `{project}.{dataset}.{table}`'
    ' WHERE person_id IN {pids}'
    ' AND {table_id} >= {CONSTANT_FACTOR})'
)

SELECT_RETRACT_DATA_COMBINED_QUERY = (
    ' SELECT *'
    ' FROM `{project}.{dataset}.{table}`'
    ' WHERE person_id NOT IN {pids}'
    ' AND {table_id} >= {CONSTANT_FACTOR}'
    ' UNION ALL'
    ' SELECT *'
    ' FROM `{project}.{dataset}.{table}`'
    ' WHERE {table_id} < {CONSTANT_FACTOR}'
)

DELETE_RETRACT_DATA_COMBINED_QUERY = (
    ' DELETE'
    ' FROM `{project}.{dataset}.{table}`'
    ' WHERE person_id IN {pids}'
    ' AND {table_id} >= {CONSTANT_FACTOR}'
)

SELECT_RETRACT_DATA_FACT_RELATIONSHIP = (
    ' SELECT *'
    ' FROM `{project}.{dataset}.{table}`'
    ' WHERE NOT ((domain_concept_id_1 = {PERSON_DOMAIN}'
    ' AND fact_id_1 IN {pids})'
    ' OR (domain_concept_id_2 = {PERSON_DOMAIN}'
    ' AND fact_id_2 IN {pids}))'
)

DELETE_RETRACT_DATA_FACT_RELATIONSHIP = (
    ' DELETE'
    ' FROM `{project}.{dataset}.{table}`'
    ' WHERE ((domain_concept_id_1 = {PERSON_DOMAIN}'
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
    :param ids: list of ids
    :return: list of dict with keys query, dataset, table, delete_flag
    """
    # If fewer pids, use DELETE statements instead of SELECT
    delete_flag = bool(len(ids) < THRESHOLD_FOR_DML)
    logger.debug('Checking existing tables for %s.%s' % (project_id, dataset_id))
    pids = int_list_to_bq(ids)
    existing_tables = list_existing_tables(project_id, dataset_id)
    site_queries = []
    unioned_mapping_queries = []
    unioned_mapping_legacy_queries = []
    unioned_queries = []
    for table in TABLES_FOR_RETRACTION:
        q_site = dict()
        q_site[DEST_DATASET] = dataset_id
        q_site[DEST_TABLE] = get_site_table(hpo_id, table)
        q_site[DELETE_FLAG] = delete_flag
        if q_site[DEST_TABLE] in existing_tables:
            if q_site[DELETE_FLAG]:
                q_site[QUERY] = DELETE_RETRACT_DATA_SITE_QUERY.format(
                                                project=project_id,
                                                dataset=q_site[DEST_DATASET],
                                                table=q_site[DEST_TABLE],
                                                pids=pids)
            else:
                q_site[QUERY] = SELECT_RETRACT_DATA_SITE_QUERY.format(
                                                project=project_id,
                                                dataset=q_site[DEST_DATASET],
                                                table=q_site[DEST_TABLE],
                                                pids=pids)
            site_queries.append(q_site)

        # death does not have mapping table
        if table is not common.DEATH:
            q_unioned_mapping = dict()
            q_unioned_mapping[DEST_DATASET] = dataset_id
            q_unioned_mapping[DEST_TABLE] = ehr_union.mapping_table_for(table)
            q_unioned_mapping[DELETE_FLAG] = delete_flag
            if q_unioned_mapping[DEST_TABLE] in existing_tables:
                if q_unioned_mapping[DELETE_FLAG]:
                    q_unioned_mapping[QUERY] = DELETE_RETRACT_MAPPING_DATA_UNIONED_QUERY.format(
                                                    project=project_id,
                                                    dataset=q_unioned_mapping[DEST_DATASET],
                                                    mapping_table=q_unioned_mapping[DEST_TABLE],
                                                    table_id=get_table_id(table),
                                                    table=UNIONED_EHR + table,
                                                    pids=pids)
                else:
                    q_unioned_mapping[QUERY] = SELECT_RETRACT_MAPPING_DATA_UNIONED_QUERY.format(
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
            q_unioned_mapping_legacy[DELETE_FLAG] = delete_flag
            if q_unioned_mapping_legacy[DEST_TABLE] in existing_tables:
                if q_unioned_mapping_legacy[DELETE_FLAG]:
                    q_unioned_mapping_legacy[QUERY] = DELETE_RETRACT_MAPPING_DATA_UNIONED_QUERY.format(
                                                    project=project_id,
                                                    dataset=q_unioned_mapping_legacy[DEST_DATASET],
                                                    mapping_table=q_unioned_mapping_legacy[DEST_TABLE],
                                                    table_id=get_table_id(table),
                                                    table=UNIONED_EHR + table,
                                                    pids=pids)
                else:
                    q_unioned_mapping_legacy[QUERY] = SELECT_RETRACT_MAPPING_DATA_UNIONED_QUERY.format(
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
        q_unioned[DELETE_FLAG] = delete_flag
        if q_unioned[DEST_TABLE] in existing_tables:
            if q_unioned[DELETE_FLAG]:
                q_unioned[QUERY] = DELETE_RETRACT_DATA_UNIONED_QUERY.format(
                                                project=project_id,
                                                dataset=q_unioned[DEST_DATASET],
                                                table=q_unioned[DEST_TABLE],
                                                pids=pids)
            else:
                q_unioned[QUERY] = SELECT_RETRACT_DATA_UNIONED_QUERY.format(
                                                project=project_id,
                                                dataset=q_unioned[DEST_DATASET],
                                                table=q_unioned[DEST_TABLE],
                                                pids=pids)
            unioned_queries.append(q_unioned)

    # Remove from person table
    q_site_person = dict()
    q_site_person[DEST_DATASET] = dataset_id
    q_site_person[DEST_TABLE] = get_site_table(hpo_id, common.PERSON)
    q_site_person[DELETE_FLAG] = delete_flag
    if q_site_person[DEST_TABLE] in existing_tables:
        if q_site_person[DELETE_FLAG]:
            q_site_person[QUERY] = DELETE_RETRACT_DATA_SITE_QUERY.format(
                                                project=project_id,
                                                dataset=q_site_person[DEST_DATASET],
                                                table=q_site_person[DEST_TABLE],
                                                pids=pids)
        else:
            q_site_person[QUERY] = SELECT_RETRACT_DATA_SITE_QUERY.format(
                                                project=project_id,
                                                dataset=q_site_person[DEST_DATASET],
                                                table=q_site_person[DEST_TABLE],
                                                pids=pids)
        site_queries.append(q_site_person)

    q_unioned_person = dict()
    q_unioned_person[DEST_DATASET] = dataset_id
    q_unioned_person[DEST_TABLE] = UNIONED_EHR + common.PERSON
    q_unioned_person[DELETE_FLAG] = delete_flag
    if q_unioned_person[DEST_TABLE] in existing_tables:
        if q_unioned_person[DELETE_FLAG]:
            q_unioned_person[QUERY] = DELETE_RETRACT_DATA_UNIONED_QUERY.format(
                                                project=project_id,
                                                dataset=q_unioned_person[DEST_DATASET],
                                                table=q_unioned_person[DEST_TABLE],
                                                pids=pids)
        else:
            q_unioned_person[QUERY] = SELECT_RETRACT_DATA_UNIONED_QUERY.format(
                                                project=project_id,
                                                dataset=q_unioned_person[DEST_DATASET],
                                                table=q_unioned_person[DEST_TABLE],
                                                pids=pids)
        unioned_queries.append(q_unioned_person)

    # Remove fact_relationship records referencing retracted person_ids
    q_site_fact_relationship = dict()
    q_site_fact_relationship[DEST_DATASET] = dataset_id
    q_site_fact_relationship[DEST_TABLE] = get_site_table(hpo_id, common.FACT_RELATIONSHIP)
    q_site_fact_relationship[DELETE_FLAG] = delete_flag
    if q_site_fact_relationship[DEST_TABLE] in existing_tables:
        if q_site_fact_relationship[DELETE_FLAG]:
            q_site_fact_relationship[QUERY] = DELETE_RETRACT_DATA_FACT_RELATIONSHIP.format(
                                                    project=project_id,
                                                    dataset=q_site_fact_relationship[DEST_DATASET],
                                                    table=q_site_fact_relationship[DEST_TABLE],
                                                    PERSON_DOMAIN=PERSON_DOMAIN,
                                                    pids=pids)
        else:
            q_site_fact_relationship[QUERY] = SELECT_RETRACT_DATA_FACT_RELATIONSHIP.format(
                                                    project=project_id,
                                                    dataset=q_site_fact_relationship[DEST_DATASET],
                                                    table=q_site_fact_relationship[DEST_TABLE],
                                                    PERSON_DOMAIN=PERSON_DOMAIN,
                                                    pids=pids)
        site_queries.append(q_site_fact_relationship)

    q_unioned_fact_relationship = dict()
    q_unioned_fact_relationship[DEST_DATASET] = dataset_id
    q_unioned_fact_relationship[DEST_TABLE] = UNIONED_EHR + common.FACT_RELATIONSHIP
    q_unioned_fact_relationship[DELETE_FLAG] = delete_flag
    if q_unioned_fact_relationship[DEST_TABLE] in existing_tables:
        if q_unioned_fact_relationship[DELETE_FLAG]:
            q_unioned_fact_relationship[QUERY] = DELETE_RETRACT_DATA_FACT_RELATIONSHIP.format(
                                                    project=project_id,
                                                    dataset=q_unioned_fact_relationship[DEST_DATASET],
                                                    table=q_unioned_fact_relationship[DEST_TABLE],
                                                    PERSON_DOMAIN=PERSON_DOMAIN,
                                                    pids=pids)
        else:
            q_unioned_fact_relationship[QUERY] = SELECT_RETRACT_DATA_FACT_RELATIONSHIP.format(
                                                    project=project_id,
                                                    dataset=q_unioned_fact_relationship[DEST_DATASET],
                                                    table=q_unioned_fact_relationship[DEST_TABLE],
                                                    PERSON_DOMAIN=PERSON_DOMAIN,
                                                    pids=pids)
        unioned_queries.append(q_unioned_fact_relationship)

    return unioned_mapping_legacy_queries + unioned_mapping_queries, unioned_queries + site_queries


def queries_to_retract_from_unioned_dataset(project_id, dataset_id, ids):
    """
    Get list of queries to remove all records in all tables associated with supplied ids

    :param project_id: identifies associated project
    :param dataset_id: identifies associated dataset
    :param ids: list of ids
    :return: list of dict with keys query, dataset, table
    """
    # If fewer pids, use DELETE statements instead of SELECT
    delete_flag = bool(len(ids) < THRESHOLD_FOR_DML)
    logger.debug('Checking existing tables for %s.%s' % (project_id, dataset_id))
    pids = int_list_to_bq(ids)
    existing_tables = list_existing_tables(project_id, dataset_id)
    unioned_mapping_queries = []
    unioned_queries = []
    for table in TABLES_FOR_RETRACTION:
        if table is not common.DEATH:
            q_unioned_mapping = dict()
            q_unioned_mapping[DEST_DATASET] = dataset_id
            q_unioned_mapping[DEST_TABLE] = ehr_union.mapping_table_for(table)
            q_unioned_mapping[DELETE_FLAG] = delete_flag
            if q_unioned_mapping[DEST_TABLE] in existing_tables:
                if q_unioned_mapping[DELETE_FLAG]:
                    q_unioned_mapping[QUERY] = DELETE_RETRACT_MAPPING_DATA_UNIONED_QUERY.format(
                                                    project=project_id,
                                                    dataset=q_unioned_mapping[DEST_DATASET],
                                                    mapping_table=q_unioned_mapping[DEST_TABLE],
                                                    table_id=get_table_id(table),
                                                    table=table,
                                                    pids=pids)
                else:
                    q_unioned_mapping[QUERY] = SELECT_RETRACT_MAPPING_DATA_UNIONED_QUERY.format(
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
        q_unioned[DELETE_FLAG] = delete_flag
        if q_unioned[DEST_TABLE] in existing_tables:
            if q_unioned[DELETE_FLAG]:
                q_unioned[QUERY] = DELETE_RETRACT_DATA_UNIONED_QUERY.format(
                                                    project=project_id,
                                                    dataset=q_unioned[DEST_DATASET],
                                                    table=q_unioned[DEST_TABLE],
                                                    pids=pids)
            else:
                q_unioned[QUERY] = SELECT_RETRACT_DATA_UNIONED_QUERY.format(
                                                    project=project_id,
                                                    dataset=q_unioned[DEST_DATASET],
                                                    table=q_unioned[DEST_TABLE],
                                                    pids=pids)
            unioned_queries.append(q_unioned)

    # retract from person
    q_unioned_person = dict()
    q_unioned_person[DEST_DATASET] = dataset_id
    q_unioned_person[DEST_TABLE] = common.PERSON
    q_unioned_person[DELETE_FLAG] = delete_flag
    if q_unioned_person[DEST_TABLE] in existing_tables:
        if q_unioned_person[DELETE_FLAG]:
            q_unioned_person[QUERY] = DELETE_RETRACT_DATA_UNIONED_QUERY.format(
                                                    project=project_id,
                                                    dataset=q_unioned_person[DEST_DATASET],
                                                    table=q_unioned_person[DEST_TABLE],
                                                    pids=pids)
        else:
            q_unioned_person[QUERY] = SELECT_RETRACT_DATA_UNIONED_QUERY.format(
                                                    project=project_id,
                                                    dataset=q_unioned_person[DEST_DATASET],
                                                    table=q_unioned_person[DEST_TABLE],
                                                    pids=pids)
        unioned_queries.append(q_unioned_person)

    q_unioned_fact_relationship = dict()
    q_unioned_fact_relationship[DEST_DATASET] = dataset_id
    q_unioned_fact_relationship[DEST_TABLE] = common.FACT_RELATIONSHIP
    q_unioned_fact_relationship[DELETE_FLAG] = delete_flag
    if q_unioned_fact_relationship[DEST_TABLE] in existing_tables:
        if q_unioned_fact_relationship[DELETE_FLAG]:
            q_unioned_fact_relationship[QUERY] = DELETE_RETRACT_DATA_FACT_RELATIONSHIP.format(
                                                    project=project_id,
                                                    dataset=q_unioned_fact_relationship[DEST_DATASET],
                                                    table=q_unioned_fact_relationship[DEST_TABLE],
                                                    PERSON_DOMAIN=PERSON_DOMAIN,
                                                    pids=pids)
        else:
            q_unioned_fact_relationship[QUERY] = SELECT_RETRACT_DATA_FACT_RELATIONSHIP.format(
                                                    project=project_id,
                                                    dataset=q_unioned_fact_relationship[DEST_DATASET],
                                                    table=q_unioned_fact_relationship[DEST_TABLE],
                                                    PERSON_DOMAIN=PERSON_DOMAIN,
                                                    pids=pids)
        unioned_queries.append(q_unioned_fact_relationship)

    return unioned_mapping_queries, unioned_queries


def queries_to_retract_from_combined_or_deid_dataset(project_id, dataset_id, ids):
    """
    Get list of queries to remove all records in all tables associated with supplied ids

    :param project_id: identifies associated project
    :param dataset_id: identifies associated dataset
    :param ids: list of ids
    :return: list of dict with keys query, dataset, table
    """
    # If fewer pids, use DELETE statements instead of SELECT
    delete_flag = bool(len(ids) < THRESHOLD_FOR_DML)
    pids = int_list_to_bq(ids)
    logger.debug('Checking existing tables for %s.%s' % (project_id, dataset_id))
    existing_tables = list_existing_tables(project_id, dataset_id)
    combined_mapping_queries = []
    combined_queries = []
    for table in TABLES_FOR_RETRACTION:
        if table is not common.DEATH:
            q_combined_mapping = dict()
            q_combined_mapping[DEST_DATASET] = dataset_id
            q_combined_mapping[DEST_TABLE] = ehr_union.mapping_table_for(table)
            q_combined_mapping[DELETE_FLAG] = delete_flag
            if q_combined_mapping[DEST_TABLE] in existing_tables:
                if q_combined_mapping[DELETE_FLAG]:
                    q_combined_mapping[QUERY] = DELETE_RETRACT_MAPPING_DATA_COMBINED_QUERY.format(
                                                    project=project_id,
                                                    dataset=q_combined_mapping[DEST_DATASET],
                                                    mapping_table=q_combined_mapping[DEST_TABLE],
                                                    table_id=get_table_id(table),
                                                    table=table,
                                                    pids=pids,
                                                    CONSTANT_FACTOR=common.RDR_ID_CONSTANT
                                                    + common.ID_CONSTANT_FACTOR)
                else:
                    q_combined_mapping[QUERY] = SELECT_RETRACT_MAPPING_DATA_COMBINED_QUERY.format(
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
        q_combined[DELETE_FLAG] = delete_flag
        if q_combined[DEST_TABLE] in existing_tables:
            if q_combined[DELETE_FLAG]:
                q_combined[QUERY] = DELETE_RETRACT_DATA_COMBINED_QUERY.format(
                                                    project=project_id,
                                                    dataset=q_combined[DEST_DATASET],
                                                    table=q_combined[DEST_TABLE],
                                                    pids=pids,
                                                    table_id=get_table_id(table),
                                                    CONSTANT_FACTOR=common.RDR_ID_CONSTANT
                                                    + common.ID_CONSTANT_FACTOR)
            else:
                q_combined[QUERY] = SELECT_RETRACT_DATA_COMBINED_QUERY.format(
                                                    project=project_id,
                                                    dataset=q_combined[DEST_DATASET],
                                                    table=q_combined[DEST_TABLE],
                                                    pids=pids,
                                                    table_id=get_table_id(table),
                                                    CONSTANT_FACTOR=common.RDR_ID_CONSTANT
                                                    + common.ID_CONSTANT_FACTOR)
            combined_queries.append(q_combined)

    # fix death query to exclude constant
    for q in combined_queries:
        if q[DEST_TABLE] is common.DEATH:
            if q[DELETE_FLAG]:
                q[QUERY] = DELETE_RETRACT_DATA_UNIONED_QUERY.format(
                                                    project=project_id,
                                                    dataset=q[DEST_DATASET],
                                                    table=q[DEST_TABLE],
                                                    pids=pids)
            else:
                q[QUERY] = SELECT_RETRACT_DATA_UNIONED_QUERY.format(
                                                    project=project_id,
                                                    dataset=q[DEST_DATASET],
                                                    table=q[DEST_TABLE],
                                                    pids=pids)

    q_combined_fact_relationship = dict()
    q_combined_fact_relationship[DEST_DATASET] = dataset_id
    q_combined_fact_relationship[DEST_TABLE] = common.FACT_RELATIONSHIP
    q_combined_fact_relationship[DELETE_FLAG] = delete_flag
    if q_combined_fact_relationship[DEST_TABLE] in existing_tables:
        if q_combined_fact_relationship[DELETE_FLAG]:
            q_combined_fact_relationship[QUERY] = DELETE_RETRACT_DATA_FACT_RELATIONSHIP.format(
                                                    project=project_id,
                                                    dataset=q_combined_fact_relationship[DEST_DATASET],
                                                    table=q_combined_fact_relationship[DEST_TABLE],
                                                    PERSON_DOMAIN=PERSON_DOMAIN,
                                                    pids=pids)
        else:
            q_combined_fact_relationship[QUERY] = SELECT_RETRACT_DATA_FACT_RELATIONSHIP.format(
                                                    project=project_id,
                                                    dataset=q_combined_fact_relationship[DEST_DATASET],
                                                    table=q_combined_fact_relationship[DEST_TABLE],
                                                    PERSON_DOMAIN=PERSON_DOMAIN,
                                                    pids=pids)
        combined_queries.append(q_combined_fact_relationship)

    return combined_mapping_queries, combined_queries


def retraction_query_runner(queries):
    query_job_ids = []
    for query_dict in queries:
        logger.debug('Retracting from %s.%s using query %s'
                     % (query_dict[DEST_DATASET], query_dict[DEST_TABLE], query_dict[QUERY]))
        if query_dict[DELETE_FLAG]:
            job_results = bq_utils.query(
                                q=query_dict[QUERY],
                                batch=True)
            rows_affected = job_results['numDmlAffectedRows']
            logger.debug('%s rows deleted from %s.%s' % (rows_affected,
                                                         query_dict[DEST_DATASET],
                                                         query_dict[DEST_TABLE]))
        else:
            job_results = bq_utils.query(
                                q=query_dict[QUERY],
                                destination_table_id=query_dict[DEST_TABLE],
                                write_disposition=WRITE_TRUNCATE,
                                destination_dataset_id=query_dict[DEST_DATASET],
                                batch=True)
        query_job_id = job_results['jobReference']['jobId']
        query_job_ids.append(query_job_id)

    incomplete_jobs = bq_utils.wait_on_jobs(query_job_ids)
    if incomplete_jobs:
        logger.debug('Failed on {count} job ids {ids}'.format(count=len(incomplete_jobs),
                                                              ids=incomplete_jobs))
        logger.debug('Terminating retraction')
        raise bq_utils.BigQueryJobWaitError(incomplete_jobs)


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


def run_retraction(project_id, pids, hpo_id, deid_flag=False):
    """
    Main function to perform retraction

    :param project_id: project to retract from
    :param pids: person/research ids
    :param hpo_id: hpo_id of the site to retract from
    :param deid_flag: Flag to indicate whether the supplied pids are person_ids or research_ids
    :return:
    """
    dataset_objs = bq_utils.list_datasets(project_id)
    dataset_ids = []
    for dataset_obj in dataset_objs:
        dataset = bq_utils.get_dataset_id_from_obj(dataset_obj)
        dataset_ids.append(dataset)
    logger.debug('Found datasets to retract from: %s' % ', '.join(dataset_ids))
    # retract from latest datasets first
    dataset_ids.sort(reverse=True)
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
            deid_mapping_queries, deid_queries = queries_to_retract_from_combined_or_deid_dataset(project_id,
                                                                                                  dataset,
                                                                                                  pids)
            retraction_query_runner(deid_mapping_queries)
            retraction_query_runner(deid_queries)
        logger.debug('Finished retracting from DEID datasets')
    else:
        logger.debug('Retracting from EHR datasets: %s' % ', '.join(ehr_datasets))
        for dataset in ehr_datasets:
            ehr_mapping_queries, ehr_queries = queries_to_retract_from_ehr_dataset(project_id,
                                                                                   dataset,
                                                                                   hpo_id,
                                                                                   pids)
            retraction_query_runner(ehr_mapping_queries)
            retraction_query_runner(ehr_queries)
        logger.debug('Finished retracting from EHR datasets')

        logger.debug('Retracting from UNIONED datasets: %s' % ', '.join(unioned_datasets))
        for dataset in unioned_datasets:
            unioned_mapping_queries, unioned_queries = queries_to_retract_from_unioned_dataset(project_id,
                                                                                               dataset,
                                                                                               pids)
            retraction_query_runner(unioned_mapping_queries)
            retraction_query_runner(unioned_queries)
        logger.debug('Finished retracting from UNIONED datasets')

        logger.debug('Retracting from COMBINED datasets: %s' % ', '.join(combined_datasets))
        for dataset in combined_datasets:
            combined_mapping_queries, combined_queries = queries_to_retract_from_combined_or_deid_dataset(project_id,
                                                                                                          dataset,
                                                                                                          pids)
            retraction_query_runner(combined_mapping_queries)
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
    pid_file_path = os.path.join(resources.tools_path, pid_file_name)
    with open(pid_file_path) as f:
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
    parser.add_argument('-i', '--pid_file',
                        action='store', dest='pid_file',
                        help='Text file containing the pids on separate lines',
                        required=True)
    parser.add_argument('-r', '--research_id', dest='deid_flag', action='store_true',
                        help='Indicates pids supplied are research ids')
    args = parser.parse_args()

    pids = extract_pids_from_file(args.pid_file)
    logger.debug('Found the following pids to retract: %s' % pids)
    run_retraction(args.project_id, pids, args.hpo_id, args.deid_flag)
    logger.debug('Retraction complete')

