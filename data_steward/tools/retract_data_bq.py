# Python imports
import argparse
import re
import logging

# Third party imports

# Project imports
import common
import bq_utils
from validation import ehr_union
from io import open

logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger('Data retraction logger')
# logger.setLevel(logging.INFO)

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

PERSON_ID = 'person_id'
RESEARCH_ID = 'research_id'

PERSON_DOMAIN = 56

NON_PID_TABLES = [common.CARE_SITE, common.LOCATION, common.FACT_RELATIONSHIP, common.PROVIDER]
# person from RDR should not be removed, but person from EHR must be
NON_EHR_TABLES = [common.PERSON]
TABLES_FOR_RETRACTION = set(common.PII_TABLES + common.AOU_REQUIRED) - set(NON_PID_TABLES + NON_EHR_TABLES)


RETRACT_DATA_SITE_QUERY = """
DELETE
FROM `{project}.{dataset}.{table}`
WHERE person_id IN (
  SELECT
    {person_research_id}
  FROM `{project}.{sandbox_dataset_id}.{pid_table_id}`
)
"""

RETRACT_MAPPING_DATA_UNIONED_QUERY = """
DELETE
FROM `{project}.{dataset}.{mapping_table}`
WHERE {table_id} IN (
  SELECT
    {table_id}
  FROM `{project}.{dataset}.{table}`
  WHERE person_id IN (
    SELECT
      {person_research_id}
    FROM `{project}.{sandbox_dataset_id}.{pid_table_id}`
  )
)
"""

RETRACT_DATA_UNIONED_QUERY = """
DELETE
FROM `{project}.{dataset}.{table}`
WHERE person_id IN (
  SELECT
    {person_research_id}
  FROM `{project}.{sandbox_dataset_id}.{pid_table_id}`
)
"""

RETRACT_MAPPING_DATA_COMBINED_QUERY = """
DELETE
FROM `{project}.{dataset}.{mapping_table}`
WHERE {table_id} IN (
  SELECT {table_id}
  FROM `{project}.{dataset}.{table}`
  WHERE person_id IN (
    SELECT
      {person_research_id}
    FROM `{project}.{sandbox_dataset_id}.{pid_table_id}`
  )
  AND {table_id} >= {CONSTANT_FACTOR}
)
"""

RETRACT_DATA_COMBINED_QUERY = """
DELETE
FROM `{project}.{dataset}.{table}`
WHERE person_id IN (
  SELECT
    {person_research_id}
  FROM `{project}.{sandbox_dataset_id}.{pid_table_id}`
)
AND {table_id} >= {CONSTANT_FACTOR}
"""

RETRACT_DATA_FACT_RELATIONSHIP = """
DELETE
FROM `{project}.{dataset}.{table}`
WHERE (
  (
    domain_concept_id_1 = {PERSON_DOMAIN}
    AND fact_id_1 IN (
      SELECT
        {person_research_id}
      FROM `{project}.{sandbox_dataset_id}.{pid_table_id}`
    )
  )
  OR
  (
    domain_concept_id_2 = {PERSON_DOMAIN}
    AND fact_id_2 IN (
      SELECT
        {person_research_id}
      FROM `{project}.{sandbox_dataset_id}.{pid_table_id}`
    )
  )
)
"""

PID_TABLE_FIELDS = [
  {
    "type": "integer",
    "name": "person_id",
    "mode": "required",
    "description": "The person_id to retract data for"
  },
  {
    "type": "integer",
    "name": "research_id",
    "mode": "nullable",
    "description": "The research_id corresponding to the person_id"
  }
]


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


def queries_to_retract_from_ehr_dataset(project_id, dataset_id, sandbox_dataset_id, hpo_id, pid_table_id):
    """
    Get list of queries to remove all records in all tables associated with supplied ids

    :param project_id: identifies associated project
    :param dataset_id: identifies associated dataset
    :param sandbox_dataset_id: identifies the dataset containing the pid table
    :param hpo_id: identifies the HPO site
    :param pid_table_id: table containing the person_ids and research_ids
    :return: list of dict with keys query, dataset, table, delete_flag
    """
    logger.info('Checking existing tables for %s.%s' % (project_id, dataset_id))
    existing_tables = list_existing_tables(project_id, dataset_id)
    site_queries = []
    unioned_mapping_queries = []
    unioned_mapping_legacy_queries = []
    unioned_queries = []
    for table in TABLES_FOR_RETRACTION:
        q_site = dict()
        q_site[DEST_DATASET] = dataset_id
        q_site[DEST_TABLE] = get_site_table(hpo_id, table)
        if q_site[DEST_TABLE] in existing_tables:
            q_site[QUERY] = RETRACT_DATA_SITE_QUERY.format(
                                                project=project_id,
                                                dataset=q_site[DEST_DATASET],
                                                table=q_site[DEST_TABLE],
                                                pid_table_id=pid_table_id,
                                                sandbox_dataset_id=sandbox_dataset_id,
                                                person_research_id=PERSON_ID)
            site_queries.append(q_site)

        # death does not have mapping table
        if table is not common.DEATH:
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
                                                    pid_table_id=pid_table_id,
                                                    sandbox_dataset_id=sandbox_dataset_id,
                                                    person_research_id=PERSON_ID)
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
                                                    pid_table_id=pid_table_id,
                                                    sandbox_dataset_id=sandbox_dataset_id,
                                                    person_research_id=PERSON_ID)
                unioned_mapping_legacy_queries.append(q_unioned_mapping_legacy)

        q_unioned = dict()
        q_unioned[DEST_DATASET] = dataset_id
        q_unioned[DEST_TABLE] = UNIONED_EHR + table
        if q_unioned[DEST_TABLE] in existing_tables:
            q_unioned[QUERY] = RETRACT_DATA_UNIONED_QUERY.format(
                                                project=project_id,
                                                dataset=q_unioned[DEST_DATASET],
                                                table=q_unioned[DEST_TABLE],
                                                pid_table_id=pid_table_id,
                                                sandbox_dataset_id=sandbox_dataset_id,
                                                person_research_id=PERSON_ID)
            unioned_queries.append(q_unioned)

    # Remove from person table
    q_site_person = dict()
    q_site_person[DEST_DATASET] = dataset_id
    q_site_person[DEST_TABLE] = get_site_table(hpo_id, common.PERSON)
    if q_site_person[DEST_TABLE] in existing_tables:
        q_site_person[QUERY] = RETRACT_DATA_SITE_QUERY.format(
                                                project=project_id,
                                                dataset=q_site_person[DEST_DATASET],
                                                table=q_site_person[DEST_TABLE],
                                                pid_table_id=pid_table_id,
                                                sandbox_dataset_id=sandbox_dataset_id,
                                                person_research_id=PERSON_ID)
        site_queries.append(q_site_person)

    q_unioned_person = dict()
    q_unioned_person[DEST_DATASET] = dataset_id
    q_unioned_person[DEST_TABLE] = UNIONED_EHR + common.PERSON
    if q_unioned_person[DEST_TABLE] in existing_tables:
        q_unioned_person[QUERY] = RETRACT_DATA_UNIONED_QUERY.format(
                                                project=project_id,
                                                dataset=q_unioned_person[DEST_DATASET],
                                                table=q_unioned_person[DEST_TABLE],
                                                pid_table_id=pid_table_id,
                                                sandbox_dataset_id=sandbox_dataset_id,
                                                person_research_id=PERSON_ID)
        unioned_queries.append(q_unioned_person)

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
                                                    pid_table_id=pid_table_id,
                                                    sandbox_dataset_id=sandbox_dataset_id,
                                                    person_research_id=PERSON_ID)
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
                                                    pid_table_id=pid_table_id,
                                                    sandbox_dataset_id=sandbox_dataset_id,
                                                    person_research_id=PERSON_ID)
        unioned_queries.append(q_unioned_fact_relationship)

    return unioned_mapping_legacy_queries + unioned_mapping_queries, unioned_queries + site_queries


def queries_to_retract_from_unioned_dataset(project_id, dataset_id, sandbox_dataset_id, pid_table_id):
    """
    Get list of queries to remove all records in all tables associated with supplied ids

    :param project_id: identifies associated project
    :param dataset_id: identifies associated dataset
    :param sandbox_dataset_id: identifies the dataset containing the pid table
    :param pid_table_id: table containing the person_ids and research_ids
    :return: list of dict with keys query, dataset, table
    """
    logger.info('Checking existing tables for %s.%s' % (project_id, dataset_id))
    existing_tables = list_existing_tables(project_id, dataset_id)
    unioned_mapping_queries = []
    unioned_queries = []
    for table in TABLES_FOR_RETRACTION:
        if table is not common.DEATH:
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
                                                    pid_table_id=pid_table_id,
                                                    sandbox_dataset_id=sandbox_dataset_id,
                                                    person_research_id=PERSON_ID)
                unioned_mapping_queries.append(q_unioned_mapping)

        q_unioned = dict()
        q_unioned[DEST_DATASET] = dataset_id
        q_unioned[DEST_TABLE] = table
        if q_unioned[DEST_TABLE] in existing_tables:
            q_unioned[QUERY] = RETRACT_DATA_UNIONED_QUERY.format(
                                                    project=project_id,
                                                    dataset=q_unioned[DEST_DATASET],
                                                    table=q_unioned[DEST_TABLE],
                                                    pid_table_id=pid_table_id,
                                                    sandbox_dataset_id=sandbox_dataset_id,
                                                    person_research_id=PERSON_ID)
            unioned_queries.append(q_unioned)

    # retract from person
    q_unioned_person = dict()
    q_unioned_person[DEST_DATASET] = dataset_id
    q_unioned_person[DEST_TABLE] = common.PERSON
    if q_unioned_person[DEST_TABLE] in existing_tables:
        q_unioned_person[QUERY] = RETRACT_DATA_UNIONED_QUERY.format(
                                                    project=project_id,
                                                    dataset=q_unioned_person[DEST_DATASET],
                                                    table=q_unioned_person[DEST_TABLE],
                                                    pid_table_id=pid_table_id,
                                                    sandbox_dataset_id=sandbox_dataset_id,
                                                    person_research_id=PERSON_ID)
        unioned_queries.append(q_unioned_person)

    q_unioned_fact_relationship = dict()
    q_unioned_fact_relationship[DEST_DATASET] = dataset_id
    q_unioned_fact_relationship[DEST_TABLE] = common.FACT_RELATIONSHIP
    if q_unioned_fact_relationship[DEST_TABLE] in existing_tables:
        q_unioned_fact_relationship[QUERY] = RETRACT_DATA_FACT_RELATIONSHIP.format(
                                                    project=project_id,
                                                    dataset=q_unioned_fact_relationship[DEST_DATASET],
                                                    table=q_unioned_fact_relationship[DEST_TABLE],
                                                    PERSON_DOMAIN=PERSON_DOMAIN,
                                                    pid_table_id=pid_table_id,
                                                    sandbox_dataset_id=sandbox_dataset_id,
                                                    person_research_id=PERSON_ID)
        unioned_queries.append(q_unioned_fact_relationship)

    return unioned_mapping_queries, unioned_queries


def queries_to_retract_from_combined_or_deid_dataset(project_id,
                                                     dataset_id,
                                                     sandbox_dataset_id,
                                                     pid_table_id,
                                                     deid_flag):
    """
    Get list of queries to remove all records in all tables associated with supplied ids

    :param project_id: identifies associated project
    :param dataset_id: identifies associated dataset
    :param sandbox_dataset_id: identifies the dataset containing the pid table
    :param pid_table_id: table containing the person_ids and research_ids
    :param deid_flag: flag indicating if running on a deid dataset
    :return: list of dict with keys query, dataset, table
    """
    logger.info('Checking existing tables for %s.%s' % (project_id, dataset_id))
    existing_tables = list_existing_tables(project_id, dataset_id)
    combined_mapping_queries = []
    combined_queries = []
    for table in TABLES_FOR_RETRACTION:
        if table is not common.DEATH:
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
                                                    pid_table_id=pid_table_id,
                                                    sandbox_dataset_id=sandbox_dataset_id,
                                                    CONSTANT_FACTOR=common.RDR_ID_CONSTANT
                                                    + common.ID_CONSTANT_FACTOR,
                                                    person_research_id=RESEARCH_ID if deid_flag else PERSON_ID)
                combined_mapping_queries.append(q_combined_mapping)

        q_combined = dict()
        q_combined[DEST_DATASET] = dataset_id
        q_combined[DEST_TABLE] = table
        if q_combined[DEST_TABLE] in existing_tables:
            q_combined[QUERY] = RETRACT_DATA_COMBINED_QUERY.format(
                                                    project=project_id,
                                                    dataset=q_combined[DEST_DATASET],
                                                    table=q_combined[DEST_TABLE],
                                                    pid_table_id=pid_table_id,
                                                    table_id=get_table_id(table),
                                                    sandbox_dataset_id=sandbox_dataset_id,
                                                    CONSTANT_FACTOR=common.RDR_ID_CONSTANT
                                                    + common.ID_CONSTANT_FACTOR,
                                                    person_research_id=RESEARCH_ID if deid_flag else PERSON_ID)
            combined_queries.append(q_combined)

    # fix death query to exclude constant
    for q in combined_queries:
        if q[DEST_TABLE] is common.DEATH:
            q[QUERY] = RETRACT_DATA_UNIONED_QUERY.format(
                                                    project=project_id,
                                                    dataset=q[DEST_DATASET],
                                                    table=q[DEST_TABLE],
                                                    pid_table_id=pid_table_id,
                                                    sandbox_dataset_id=sandbox_dataset_id,
                                                    person_research_id=RESEARCH_ID if deid_flag else PERSON_ID)

    q_combined_fact_relationship = dict()
    q_combined_fact_relationship[DEST_DATASET] = dataset_id
    q_combined_fact_relationship[DEST_TABLE] = common.FACT_RELATIONSHIP
    if q_combined_fact_relationship[DEST_TABLE] in existing_tables:
        q_combined_fact_relationship[QUERY] = RETRACT_DATA_FACT_RELATIONSHIP.format(
                                                    project=project_id,
                                                    dataset=q_combined_fact_relationship[DEST_DATASET],
                                                    table=q_combined_fact_relationship[DEST_TABLE],
                                                    PERSON_DOMAIN=PERSON_DOMAIN,
                                                    pid_table_id=pid_table_id,
                                                    sandbox_dataset_id=sandbox_dataset_id,
                                                    person_research_id=RESEARCH_ID if deid_flag else PERSON_ID)
        combined_queries.append(q_combined_fact_relationship)

    return combined_mapping_queries, combined_queries


def retraction_query_runner(queries):
    query_job_ids = []
    for query_dict in queries:
        logger.info('Retracting from %s.%s using query %s'
                     % (query_dict[DEST_DATASET], query_dict[DEST_TABLE], query_dict[QUERY]))
        job_results = bq_utils.query(
                            q=query_dict[QUERY],
                            batch=True)
        rows_affected = job_results['numDmlAffectedRows']
        logger.info('%s rows deleted from %s.%s' % (rows_affected,
                                                     query_dict[DEST_DATASET],
                                                     query_dict[DEST_TABLE]))
        query_job_id = job_results['jobReference']['jobId']
        query_job_ids.append(query_job_id)

    incomplete_jobs = bq_utils.wait_on_jobs(query_job_ids)
    if incomplete_jobs:
        logger.info('Failed on {count} job ids {ids}'.format(count=len(incomplete_jobs),
                                                              ids=incomplete_jobs))
        logger.info('Terminating retraction')
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


def run_retraction(project_id, sandbox_dataset_id, pid_table_id, hpo_id, dataset_ids=None):
    """
    Main function to perform retraction
    pid table must follow schema described above in PID_TABLE_FIELDS and must reside in sandbox_dataset_id
    This function removes rows from all tables containing person_ids if they exist in pid_table_id

    :param project_id: project to retract from
    :param sandbox_dataset_id: identifies the dataset containing the pid table
    :param pid_table_id: table containing the person_ids and research_ids
    :param hpo_id: hpo_id of the site to retract from
    :param dataset_ids: datasets to retract from. If None, retracts from all datasets
    :return:
    """
    if dataset_ids is None:
        dataset_objs = bq_utils.list_datasets(project_id)
        dataset_ids = []
        for dataset_obj in dataset_objs:
            dataset = bq_utils.get_dataset_id_from_obj(dataset_obj)
            dataset_ids.append(dataset)
        logger.info('Found datasets to retract from: %s' % ', '.join(dataset_ids))
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

    logger.info('Retracting from EHR datasets: %s' % ', '.join(ehr_datasets))
    for dataset in ehr_datasets:
        ehr_mapping_queries, ehr_queries = queries_to_retract_from_ehr_dataset(project_id,
                                                                               dataset,
                                                                               sandbox_dataset_id,
                                                                               hpo_id,
                                                                               pid_table_id)
        retraction_query_runner(ehr_mapping_queries)
        retraction_query_runner(ehr_queries)
    logger.info('Finished retracting from EHR datasets')

    logger.info('Retracting from UNIONED datasets: %s' % ', '.join(unioned_datasets))
    for dataset in unioned_datasets:
        unioned_mapping_queries, unioned_queries = queries_to_retract_from_unioned_dataset(project_id,
                                                                                           dataset,
                                                                                           sandbox_dataset_id,
                                                                                           pid_table_id)
        retraction_query_runner(unioned_mapping_queries)
        retraction_query_runner(unioned_queries)
    logger.info('Finished retracting from UNIONED datasets')

    logger.info('Retracting from COMBINED datasets: %s' % ', '.join(combined_datasets))
    for dataset in combined_datasets:
        combined_mapping_queries, combined_queries = queries_to_retract_from_combined_or_deid_dataset(project_id,
                                                                                                      dataset,
                                                                                                      sandbox_dataset_id,
                                                                                                      pid_table_id,
                                                                                                      deid_flag=False)
        retraction_query_runner(combined_mapping_queries)
        retraction_query_runner(combined_queries)
    logger.info('Finished retracting from COMBINED datasets')

    # TODO ensure the correct research_ids for persons_ids are used for each deid retraction
    logger.info('Retracting from DEID datasets: %s' % ', '.join(deid_datasets))
    for dataset in deid_datasets:
        deid_mapping_queries, deid_queries = queries_to_retract_from_combined_or_deid_dataset(project_id,
                                                                                              dataset,
                                                                                              sandbox_dataset_id,
                                                                                              pid_table_id,
                                                                                              deid_flag=True)
        retraction_query_runner(deid_mapping_queries)
        retraction_query_runner(deid_queries)
    logger.info('Finished retracting from DEID datasets')


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


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Runs retraction on specified datasets or all datasets in project. '
                                                 'Uses project_id, sandbox_dataset_id and pid_table_id to determine '
                                                 'the pids to retract data for. The pid_table_id needs to contain '
                                                 'the person_id and research_id columns specified in the schema above, '
                                                 'but research_id can be null if deid has not been run yet. '
                                                 'hpo_id is used to retract from ehr datasets.',
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('-p', '--project_id',
                        action='store', dest='project_id',
                        help='Identifies the project to retract data from',
                        required=True)
    parser.add_argument('-s', '--sandbox_dataset_id',
                        action='store', dest='sandbox_dataset_id',
                        help='Identifies the dataset containing the pid table',
                        required=True)
    parser.add_argument('-t', '--pid_table_id',
                        action='store', dest='pid_table_id',
                        help='Identifies the table containing the person_ids and research_ids for retraction',
                        required=True)
    parser.add_argument('-i', '--hpo_id',
                        action='store', dest='hpo_id',
                        help='Identifies the site to retract data from',
                        required=True)
    parser.add_argument('-d', '--dataset_ids',
                        nargs='*', dest='dataset_ids',
                        help='Optional. Identifies the datasets to retract from. Format: -d dataset_1 "dataset 2" '
                             'If unspecified, retracts from all datasets in project',
                        required=False)
    args = parser.parse_args()

    run_retraction(args.project_id, args.sandbox_dataset_id, args.pid_table_id, args.hpo_id, args.dataset_ids)
    logger.info('Retraction complete')

