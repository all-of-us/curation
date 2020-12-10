"""
This script retracts rows for specified pids from tables in specific types of datasets in the project
The pids must be specified via a pid table containing a person_id and research_id
The pid table must be located in the sandbox_dataset
The schema for the pid table is specified under PID_TABLE_FIELDS
Datasets are categorized by type (ehr/unioned/combined/deid) and retraction is performed on each type of dataset
"""
# Python imports
import argparse
import logging

# Third party imports

# Project imports
from utils import pipeline_logging
import common
import bq_utils
from validation import ehr_union
from retraction import retract_utils as ru

LOGGER = logging.getLogger(__name__)

UNIONED_EHR = 'unioned_ehr_'
QUERY = 'QUERY'
DEST_TABLE = 'DEST_TABLE'
DEST_DATASET = 'DEST_DATASET'
WRITE_TRUNCATE = 'WRITE_TRUNCATE'

PERSON_ID = 'person_id'
RESEARCH_ID = 'research_id'

PERSON_DOMAIN = 56

NON_PID_TABLES = [
    common.CARE_SITE, common.LOCATION, common.FACT_RELATIONSHIP, common.PROVIDER
]
OTHER_PID_TABLES = [common.OBSERVATION_PERIOD]

# person from RDR should not be removed, but person from EHR must be
NON_EHR_TABLES = [common.PERSON]
TABLES_FOR_RETRACTION = set(common.PII_TABLES + common.AOU_REQUIRED +
                            OTHER_PID_TABLES) - set(NON_PID_TABLES +
                                                    NON_EHR_TABLES)

RETRACT_DATA_SITE_QUERY = """
DELETE
FROM `{project}.{dataset}.{table}`
WHERE person_id IN (
  SELECT
    {person_research_id}
  FROM `{pid_project}.{sandbox_dataset_id}.{pid_table_id}`
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
    FROM `{pid_project}.{sandbox_dataset_id}.{pid_table_id}`
  )
)
"""

RETRACT_DATA_UNIONED_QUERY = """
DELETE
FROM `{project}.{dataset}.{table}`
WHERE person_id IN (
  SELECT
    {person_research_id}
  FROM `{pid_project}.{sandbox_dataset_id}.{pid_table_id}`
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
    FROM `{pid_project}.{sandbox_dataset_id}.{pid_table_id}`
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
  FROM `{pid_project}.{sandbox_dataset_id}.{pid_table_id}`
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
      FROM `{pid_project}.{sandbox_dataset_id}.{pid_table_id}`
    )
  )
  OR
  (
    domain_concept_id_2 = {PERSON_DOMAIN}
    AND fact_id_2 IN (
      SELECT
        {person_research_id}
      FROM `{pid_project}.{sandbox_dataset_id}.{pid_table_id}`
    )
  )
)
"""

PID_TABLE_FIELDS = [{
    "type": "integer",
    "name": "person_id",
    "mode": "required",
    "description": "The person_id to retract data for"
}, {
    "type": "integer",
    "name": "research_id",
    "mode": "nullable",
    "description": "The research_id corresponding to the person_id"
}]


def get_site_table(hpo_id, table):
    return hpo_id + '_' + table


def get_table_id(table):
    if table == common.DEATH:
        return common.PERSON + '_id'
    else:
        return table + '_id'


def list_existing_tables(project_id, dataset_id):
    existing_tables = []
    all_table_objs = bq_utils.list_tables(project_id=project_id,
                                          dataset_id=dataset_id)
    for table_obj in all_table_objs:
        table_id = bq_utils.get_table_id_from_obj(table_obj)
        existing_tables.append(table_id)
    return existing_tables


def queries_to_retract_from_ehr_dataset(project_id, dataset_id, pid_project_id,
                                        sandbox_dataset_id, hpo_id,
                                        pid_table_id):
    """
    Get list of queries to remove all records in all tables associated with supplied ids

    :param project_id: identifies associated project
    :param dataset_id: identifies associated dataset
    :param pid_project_id: identifies the project containing the sandbox dataset
    :param sandbox_dataset_id: identifies the dataset containing the pid table
    :param hpo_id: identifies the HPO site
    :param pid_table_id: table containing the person_ids and research_ids
    :return: list of dict with keys query, dataset, table, delete_flag
    """
    LOGGER.info(f'Checking existing tables for {project_id}.{dataset_id}')
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
                pid_project=pid_project_id,
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
                q_unioned_mapping[
                    QUERY] = RETRACT_MAPPING_DATA_UNIONED_QUERY.format(
                        project=project_id,
                        pid_project=pid_project_id,
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
            q_unioned_mapping_legacy[
                DEST_TABLE] = UNIONED_EHR + ehr_union.mapping_table_for(table)
            if q_unioned_mapping_legacy[DEST_TABLE] in existing_tables:
                q_unioned_mapping_legacy[
                    QUERY] = RETRACT_MAPPING_DATA_UNIONED_QUERY.format(
                        project=project_id,
                        pid_project=pid_project_id,
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
                pid_project=pid_project_id,
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
            pid_project=pid_project_id,
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
            pid_project=pid_project_id,
            dataset=q_unioned_person[DEST_DATASET],
            table=q_unioned_person[DEST_TABLE],
            pid_table_id=pid_table_id,
            sandbox_dataset_id=sandbox_dataset_id,
            person_research_id=PERSON_ID)
        unioned_queries.append(q_unioned_person)

    # Remove fact_relationship records referencing retracted person_ids
    q_site_fact_relationship = dict()
    q_site_fact_relationship[DEST_DATASET] = dataset_id
    q_site_fact_relationship[DEST_TABLE] = get_site_table(
        hpo_id, common.FACT_RELATIONSHIP)
    if q_site_fact_relationship[DEST_TABLE] in existing_tables:
        q_site_fact_relationship[QUERY] = RETRACT_DATA_FACT_RELATIONSHIP.format(
            project=project_id,
            pid_project=pid_project_id,
            dataset=q_site_fact_relationship[DEST_DATASET],
            table=q_site_fact_relationship[DEST_TABLE],
            PERSON_DOMAIN=PERSON_DOMAIN,
            pid_table_id=pid_table_id,
            sandbox_dataset_id=sandbox_dataset_id,
            person_research_id=PERSON_ID)
        site_queries.append(q_site_fact_relationship)

    q_unioned_fact_relationship = dict()
    q_unioned_fact_relationship[DEST_DATASET] = dataset_id
    q_unioned_fact_relationship[
        DEST_TABLE] = UNIONED_EHR + common.FACT_RELATIONSHIP
    if q_unioned_fact_relationship[DEST_TABLE] in existing_tables:
        q_unioned_fact_relationship[
            QUERY] = RETRACT_DATA_FACT_RELATIONSHIP.format(
                project=project_id,
                pid_project=pid_project_id,
                dataset=q_unioned_fact_relationship[DEST_DATASET],
                table=q_unioned_fact_relationship[DEST_TABLE],
                PERSON_DOMAIN=PERSON_DOMAIN,
                pid_table_id=pid_table_id,
                sandbox_dataset_id=sandbox_dataset_id,
                person_research_id=PERSON_ID)
        unioned_queries.append(q_unioned_fact_relationship)

    return unioned_mapping_legacy_queries + unioned_mapping_queries, unioned_queries + site_queries


def queries_to_retract_from_unioned_dataset(project_id, dataset_id,
                                            pid_project_id, sandbox_dataset_id,
                                            pid_table_id):
    """
    Get list of queries to remove all records in all tables associated with supplied ids

    :param project_id: identifies associated project
    :param dataset_id: identifies associated dataset
    :param pid_project_id: identifies the project containing the sandbox dataset
    :param sandbox_dataset_id: identifies the dataset containing the pid table
    :param pid_table_id: table containing the person_ids and research_ids
    :return: list of dict with keys query, dataset, table
    """
    LOGGER.info(f'Checking existing tables for {project_id}.{dataset_id}')
    existing_tables = list_existing_tables(project_id, dataset_id)
    unioned_mapping_queries = []
    unioned_queries = []
    for table in TABLES_FOR_RETRACTION:
        if table is not common.DEATH:
            q_unioned_mapping = dict()
            q_unioned_mapping[DEST_DATASET] = dataset_id
            q_unioned_mapping[DEST_TABLE] = ehr_union.mapping_table_for(table)
            if q_unioned_mapping[DEST_TABLE] in existing_tables:
                q_unioned_mapping[
                    QUERY] = RETRACT_MAPPING_DATA_UNIONED_QUERY.format(
                        project=project_id,
                        pid_project=pid_project_id,
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
                pid_project=pid_project_id,
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
            pid_project=pid_project_id,
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
        q_unioned_fact_relationship[
            QUERY] = RETRACT_DATA_FACT_RELATIONSHIP.format(
                project=project_id,
                pid_project=pid_project_id,
                dataset=q_unioned_fact_relationship[DEST_DATASET],
                table=q_unioned_fact_relationship[DEST_TABLE],
                PERSON_DOMAIN=PERSON_DOMAIN,
                pid_table_id=pid_table_id,
                sandbox_dataset_id=sandbox_dataset_id,
                person_research_id=PERSON_ID)
        unioned_queries.append(q_unioned_fact_relationship)

    return unioned_mapping_queries, unioned_queries


def queries_to_retract_from_combined_or_deid_dataset(
    project_id, dataset_id, pid_project_id, sandbox_dataset_id, pid_table_id,
    retraction_type, deid_flag):
    """
    Get list of queries to remove all records in all tables associated with supplied ids

    :param project_id: identifies associated project
    :param dataset_id: identifies associated dataset
    :param pid_project_id: identifies the project containing the sandbox dataset
    :param sandbox_dataset_id: identifies the dataset containing the pid table
    :param pid_table_id: table containing the person_ids and research_ids
    :param retraction_type: string indicating whether all data needs to be removed, including RDR,
        or if RDR data needs to be kept intact. Can take the values 'rdr_and_ehr' or 'only_ehr'
    :param deid_flag: flag indicating if running on a deid dataset
    :return: list of dict with keys query, dataset, table
    """
    LOGGER.info(f'Checking existing tables for {project_id}.{dataset_id}')
    existing_tables = list_existing_tables(project_id, dataset_id)

    # retract from ehr and rdr or only ehr
    if retraction_type == 'rdr_and_ehr':
        LOGGER.info(f'Retracting from RDR and EHR data for {dataset_id}')
        constant_factor_rdr = 0
    elif retraction_type == 'only_ehr':
        LOGGER.info(
            f'Retracting from EHR data while retaining RDR for {dataset_id}')
        constant_factor_rdr = common.RDR_ID_CONSTANT + common.ID_CONSTANT_FACTOR
    else:
        raise ValueError(f'{retraction_type} is not a valid retraction type')

    combined_mapping_queries = []
    combined_queries = []
    for table in TABLES_FOR_RETRACTION:
        if table is not common.DEATH:
            q_combined_mapping = dict()
            q_combined_mapping[DEST_DATASET] = dataset_id
            q_combined_mapping[DEST_TABLE] = ehr_union.mapping_table_for(table)
            if q_combined_mapping[DEST_TABLE] in existing_tables:
                q_combined_mapping[
                    QUERY] = RETRACT_MAPPING_DATA_COMBINED_QUERY.format(
                        project=project_id,
                        pid_project=pid_project_id,
                        dataset=q_combined_mapping[DEST_DATASET],
                        mapping_table=q_combined_mapping[DEST_TABLE],
                        table_id=get_table_id(table),
                        table=table,
                        pid_table_id=pid_table_id,
                        sandbox_dataset_id=sandbox_dataset_id,
                        CONSTANT_FACTOR=constant_factor_rdr,
                        person_research_id=RESEARCH_ID
                        if deid_flag else PERSON_ID)
                combined_mapping_queries.append(q_combined_mapping)

        q_combined = dict()
        q_combined[DEST_DATASET] = dataset_id
        q_combined[DEST_TABLE] = table
        if q_combined[DEST_TABLE] in existing_tables:
            q_combined[QUERY] = RETRACT_DATA_COMBINED_QUERY.format(
                project=project_id,
                pid_project=pid_project_id,
                dataset=q_combined[DEST_DATASET],
                table=q_combined[DEST_TABLE],
                pid_table_id=pid_table_id,
                table_id=get_table_id(table),
                sandbox_dataset_id=sandbox_dataset_id,
                CONSTANT_FACTOR=constant_factor_rdr,
                person_research_id=RESEARCH_ID if deid_flag else PERSON_ID)
            combined_queries.append(q_combined)

    if retraction_type == 'rdr_and_ehr':
        # retract from person
        q_combined_person = dict()
        q_combined_person[DEST_DATASET] = dataset_id
        q_combined_person[DEST_TABLE] = common.PERSON
        if q_combined_person[DEST_TABLE] in existing_tables:
            q_combined_person[QUERY] = RETRACT_DATA_UNIONED_QUERY.format(
                project=project_id,
                pid_project=pid_project_id,
                dataset=q_combined_person[DEST_DATASET],
                table=q_combined_person[DEST_TABLE],
                pid_table_id=pid_table_id,
                sandbox_dataset_id=sandbox_dataset_id,
                person_research_id=RESEARCH_ID if deid_flag else PERSON_ID)
            combined_queries.append(q_combined_person)

    # fix death query to exclude constant
    for q in combined_queries:
        if q[DEST_TABLE] is common.DEATH:
            q[QUERY] = RETRACT_DATA_UNIONED_QUERY.format(
                project=project_id,
                pid_project=pid_project_id,
                dataset=q[DEST_DATASET],
                table=q[DEST_TABLE],
                pid_table_id=pid_table_id,
                sandbox_dataset_id=sandbox_dataset_id,
                person_research_id=RESEARCH_ID if deid_flag else PERSON_ID)

    q_combined_fact_relationship = dict()
    q_combined_fact_relationship[DEST_DATASET] = dataset_id
    q_combined_fact_relationship[DEST_TABLE] = common.FACT_RELATIONSHIP
    if q_combined_fact_relationship[DEST_TABLE] in existing_tables:
        q_combined_fact_relationship[
            QUERY] = RETRACT_DATA_FACT_RELATIONSHIP.format(
                project=project_id,
                pid_project=pid_project_id,
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
        LOGGER.info(
            f'Retracting from {query_dict[DEST_DATASET]}.{query_dict[DEST_TABLE]} '
            f'using query {query_dict[QUERY]}')
        job_results = bq_utils.query(q=query_dict[QUERY], batch=True)
        rows_affected = job_results['numDmlAffectedRows']
        LOGGER.info(
            f'{rows_affected} rows deleted from {query_dict[DEST_DATASET]}.{query_dict[DEST_TABLE]}'
        )
        query_job_id = job_results['jobReference']['jobId']
        query_job_ids.append(query_job_id)

    incomplete_jobs = bq_utils.wait_on_jobs(query_job_ids)
    if incomplete_jobs:
        LOGGER.info(
            f'Failed on {len(incomplete_jobs)} job ids {incomplete_jobs}')
        LOGGER.info('Terminating retraction')
        raise bq_utils.BigQueryJobWaitError(incomplete_jobs)


def run_bq_retraction(project_id, sandbox_dataset_id, pid_project_id,
                      pid_table_id, hpo_id, dataset_ids_str, retraction_type):
    """
    Main function to perform retraction
    pid table must follow schema described above in PID_TABLE_FIELDS and must reside in sandbox_dataset_id
    This function removes rows from all tables containing person_ids if they exist in pid_table_id

    :param project_id: project to retract from
    :param sandbox_dataset_id: identifies the dataset containing the pid table
    :param pid_project_id: identifies the dataset containing the sandbox dataset
    :param pid_table_id: table containing the person_ids and research_ids
    :param hpo_id: hpo_id of the site to retract from
    :param dataset_ids_str: string of datasets to retract from separated by a space. If set to 'all_datasets',
        retracts from all datasets. If set to 'none', skips retraction from BigQuery datasets
    :param retraction_type: string indicating whether all data needs to be removed, including RDR,
        or if RDR data needs to be kept intact. Can take the values 'rdr_and_ehr' or 'only_ehr'
    :return:
    """
    dataset_ids = ru.get_datasets_list(project_id, dataset_ids_str)

    deid_datasets = []
    combined_datasets = []
    unioned_datasets = []
    ehr_datasets = []
    for dataset in dataset_ids:
        if ru.is_deid_dataset(dataset):
            deid_datasets.append(dataset)
        elif ru.is_combined_dataset(dataset):
            combined_datasets.append(dataset)
        elif ru.is_unioned_dataset(dataset):
            unioned_datasets.append(dataset)
        elif ru.is_ehr_dataset(dataset):
            ehr_datasets.append(dataset)

    # skip ehr datasets if hpo_id is indicated as none
    if hpo_id == 'none':
        LOGGER.info(
            '"RETRACTION_HPO_ID" set to "none", skipping retraction from EHR datasets'
        )
        ehr_datasets = []

    LOGGER.info(f"Retracting from EHR datasets: {', '.join(ehr_datasets)}")
    for dataset in ehr_datasets:
        ehr_mapping_queries, ehr_queries = queries_to_retract_from_ehr_dataset(
            project_id, dataset, pid_project_id, sandbox_dataset_id, hpo_id,
            pid_table_id)
        retraction_query_runner(ehr_mapping_queries)
        retraction_query_runner(ehr_queries)
    LOGGER.info('Finished retracting from EHR datasets')

    LOGGER.info(
        f"Retracting from UNIONED datasets: {', '.join(unioned_datasets)}")
    for dataset in unioned_datasets:
        unioned_mapping_queries, unioned_queries = queries_to_retract_from_unioned_dataset(
            project_id, dataset, pid_project_id, sandbox_dataset_id,
            pid_table_id)
        retraction_query_runner(unioned_mapping_queries)
        retraction_query_runner(unioned_queries)
    LOGGER.info('Finished retracting from UNIONED datasets')

    LOGGER.info(
        f"Retracting from COMBINED datasets: {', '.join(combined_datasets)}")
    for dataset in combined_datasets:
        combined_mapping_queries, combined_queries = queries_to_retract_from_combined_or_deid_dataset(
            project_id,
            dataset,
            pid_project_id,
            sandbox_dataset_id,
            pid_table_id,
            retraction_type,
            deid_flag=False)
        retraction_query_runner(combined_mapping_queries)
        retraction_query_runner(combined_queries)
    LOGGER.info('Finished retracting from COMBINED datasets')

    # TODO ensure the correct research_ids for persons_ids are used for each deid retraction
    LOGGER.info(f"Retracting from DEID datasets: {', '.join(deid_datasets)}")
    for dataset in deid_datasets:
        deid_mapping_queries, deid_queries = queries_to_retract_from_combined_or_deid_dataset(
            project_id,
            dataset,
            pid_project_id,
            sandbox_dataset_id,
            pid_table_id,
            retraction_type,
            deid_flag=True)
        retraction_query_runner(deid_mapping_queries)
        retraction_query_runner(deid_queries)
    LOGGER.info('Finished retracting from DEID datasets')


if __name__ == '__main__':
    pipeline_logging.configure(logging.DEBUG, add_console_handler=True)

    parser = argparse.ArgumentParser(
        description=
        'Runs retraction on specified datasets or all datasets in project. '
        'Uses project_id, sandbox_dataset_id and pid_table_id to determine '
        'the pids to retract data for. The pid_table_id needs to contain '
        'the person_id and research_id columns specified in the schema above, '
        'but research_id can be null if deid has not been run yet. '
        'hpo_id is used to retract from ehr datasets.',
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('-p',
                        '--project_id',
                        action='store',
                        dest='project_id',
                        help='Identifies the project to retract data from',
                        required=True)
    parser.add_argument(
        '-q',
        '--pid_project_id',
        action='store',
        dest='pid_project_id',
        help='Identifies the project containing the sandbox dataset',
        required=True)
    parser.add_argument('-s',
                        '--sandbox_dataset_id',
                        action='store',
                        dest='sandbox_dataset_id',
                        help='Identifies the dataset containing the pid table',
                        required=True)
    parser.add_argument(
        '-t',
        '--pid_table_id',
        action='store',
        dest='pid_table_id',
        help=
        'Identifies the table containing the person_ids and research_ids for retraction',
        required=True)
    parser.add_argument('-i',
                        '--hpo_id',
                        action='store',
                        dest='hpo_id',
                        help='Identifies the site to retract data from',
                        required=True)
    parser.add_argument(
        '-d',
        '--dataset_ids',
        action='store',
        dest='dataset_ids',
        help='Identifies the datasets to retract from, separated by spaces'
        'Format: "dataset_id_1 dataset_id_2 dataset_id_3" and so on'
        'If set to "none", skips retraction from BigQuery datasets'
        'If set to "all_datasets", retracts from all datasets in project',
        required=True)
    parser.add_argument(
        '-r',
        '--retraction_type',
        action='store',
        dest='retraction_type',
        help='Identifies whether all data needs to be removed, including RDR,'
        'or if RDR data needs to be kept intact. Can take the values "rdr_and_ehr" or "only_ehr"',
        required=True)
    args = parser.parse_args()

    run_bq_retraction(args.project_id, args.sandbox_dataset_id,
                      args.pid_project_id, args.pid_table_id, args.hpo_id,
                      args.dataset_ids, args.retraction_type)
    LOGGER.info('Retraction complete')
