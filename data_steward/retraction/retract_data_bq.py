"""
This script retracts rows for specified pids from tables in specific types of datasets in the project
The pids must be specified via a pid table containing a person_id and research_id
The pid table must be located in the sandbox_dataset
The schema for the pid table is specified under PID_TABLE_FIELDS
Retraction is performed for each dataset based on its category
"""
# Python imports
import argparse
import logging

# Third party imports

# Project imports
from utils import pipeline_logging, bq
import common
from common import JINJA_ENV, ID_CONSTANT_FACTOR
from retraction import retract_utils as ru

LOGGER = logging.getLogger(__name__)

UNIONED_EHR = 'unioned_ehr_'
SITE = 'site'
UNIONED = 'unioned'
TABLES = 'tables'

PERSON_ID = 'person_id'
RESEARCH_ID = 'research_id'

RETRACTION_RDR_EHR = 'rdr_and_ehr'
RETRACTION_EHR = 'only_ehr'

NONE_STR = 'none'

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

PERSON_ID_QUERY = """
SELECT
  {{person_research_id}}
FROM `{{pid_project}}.{{sandbox_dataset_id}}.{{pid_table_id}}`
"""

ID_CONST_CONDITION = """
AND {{table_id}} > {{id_constant}}"""

RETRACT_DATA_TABLE_QUERY = """
DELETE
FROM `{{project}}.{{dataset}}.{{table}}`
WHERE person_id IN (
  {{person_id_query}}
)
{% if id_const_condition is defined %}{{id_const_condition}}
{% endif %}
"""

RETRACT_DATA_FACT_RELATIONSHIP = """
DELETE
FROM `{{project}}.{{dataset}}.{{table}}`
WHERE (
  (
    domain_concept_id_1 = {{PERSON_DOMAIN}}
    AND fact_id_1 IN (
      {{person_id_query}}
    )
  )
  OR
  (
    domain_concept_id_2 = {{PERSON_DOMAIN}}
    AND fact_id_2 IN (
      {{person_id_query}}
    )
  )
)"""

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
    """
    Return hpo table for site

    :param hpo_id: identifies the hpo site as str
    :param table: identifies the cdm table as str
    :return: cdm table name for the site as str
    """
    return f'{hpo_id}_{table}'


def get_table_id(table):
    """
    Returns primary key for the CDM table

    :param table: CDM table as str
    :return: primary key as str
    """
    return f'{common.PERSON}_id' if table == common.DEATH else f'{table}_id'


def queries_to_retract_from_ehr_dataset(client, project_id, dataset_id, hpo_id,
                                        person_id_query):
    """
    Get list of queries to remove all records in all tables associated with supplied ids

    :param client: bigquery client
    :param project_id: identifies associated project
    :param dataset_id: identifies associated dataset
    :param hpo_id: identifies the HPO site
    :param person_id_query: query to select person_ids to retract
    :return: list of queries to run
    """
    LOGGER.info(f'Checking existing tables for {project_id}.{dataset_id}')
    existing_tables = [
        table.table_id
        for table in client.list_tables(f'{project_id}.{dataset_id}')
    ]
    queries = {SITE: [], UNIONED: []}
    tables_to_retract = TABLES_FOR_RETRACTION | set(NON_EHR_TABLES)
    for table in tables_to_retract:
        table_names = {
            SITE: get_site_table(hpo_id, table),
            UNIONED: UNIONED_EHR + table
        }
        for table_type in [SITE, UNIONED]:
            if table_names[table_type] in existing_tables:
                q_site = JINJA_ENV.from_string(RETRACT_DATA_TABLE_QUERY).render(
                    project=project_id,
                    dataset=dataset_id,
                    table=table_names[table_type],
                    person_id_query=person_id_query)
                queries[table_type].append(q_site)

    # Remove fact_relationship records referencing retracted person_ids
    fact_rel_table_names = {
        SITE: get_site_table(hpo_id, common.FACT_RELATIONSHIP),
        UNIONED: UNIONED_EHR + common.FACT_RELATIONSHIP
    }
    for table_type in [SITE, UNIONED]:
        if fact_rel_table_names[table_type] in existing_tables:
            q_site_fact_relationship = JINJA_ENV.from_string(
                RETRACT_DATA_FACT_RELATIONSHIP).render(
                    project=project_id,
                    dataset=dataset_id,
                    table=fact_rel_table_names[table_type],
                    PERSON_DOMAIN=PERSON_DOMAIN,
                    person_id_query=person_id_query)
            queries[table_type].append(q_site_fact_relationship)

    return queries[UNIONED] + queries[SITE]


def queries_to_retract_from_dataset(client,
                                    project_id,
                                    dataset_id,
                                    person_id_query,
                                    retraction_type=None):
    """
    Get list of queries to remove all records in all tables associated with supplied ids

    :param client: bigquery client
    :param project_id: identifies associated project
    :param dataset_id: identifies associated dataset
    :param person_id_query: query to select person_ids to retract
    :param retraction_type: string indicating whether all data needs to be removed, including RDR,
        or if RDR data needs to be kept intact. Can take the values 'rdr_and_ehr' or 'only_ehr'
    :return: list of dict with keys query, dataset, table
    """
    LOGGER.info(f'Checking existing tables for {project_id}.{dataset_id}')
    existing_tables = [
        table.table_id
        for table in client.list_tables(f'{project_id}.{dataset_id}')
    ]
    queries = {TABLES: []}
    tables_to_retract = set(list(TABLES_FOR_RETRACTION))
    # Ignore RDR rows using id constant factor if retraction type is 'only_ehr'
    id_const = 2 * ID_CONSTANT_FACTOR
    if ru.is_unioned_dataset(
            dataset_id) or retraction_type == RETRACTION_RDR_EHR:
        tables_to_retract |= set(NON_EHR_TABLES)
        id_const = 0
    for table in tables_to_retract:
        id_const_condition = JINJA_ENV.from_string(ID_CONST_CONDITION).render(
            table_id=get_table_id(table), id_constant=id_const)
        if table in existing_tables:
            if table in [common.DEATH, common.PERSON]:
                q_dataset = JINJA_ENV.from_string(
                    RETRACT_DATA_TABLE_QUERY).render(
                        project=project_id,
                        dataset=dataset_id,
                        table=table,
                        person_id_query=person_id_query)
                queries[TABLES].append(q_dataset)
            else:
                q_dataset = JINJA_ENV.from_string(
                    RETRACT_DATA_TABLE_QUERY).render(
                        project=project_id,
                        dataset=dataset_id,
                        table=table,
                        table_id=get_table_id(table),
                        person_id_query=person_id_query,
                        id_const_condition=id_const_condition)
                queries[TABLES].append(q_dataset)

    table = common.FACT_RELATIONSHIP
    if table in existing_tables:
        q_fact_relationship = JINJA_ENV.from_string(
            RETRACT_DATA_FACT_RELATIONSHIP).render(
                project=project_id,
                dataset=dataset_id,
                table=table,
                PERSON_DOMAIN=PERSON_DOMAIN,
                person_id_query=person_id_query)
        queries[TABLES].append(q_fact_relationship)

    return queries[TABLES]


def retraction_query_runner(client, queries):
    for query in queries:
        job = client.query(query)
        LOGGER.info(f'Running query for job_id {job.job_id}')
        result = job.result()
        LOGGER.info(
            f'Removed {job.num_dml_affected_rows} rows for job_id {job.job_id}')
    return


def run_bq_retraction(project_id, sandbox_dataset_id, pid_project_id,
                      pid_table_id, hpo_id, dataset_ids_list, retraction_type):
    """
    Main function to perform retraction
    pid table must follow schema described above in PID_TABLE_FIELDS and must reside in sandbox_dataset_id
    This function removes rows from all tables containing person_ids if they exist in pid_table_id

    :param project_id: project to retract from
    :param sandbox_dataset_id: identifies the dataset containing the pid table
    :param pid_project_id: identifies the dataset containing the sandbox dataset
    :param pid_table_id: table containing the person_ids and research_ids
    :param hpo_id: hpo_id of the site to retract from
    :param dataset_ids_list: list of datasets to retract from separated by a space. If containing only 'all_datasets',
        retracts from all datasets. If containing only 'none', skips retraction from BigQuery datasets
    :param retraction_type: string indicating whether all data needs to be removed, including RDR,
        or if RDR data needs to be kept intact. Can take the values 'rdr_and_ehr' or 'only_ehr'
    :return:
    """
    client = bq.get_client(project_id)
    dataset_ids = ru.get_datasets_list(project_id, dataset_ids_list)

    queries = []
    for dataset in dataset_ids:
        if ru.is_deid_dataset(dataset):
            LOGGER.info(f"Retracting from DEID dataset {dataset}")
            research_id_query = JINJA_ENV.from_string(PERSON_ID_QUERY).render(
                person_research_id=RESEARCH_ID,
                pid_project=pid_project_id,
                sandbox_dataset_id=sandbox_dataset_id,
                pid_table_id=pid_table_id)
            queries = queries_to_retract_from_dataset(client, project_id,
                                                      dataset,
                                                      research_id_query,
                                                      retraction_type)
        else:
            person_id_query = JINJA_ENV.from_string(PERSON_ID_QUERY).render(
                person_research_id=PERSON_ID,
                pid_project=pid_project_id,
                sandbox_dataset_id=sandbox_dataset_id,
                pid_table_id=pid_table_id)
            if ru.is_combined_dataset(dataset):
                LOGGER.info(f"Retracting from Combined dataset {dataset}")
                queries = queries_to_retract_from_dataset(
                    client, project_id, dataset, person_id_query)
            elif ru.is_unioned_dataset(dataset):
                LOGGER.info(f"Retracting from Unioned dataset {dataset}")
                queries = queries_to_retract_from_dataset(
                    client, project_id, dataset, person_id_query)
            elif ru.is_ehr_dataset(dataset):
                if hpo_id == NONE_STR:
                    LOGGER.info(
                        f'"RETRACTION_HPO_ID" set to "{NONE_STR}", skipping retraction from {dataset}'
                    )
                else:
                    LOGGER.info(f"Retracting from EHR dataset {dataset}")
                    queries = queries_to_retract_from_ehr_dataset(
                        client, project_id, dataset, hpo_id, person_id_query)
        retraction_query_runner(client, queries)
    LOGGER.info('Retraction complete')
    return


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
        nargs='+',
        help=(
            'Identifies the datasets to retract from, separated by spaces '
            'specified as -d dataset_id_1 dataset_id_2 dataset_id_3 and so on. '
            f'If set as -d {NONE_STR}, skips retraction from BigQuery datasets. '
            'If set as -d all_datasets, retracts from all datasets in project.'
        ),
        required=True)
    parser.add_argument(
        '-r',
        '--retraction_type',
        action='store',
        dest='retraction_type',
        help=(
            f'Identifies whether all data needs to be removed, including RDR, '
            f'or if RDR data needs to be kept intact. Can take the values '
            f'"{RETRACTION_RDR_EHR}" or "{RETRACTION_EHR}"'),
        required=True)
    args = parser.parse_args()

    run_bq_retraction(args.project_id, args.sandbox_dataset_id,
                      args.pid_project_id, args.pid_table_id, args.hpo_id,
                      args.dataset_ids, args.retraction_type)
