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
from utils import pipeline_logging
from gcloud.bq import BigQueryClient
from common import (CARE_SITE, CATI_TABLES, DEATH, FACT_RELATIONSHIP, JINJA_ENV,
                    LOCATION, OBSERVATION_PERIOD, PERSON, PII_TABLES, PROVIDER)
from resources import mapping_table_for
from retraction import retract_utils as ru

LOGGER = logging.getLogger(__name__)

UNIONED_EHR = 'unioned_ehr_'
SITE = 'site'
UNIONED = 'unioned'
TABLES = 'tables'

PERSON_ID = 'person_id'
RESEARCH_ID = 'research_id'

RETRACTION_RDR_EHR = 'rdr_and_ehr'
RETRACTION_ONLY_EHR = 'only_ehr'

NONE_STR = 'none'

PERSON_DOMAIN = 56

NON_PID_TABLES = [CARE_SITE, LOCATION, FACT_RELATIONSHIP, PROVIDER]
OTHER_PID_TABLES = [OBSERVATION_PERIOD]

# person from RDR should not be removed, but person from EHR must be
NON_EHR_TABLES = [PERSON]
TABLES_FOR_RETRACTION = set(PII_TABLES + CATI_TABLES +
                            OTHER_PID_TABLES) - set(NON_PID_TABLES +
                                                    NON_EHR_TABLES)

PERSON_ID_QUERY = """
SELECT
  {{person_research_id}}
FROM `{{pid_project}}.{{sandbox_dataset_id}}.{{pid_table_id}}`
"""

ONLY_EHR_CONDITION = """
AND {{table}}_id IN (
    SELECT {{table}}_id FROM `{{project}}.{{dataset}}._mapping_{{table}}`
    WHERE src_hpo_id != 'rdr'
)
"""

ONLY_EHR_CONDITION_DEID = """
AND {{table}}_id IN (
    SELECT {{table}}_id FROM `{{project}}.{{dataset}}.{{table}}_ext`
    WHERE src_id LIKE 'EHR%'
)
"""

RETRACT_DATA_TABLE_QUERY = """
{% if sandbox %}
CREATE TABLE `{{project}}.{{sb_dataset}}.{{sb_table}}` AS SELECT *
{% else %}
DELETE
{% endif %}
FROM `{{project}}.{{dataset}}.{{table}}`
WHERE person_id IN (
  {{person_id_query}}
)
{% if only_ehr_condition is defined %}{{only_ehr_condition}}
{% endif %}
"""

RETRACT_DATA_FACT_RELATIONSHIP = """
{% if sandbox %}
CREATE TABLE `{{project}}.{{sb_dataset}}.{{sb_table}}` AS SELECT *
{% else %}
DELETE
{% endif %}
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
    return f'{PERSON}_id' if table == DEATH else f'{table}_id'


def queries_to_retract_from_ehr_dataset(client, dataset_id, sb_dataset_id,
                                        hpo_id, person_id_query,
                                        skip_sandboxing):
    """
    Get list of queries to remove all records in all tables associated with supplied ids

    :param client: a BigQueryClient
    :param dataset_id: identifies associated dataset
    :param sb_dataset_id: identifies sandbox dataset when skip_sandboxing==False
    :param hpo_id: identifies the HPO site
    :param person_id_query: query to select person_ids to retract
    :param skip_sandboxing: True if you wish not to sandbox the retracted data.
    :return: list of queries to run
    """
    LOGGER.info(f'Checking existing tables for {client.project}.{dataset_id}')
    existing_tables = [
        table.table_id
        for table in client.list_tables(f'{client.project}.{dataset_id}')
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
                if not skip_sandboxing:
                    q_sandbox = JINJA_ENV.from_string(
                        RETRACT_DATA_TABLE_QUERY).render(
                            project=client.project,
                            dataset=dataset_id,
                            table=table_names[table_type],
                            person_id_query=person_id_query,
                            sandbox=True,
                            sb_dataset=sb_dataset_id,
                            sb_table=
                            f'retract_{dataset_id}_{table_names[table_type]}')
                    queries[table_type].append(q_sandbox)

                q_site = JINJA_ENV.from_string(RETRACT_DATA_TABLE_QUERY).render(
                    project=client.project,
                    dataset=dataset_id,
                    table=table_names[table_type],
                    person_id_query=person_id_query)
                queries[table_type].append(q_site)

    # Remove fact_relationship records referencing retracted person_ids
    fact_rel_table_names = {
        SITE: get_site_table(hpo_id, FACT_RELATIONSHIP),
        UNIONED: UNIONED_EHR + FACT_RELATIONSHIP
    }
    for table_type in [SITE, UNIONED]:
        if fact_rel_table_names[table_type] in existing_tables:
            if not skip_sandboxing:
                q_sandbox_fact_relationship = JINJA_ENV.from_string(
                    RETRACT_DATA_FACT_RELATIONSHIP
                ).render(
                    project=client.project,
                    dataset=dataset_id,
                    table=fact_rel_table_names[table_type],
                    PERSON_DOMAIN=PERSON_DOMAIN,
                    person_id_query=person_id_query,
                    sandbox=True,
                    sb_dataset=sb_dataset_id,
                    sb_table=
                    f'retract_{dataset_id}_{fact_rel_table_names[table_type]}')

                queries[table_type].append(q_sandbox_fact_relationship)

            q_site_fact_relationship = JINJA_ENV.from_string(
                RETRACT_DATA_FACT_RELATIONSHIP).render(
                    project=client.project,
                    dataset=dataset_id,
                    table=fact_rel_table_names[table_type],
                    PERSON_DOMAIN=PERSON_DOMAIN,
                    person_id_query=person_id_query)
            queries[table_type].append(q_site_fact_relationship)

    return queries[UNIONED] + queries[SITE]


def queries_to_retract_from_dataset(client: BigQueryClient,
                                    dataset_id,
                                    sb_dataset_id,
                                    person_id_query,
                                    skip_sandboxing,
                                    retraction_type=None):
    """
    Get list of queries to remove all records in all tables associated with supplied ids

    :param client: BigQueryClient object
    :param dataset_id: identifies associated dataset
    :param sb_dataset_id: identifies sandbox dataset when skip_sandboxing==False
    :param person_id_query: query to select person_ids to retract
    :param skip_sandboxing: True if you wish not to sandbox the retracted data.
    :param retraction_type: string indicating whether all data needs to be removed, including RDR,
        or if RDR data needs to be kept intact. Can take the values 'rdr_and_ehr' or 'only_ehr'
    :return: list of dict with keys query, dataset, table
    """
    LOGGER.info(f'Checking existing tables for {client.project}.{dataset_id}')
    existing_tables = [
        table.table_id
        for table in client.list_tables(f'{client.project}.{dataset_id}')
    ]
    tables_to_retract = [
        table for table in existing_tables
        if any(col.name == 'person_id' for col in client.get_table(
            f'{client.project}.{dataset_id}.{table}').schema)
    ]
    LOGGER.info(
        f"Tables to retract in {dataset_id}:\n"
        f"Tables with person_id column... {', '.join(tables_to_retract)}\n"
        f"Tables without person_id column but need retraction... {FACT_RELATIONSHIP}\n"
    )

    queries = {TABLES: []}

    for table in tables_to_retract:

        if retraction_type == RETRACTION_ONLY_EHR:
            if table == DEATH:
                pass

            elif ru.is_deid_dataset(dataset_id):
                if not client.table_exists(f'{table}_ext', dataset_id):
                    LOGGER.info(f"{table} does not have a extension table. "
                                f"Assuming it does not contain EHR data. "
                                f"Skipping retracing {table}...")
                    continue
                only_ehr_condition = JINJA_ENV.from_string(
                    ONLY_EHR_CONDITION_DEID).render(project=client.project,
                                                    dataset=dataset_id,
                                                    table=table)

            elif ru.is_combined_dataset(dataset_id):

                if not client.table_exists(mapping_table_for(table),
                                           dataset_id):
                    LOGGER.info(f"{table} does not have a mapping table. "
                                f"Assuming it does not contain EHR data. "
                                f"Skipping retracing {table}...")
                    continue

                only_ehr_condition = JINJA_ENV.from_string(
                    ONLY_EHR_CONDITION).render(project=client.project,
                                               dataset=dataset_id,
                                               table=table)

        else:
            only_ehr_condition = ''

        if table in [DEATH, PERSON]:
            if table == PERSON and retraction_type == RETRACTION_ONLY_EHR:
                LOGGER.info(
                    f"{PERSON} has only RDR data..."
                    f"No retraction needed for {PERSON} when {RETRACTION_ONLY_EHR}"
                    f"Skipping {PERSON}")
                continue
            if not skip_sandboxing:
                q_sandbox = JINJA_ENV.from_string(
                    RETRACT_DATA_TABLE_QUERY).render(
                        project=client.project,
                        dataset=dataset_id,
                        table=table,
                        person_id_query=person_id_query,
                        sandbox=True,
                        sb_dataset=sb_dataset_id,
                        sb_table=f'retract_{dataset_id}_{table}')
                queries[TABLES].append(q_sandbox)
            q_dataset = JINJA_ENV.from_string(RETRACT_DATA_TABLE_QUERY).render(
                project=client.project,
                dataset=dataset_id,
                table=table,
                person_id_query=person_id_query)
            queries[TABLES].append(q_dataset)
        else:
            if not skip_sandboxing:
                q_sandbox = JINJA_ENV.from_string(
                    RETRACT_DATA_TABLE_QUERY).render(
                        project=client.project,
                        dataset=dataset_id,
                        table=table,
                        table_id=get_table_id(table),
                        person_id_query=person_id_query,
                        only_ehr_condition=only_ehr_condition,
                        sandbox=True,
                        sb_dataset=sb_dataset_id,
                        sb_table=f'retract_{dataset_id}_{table}')
                queries[TABLES].append(q_sandbox)
            q_dataset = JINJA_ENV.from_string(RETRACT_DATA_TABLE_QUERY).render(
                project=client.project,
                dataset=dataset_id,
                table=table,
                table_id=get_table_id(table),
                person_id_query=person_id_query,
                only_ehr_condition=only_ehr_condition)
            queries[TABLES].append(q_dataset)

    table = FACT_RELATIONSHIP
    if table in existing_tables:
        if not skip_sandboxing:
            q_sandbox = JINJA_ENV.from_string(
                RETRACT_DATA_FACT_RELATIONSHIP).render(
                    project=client.project,
                    dataset=dataset_id,
                    table=table,
                    PERSON_DOMAIN=PERSON_DOMAIN,
                    person_id_query=person_id_query,
                    sandbox=True,
                    sb_dataset=sb_dataset_id,
                    sb_table=f'retract_{dataset_id}_{table}')
            queries[TABLES].append(q_sandbox)
        q_fact_relationship = JINJA_ENV.from_string(
            RETRACT_DATA_FACT_RELATIONSHIP).render(
                project=client.project,
                dataset=dataset_id,
                table=table,
                PERSON_DOMAIN=PERSON_DOMAIN,
                person_id_query=person_id_query)
        queries[TABLES].append(q_fact_relationship)

    return queries[TABLES]


def retraction_query_runner(client, queries):
    """
    Runs the retraction queries one by one.
    Args:
        client: BigQuery Client
        queries: List of queries to run
    """
    for query in queries:
        job = client.query(query)
        LOGGER.info(f'Running query for job_id {job.job_id}. Query:\n{query}')
        result = job.result()
        LOGGER.info(
            f'Removed {job.num_dml_affected_rows} rows for job_id {job.job_id}')
    return


def run_bq_retraction(project_id,
                      sandbox_dataset_id,
                      pid_project_id,
                      pid_table_id,
                      hpo_id,
                      dataset_ids_list,
                      retraction_type,
                      skip_sandboxing=False,
                      bq_client=None):
    """
    Main function to perform retraction.
    pid table must follow schema described above in PID_TABLE_FIELDS and must reside in sandbox_dataset_id.
    This function removes rows from all tables containing person_ids if they exist in pid_table_id.
    If only_ehr is specified, it removes only the records from EHR.

    :param project_id: project to retract from
    :param sandbox_dataset_id: identifies the dataset containing the pid table
    :param pid_project_id: identifies the dataset containing the sandbox dataset
    :param pid_table_id: table containing the person_ids and research_ids
    :param hpo_id: hpo_id of the site to retract from
    :param dataset_ids_list: list of datasets to retract from separated by a space. If containing only 'all_datasets',
        retracts from all datasets. If containing only 'none', skips retraction from BigQuery datasets
    :param retraction_type: string indicating whether all data needs to be removed, including RDR,
        or if RDR data needs to be kept intact. Can take the values 'rdr_and_ehr' or 'only_ehr'
    :param skip_sandboxing: True if you wish not to sandbox the retracted data.    
    :param bq_client: BigQuery client. Reuse the client if one already exists. If not, a new one will be created.
    :return:
    """
    if bq_client:
        client = bq_client
    else:
        client = BigQueryClient(project_id)

    dataset_ids = ru.get_datasets_list(client, dataset_ids_list)
    queries = []
    for dataset in dataset_ids:

        if ru.is_deid_dataset(dataset) or ru.is_deid_fitbit_dataset(dataset):
            person_research_id = RESEARCH_ID
        else:
            person_research_id = PERSON_ID

        pid_rid_query = JINJA_ENV.from_string(PERSON_ID_QUERY).render(
            person_research_id=person_research_id,
            pid_project=pid_project_id,
            sandbox_dataset_id=sandbox_dataset_id,
            pid_table_id=pid_table_id)

        if ru.is_deid_dataset(dataset) or ru.is_deid_fitbit_dataset(
                dataset) or ru.is_combined_dataset(
                    dataset) or ru.is_fitbit_dataset(dataset):
            queries = queries_to_retract_from_dataset(client, dataset,
                                                      sandbox_dataset_id,
                                                      pid_rid_query,
                                                      skip_sandboxing,
                                                      retraction_type)
        elif ru.is_unioned_dataset(dataset):
            queries = queries_to_retract_from_dataset(client, dataset,
                                                      sandbox_dataset_id,
                                                      pid_rid_query,
                                                      skip_sandboxing)
        elif ru.is_ehr_dataset(dataset):
            if hpo_id == NONE_STR:
                LOGGER.info(
                    f'"RETRACTION_HPO_ID" set to "{NONE_STR}", skipping retraction from {dataset}'
                )
            else:
                queries = queries_to_retract_from_ehr_dataset(
                    client, dataset, sandbox_dataset_id, hpo_id, pid_rid_query,
                    skip_sandboxing)

        LOGGER.info(f"Started retracting from dataset {dataset}")
        retraction_query_runner(client, queries)
        LOGGER.info(f"Completed retracting from dataset {dataset}")

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
            f'"{RETRACTION_RDR_EHR}" or "{RETRACTION_ONLY_EHR}"'),
        required=True)
    args = parser.parse_args()

    run_bq_retraction(args.project_id, args.sandbox_dataset_id,
                      args.pid_project_id, args.pid_table_id, args.hpo_id,
                      args.dataset_ids, args.retraction_type)
