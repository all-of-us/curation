# Project imports
from constants.cdr_cleaner import clean_cdr as clean_consts
import constants.cdr_cleaner.clean_cdr as cdr_consts
from notebooks import bq
from constants import bq_utils as bq_consts
from sandbox import get_sandbox_dataset_id

# Query to create tables in sandbox with rows that will be removed per cleaning rule
SANDBOX_QUERY = """
CREATE OR REPLACE TABLE `{project}.{sandbox_dataset}.{intermediary_table}` AS (
SELECT *
FROM `{project}.{dataset}.{table}`
WHERE person_id IN({pids}))
"""

# Query to truncate existing tables to remove PIDs based on cleaning rule criteria
CLEAN_QUERY = """
SELECT *
FROM `{project}.{dataset}.{table}`
WHERE person_id NOT IN({pids})
"""

# Query to list all tables within a dataset that contains person_id in the schema
PERSON_TABLE_QUERY = """
SELECT table_name
FROM `{project}.{dataset}.INFORMATION_SCHEMA.COLUMNS`
WHERE COLUMN_NAME = 'person_id'
"""


def get_tables_with_person_id(project_id, dataset_id):
    """
    Get list of tables that have a person_id column, excluding mapping tables
    """
    person_table_query = PERSON_TABLE_QUERY.format(project=project_id,
                                                   dataset=dataset_id)
    person_table_list = list(
        bq.query(person_table_query).table_name.get_values())

    # exclude mapping tables from list, to be removed after all cleaning rules
    for item in person_table_list:
        if item.startswith('_mapping'):
            person_table_list.remove(item)

    return person_table_list


def get_sandbox_queries(project_id, dataset_id, pids, ticket_number):
    """
    Returns a list of queries of all tables to be added to the datasets sandbox. These tables include all rows from all
    effected tables that include PIDs that will be removed by a specific cleaning rule.

    :param project_id: bq project_id
    :param dataset_id: bq dataset_id
    :param pids: list of person_ids from cleaning rule that need to be sandboxed and removed
    :param ticket_number: ticket number from jira that will be appended to the end of the sandbox table names
    :return: list of CREATE OR REPLACE queries to create tables in sandbox
    """
    person_tables_list = get_tables_with_person_id(project_id, dataset_id)
    queries_list = []

    for table in person_tables_list:
        sandbox_queries = dict()
        sandbox_queries[cdr_consts.QUERY] = SANDBOX_QUERY.format(
            dataset=dataset_id,
            project=project_id,
            table=table,
            sandbox_dataset=get_sandbox_dataset_id(dataset_id),
            intermediary_table=table + '_' + ticket_number,
            pids=pids)
        queries_list.append(sandbox_queries)

    return queries_list


def get_remove_personid_queries(project_id, dataset_id, pids):
    """
    Returns a list of queries in which the table will be truncated with clean data, ie: all removed PIDs from all
    datasets based on a cleaning rule.

    :param project_id: b1 project_id
    :param dataset_id: bq dataset_id
    :param pids: list of person_ids from cleaning rule that need to be sandboxed and removed
    :return: list of select statements that will truncate the existing tables with clean data
    """
    person_tables_list = get_tables_with_person_id(project_id, dataset_id)
    queries_list = []

    for table in person_tables_list:
        delete_queries = CLEAN_QUERY.format(project=project_id,
                                            dataset=dataset_id,
                                            table=table,
                                            pids=pids)
        queries_list.append({
            clean_consts.QUERY: delete_queries,
            clean_consts.DESTINATION_TABLE: table,
            clean_consts.DESTINATION_DATASET: dataset_id,
            clean_consts.DISPOSITION: bq_consts.WRITE_TRUNCATE
        })

    return queries_list
