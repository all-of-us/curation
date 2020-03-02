from constants.cdr_cleaner import clean_cdr as clean_consts
import constants.cdr_cleaner.clean_cdr as cdr_consts
from notebooks import bq
from constants import bq_utils as bq_consts
from sandbox import get_sandbox_dataset_id

SANDBOX_QUERY = """
CREATE OR REPLACE TABLE `{project}.{sandbox_dataset}.{intermediary_table}` AS (
SELECT *
FROM `{project}.{dataset}.{table}`
WHERE person_id IN({pid_query}))
"""

CLEAN_QUERY = """
SELECT *
FROM `{project}.{dataset}.{table}`
WHERE person_id NOT IN({pid_query})
"""

PERSON_TABLE_QUERY = """
SELECT table_name
FROM `{project}.{dataset}.INFORMATION_SCHEMA.COLUMNS`
WHERE COLUMN_NAME = 'person_id'
"""


def get_tables_with_person_id(project_id, dataset_id):
    """
    Get list of tables that have a person_id column
    """
    person_table_query = PERSON_TABLE_QUERY.format(project=project_id,
                                                   dataset=dataset_id)
    person_tables_df = bq.query(person_table_query)
    person_table_list = list(person_tables_df.table_name.get_values())

    # exclude mapping tables from list, to be removed after all cleaning rules
    for item in person_table_list:
        if item.startswith('_mapping'):
            person_table_list.remove(item)

    return person_table_list


def get_sandbox_queries(project_id, dataset_id, pids_query, ticket_number):
    person_tables_list = get_tables_with_person_id(project_id, dataset_id)
    queries_list = []
    pid_query = pids_query.format(project=project_id, dataset=dataset_id)

    for table in person_tables_list:
        sandbox_queries = dict()
        sandbox_queries[cdr_consts.QUERY] = SANDBOX_QUERY.format(
            dataset=dataset_id,
            project=project_id,
            table=table,
            sandbox_dataset=get_sandbox_dataset_id(dataset_id),
            intermediary_table=table + '_' + ticket_number,
            pid_query=pid_query)
        queries_list.append(sandbox_queries)

    return queries_list


def get_remove_personid_queries(project_id, dataset_id, pids_query):
    person_tables_list = get_tables_with_person_id(project_id, dataset_id)
    queries_list = []
    pid_query = pids_query.format(project=project_id, dataset=dataset_id)

    for table in person_tables_list:
        delete_queries = CLEAN_QUERY.format(project=project_id,
                                            dataset=dataset_id,
                                            table=table,
                                            pid_query=pid_query)
        queries_list.append({
            clean_consts.QUERY: delete_queries,
            clean_consts.DESTINATION_TABLE: table,
            clean_consts.DESTINATION_DATASET: dataset_id,
            clean_consts.DISPOSITION: bq_consts.WRITE_TRUNCATE
        })

    return queries_list
