# Python imports
import argparse
import logging
import sys

# Third party imports
from googleapiclient.errors import HttpError

# Project imports
import bq_utils
import common

logging.basicConfig(
    stream=sys.stdout,
    level=logging.WARNING,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

PARTICIPANT_ROWS = """
SELECT '{table}' AS table_id, COUNT(*) AS count
FROM `{project}.{dataset}.{table}`
WHERE person_id IN ({pids_string})
"""

EHR_QUALIFIER = """
AND {table_id} > {const}
"""

UNION_ALL = """
UNION ALL
"""

PID_QUERY = """
SELECT person_id
FROM `{pid_project}.{sandbox_dataset}.{pid_table}`
"""

DATASET_ID = 'dataset_id'
TABLE_ID = 'table_id'
COUNT = 'count'

# Query to list all tables within a dataset that contains person_id in the schema
PERSON_TABLE_QUERY = """
SELECT table_name, column_name
FROM `{project}.{dataset}.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name IN
(SELECT table_name
FROM `{project}.{dataset}.INFORMATION_SCHEMA.COLUMNS`
WHERE COLUMN_NAME = 'person_id'
AND ordinal_position = 2)
AND ordinal_position = 1
"""

TABLE_NAME_COLUMN = 'table_name'
COLUMN_NAME = 'column_name'


def get_tables_with_person_id(project_id, dataset_id):
    """
    Get list of tables that have a person_id column, excluding mapping tables
    """
    person_table_query = PERSON_TABLE_QUERY.format(project=project_id,
                                                   dataset=dataset_id)
    response = bq_utils.query(person_table_query)
    person_tables = bq_utils.response2rows(response)
    # exclude mapping tables from list, to be removed after all cleaning rules
    return [[table_row[TABLE_NAME_COLUMN], table_row[COLUMN_NAME]]
            for table_row in person_tables]


def get_pid_counts(project_id, dataset_id, hpo_id, pids_string):
    """
    Returns a list of queries in which the table will be truncated with clean data, ie: all removed PIDs from all
    datasets based on a cleaning rule.

    :param project_id: identifies the project
    :param dataset_id: identifies the dataset
    :param pids_string: string containing pids or pid_query
    :return: list of select statements that will get counts of participant data
    """
    person_tables_list = get_tables_with_person_id(project_id, dataset_id)
    pid_query_list = []
    pid_query_ehr_list = []
    count_list = []

    for table, table_id in person_tables_list:
        pid_table_query = PARTICIPANT_ROWS.format(project=project_id,
                                                  dataset=dataset_id,
                                                  table=table,
                                                  pids_string=pids_string)
        pid_table_query_ehr = pid_table_query + EHR_QUALIFIER.format(
            table_id=table_id,
            const=common.ID_CONSTANT_FACTOR + common.RDR_ID_CONSTANT)
        pid_query_list.append(pid_table_query)
        pid_query_ehr_list.append(pid_table_query_ehr)

    if len(pid_query_list) > 20:
        pid_query_list = [
            pid_query for pid_query in pid_query_list if hpo_id in pid_query
        ]
        pid_query_ehr_list = [
            pid_query for pid_query in pid_query_ehr_list if hpo_id in pid_query
        ]
    unioned_query = UNION_ALL.join(pid_query_list)
    unioned_ehr_query = UNION_ALL.join(pid_query_ehr_list)
    if unioned_query:
        count_list = run_count_pid_query(unioned_query)
        count_list_ehr = run_count_pid_query(unioned_ehr_query)
        count_list.sort(key=lambda table_row: table_row[0])
        count_list_ehr.sort(key=lambda ehr_table_row: ehr_table_row[0])
        for idx, ehr_table_row in enumerate(count_list_ehr):
            count_list[idx].append(ehr_table_row[1])
    return count_list


def run_count_pid_query(query):
    result_list = []
    query_response = bq_utils.query(query)
    results = bq_utils.response2rows(query_response)
    for result_row in results:
        result_list.append([result_row[TABLE_ID], result_row[COUNT]])
    return result_list


def estimate_prevalence(project_id, hpo_id, pids_string):
    """

    :param project_id: 
    :param pids_string: 
    :return: 
    """

    all_datasets = bq_utils.list_datasets(project_id)
    all_dataset_ids = [
        bq_utils.get_dataset_id_from_obj(dataset) for dataset in all_datasets
    ]
    for dataset_id in all_dataset_ids:
        try:
            count_summaries = get_pid_counts(project_id, dataset_id, hpo_id,
                                             pids_string)
            dataset_count = sum([table_row[1] for table_row in count_summaries])
            if dataset_count > 0:
                for table_row in count_summaries:
                    if table_row[1] > 0:
                        print(
                            f'DATASET: {dataset_id} TABLE: {table_row[0]} RDR+EHR: {table_row[1]} EHR: {table_row[2]}'
                        )
        except HttpError:
            logging.exception('Dataset %s could not be analyzed' % dataset_id)
    return


def get_pids(pid_list=None,
             pid_project_id=None,
             sandbox_dataset_id=None,
             pid_table_id=None):
    if pid_list:
        # convert to string and trim the brackets off
        pid_list = [int(pid) for pid in pid_list]
        return str(pid_list)[1:-1]
    elif pid_project_id and sandbox_dataset_id and pid_table_id:
        pid_query = PID_QUERY.format(pid_project=pid_project_id,
                                     sandbox_dataset=sandbox_dataset_id,
                                     pid_table=pid_table_id)
        return pid_query
    else:
        raise ValueError('Please specify pids or pid_table')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Estimates the prevalence of specified pids in the project',
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('-p',
                        '--project_id',
                        action='store',
                        dest='project_id',
                        help='Identifies the project to retract data from',
                        required=True)
    parser.add_argument('-o',
                        '--hpo_id',
                        action='store',
                        dest='hpo_id',
                        help='Identifies the site submitting the person_ids',
                        required=True)
    parser.add_argument(
        '-q',
        '--pid_project_id',
        action='store',
        dest='pid_project_id',
        help='Identifies the project containing the sandbox dataset',
        required=False)
    parser.add_argument('-s',
                        '--sandbox_dataset_id',
                        action='store',
                        dest='sandbox_dataset_id',
                        help='Identifies the dataset containing the pid table',
                        required=False)
    parser.add_argument('-t',
                        '--pid_table_id',
                        action='store',
                        dest='pid_table_id',
                        help='Identifies the table containing the person_ids',
                        required=False)
    parser.add_argument('-i',
                        '--pid_list',
                        nargs='+',
                        dest='pid_list',
                        help='Person_ids to check for',
                        required=False)

    args = parser.parse_args()

    pids_string = get_pids(args.pid_list, args.pid_project_id,
                           args.sandbox_dataset_id, args.pid_table_id)

    estimate_prevalence(args.project_id, args.hpo_id, pids_string)
