# Python imports
import argparse
import logging
import sys

# Project imports
import bq_utils

logging.basicConfig(
    stream=sys.stdout,
    level=logging.WARNING,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

PARTICIPANT_COUNTS = """
SELECT COUNT(*) as n
FROM `{project}.{dataset}.{table}`
WHERE person_id IN ({pids_string})
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
SELECT table_name
FROM `{project}.{dataset}.INFORMATION_SCHEMA.COLUMNS`
WHERE COLUMN_NAME = 'person_id'
"""

TABLE_NAME_COLUMN = 'table_name'


def get_tables_with_person_id(project_id, dataset_id):
    """
    Get list of tables that have a person_id column, excluding mapping tables
    """
    person_table_query = PERSON_TABLE_QUERY.format(project=project_id,
                                                   dataset=dataset_id)
    response = bq_utils.query(person_table_query)
    person_tables = bq_utils.response2rows(response)
    # exclude mapping tables from list, to be removed after all cleaning rules
    return [table_row[TABLE_NAME_COLUMN] for table_row in person_tables]


def get_pid_counts(project_id, dataset_id, pids_string):
    """
    Returns a list of queries in which the table will be truncated with clean data, ie: all removed PIDs from all
    datasets based on a cleaning rule.

    :param project_id: identifies the project
    :param dataset_id: identifies the dataset
    :param pids_string: string containing pids or pid_query
    :return: list of select statements that will get counts of participant data
    """
    person_tables_list = get_tables_with_person_id(project_id, dataset_id)
    count_summaries = []

    for table in person_tables_list:
        count_pid_query = PARTICIPANT_COUNTS.format(project=project_id,
                                                    dataset=dataset_id,
                                                    table=table,
                                                    pids_string=pids_string)
        count = run_count_pid_query(count_pid_query)
        count_summaries.append({TABLE_ID: table, COUNT: count})
    return count_summaries


def run_count_pid_query(query):
    query_response = bq_utils.query(query)
    result = bq_utils.response2rows(query_response)
    count = int(result[0]['n'])
    return count


def estimate_prevalence(project_id, pids_string):
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
        count_summaries = get_pid_counts(project_id, dataset_id, pids_string)
        dataset_count = sum([table_row[COUNT] for table_row in count_summaries])
        if dataset_count > 0:
            logging.info(f'{dataset_id} {dataset_count}')
            for table_row in count_summaries:
                logging.warning(
                    f'{dataset_id} {table_row[TABLE_ID]} {table_row[COUNT]}')
        else:
            logging.warning(f'{dataset_id} {dataset_count}')
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

    estimate_prevalence(args.project_id, pids_string)
