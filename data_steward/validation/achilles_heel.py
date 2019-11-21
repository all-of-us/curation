import logging
import os
import re

import bq_utils
import resources
from validation import sql_wrangle
from io import open

ACHILLES_HEEL_RESULTS = 'achilles_heel_results'
ACHILLES_RESULTS_DERIVED = 'achilles_results_derived'
ACHILLES_HEEL_TABLES = [ACHILLES_HEEL_RESULTS, ACHILLES_RESULTS_DERIVED]
PREFIX_PLACEHOLDER = 'synpuf_100.'
TEMP_PREFIX = 'temp.'
TEMP_TABLE_PATTERN = re.compile('\s*INTO\s+([^\s]+)')
SPLIT_PATTERN = ';zzzzzz'
TRUNCATE_TABLE_PATTERN = re.compile('\s*truncate\s+table\s+([^\s]+)')
DROP_TABLE_PATTERN = re.compile('\s*drop\s+table\s+([^\s]+)')

ACHILLES_HEEL_DML = os.path.join(resources.resource_path, 'achilles_heel_dml.sql')


def remove_sql_comment_from_string(string):
    """ takes a string of the form : part of query -- comment and returns only the former.

    :string: part of sql query -- comment type strings
    :returns: the part of the sql query

    """
    query_part = string.strip().split('--')[0].strip()
    return query_part


def _extract_sql_queries(heel_dml_path):
    all_query_parts_list = []  # pair (type, query/table_name)
    with open(heel_dml_path, 'r') as heel_script:
        for line in heel_script:
            part = remove_sql_comment_from_string(line)
            if part == '':
                continue
            all_query_parts_list.append(part)

    queries = []
    all_query_string = 'zzzzzz'.join(all_query_parts_list)
    for query in re.split(SPLIT_PATTERN, all_query_string):
        query = query.strip()
        query = query.replace('zzzzzz', ' ')
        if query != '':
            queries.append(query)

    return queries


def _get_heel_commands(hpo_id):
    raw_commands = _extract_sql_queries(ACHILLES_HEEL_DML)
    commands = [sql_wrangle.qualify_tables(cmd, hpo_id) for cmd in raw_commands]
    for command in commands:
        yield command


def load_heel(hpo_id):
    commands = _get_heel_commands(hpo_id)
    for type, command in commands:
        bq_utils.query(command)


def run_heel(hpo_id):
    """
    Run heel commands

    :param hpo_id:  string name for the hpo identifier
    :returns: None
    :raises RuntimeError: Raised if BigQuery takes longer than 30 seconds
        to complete a job on a temporary table
    """
    # very long test
    commands = _get_heel_commands(hpo_id)
    count = 0
    for command in commands:
        count = count + 1
        logging.info(' ---- running query # {}'.format(count))
        logging.info(' ---- Running `%s`...\n' % command)
        if sql_wrangle.is_to_temp_table(command):
            table_id = sql_wrangle.get_temp_table_name(command)
            query = sql_wrangle.get_temp_table_query(command)
            insert_query_job_result = bq_utils.query(query, False, table_id)
            query_job_id = insert_query_job_result['jobReference']['jobId']

            incomplete_jobs = bq_utils.wait_on_jobs([query_job_id])
            if len(incomplete_jobs) > 0:
                logging.critical('tempresults doesnt get created in 30 secs')
                raise RuntimeError('Tempresults taking too long to create')
        elif sql_wrangle.is_truncate(command):
            table_id = sql_wrangle.get_truncate_table_name(command)
            query = 'DELETE FROM %s WHERE TRUE' % table_id
            bq_utils.query(query)
        elif sql_wrangle.is_drop(command):
            table_id = sql_wrangle.get_drop_table_name(command)
            bq_utils.delete_table(table_id)
        else:
            bq_utils.query(command)


def create_tables(hpo_id, drop_existing=False):
    """
    Create the achilles related tables
    :param hpo_id: associated hpo id
    :param drop_existing: if True, drop existing tables
    :return:
    """
    for table_name in ACHILLES_HEEL_TABLES:
        table_id = bq_utils.get_table_id(hpo_id, table_name)
        bq_utils.create_standard_table(table_name, table_id, drop_existing)
