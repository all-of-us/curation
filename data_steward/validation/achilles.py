import os
import time

import bq_utils
import resources
import sql_wrangle

ACHILLES_ANALYSIS = 'achilles_analysis'
ACHILLES_RESULTS = 'achilles_results'
ACHILLES_RESULTS_DIST = 'achilles_results_dist'
ACHILLES_TABLES = [ACHILLES_ANALYSIS, ACHILLES_RESULTS, ACHILLES_RESULTS_DIST]
ACHILLES_DML_SQL_PATH = os.path.join(resources.resource_path, 'achilles_dml.sql')
END_OF_IMPORTING_LOOKUP_MARKER = 'end of importing values into analysis lookup'


def _get_load_analysis_commands(hpo_id):
    raw_commands = sql_wrangle.get_commands(ACHILLES_DML_SQL_PATH)
    commands = map(lambda cmd: sql_wrangle.qualify_tables(cmd, hpo_id), raw_commands)
    for command in commands:
        if END_OF_IMPORTING_LOOKUP_MARKER in command.lower():
            break
        yield command


def _get_run_analysis_commands(hpo_id):
    raw_commands = sql_wrangle.get_commands(ACHILLES_DML_SQL_PATH)
    commands = map(lambda cmd: sql_wrangle.qualify_tables(cmd, hpo_id), raw_commands)
    i = 0
    for command in commands:
        if END_OF_IMPORTING_LOOKUP_MARKER in command.lower():
            break
        i += 1
    return commands[i:]


def load_analyses(hpo_id):
    """
    Populate achilles lookup table
    :param hpo_id:
    :return:
    """
    commands = _get_load_analysis_commands(hpo_id)
    for command in commands:
        bq_utils.query(command)


def run_analyses(hpo_id):
    """
    Run the achilles analyses
    :param hpo_id:
    :return:
    """
    commands = _get_run_analysis_commands(hpo_id)
    for command in commands:
        print 'Running `%s`...\n' % command
        if sql_wrangle.is_to_temp_table(command):
            table_id = sql_wrangle.get_temp_table_name(command)
            query = sql_wrangle.get_temp_table_query(command)
            bq_utils.query(query, False, table_id)
            time.sleep(6)
        elif sql_wrangle.is_truncate(command):
            table_id = sql_wrangle.get_truncate_table_name(command)
            if bq_utils.table_exists(table_id):
                bq_utils.delete_table(table_id)
            # query = 'DELETE FROM %s WHERE TRUE' % table_id
            # bq_utils.query(query)
        elif sql_wrangle.is_drop(command):
            table_id = sql_wrangle.get_drop_table_name(command)
            if bq_utils.table_exists(table_id):
                bq_utils.delete_table(table_id)
        else:
            bq_utils.query(command)
        time.sleep(0.5)


def create_tables(hpo_id, drop_existing=False):
    """
    Create the achilles related tables
    :param hpo_id: associated hpo id
    :param drop_existing: if True, drop existing tables
    :return:
    """
    for table_name in ACHILLES_TABLES:
        table_id = bq_utils.get_table_id(hpo_id, table_name)
        bq_utils.create_standard_table(table_name, table_id, drop_existing)
