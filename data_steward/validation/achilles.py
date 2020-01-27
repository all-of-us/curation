import json
import logging
import os

import app_identity
import bq_utils
import resources
from validation import sql_wrangle
from constants import bq_utils as bq_consts

ACHILLES_ANALYSIS = 'achilles_analysis'
ACHILLES_RESULTS = 'achilles_results'
ACHILLES_RESULTS_DIST = 'achilles_results_dist'
ACHILLES_TABLES = [ACHILLES_ANALYSIS, ACHILLES_RESULTS, ACHILLES_RESULTS_DIST]
ACHILLES_DML_SQL_PATH = os.path.join(resources.resource_path,
                                     'achilles_dml.sql')
INSERT_INTO = 'insert into'


def _get_run_analysis_commands(hpo_id):
    raw_commands = sql_wrangle.get_commands(ACHILLES_DML_SQL_PATH)
    commands = [sql_wrangle.qualify_tables(cmd, hpo_id) for cmd in raw_commands]
    return commands


def load_analyses(hpo_id):
    """
    Populate achilles lookup table
    :param hpo_id: hpo_id of the site to run achilles on
    :return: None
    """
    project_id = app_identity.get_application_id()
    dataset_id = bq_utils.get_dataset_id()
    if hpo_id is None:
        table_prefix = ""
    else:
        table_prefix = hpo_id + '_'
    table_name = table_prefix + ACHILLES_ANALYSIS
    csv_path = os.path.join(resources.resource_path, ACHILLES_ANALYSIS + '.csv')
    json_path = os.path.join(resources.fields_path, ACHILLES_ANALYSIS + '.json')
    with open(json_path, 'r') as f:
        schema = json.load(f)
    bq_utils.load_table_from_csv(project_id, dataset_id, table_name, csv_path,
                                 schema)


def drop_or_truncate_table(command):
    """
    Deletes or truncates table
    Previously, deletion was used for both truncate and drop, and this function retains the behavior
    :param command: query to run
    :return: None
    """
    if sql_wrangle.is_truncate(command):
        table_id = sql_wrangle.get_truncate_table_name(command)
    else:
        table_id = sql_wrangle.get_drop_table_name(command)
    if bq_utils.table_exists(table_id):
        bq_utils.delete_table(table_id)


def run_analysis_job(command):
    """
    Runs command query and waits for job completion
    :param command: query to run
    :return: None
    :raises RuntimeError: Raised if job takes too long to complete
    """
    if sql_wrangle.is_to_temp_table(command):
        logging.info('Running achilles temp query %s' % command)
        table_id = sql_wrangle.get_temp_table_name(command)
        query = sql_wrangle.get_temp_table_query(command)
        job_result = bq_utils.query(query, destination_table_id=table_id)
    else:
        logging.info('Running achilles load query %s' % command)
        job_result = bq_utils.query(command)
    job_id = job_result['jobReference']['jobId']
    incomplete_jobs = bq_utils.wait_on_jobs([job_id])
    if len(incomplete_jobs) > 0:
        logging.info('Job id %s taking too long' % job_id)
        raise RuntimeError('Job id %s taking too long' % job_id)


def run_analyses(hpo_id):
    """
    Run the achilles analyses
    :param hpo_id: hpo_id of the site to run on
    :return: None
    """
    commands = _get_run_analysis_commands(hpo_id)
    for command in commands:
        if sql_wrangle.is_truncate(command) or sql_wrangle.is_drop(command):
            drop_or_truncate_table(command)
        else:
            run_analysis_job(command)


def create_tables(hpo_id, drop_existing=False):
    """
    Create the achilles related tables
    :param hpo_id: associated hpo id
    :param drop_existing: if True, drop existing tables
    :return: None
    """
    for table_name in ACHILLES_TABLES:
        table_id = bq_utils.get_table_id(hpo_id, table_name)
        bq_utils.create_standard_table(table_name, table_id, drop_existing)
