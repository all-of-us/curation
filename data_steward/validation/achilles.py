# Python imports
import logging
import os

# Project imports
import app_identity
import bq_utils
import resources
import common
from validation import sql_wrangle

ACHILLES_ANALYSIS = 'achilles_analysis'
ACHILLES_RESULTS = 'achilles_results'
ACHILLES_RESULTS_DIST = 'achilles_results_dist'
ACHILLES_TABLES = [ACHILLES_ANALYSIS, ACHILLES_RESULTS, ACHILLES_RESULTS_DIST]
ACHILLES_DML_SQL_PATH = os.path.join(resources.resource_files_path,
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
    table_name = resources.get_table_id(table_name=ACHILLES_ANALYSIS,
                                        hpo_id=hpo_id)
    csv_path = os.path.join(resources.resource_files_path,
                            f'{ACHILLES_ANALYSIS}.csv')
    schema = resources.fields_for(ACHILLES_ANALYSIS)
    bq_utils.load_table_from_csv(project_id, dataset_id, table_name, csv_path,
                                 schema)


def drop_or_truncate_table(client, command):
    """
    Deletes or truncates table
    Previously, deletion was used for both truncate and drop, and this function retains the behavior
    :param client: a BigQueryClient
    :param command: query to run
    :return: None
    """
    if sql_wrangle.is_truncate(command):
        table_id = sql_wrangle.get_truncate_table_name(command)
    else:
        table_id = sql_wrangle.get_drop_table_name(command)
    if client.table_exists(table_id):
        assert (table_id not in common.VOCABULARY_TABLES)
        client.delete_table(
            f'{os.environ.get("BIGQUERY_DATASET_ID")}.{table_id}')


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


def run_analyses(client, hpo_id):
    """
    Run the achilles analyses
    :param client: a BigQueryClient
    :param hpo_id: hpo_id of the site to run on
    :return: None
    """
    commands = _get_run_analysis_commands(hpo_id)
    for command in commands:
        if sql_wrangle.is_truncate(command) or sql_wrangle.is_drop(command):
            drop_or_truncate_table(client, command)
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
        table_id = resources.get_table_id(table_name, hpo_id=hpo_id)
        bq_utils.create_standard_table(table_name, table_id, drop_existing)
