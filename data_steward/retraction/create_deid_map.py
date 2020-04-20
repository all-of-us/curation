"""
Background

DC-764 - Rename or create _deid_map for datasets without

Currently, some older datasets do not contain the _deid_map table in the combined dataset. This is needed for
retractions to obtain the correct research_id for the deid datasets.

Create a module in the retraction package that generates all _deid_map tables for each release in the combined dataset
if there is an associated deid_dataset. This will be created by querying the observation table in combined and joining
to the observation table in deid on observation_id. Before the table creation, check to see if _deid_map or deid_map
exists, if deid_map exists rename for consistency to _deid_map.
"""

# Python imports
import re
import argparse
import logging

# Third party imports

# Project imports
import utils.bq
import bq_utils
from retraction.retract_utils import DEID_REGEX

CURRENT_RELEASE_REGEX = re.compile('R\d{4}q\dr\d')

RENAME_DEID_MAP_TABLE_QUERY = """
CREATE OR REPLACE TABLE `{project}.{dataset}._deid_map` AS (
SELECT *
FROM `{project}.{dataset}.deid_map`
)
"""
CREATE_DEID_MAP_TABLE_QUERY = """
CREATE OR REPLACE TABLE `{project}.{dataset}._deid_map` AS (
SELECT c.person_id, d.person_id AS research_id
FROM `{project}.{dataset}.observation` c
JOIN `{project}.{dataset}.observation` d
ON c.observation_id = d.observation_id
)
"""


def get_combined_datasets_for_deid_map(project_id):
    """
    List all datasets in given project_id, filter out datasets that contain a deid dataset and collect the corresponding
    combined dataset
    :param project_id: bq name of project_id
    :return: list of combined_datasets that should contain a _deid_map table
    """
    all_datasets_obj = utils.bq.list_datasets(project_id)
    all_datasets = [d.dataset_id for d in all_datasets_obj]
    deid_datasets = []

    # Keep only deid datasets with standard cdr release naming convention
    release_datasets = [
        d for d in all_datasets if CURRENT_RELEASE_REGEX.match(d)
    ]
    # Filter for deid datasets
    for dataset in release_datasets:
        if bool(re.match(DEID_REGEX, dataset)) is True:
            deid_datasets.append(dataset)
    # Find corresponding combined dataset for each deid dataset
    combined_datasets = get_corresponding_combined_dataset(
        all_datasets, deid_datasets)
    return combined_datasets


def get_corresponding_combined_dataset(all_datasets, deid_datasets):
    """
    Takes in all datasets in a project and a list of all the deid datasets in that project and returns the corresponding
    combined dataset to that deid dataset
    :param all_datasets: list of all datasets in the given bq project
    :param deid_datasets: list of all deid datasets in the given bq project
    :return: list of combine datasets
    """
    combined_datasets = []
    # Get release from deid dataset for both older and current release tags and search for combined dataset
    for d in deid_datasets:
        release = d.split('_')[0]
        older_release = release[1:]
        combined, older_combined = release + '_combined', older_release + '_combined'

        if combined in all_datasets:
            combined_datasets.append(combined)
        if older_combined in all_datasets:
            combined_datasets.append(older_combined)
    # Remove duplicates
    combined_datasets = list(set(combined_datasets))
    return combined_datasets


def check_if_deid_map_exists(project_id, dataset):
    """
    Checks if _deid_map table exists in the given dataset.
    :param project_id: bq name of project_id
    :param dataset: bq name of dataset
    :return: returns True, False or 'rename required' which had 'deid_map' table instead of '_deid_map'
    """
    table_info_df = utils.bq.get_table_info_for_dataset(project_id, dataset)
    column_list = table_info_df['table_name'].tolist()

    if 'deid_map' in column_list:
        return 'rename required'
    if '_deid_map' in column_list:
        return True
    if ['deid_map', '_deid_map'] not in column_list:
        return False


def rename_deid_map_table_query(project_id, dataset):
    return RENAME_DEID_MAP_TABLE_QUERY.format(project=project_id,
                                              dataset=dataset)


def create_deid_map_table_query(project, dataset):
    return CREATE_DEID_MAP_TABLE_QUERY.format(project=project, dataset=dataset)


def create_deid_map_table_queries(project):
    """
    Creates a query list to run to create or rename _deid_map tables in each combined dataset that has a deid dataset
    :param project: bq name of project_id
    :return: list of queries to run
    """
    combined_datasets = get_combined_datasets_for_deid_map(project)
    queries = []
    for dataset in combined_datasets:
        check = check_if_deid_map_exists(project, dataset)
        if check is True:
            continue
        if check is False:
            queries.append(create_deid_map_table_query(project, dataset))
        if check == 'rename required':
            queries.append(rename_deid_map_table_query(project, dataset))
    return queries


def run_queries(queries):
    """
    Function that will run the queires to create '_deid_map' tables

    :param queries: list of queries
    """
    query_job_ids = []
    for query_dict in queries:
        logging.info('Creating or renaming _deid_map table with query: %s' %
                     (query_dict['query']))
        job_results = bq_utils.query(q=query_dict['query'], batch=True)
        rows_affected = job_results['numDmlAffectedRows']
        logging.info('%s rows written to _deid_map table' % rows_affected)
        query_job_id = job_results['jobReference']['jobId']
        query_job_ids.append(query_job_id)

    incomplete_jobs = bq_utils.wait_on_jobs(query_job_ids)
    if incomplete_jobs:
        logging.info('Failed on {count} job ids {ids}'.format(
            count=len(incomplete_jobs), ids=incomplete_jobs))
        logging.info('Terminating _deid_map creation')
        raise bq_utils.BigQueryJobWaitError(incomplete_jobs)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description=
        'Collects all datasets in a project and renames the deid_map table to _deid_map table if exists. If '
        '_deid_map table does not exist in combine dataset, script will create if there is an associated '
        'deid dataset',
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('-p',
                        '--project_id',
                        action='store',
                        dest='project_id',
                        help='Identifies the project to retract data from',
                        required=True)
    args = parser.parse_args()
    query_list = create_deid_map_table_queries(args.project_id)
    # run_queries(query_list)
    logging.info('Creation of _deid_maps complete')
