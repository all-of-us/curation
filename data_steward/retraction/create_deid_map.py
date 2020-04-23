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
import pandas as pd

# Project imports
import utils.bq
import bq_utils
from retraction.retract_utils import DEID_REGEX
from constants.retraction import create_deid_map as consts


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
        d for d in all_datasets if consts.CURRENT_RELEASE_REGEX.match(d)
    ]
    # Filter for deid datasets
    for dataset in release_datasets:
        if bool(re.match(DEID_REGEX, dataset)) is True:
            deid_datasets.append(dataset)
    # Find corresponding combined dataset for each deid dataset
    deid_and_combined_df = get_corresponding_combined_dataset(
        all_datasets, deid_datasets)
    df_row_count = len(deid_and_combined_df.index)
    logging.info('%s datasets with combined and corresponding deid.' %
                 df_row_count)
    return deid_and_combined_df


def get_corresponding_combined_dataset(all_datasets, deid_datasets):
    """
    Takes in all datasets in a project and a list of all the deid datasets in that project and returns the corresponding
    combined dataset to that deid dataset
    :param all_datasets: list of all datasets in the given bq project
    :param deid_datasets: list of all deid datasets in the given bq project
    :return: list of combine datasets
    """
    deid_and_combined_datasets_df = pd.DataFrame(
        columns=['deid_dataset', 'combined_dataset'])
    # Get release from deid dataset for both older and current release tags and search for combined dataset
    for d in deid_datasets:
        release = d.split('_')[0]
        prefix = release[1:]
        combined = prefix + '_combined'

        if combined in all_datasets:
            new_row = pd.Series({
                'deid_dataset': d,
                'combined_dataset': combined
            })
            deid_and_combined_datasets_df = deid_and_combined_datasets_df.append(
                new_row, ignore_index=True)
        else:
            logging.info('combined dataset not found for %s' % d)

    return deid_and_combined_datasets_df


def check_if_deid_map_exists(project_id, dataset):
    """
    Checks if _deid_map table exists in the given dataset.
    :param project_id: bq name of project_id
    :param dataset: bq name of dataset
    :return: returns True, False or 'rename required' which had 'deid_map' table instead of '_deid_map'
    """
    table_info_df = utils.bq.get_table_info_for_dataset(project_id, dataset)
    column_list = table_info_df['table_name'].tolist()

    if '_deid_map' in column_list:
        return consts.SKIP
    elif 'deid_map' in column_list:
        return consts.RENAME
    else:
        return consts.CREATE


def create_deid_map_table_queries(project):
    """
    Creates a query list to run to create or rename _deid_map tables in each combined dataset that has a deid dataset
    :param project: bq name of project_id
    :return: list of queries to run
    """
    deid_and_combined_df = get_combined_datasets_for_deid_map(project)
    combined_datasets_list = deid_and_combined_df['combined_dataset'].to_list()
    # remove duplicate combined datasets in list
    combined_datasets_list = list(set(combined_datasets_list))
    queries = []
    for dataset in combined_datasets_list:
        check = check_if_deid_map_exists(project, dataset)
        if check == 'skip':
            continue
        if check == 'rename':
            queries.append(
                consts.RENAME_DEID_MAP_TABLE_QUERY.format(project=project,
                                                          dataset=dataset))
        if check == 'create':
            queries.append(
                consts.CREATE_DEID_MAP_TABLE_QUERY.format(project=project,
                                                          dataset=dataset))
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
        logging.info('_deid_map table created.')
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
    run_queries(query_list)
    logging.info('Creation of _deid_maps complete')
