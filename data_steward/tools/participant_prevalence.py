# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.3.0
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# Python imports
import argparse
import logging

# Third party imports
from google.api_core.exceptions import BadRequest
import pandas as pd

# Project imports
from utils import bq
import common
from constants.tools import participant_prevalence as consts
from cdr_cleaner.cleaning_rules import sandbox_and_remove_pids as srp


def get_pids(pid_list=None,
             pid_project_id=None,
             sandbox_dataset_id=None,
             pid_table_id=None):
    """
    Converts either 
     - a list of integer pids into a bq-compatible string containing the pids or
     - a project_id, dataset_id and table_id into a SELECT query that selects pids from the table

    :param pid_list: list of pids
    :param pid_project_id: identifies project containing the sandbox dataset
    :param sandbox_dataset_id: identifies dataset containing the pid table
    :param pid_table_id: identifies the table containing pids to consider
    :return: bq-compatible string or SELECT query that selects pids from table
    """
    if pid_list:
        # convert to string and trim the brackets off
        pid_list = [int(pid) for pid in pid_list]
        bq_pid_str = str(pid_list)[1:-1]
        logging.info(f"Generated BQ pid string: {bq_pid_str}")
        return bq_pid_str
    elif pid_project_id and sandbox_dataset_id and pid_table_id:
        pid_query = consts.PID_QUERY.format(pid_project=pid_project_id,
                                            sandbox_dataset=sandbox_dataset_id,
                                            pid_table=pid_table_id)
        logging.info(f"Generated pid query: {pid_query}")
        return pid_query
    else:
        raise ValueError('Please specify pids or pid_table')


def get_cdm_tables_with_person_id(project_id, dataset_id):
    """
    Get df of table_ids(first column) and table names that have a person_id column as the second column
    
    :param project_id: identifies the project
    :param dataset_id: identifies the dataset
    :return list containing tables
    """
    person_table_query = consts.PERSON_TABLE_QUERY.format(project=project_id,
                                                          dataset=dataset_id)
    result_df = bq.query(person_table_query)
    tables_list = result_df.get(consts.TABLE_NAME_COLUMN).to_list()
    return tables_list


def get_pid_counts(project_id, dataset_id, hpo_id, pids_string, for_cdm):
    """
    Returns dataframe with table_name, all_counts and ehr_counts of rows pertaining to the participants

    :param project_id: identifies the project
    :param dataset_id: identifies the dataset
    :param hpo_id: identifies the hpo site that submitted the pids
    :param pids_string: string containing pids or pid_query
    :param for_cdm: Boolean indicating if counting only cdm tables (excl. person)
    :return: df containing table_name, all_counts and ehr_counts
    """
    cdm_person_tables = get_cdm_tables_with_person_id(project_id, dataset_id)
    all_person_tables = srp.get_tables_with_person_id(project_id, dataset_id)
    if for_cdm:
        person_tables = cdm_person_tables
    else:
        person_tables = list(set(all_person_tables) - set(cdm_person_tables))
    count_df = pd.DataFrame(
        columns=[consts.TABLE_ID, consts.ALL_COUNT, consts.EHR_COUNT])
    pid_query_list = []
    for table in person_tables:
        count_types = consts.SELECT_ALL_COUNT + ',' + consts.SELECT_ZERO_COUNT
        if for_cdm:
            count_types = consts.SELECT_ALL_COUNT + ',' + consts.SELECT_EHR_COUNT.format(
                table_id=table + '_id',
                const=common.ID_CONSTANT_FACTOR + common.RDR_ID_CONSTANT)
        pid_table_query = consts.PARTICIPANT_ROWS.format(
            project=project_id,
            dataset=dataset_id,
            table=table,
            count_types=count_types,
            pids_string=pids_string)
        pid_query_list.append(pid_table_query)
    if len(pid_query_list) > 20:
        pid_query_list = [
            pid_query for pid_query in pid_query_list if hpo_id in pid_query
        ]
    unioned_query = consts.UNION_ALL.join(pid_query_list)
    if unioned_query:
        count_df = bq.query(unioned_query)
    return count_df


def estimate_prevalence(project_id, hpo_id, pids_string):
    """
    Logs dataset_name, table_name, all_count and ehr_count to count rows pertaining to pids

    :param project_id: identifies the project
    :param hpo_id: Identifies the hpo site that submitted the pids
    :param pids_string: string containing query or pids in bq string format
    :return: 
    """

    all_datasets = bq.list_datasets(project_id)
    for dataset in all_datasets:
        dataset_id = dataset.dataset_id
        try:
            count_df_cdm = get_pid_counts(project_id,
                                          dataset_id,
                                          hpo_id,
                                          pids_string,
                                          for_cdm=True)
            count_df_all = get_pid_counts(project_id,
                                          dataset_id,
                                          hpo_id,
                                          pids_string,
                                          for_cdm=False)
            count_summaries = pd.concat([count_df_cdm, count_df_all])

            if 'ehr' in dataset_id:
                count_summaries[consts.EHR_COUNT] = count_summaries[
                    consts.ALL_COUNT]

            non_zero_counts = count_summaries[
                count_summaries[consts.ALL_COUNT] > 0].get_values()
            if non_zero_counts.size > 0:
                for count_row in non_zero_counts:
                    logging.info(
                        'DATASET_ID: {}\tTABLE_ID: {}\tALL_COUNT: {}\tEHR_COUNT: {}'
                        .format(dataset_id, *count_row))
        except BadRequest:
            logging.exception('Dataset %s could not be analyzed' % dataset_id)


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
