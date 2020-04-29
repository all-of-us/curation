# Python imports
import re
import argparse
import logging

# Third party imports
import pandas as pd

# Project imports
from utils import bq
import bq_utils
from retraction.retract_utils import DEID_REGEX
from cdr_cleaner.cleaning_rules import sandbox_and_remove_pids
from constants.cdr_cleaner import clean_cdr as clean_consts
from sandbox import get_sandbox_dataset_id, create_sandbox_dataset
from constants import bq_utils as bq_consts

DEACTIVATED_PIDS_QUERY = """
SELECT DISTINCT *
FROM `{project}.{dataset}.{table}`
"""
RESEARCH_ID_QUERY = """
SELECT DISTINCT research_id
FROM `{project}.{prefix_regex}_combined._deid_map`
WHERE person_id = {pid}
"""
CHECK_PID_EXIST_QUERY = """
SELECT
COUNT(*) AS count
FROM `{project}.{dataset}.{table}`
WHERE person_id = {pid}
"""
# Queries to create tables in associated sandbox with rows that will be removed per cleaning rule, two different queries
# for tables with standard entry dates vs. tables with start and end dates
SANDBOX_QUERY_DATE = """
CREATE OR REPLACE TABLE `{project}.{sandbox_dataset}.{intermediary_table}` AS (
SELECT *
FROM `{project}.{dataset}.{table}`
WHERE person_id = {pid}
AND {date_column} >= (SELECT deactivated_date
FROM `{deactivated_pids_project}.{deactivated_pids_dataset}.{deactivated_pids_table}`
WHERE person_id = {pid}))
"""
SANDBOX_QUERY_END_DATE = """
CREATE OR REPLACE TABLE `{project}.{sandbox_dataset}.{intermediary_table}` AS (
SELECT *
FROM `{project}.{dataset}.{table}`
WHERE person_id = {pid}
AND (CASE WHEN {end_date_column} IS NOT NULL THEN {end_date_column} >= (SELECT deactivated_date
FROM `{deactivated_pids_project}.{deactivated_pids_dataset}.{deactivated_pids_table}`
WHERE person_id = {pid} ) ELSE CASE WHEN {end_date_column} IS NULL THEN {start_date_column} >= (
SELECT deactivated_date
FROM `{deactivated_pids_project}.{deactivated_pids_dataset}.{deactivated_pids_table}`
WHERE person_id = {pid}) END END))
"""
# Queries to truncate existing tables to remove deactivated EHR PIDS, two different queries for
# tables with standard entry dates vs. tables with start and end dates
CLEAN_QUERY_DATE = """
SELECT *
FROM `{project}.{dataset}.{table}`
WHERE person_id != {pid}
AND {date_column} < (SELECT deactivated_date
FROM `{deactivated_pids_project}.{deactivated_pids_dataset}.{deactivated_pids_table}`
WHERE person_id = {pid})
"""
CLEAN_QUERY_END_DATE = """
SELECT *
FROM `{project}.{dataset}.{table}`
WHERE person_id != {pid}
AND (CASE WHEN {end_date_column} IS NOT NULL THEN {end_date_column} < (SELECT deactivated_date
FROM `{deactivated_pids_project}.{deactivated_pids_dataset}.{deactivated_pids_table}`
WHERE person_id = {pid} ) ELSE CASE WHEN {end_date_column} IS NULL THEN {start_date_column} < (
SELECT deactivated_date
FROM `{deactivated_pids_project}.{deactivated_pids_dataset}.{deactivated_pids_table}`
WHERE person_id = {pid}) END END)
"""
# Deactivated participant table fields to query off of
PID_TABLE_FIELDS = [{
    "type": "integer",
    "name": "person_id",
    "mode": "required",
    "description": "The person_id to retract data for"
}, {
    "type": "date",
    "name": "deactivated_date",
    "mode": "required",
    "description": "The deactivation date to base retractions on"
}]


def get_pids_datasets_and_tables(project_id):
    """
    Get list of tables that have a person_id column; along with a separate
    list of the datasets containing pids
    """
    dataset_obj = bq.list_datasets(project_id)
    datasets = [d.dataset_id for d in dataset_obj]

    pids_table_list = []
    pids_dataset_list = []
    for dataset in datasets:
        pid_tables = sandbox_and_remove_pids.get_tables_with_person_id(
            project_id, dataset)
        if pid_tables:
            pids_table_list.extend(pid_tables)
            pids_dataset_list.append(dataset)

    return pids_table_list, pids_dataset_list


def get_date_info_for_pids_tables(project_id):
    """
    Loop through tables within all datasets and determine if the table has an end_date date or a date field. Filtering
    out person and death table and keeping only tables with PID and an upload or start/end date associated.

    :param project_id: bq name of project_id
    :return: filtered dataframe which includes the following columns for each table in each dataset with a person_id
    'project_id', 'dataset_id', 'table', 'date_column', 'start_date_column', 'end_date_column'
    """
    logging.info('getting all tables with person id in %s' % project_id)
    pids_tables, pids_datasets = get_pids_datasets_and_tables(project_id)
    # Create empty df to append to for final output
    date_fields_info_df = pd.DataFrame()

    # Loop through datasets
    logging.info(
        'Looping through datasets to filter and create dataframe with correct date field to determine retraction'
    )
    for dataset in pids_datasets:
        # Get table info
        table_info_df = bq.get_table_info_for_dataset(project_id, dataset)
        # Filter out to only records with pids
        pids_tables_df = table_info_df[table_info_df['table_name'].isin(
            pids_tables)]

        # Keep only records with datatype of 'DATE'
        date_fields_df = pids_tables_df[pids_tables_df['data_type'] == 'DATE']

        # Create empty df to append to, keeping only one record per table
        df_to_append = pd.DataFrame(columns=[
            'project_id', 'dataset_id', 'table', 'date_column',
            'start_date_column', 'end_date_column'
        ])
        df_to_append['project_id'] = date_fields_df['table_catalog']
        df_to_append['dataset_id'] = date_fields_df['table_schema']
        df_to_append['table'] = date_fields_df['table_name']
        df_to_append = df_to_append.drop_duplicates()

        # Create new df to loop through date time fields
        df_to_iterate = pd.DataFrame(
            columns=['project_id', 'dataset_id', 'table', 'column'])
        df_to_iterate['project_id'] = date_fields_df['table_catalog']
        df_to_iterate['dataset_id'] = date_fields_df['table_schema']
        df_to_iterate['table'] = date_fields_df['table_name']
        df_to_iterate['column'] = date_fields_df['column_name']

        # Remove person table and death table
        df_to_append = df_to_append[~df_to_append.table.str.
                                    contains('death', 'person')]
        df_to_iterate = df_to_iterate[~df_to_iterate.table.str.
                                      contains('death', 'person')]

        # Filter through date columns and append to the appropriate column
        for i, row in df_to_iterate.iterrows():
            column = getattr(row, 'column')
            table = getattr(row, 'table')
            if 'start_date' in column:
                df_to_append.loc[df_to_append.table == table,
                                 'start_date_column'] = column
            elif 'end_date' in column:
                df_to_append.loc[df_to_append.table == table,
                                 'end_date_column'] = column
            else:
                df_to_append.loc[df_to_append.table == table,
                                 'date_column'] = column

        date_fields_info_df = date_fields_info_df.append(df_to_append)

    return date_fields_info_df


def is_deid_dataset(dataset_id):
    """
    Determine if a dataset is deid using regex defined above
    :param dataset_id: dataset_id to verify if it is deid

    :return: boolean
    """
    return bool(re.match(DEID_REGEX, dataset_id))


def get_research_id(project, dataset, pid):
    """
    For deid datasets, this function queries the _deid_map table in the associated combined dataset based on the release
    regex prefix; to return the research_id for the specific pid passed

    :param project: bq name of project
    :param dataset: bq name of dataset
    :param pid: person_id passed to receive associated research_id
    :return: research_id or None if it does not exist for that person_id
    """
    # Get non deid dataset prefix
    prefix = dataset.split('_')[0]
    prefix = prefix.replace('R', '')

    research_id_df = bq.query(
        RESEARCH_ID_QUERY.format(project=project, prefix_regex=prefix, pid=pid))
    if research_id_df.empty:
        logging.info('no research_id associated with person_id: %s ' % pid)
        return None
    else:
        return research_id_df.to_string()


def check_pid_exist(pid, date_row):
    """
    Queries the table that retraction will take place, to see if the PID exists before creating query
    :param pid: person_id
    :param date_row:
    :return:
    """
    check_pid_df = bq.query(
        CHECK_PID_EXIST_QUERY.format(project=date_row.project_id,
                                     dataset=date_row.dataset_id,
                                     table=date_row.table,
                                     pid=pid))
    return bool(check_pid_df.get_value(0, 'count') > 0)


def create_queries(project_id, ticket_number, pids_project_id, pids_dataset_id,
                   pids_table):
    """
    Creates sandbox and truncate queries to run for EHR deactivated retraction

    :param project_id: bq name of project
    :param ticket_number: Jira ticket number to identify and title sandbox table
    :param pids_project_id: deactivated ehr pids table in bq's project_id
    :param pids_dataset_id: deactivated ehr pids table in bq's dataset_id
    :param pids_table: deactivated pids table in bq's table name
    :return: list of queries to run
    """
    queries_list = []
    # Hit bq and receive df of deactivated ehr pids and deactivated date
    deactivated_ehr_pids_df = bq.query(
        DEACTIVATED_PIDS_QUERY.format(project=pids_project_id,
                                      dataset=pids_dataset_id,
                                      table=pids_table), project_id)

    date_columns_df = get_date_info_for_pids_tables(project_id)
    logging.info(
        'Dataframe creation complete to loop through to create retraction queries'
    )

    # Loop through the deactivated PIDS df to create queries based on the retractions needed per PID table
    logging.info(
        'looping through deactivated pids df to create queries based on teh retractions needed per pid table'
    )
    for ehr_row in deactivated_ehr_pids_df.itertuples(index=False):
        for date_row in date_columns_df.itertuples(index=False):
            # Determine if dataset is deid to correctly pull pid or research_id and check if ID exists in dataset or if
            # already retracted
            if is_deid_dataset(date_row.dataset_id) is True:
                pid = get_research_id(date_row.project_id, date_row.dataset_id,
                                      ehr_row.person_id)
            else:
                pid = ehr_row.person_id

            # Check if PID is in table
            if check_pid_exist(pid, date_row) is True:
                # Get or create sandbox dataset
                sandbox_dataset = get_sandbox_dataset_id(date_row.dataset_id)
                if sandbox_dataset not in bq.list_datasets(project_id):
                    logging.info('%s dataset does not exist, creating now' %
                                 sandbox_dataset)
                    create_sandbox_dataset(date_row.project_id,
                                           date_row.dataset_id)

                # Create queries based on if the date field is null, if True, will create query based on end_date/start_date
                if not pd.isnull(date_row.date_column):
                    sandbox_query = SANDBOX_QUERY_DATE.format(
                        project=date_row.project_id,
                        sandbox_dataset=sandbox_dataset,
                        intermediary_table=ticket_number + '_' + date_row.table,
                        dataset=date_row.dataset_id,
                        table=date_row.table,
                        pid=pid,
                        deactivated_pids_project=pids_project_id,
                        deactivated_pids_dataset=pids_dataset_id,
                        deactivated_pids_table=pids_table,
                        date_column=date_row.date_column)
                    queries_list.append({
                        clean_consts.QUERY: sandbox_query,
                        clean_consts.DESTINATION_DATASET: sandbox_dataset,
                        clean_consts.DESTINATION_TABLE: date_row.table
                    })
                    clean_query = CLEAN_QUERY_DATE.format(
                        project=date_row.project_id,
                        dataset=date_row.dataset_id,
                        table=date_row.table,
                        pid=pid,
                        deactivated_pids_project=pids_project_id,
                        deactivated_pids_dataset=pids_dataset_id,
                        deactivated_pids_table=pids_table,
                        date_column=date_row.date_column)
                    queries_list.append({
                        clean_consts.QUERY: clean_query,
                        clean_consts.DESTINATION_DATASET: date_row.dataset_id,
                        clean_consts.DESTINATION_TABLE: date_row.table,
                        clean_consts.DISPOSITION: bq_consts.WRITE_TRUNCATE
                    })
                else:
                    sandbox_query = SANDBOX_QUERY_END_DATE.format(
                        project=date_row.project_id,
                        sandbox_dataset=sandbox_dataset,
                        intermediary_table=ticket_number + '_' + date_row.table,
                        dataset=date_row.dataset_id,
                        table=date_row.table,
                        pid=pid,
                        deactivated_pids_project=pids_project_id,
                        deactivated_pids_dataset=pids_dataset_id,
                        deactivated_pids_table=pids_table,
                        end_date_column=date_row.end_date_column,
                        start_date_column=date_row.start_date_column)
                    queries_list.append({
                        clean_consts.QUERY: sandbox_query,
                        clean_consts.DESTINATION_DATASET: sandbox_dataset,
                        clean_consts.DESTINATION_TABLE: date_row.table
                    })
                    clean_query = CLEAN_QUERY_END_DATE.format(
                        project=date_row.project_id,
                        dataset=date_row.dataset_id,
                        table=date_row.table,
                        pid=pid,
                        deactivated_pids_project=pids_project_id,
                        deactivated_pids_dataset=pids_dataset_id,
                        deactivated_pids_table=pids_table,
                        end_date_column=date_row.end_date_column,
                        start_date_column=date_row.start_date_column)
                    queries_list.append({
                        clean_consts.QUERY: clean_query,
                        clean_consts.DESTINATION_DATASET: date_row.dataset_id,
                        clean_consts.DESTINATION_TABLE: date_row.table,
                        clean_consts.DISPOSITION: bq_consts.WRITE_TRUNCATE
                    })
            else:
                # break out of loop to create query, if pid does not exist in table
                continue
    print(
        'Query list complete, retracting ehr deactivated PIDS from the following datasets: %s'
        % date_columns_df['dataset_id'].tolist())
    logging.info(
        'Query list complete, retracting ehr deactivated PIDS from the following datasets: %s'
        % date_columns_df['dataset_id'].tolist())
    return queries_list


def run_queries(queries):
    """
    Function that will perform the retraction.

    :param queries: list of queries to run retraction
    """
    query_job_ids = []
    for query_dict in queries:
        if 'sandbox' in query_dict['destination_dataset_id']:
            logging.info('Writing rows to be retracted to, using query %s' %
                         (query_dict['query']))
            job_results = bq_utils.query(q=query_dict['query'], batch=True)
            logging.info('%s table written to %s' %
                         (query_dict['destination_table_id'],
                          query_dict['destination_dataset_id']))
            query_job_id = job_results['jobReference']['jobId']
            query_job_ids.append(query_job_id)
        else:
            logging.info('Truncating table with clean data, using query %s' %
                         (query_dict['query']))
            job_results = bq_utils.query(q=query_dict['query'], batch=True)
            logging.info('%s table updated with clean rows in %s' %
                         (query_dict['destination_table_id'],
                          query_dict['destination_dataset_id']))
            query_job_id = job_results['jobReference']['jobId']
            query_job_ids.append(query_job_id)

    incomplete_jobs = bq_utils.wait_on_jobs(query_job_ids)
    if incomplete_jobs:
        logging.info('Failed on {count} job ids {ids}'.format(
            count=len(incomplete_jobs), ids=incomplete_jobs))
        logging.info('Terminating retraction')
        raise bq_utils.BigQueryJobWaitError(incomplete_jobs)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description=
        'Runs retraction of deactivated EHR participants on all datasets in project. The rows to be retracted'
        'are based on the date entered and will be retracted if the date entered is before the retraction date'
        'Uses project_id, deactivated pid_table_id to determine '
        'the pids to retract data for. The pid_table_id needs to contain '
        'the person_id and research_id and deactivated_date columns specified in the schema above, '
        'but research_id can be null if deid has not been run yet. ',
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('-p',
                        '--project_id',
                        action='store',
                        dest='project_id',
                        help='Identifies the project to retract data from',
                        required=True)
    parser.add_argument('-t',
                        '--ticket_number',
                        action='store',
                        dest='ticket_number',
                        help='Ticket number to append to sandbox table names',
                        required=True)
    parser.add_argument(
        '-pp',
        '--pids_project_id',
        action='store',
        dest='pids_project_id',
        help='Identifies the project where the pids table is stored',
        required=True)
    parser.add_argument(
        '-pd',
        '--pids_dataset_id',
        action='store',
        dest='pids_dataset_id',
        help='Identifies the dataset where the pids table is stored',
        required=True)
    parser.add_argument('-pt',
                        '--pids_table',
                        action='store',
                        dest='pids_table',
                        help='Identifies the table where the pids are stored',
                        required=True)
    args = parser.parse_args()
    query_list = create_queries(args.project_id, args.ticket_number,
                                args.pids_project_id, args.pids_dataset_id,
                                args.pids_table)
    run_queries(query_list)
    logging.info('Retraction complete')
