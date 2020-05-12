# Python imports
import re
import argparse
import logging
import os
from datetime import datetime

# Third party imports
import pandas as pd
from jinja2 import Environment

# Project imports
from utils import bq
from retraction.retract_utils import DEID_REGEX
from constants.cdr_cleaner import clean_cdr as clean_consts
from sandbox import check_and_create_sandbox_dataset, get_sandbox_dataset_id
from constants import bq_utils as bq_consts

LOGGER = logging.getLogger(__name__)
LOGS_PATH = 'LOGS'

jinja_env = Environment(
    # help protect against cross-site scripting vulnerabilities
    autoescape=True,
    # block tags on their own lines
    # will not cause extra white space
    trim_blocks=True,
    lstrip_blocks=True,
    # syntax highlighting should be better
    # with these comment delimiters
    comment_start_string='--',
    comment_end_string=' --')

TABLE_INFORMATION_SCHEMA = jinja_env.from_string("""
SELECT *
FROM `{{project}}.{{dataset}}.INFORMATION_SCHEMA.COLUMNS`
""")
DEACTIVATED_PIDS_QUERY = jinja_env.from_string("""
SELECT DISTINCT *
FROM `{{project}}.{{dataset}}.{{table}}`
""")
RESEARCH_ID_QUERY = jinja_env.from_string("""
SELECT DISTINCT research_id
FROM `{{project}}.{{prefix_regex}}_combined._deid_map`
WHERE person_id = {{pid}}
""")
CHECK_PID_EXIST_QUERY = jinja_env.from_string("""
SELECT
COUNT(*) AS count
FROM `{{project}}.{{dataset}}.{{table}}`
WHERE person_id = {{pid}}
""")
# Queries to create tables in associated sandbox with rows that will be removed per cleaning rule, two different queries
# for tables with standard entry dates vs. tables with start and end dates
SANDBOX_QUERY_DATE = jinja_env.from_string("""
CREATE OR REPLACE TABLE `{{project}}.{{sandbox_dataset}}.{{intermediary_table}}` AS (
SELECT *
FROM `{{project}}.{{dataset}}.{{table}}`
WHERE person_id = {pid}
AND {{date_column}} >= (SELECT deactivated_date
FROM `{{deactivated_pids_project}}.{{deactivated_pids_dataset}}.{{deactivated_pids_table}}`
WHERE person_id = {{pid}}))
""")
SANDBOX_QUERY_END_DATE = jinja_env.from_string("""
CREATE OR REPLACE TABLE `{{project}}.{{sandbox_dataset}}.{{intermediary_table}}` AS (
SELECT *
FROM `{{project}}.{{dataset}}.{{table}}`
WHERE person_id = {{pid}}
AND (CASE WHEN {{end_date_column}} IS NOT NULL THEN {{end_date_column}} >= (SELECT deactivated_date
FROM `{{deactivated_pids_project}}.{{deactivated_pids_dataset}}.{{deactivated_pids_table}}`
WHERE person_id = {{pid}} ) ELSE CASE WHEN {{end_date_column}} IS NULL THEN {{start_date_column}} >= (
SELECT deactivated_date
FROM `{{deactivated_pids_project}}.{{deactivated_pids_dataset}}.{{deactivated_pids_table}}`
WHERE person_id = {{pid}}) END END))
""")
# Queries to truncate existing tables to remove deactivated EHR PIDS, two different queries for
# tables with standard entry dates vs. tables with start and end dates
CLEAN_QUERY_DATE = jinja_env.from_string("""
SELECT *
FROM `{{project}}.{{dataset}}.{{table}}`
WHERE person_id != {{pid}}
AND {{date_column}} < (SELECT deactivated_date
FROM `{{deactivated_pids_project}}.{{deactivated_pids_dataset}}.{{deactivated_pids_table}}`
WHERE person_id = {{pid}})
""")
CLEAN_QUERY_END_DATE = jinja_env.from_string("""
SELECT *
FROM `{{project}}.{{dataset}}.{{table}}`
WHERE person_id != {{pid}}
AND (CASE WHEN {{end_date_column}} IS NOT NULL THEN {{end_date_column}} < (SELECT deactivated_date
FROM `{{deactivated_pids_project}}.{{deactivated_pids_dataset}}.{{deactivated_pids_table}}`
WHERE person_id = {{pid}} ) ELSE CASE WHEN {{end_date_column}} IS NULL THEN {{start_date_column}} < (
SELECT deactivated_date
FROM `{{deactivated_pids_project}}.{{deactivated_pids_dataset}}.{{deactivated_pids_table}}`
WHERE person_id = {{pid}}) END END)
""")
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


def add_console_logging(add_handler):
    """
    This config should be done in a separate module, but that can wait
    until later.  Useful for debugging.
    """
    try:
        os.makedirs(LOGS_PATH)
    except OSError:
        # directory already exists.  move on.
        pass

    name = datetime.now().strftime(
        os.path.join(LOGS_PATH, 'ehr_deactivated_retraction-%Y-%m-%d.log'))
    logging.basicConfig(filename=name,
                        level=logging.INFO,
                        format='{asctime} - {name} - {levelname} - {message}',
                        style='{')

    if add_handler:
        handler = logging.StreamHandler()
        handler.setLevel(logging.INFO)
        formatter = logging.Formatter('{levelname} - {name} - {message}',
                                      style='{')
        handler.setFormatter(formatter)
        logging.getLogger('').addHandler(handler)


def get_pids_table_info(project_id, dataset_id):
    """
    This function gets all table information for the dataset and filters to only retain tables with the field person_id

    :param project_id: bq name of project_id
    :param dataset_id: ba name of dataset_id
    :return: dataframe with table information schema from BigQuery
    """
    all_table_info_query = TABLE_INFORMATION_SCHEMA.render(project=project_id,
                                                           dataset=dataset_id)
    result_df = bq.query(q=all_table_info_query, project_id=project_id)
    # Get list of tables that contain person_id
    pids_tables = []
    for i, row in result_df.iterrows():
        column = getattr(row, 'column_name')
        table = getattr(row, 'table_name')
        if 'person_id' in column:
            pids_tables.append(table)
    pids_tables_info_df = result_df[result_df['table_name'].isin(pids_tables)]
    return pids_tables_info_df

def get_date_info_for_pids_tables(project_id):
    """
    Loop through tables within all datasets and determine if the table has an end_date date or a date field. Filtering
    out person and death table and keeping only tables with PID and an upload or start/end date associated.

    :param project_id: bq name of project_id
    :return: filtered dataframe which includes the following columns for each table in each dataset with a person_id
    'project_id', 'dataset_id', 'table', 'date_column', 'start_date_column', 'end_date_column'
    """
    # Create empty df to append to for final output
    date_fields_info_df = pd.DataFrame()

    # Loop through datasets
    LOGGER.info(
        "Looping through datasets to filter and create dataframe with correct date field to determine retraction"
    )

    # dataset_obj = bq.list_datasets(project_id)
    # datasets = [d.dataset_id for d in dataset_obj]
    datasets = ['R2019q4r4_deid', '2019q4r4_combined', '2019q4r1_combined', 'R2019q4r1_deid', 'R2018q3r2_deid']

    for dataset in datasets:
        # Get table info for tables with pids
        pids_tables_df = get_pids_table_info(project_id, dataset)

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

    research_id_df = bq.query(q=RESEARCH_ID_QUERY.render(project=project, prefix_regex=prefix, pid=pid),
                              project_id=project)
    if research_id_df.empty:
        LOGGER.info(f"no research_id associated with person_id {pid}")
        return None

    return research_id_df['research_id'].iloc[0]


def check_pid_exist(pid, date_row, project_id):
    """
    Queries the table that retraction will take place, to see if the PID exists before creating query
    :param pid: person_id
    :param date_row: row that is being iterated through to create query
    :param project_id: bq name of project_id
    :return: Boolean if pid exists in table
    """
    check_pid_df = bq.query(q=CHECK_PID_EXIST_QUERY.render(project=date_row.project_id,
                                     dataset=date_row.dataset_id,
                                     table=date_row.table,
                                     pid=pid),
                            project_id=project_id)
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
    dataset_list = set()
    # Hit bq and receive df of deactivated ehr pids and deactivated date
    deactivated_ehr_pids_df = bq.query(q=DEACTIVATED_PIDS_QUERY.render(project=pids_project_id,
                                       dataset=pids_dataset_id,
                                       table=pids_table),
                                       project_id=project_id)

    date_columns_df = get_date_info_for_pids_tables(project_id)
    LOGGER.info(
        "Dataframe creation complete. DF to be used for creation of retraction queries."
    )

    LOGGER.info(
        "Looping through the deactivated PIDS df to create queries based on the retractions needed per PID table"
    )
    for ehr_row in deactivated_ehr_pids_df.itertuples(index=False):
        for date_row in date_columns_df.itertuples(index=False):
            # Determine if dataset is deid to correctly pull pid or research_id and check if ID exists in dataset or if
            # already retracted
            if re.match(DEID_REGEX, date_row.dataset_id):
                pid = get_research_id(date_row.project_id, date_row.dataset_id,
                                      ehr_row.person_id)
            else:
                pid = ehr_row.person_id

            # Check if PID is in table
            if pid is not None and check_pid_exist(pid, date_row, project_id) is True:
                dataset_list.add(date_row.dataset_id)
                # Get or create sandbox dataset
                sandbox_dataset = get_sandbox_dataset_id(date_row.dataset_id)
                #sandbox_dataset = check_and_create_sandbox_dataset(date_row.project_id, date_row.dataset_id)

                # Create queries based on if the date field is null, if True, will create query based on end_date/start_date
                if not pd.isnull(date_row.date_column):
                    sandbox_query = SANDBOX_QUERY_DATE.render(
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
                        clean_consts.DESTINATION_TABLE: date_row.table,
                        'type': 'sandbox'
                    })
                    clean_query = CLEAN_QUERY_DATE.render(
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
                        clean_consts.DISPOSITION: bq_consts.WRITE_TRUNCATE,
                        'type': 'retraction'
                    })
                else:
                    sandbox_query = SANDBOX_QUERY_END_DATE.render(
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
                        clean_consts.DESTINATION_TABLE: date_row.table,
                        'type': 'sandbox'
                    })
                    clean_query = CLEAN_QUERY_END_DATE.render(
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
                        clean_consts.DISPOSITION: bq_consts.WRITE_TRUNCATE,
                        'type': 'retraction'
                    })
            else:
                # break out of loop to create query, if pid does not exist in table
                continue
    LOGGER.info(
        f"Query list complete, retracting ehr deactivated PIDS from the following datasets: "
        f"{dataset_list}")
    return queries_list


def run_queries(queries, client):
    """
    Function that will perform the retraction.

    :param queries: list of queries to run retraction
    """
    query_job_ids = []
    for query_dict in queries:
        if query_dict['type'] == 'sandbox':
            LOGGER.info(
                f"Writing rows to be retracted to, using query {query_dict['query']}"
            )
            response = client.query(query_dict['query'])
            LOGGER.info(
                f"{query_dict['destination_table_id']} table written to {query_dict['destination_dataset_id']}"
            )
            query_job_id = response.job_id
            query_job_ids.append(query_job_id)
        else:
            LOGGER.info(
                f"Truncating table with clean data, using query {query_dict['query']}"
            )
            response = client.query(query_dict['query'])
            LOGGER.info(
                f"{query_dict['destination_table_id']} table updated with clean rows in "
                f"{query_dict['destination_dataset_id']}")
            query_job_id = response.job_id
            query_job_ids.append(query_job_id)

    incomplete_jobs = response.exception()
    if incomplete_jobs is not None:
        count = len(incomplete_jobs)
        if incomplete_jobs:
            LOGGER.info(f"Failed on {count} job ids {incomplete_jobs}")
            LOGGER.info("Terminating retraction")
            raise incomplete_jobs


def parse_args(raw_args=None):
    parser = argparse.ArgumentParser(
        description=
        'Runs retraction of deactivated EHR participants on all datasets in project. The rows to be retracted '
        'are based on the date entered and will be retracted if the date entered is before the retraction. '
        'Uses project_id, deactivated pid_table_id to determine '
        'the pids to retract data for. The pid_table_id needs to contain '
        'the person_id and deactivated_date columns specified in the schema above, '
        'but research_id can be null if deid has not been run yet. ',
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('-p',
                        '--project-id',
                        action='store',
                        dest='project_id',
                        help='Identifies the project to retract data from',
                        required=True)
    parser.add_argument('-t',
                        '--ticket-number',
                        action='store',
                        dest='ticket_number',
                        help='Ticket number to append to sandbox table names',
                        required=True)
    parser.add_argument(
        '-i',
        '--pids-project-id',
        action='store',
        dest='pids_project_id',
        help='Identifies the project where the pids table is stored',
        required=True)
    parser.add_argument(
        '-d',
        '--pids-dataset-id',
        action='store',
        dest='pids_dataset_id',
        help='Identifies the dataset where the pids table is stored',
        required=True)
    parser.add_argument('-s',
                        '--pids-table',
                        action='store',
                        dest='pids_table',
                        help='Identifies the table where the pids are stored',
                        required=True)
    parser.add_argument('-c',
                        '--console-log',
                        dest='console_log',
                        action='store_true',
                        required=False,
                        help='Log to the console as well as to a file.')
    return parser.parse_args(raw_args)


def main(args=None):
    args = parse_args(args)
    add_console_logging(args.console_log)
    query_list = create_queries(args.project_id, args.ticket_number,
                                args.pids_project_id, args.pids_dataset_id,
                                args.pids_table)
    client = bq.get_client(args.project_id)
    #run_queries(query_list, client)
    LOGGER.info("Retraction complete")


if __name__ == '__main__':
    main()
