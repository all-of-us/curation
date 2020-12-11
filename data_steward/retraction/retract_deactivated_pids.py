# Python imports
import argparse
import logging
import re

# Third party imports
import pandas as pd
import google.cloud.bigquery as gbq

# Project imports
from utils import bq, pipeline_logging
import retraction.retract_utils as ru
import sandbox as sb
import constants.retraction.retract_deactivated_pids as consts
from constants import bq_utils as bq_consts
from constants.cdr_cleaner import clean_cdr as clean_consts
from common import JINJA_ENV

LOGGER = logging.getLogger(__name__)

ISSUE_NUMBERS = ["DC-1184"]

TABLE_INFORMATION_SCHEMA = JINJA_ENV.from_string("""
SELECT *
FROM `{{project}}.{{dataset}}.INFORMATION_SCHEMA.COLUMNS`
""")

DEACTIVATED_PIDS_QUERY = JINJA_ENV.from_string("""
SELECT DISTINCT *
FROM `{{project}}.{{dataset}}.{{table}}`
""")

RESEARCH_ID_QUERY = JINJA_ENV.from_string("""
SELECT DISTINCT research_id
FROM `{{project}}.{{prefix_regex}}_combined._deid_map`
WHERE person_id = {{pid}}
""")

# Queries to create tables in associated sandbox with rows that will be removed per cleaning rule
SANDBOX_QUERY = JINJA_ENV.from_string("""
CREATE OR REPLACE TABLE `{{sandbox_ref.project}}.{{sandbox_ref.dataset_id}}.{{sandbox_ref.table_id}}` s AS (
SELECT *
FROM `{{table_ref.project}}.{{table_ref.dataset_id}}.{{table_ref.table_id}}` t
{% if is_deid %}
JOIN `{{pid_rid_table.project}}.{{pid_rid_table.dataset_id}}.{{pid_rid_table.table_id}}` p
ON t.person_id = p.research_id
JOIN `{{deact_pids_table.project}}.{{deact_pids_table.dataset_id}}.{{deact_pids_table.table_id}}` d
ON p.person_id = d.person_id
{% else %}
JOIN `{{deact_pids_table.project}}.{{deact_pids_table.dataset_id}}.{{deact_pids_table.table_id}}` d
USING (person_id)
{% endif %}
{% if has_start_date %}
WHERE COALESCE({{end_date}}, EXTRACT(DATE FROM {{end_datetime}}),
    {{start_date}}, EXTRACT(DATE FROM {{start_datetime}})) >= d.deactivated_date
{% else %}
WHERE COALESCE({{date}}, EXTRACT(DATE FROM {{datetime}})) >= d.deactivated_date
{% endif %})
""")

# Queries to truncate existing tables to remove deactivated EHR PIDS, two different queries for
# tables with standard entry dates vs. tables with start and end dates
CLEAN_QUERY = JINJA_ENV.from_string("""
SELECT *
FROM `{{table_ref.project}}.{{table_ref.dataset_id}}.{{table_ref.table_id}}`
EXCEPT DISTINCT
SELECT *
FROM `{{sandbox_ref.project}}.{{sandbox_ref.dataset_id}}.{{sandbox_ref.table_id}}`
""")

CHECK_PID_EXIST_DATE_QUERY = JINJA_ENV.from_string("""
SELECT
COALESCE(COUNT(*), 0) AS count
FROM `{{project}}.{{dataset}}.{{table}}`
WHERE person_id IN (SELECT person_id
FROM `{{deactivated_pids_project}}.{{deactivated_pids_dataset}}.{{deactivated_pids_table}}`)
AND {{date_column}} > (SELECT MIN(deactivated_date)
FROM `{{deactivated_pids_project}}.{{deactivated_pids_dataset}}.{{deactivated_pids_table}}`)
""")

CHECK_PID_EXIST_END_DATE_QUERY = JINJA_ENV.from_string("""
SELECT
COALESCE(COUNT(*), 0) AS count
FROM `{{project}}.{{dataset}}.{{table}}`
WHERE person_id IN (SELECT person_id
FROM `{{deactivated_pids_project}}.{{deactivated_pids_dataset}}.{{deactivated_pids_table}}`)
AND (CASE WHEN {{end_date_column}} IS NOT NULL THEN {{end_date_column}} > (SELECT MIN(deactivated_date)
FROM `{{deactivated_pids_project}}.{{deactivated_pids_dataset}}.{{deactivated_pids_table}}`) 
ELSE CASE WHEN {{end_date_column}} IS NULL THEN {{start_date_column}} > (
SELECT MIN(deactivated_date)
FROM `{{deactivated_pids_project}}.{{deactivated_pids_dataset}}.{{deactivated_pids_table}}`) END END)
""")

# Queries to create tables in associated sandbox with rows that will be removed per cleaning rule.
# Two different queries 1. tables containing standard entry dates 2. tables with start and end dates
SANDBOX_QUERY_DATE = JINJA_ENV.from_string("""
SELECT *
FROM `{{project}}.{{dataset}}.{{table}}`
WHERE person_id = {{pid}}
AND {{date_column}} >= (SELECT deactivated_date
FROM `{{deactivated_pids_project}}.{{deactivated_pids_dataset}}.{{deactivated_pids_table}}`
WHERE person_id = {{pid}})
""")

SANDBOX_QUERY_END_DATE = JINJA_ENV.from_string("""
SELECT *
FROM `{{project}}.{{dataset}}.{{table}}`
WHERE person_id = {{pid}}
AND (CASE WHEN {{end_date_column}} IS NOT NULL THEN {{end_date_column}} >= (SELECT deactivated_date
FROM `{{deactivated_pids_project}}.{{deactivated_pids_dataset}}.{{deactivated_pids_table}}`
WHERE person_id = {{pid}} ) ELSE CASE WHEN {{end_date_column}} IS NULL THEN {{start_date_column}} >= (
SELECT deactivated_date
FROM `{{deactivated_pids_project}}.{{deactivated_pids_dataset}}.{{deactivated_pids_table}}`
WHERE person_id = {{pid}}) END END)
""")

# Queries to truncate existing tables to remove deactivated EHR PIDS, two different queries for
# tables with standard entry dates vs. tables with start and end dates
CLEAN_QUERY_DATE = JINJA_ENV.from_string("""
SELECT *
FROM `{{project}}.{{dataset}}.{{table}}`
WHERE person_id != {{pid}}
OR (person_id = {{pid}}
AND {{date_column}} < (SELECT deactivated_date
FROM `{{deactivated_pids_project}}.{{deactivated_pids_dataset}}.{{deactivated_pids_table}}`
WHERE person_id = {{pid}}))
""")

CLEAN_QUERY_END_DATE = JINJA_ENV.from_string("""
SELECT *
FROM `{{project}}.{{dataset}}.{{table}}`
WHERE person_id != {{pid}}
OR (person_id = {{pid}}
AND (CASE WHEN {{end_date_column}} IS NOT NULL THEN {{end_date_column}} < (SELECT deactivated_date
FROM `{{deactivated_pids_project}}.{{deactivated_pids_dataset}}.{{deactivated_pids_table}}`
WHERE person_id = {{pid}} ) ELSE CASE WHEN {{end_date_column}} IS NULL THEN {{start_date_column}} < (
SELECT deactivated_date
FROM `{{deactivated_pids_project}}.{{deactivated_pids_dataset}}.{{deactivated_pids_table}}`
WHERE person_id = {{pid}}) END END))
""")

# Deactivated participant table fields to query off of
PID_TABLE_FIELDS = [[{
    "type": "integer",
    "name": "person_id",
    "mode": "required",
    "description": "The person_id to retract data for"
}, {
    "type": "date",
    "name": "deactivated_date",
    "mode": "required",
    "description": "The deactivation date to base retractions on"
}]]


def get_pids_table_info(project_id, dataset_id, client):
    """
    This function gets all table information for the dataset and filters to only retain tables with the field person_id

    :param project_id: bq name of project_id
    :param dataset_id: ba name of dataset_id
    :param client: bq client object
    :return: dataframe with table information schema from BigQuery
    """
    all_table_info_query = TABLE_INFORMATION_SCHEMA.render(project=project_id,
                                                           dataset=dataset_id)
    result_df = client.query(all_table_info_query).to_dataframe()
    # Get list of tables that contain person_id
    pids_tables = []
    for _, row in result_df.iterrows():
        column = getattr(row, 'column_name')
        table = getattr(row, 'table_name')
        if 'person_id' in column:
            pids_tables.append(table)

    pids_table_info_df = pd.DataFrame()

    if pids_tables and len(result_df) != 0:
        pids_table_info_df = result_df[result_df['table_name'].isin(
            pids_tables)]

    return pids_table_info_df


def get_table_dates_info(client, project_id, dataset_id):
    """
    This function returns a dict with tables containing pids and date columns

    :param project_id: bq name of project_id
    :param dataset_id: ba name of dataset_id
    :param client: bq client object
    :return: dataframe with key table and date columns as values
    """
    table_cols_query = TABLE_INFORMATION_SCHEMA.render(project=project_id,
                                                       dataset=dataset_id)
    table_cols_df = client.query(table_cols_query).to_dataframe()

    pids_tables = table_cols_df[table_cols_df['column_name'] ==
                                'person_id']['table_name']
    date_tables_df = table_cols_df[table_cols_df['column_name'].str.contains(
        "date")]

    dates_info = {}
    for table in pids_tables:
        date_cols = date_tables_df[date_tables_df['table_name'] ==
                                   table]['column_name']
        if date_cols.any():
            dates_info[table] = date_cols.to_list()

    return dates_info


def get_date_info_for_pids_tables(project_id, client, datasets=None):
    """
    Loop through tables within all datasets and determine if the table has an end_date date or a date field. Filtering
    out the person table and keeping only tables with PID and an upload or start/end date associated.

    :param project_id: bq name of project_id
    :param client: bq client object
    :param datasets: optional parameter to give list of datasets, otherwise will loop through all datasets in project_id
    :return: filtered dataframe which includes the following columns for each table in each dataset with a person_id
    'project_id', 'dataset_id', 'table', 'date_column', 'start_date_column', 'end_date_column'
    """
    # Create empty df to append to for final output
    date_fields_info_df = pd.DataFrame()

    # Loop through datasets
    LOGGER.info(
        "Looping through datasets to filter and create dataframe with correct date field to determine retraction"
    )

    if datasets is None:
        dataset_obj = client.list_datasets(project_id)
        datasets = [d.dataset_id for d in dataset_obj]
    else:
        datasets = datasets

    # Remove synthetic data, vocabulary, curation sandbox and previous naming convention datasets
    prefixes = ('SR', 'vocabulary', 'curation', 'combined', '2018', 'R2018',
                'rdr')
    datasets = [x for x in datasets if not x.startswith(prefixes)]

    for dataset in datasets:
        LOGGER.info(f'Starting to iterate through dataset: {dataset}')

        # Get table info for tables with pids
        pids_tables_df = get_pids_table_info(project_id, dataset, client)

        # Check to see if dataset is empty, if empty break out of loop
        if pids_tables_df.empty:
            LOGGER.info(
                f'No tables in dataset:{dataset}, skipping over dataset')
            continue

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

        # Remove person table
        df_to_append = df_to_append[~df_to_append.table.str.contains('person')]
        df_to_iterate = df_to_iterate[~df_to_iterate.table.str.contains('person'
                                                                       )]

        # Filter through date columns and append to the appropriate column
        for _, row in df_to_iterate.iterrows():
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
        LOGGER.info(f'Iteration complete through dataset: {dataset}')

    return date_fields_info_df


def get_research_id(project, dataset, pid, client):
    """
    For deid datasets, this function queries the _deid_map table in the associated combined dataset based on the release
    regex prefix; to return the research_id for the specific pid passed

    :param project: bq name of project
    :param dataset: bq name of dataset
    :param pid: person_id passed to receive associated research_id
    :param client: bq client object
    :return: research_id or None if it does not exist for that person_id
    """
    # Get non deid dataset prefix
    prefix = dataset.split('_')[0]
    prefix = prefix.replace('R', '')

    research_id_df = client.query(
        RESEARCH_ID_QUERY.render(project=project, prefix_regex=prefix,
                                 pid=pid)).to_dataframe()
    if research_id_df.empty:
        LOGGER.info(f"no research_id associated with person_id {pid}")
        return None

    return research_id_df['research_id'].iloc[0]


def check_pid_exist(date_row, client, pids_project_id, pids_dataset_id,
                    pids_table):
    """
    Queries the table that retraction will take place, to see if the PID exists after the deactivation date,
    before creating query
    :param date_row: row that is being iterated through to create query
    :param client: bq client object
    :param pids_project_id: bq name of project_id for deactivated pids table
    :param pids_dataset_id: bq name of dataset_id for deactivated pids table
    :param pids_table: bq name of table for deactivated pids table
    :return: count of records in query
    """
    if pd.isnull(date_row.date_column):
        check_pid_df = client.query(
            CHECK_PID_EXIST_END_DATE_QUERY.render(
                project=date_row.project_id,
                dataset=date_row.dataset_id,
                table=date_row.table,
                deactivated_pids_project=pids_project_id,
                deactivated_pids_dataset=pids_dataset_id,
                deactivated_pids_table=pids_table,
                end_date_column=date_row.end_date_column,
                start_date_column=date_row.start_date_column)).to_dataframe()
    else:
        check_pid_df = client.query(
            CHECK_PID_EXIST_DATE_QUERY.render(
                project=date_row.project_id,
                dataset=date_row.dataset_id,
                table=date_row.table,
                date_column=date_row.date_column,
                deactivated_pids_project=pids_project_id,
                deactivated_pids_dataset=pids_dataset_id,
                deactivated_pids_table=pids_table)).to_dataframe()
    return check_pid_df.loc[0, 'count']


def get_date_cols_dict(date_cols_list):
    date_cols_dict = {}
    for date_col in date_cols_list:
        if consts.START_DATETIME in date_col:
            date_cols_dict[consts.START_DATETIME] = date_col
        elif consts.START_DATE in date_col:
            date_cols_dict[consts.START_DATE] = date_col
        elif consts.END_DATETIME in date_col:
            date_cols_dict[consts.END_DATETIME] = date_col
        elif consts.END_DATE in date_col:
            date_cols_dict[consts.END_DATE] = date_col
        elif consts.DATETIME in date_col:
            date_cols_dict[consts.DATETIME] = date_col
        elif consts.DATE in date_col:
            date_cols_dict[consts.DATE] = date_col
    return date_cols_dict


def generate_queries(client, project_id, dataset_id, pid_rid_table_ref,
                     deact_pids_table_ref):
    table_dates = get_table_dates_info(client, project_id, dataset_id)
    sandbox_dataset = sb.get_sandbox_dataset_id(dataset_id)
    is_deid = ru.is_deid_dataset(dataset_id)
    sandbox_queries = []
    clean_queries = []
    for table in table_dates:
        table_ref = gbq.TableReference.from_string(
            f"{project_id}.{dataset_id}.{table}")
        sandbox_table = f"{'_'.join(ISSUE_NUMBERS).lower().replace('-', '_')}_{table}"
        sandbox_ref = gbq.TableReference.from_string(
            f"{project_id}.{sandbox_dataset}.{sandbox_table}")
        date_cols = get_date_cols_dict(table_dates[table])
        has_start_date = consts.START_DATE in date_cols
        sandbox_queries.append({
            clean_consts.QUERY:
                SANDBOX_QUERY.render(table_ref=table_ref,
                                     sandbox_ref=sandbox_ref,
                                     pid_rid_table=pid_rid_table_ref,
                                     deact_pids_table=deact_pids_table_ref,
                                     is_deid=is_deid,
                                     has_start_date=has_start_date,
                                     **date_cols)
        })
        clean_queries.append({
            clean_consts.QUERY:
                CLEAN_QUERY.render(table_ref=table_ref,
                                   sandbox_ref=sandbox_ref),
            clean_consts.DESTINATION_TABLE:
                table,
            clean_consts.DESTINATION_DATASET:
                dataset_id,
            clean_consts.DISPOSITION:
                bq_consts.WRITE_TRUNCATE
        })
    return sandbox_queries + clean_queries


def create_queries(project_id,
                   ticket_number,
                   pids_project_id,
                   pids_dataset_id,
                   pids_table,
                   datasets=None):
    """
    Creates sandbox and truncate queries to run for EHR deactivated retraction

    :param project_id: Identifies the project where data is being retracted
    :param ticket_number: Jira ticket number to identify and title sandbox table
    :param pids_project_id: Identifies the project containing deactivated pids table
    :param pids_dataset_id: Identifies the dataset containing deactivated pids table
    :param pids_table: Name of the deactivated pids table. This table should have
      the following fields: (person_id: int, suspension_status: string, deactivated_date: date).
    :param datasets: (optional) List of datasets to retract from. If not provided,
      retraction will be performed from all datasets in project referred to by `project_id`.
    :return: list of query dictionaries

    NOTE: For dataset_ids matching `retraction.retract_utils.DEID_REGEX`, associated research_ids
    retrieved from an inferred combined dataset are used for retraction.
    """
    queries_list = []
    dataset_list = set()
    final_date_column_df = pd.DataFrame()
    # Hit bq and receive df of deactivated ehr pids and deactivated date
    client = bq.get_client(project_id)
    deactivated_ehr_pids_df = client.query(
        DEACTIVATED_PIDS_QUERY.render(project=pids_project_id,
                                      dataset=pids_dataset_id,
                                      table=pids_table)).to_dataframe()
    if datasets is None:
        date_columns_df = get_date_info_for_pids_tables(project_id, client)
    else:
        date_columns_df = get_date_info_for_pids_tables(project_id, client,
                                                        datasets)
    LOGGER.info(
        "Dataframe creation complete. DF to be used for creation of retraction queries."
    )
    for date_row in date_columns_df.itertuples(index=False):
        # Filter to only include tables containing deactivated pids with the earliest deactivated date
        LOGGER.info(
            f'Checking table: {date_row.project_id}.{date_row.dataset_id}.{date_row.table}'
        )
        if check_pid_exist(date_row, client, pids_project_id, pids_dataset_id,
                           pids_table):
            dataset_list.add(date_row.dataset_id)
            row = {
                'project_id': date_row.project_id,
                'dataset_id': date_row.dataset_id,
                'table': date_row.table,
                'date_column': date_row.date_column,
                'start_date_column': date_row.start_date_column,
                'end_date_column': date_row.end_date_column
            }
            final_date_column_df = final_date_column_df.append(
                row, ignore_index=True)

    LOGGER.info(
        "Looping through the deactivated PIDS df to create queries based on the retractions needed per PID table"
    )
    for ehr_row in deactivated_ehr_pids_df.itertuples(index=False):
        LOGGER.info(f'Creating retraction queries for PID: {ehr_row.person_id}')
        for date_row in final_date_column_df.itertuples(index=False):
            # Determine if dataset is deid to correctly pull pid or research_id and check if ID exists in dataset or if
            # already retracted
            if re.match(ru.DEID_REGEX, date_row.dataset_id):
                pid = get_research_id(date_row.project_id, date_row.dataset_id,
                                      ehr_row.person_id, client)
            else:
                pid = ehr_row.person_id

            # Get or create sandbox dataset
            sandbox_dataset = sb.check_and_create_sandbox_dataset(
                date_row.project_id, date_row.dataset_id)

            # Create queries based on type of date field
            LOGGER.info(
                f'Creating Query to retract {pid} from {date_row.dataset_id}.{date_row.table}'
            )
            if pd.isnull(date_row.date_column):
                sandbox_query = SANDBOX_QUERY_END_DATE.render(
                    project=date_row.project_id,
                    sandbox_dataset=sandbox_dataset,
                    dataset=date_row.dataset_id,
                    table=date_row.table,
                    pid=pid,
                    deactivated_pids_project=pids_project_id,
                    deactivated_pids_dataset=pids_dataset_id,
                    deactivated_pids_table=pids_table,
                    end_date_column=date_row.end_date_column,
                    start_date_column=date_row.start_date_column)
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
            else:
                sandbox_query = SANDBOX_QUERY_DATE.render(
                    project=date_row.project_id,
                    sandbox_dataset=sandbox_dataset,
                    dataset=date_row.dataset_id,
                    table=date_row.table,
                    pid=pid,
                    deactivated_pids_project=pids_project_id,
                    deactivated_pids_dataset=pids_dataset_id,
                    deactivated_pids_table=pids_table,
                    date_column=date_row.date_column)
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
                clean_consts.QUERY:
                    sandbox_query,
                clean_consts.DESTINATION:
                    date_row.project_id + '.' + sandbox_dataset + '.' +
                    (ticket_number + '_' + date_row.table),
                clean_consts.DESTINATION_DATASET:
                    date_row.dataset_id,
                clean_consts.DESTINATION_TABLE:
                    date_row.table,
                clean_consts.DISPOSITION:
                    bq_consts.WRITE_APPEND,
                'type':
                    'sandbox'
            })
            queries_list.append({
                clean_consts.QUERY:
                    clean_query,
                clean_consts.DESTINATION:
                    date_row.project_id + '.' + date_row.dataset_id + '.' +
                    date_row.table,
                clean_consts.DESTINATION_DATASET:
                    date_row.dataset_id,
                clean_consts.DESTINATION_TABLE:
                    date_row.table,
                clean_consts.DISPOSITION:
                    bq_consts.WRITE_TRUNCATE,
                'type':
                    'retraction'
            })
    LOGGER.info(
        f"Query list complete, retracting ehr deactivated PIDS from the following datasets: "
        f"{dataset_list}")
    return queries_list


def run_queries(queries, client):
    """
    Function that will perform the retraction.

    :param queries: list of queries to run retraction
    :param client: bq client object
    """
    incomplete_jobs = []
    for query_dict in queries:
        # Set configuration.query
        job_config = gbq.QueryJobConfig(
            use_query_cache=False,
            destination=query_dict['destination'],
            write_disposition=query_dict['write_disposition'],
            create_disposition='CREATE_IF_NEEDED')

        if query_dict['type'] == 'sandbox':
            LOGGER.info(
                f"Writing rows to be retracted, using query {query_dict['query']}"
            )
            job = client.query(query=query_dict['query'], job_config=job_config)
            job.result()
            if job.exception():
                incomplete_jobs.append(job)
            else:
                LOGGER.info(
                    f"{query_dict['destination_table_id']} table written to {query_dict['destination_dataset_id']}"
                )
        else:
            LOGGER.info(
                f"Truncating table with clean data, using query {query_dict['query']}"
            )
            job = client.query(query_dict['query'], job_config=job_config)
            job.result()
            if job.exception():
                incomplete_jobs.append(job)
            else:
                LOGGER.info(
                    f"{query_dict['destination_table_id']} table updated with clean rows in "
                    f"{query_dict['destination_dataset_id']}")

    if incomplete_jobs:
        count = len(incomplete_jobs)
        LOGGER.info(f"Failed on {count} job ids {incomplete_jobs}")
        LOGGER.info("Terminating retraction")


def parse_args(raw_args=None):
    parser = argparse.ArgumentParser(
        description=
        'Runs retraction of deactivated EHR participants on specified datasets',
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
    parser.add_argument(
        '-l',
        '--dataset_list',
        dest='dataset_list',
        action='append',
        required=False,
        help=
        'Optional parameter, list of datasets to run retraction on vs. entire project send '
        'multiple as separate argument ie: -l dataset_1 -l dataset_2')
    parser.add_argument('-c',
                        '--console-log',
                        dest='console_log',
                        action='store_true',
                        required=False,
                        help='Log to the console as well as to a file.')
    return parser.parse_args(raw_args)


def main(args=None):
    pipeline_logging.configure(logging.DEBUG, add_console_handler=True)
    args = parse_args(args)
    query_list = create_queries(args.project_id, args.ticket_number,
                                args.pids_project_id, args.pids_dataset_id,
                                args.pids_table, args.dataset_list)
    client = bq.get_client(args.project_id)
    run_queries(query_list, client)
    LOGGER.info("Retraction complete")


if __name__ == '__main__':
    main()
