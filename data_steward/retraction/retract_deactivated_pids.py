# Python imports
import argparse
import logging
from concurrent.futures import TimeoutError as TOError

# Third party imports
import google.cloud.bigquery as gbq
from google.cloud.exceptions import GoogleCloudError

import constants.retraction.retract_deactivated_pids as consts
import retraction.retract_utils as ru
from common import JINJA_ENV
from constants import bq_utils as bq_consts
from constants.cdr_cleaner import clean_cdr as cdr_consts
# Project imports
from utils import bq, pipeline_logging, sandbox as sb

LOGGER = logging.getLogger(__name__)

ISSUE_NUMBERS = ["DC-686", "DC-1184"]

TABLE_INFORMATION_SCHEMA = JINJA_ENV.from_string("""
SELECT *
FROM `{{project}}.{{dataset}}.INFORMATION_SCHEMA.COLUMNS`
""")

# Queries to create tables in associated sandbox with rows that will be removed per cleaning rule
SANDBOX_QUERY = JINJA_ENV.from_string("""
CREATE OR REPLACE TABLE `{{sandbox_ref.project}}.{{sandbox_ref.dataset_id}}.{{sandbox_ref.table_id}}` AS (
SELECT t.*
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

{% if has_mapping or has_ext %}
LEFT JOIN `{{mapping_ext_ref.project}}.{{mapping_ext_ref.dataset_id}}.{{mapping_ext_ref.table_id}}` m
USING ({{table_id}})
{% if has_mapping %}
WHERE src_hpo_id != 'PPI/PM'
{% elif has_ext %}
WHERE src_id != 'PPI/PM'
{% endif %}
{% endif %}

{% if has_start_date %}
AND (COALESCE({{end_date}}, EXTRACT(DATE FROM {{end_datetime}}),
    {{start_date}}, EXTRACT(DATE FROM {{start_datetime}})) >= d.deactivated_date
{% if table_ref.table_id == 'drug_exposure' %}
OR verbatim_end_date >= d.deactivated_date)
{% else %} )
{% endif %}
{% elif table_ref.table_id == 'death' %}
WHERE COALESCE(death_date, EXTRACT(DATE FROM death_datetime)) >= d.deactivated_date
{% elif table_ref.table_id in ['drug_era', 'condition_era', 'dose_era', 'payer_plan_period']  %}
AND COALESCE({{table_ref.table_id + '_end_date'}}, {{table_ref.table_id + '_start_date'}}) >= d.deactivated_date
{% else %}
AND COALESCE({{date}}, EXTRACT(DATE FROM {{datetime}})) >= d.deactivated_date
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


def get_date_cols_dict(date_cols_list):
    """
    Converts list of date/datetime columns into dictionary mappings

    Assumes each date column has a corresponding datetime column due to OMOP specifications
    If a date column does not have a corresponding datetime column, skips it
    Used for determining available dates based on order of precedence stated in the SANDBOX_QUERY
    end_date > end_datetime > start_date > start_datetime. Non-conforming dates are factored into
    the query separately, e.g. verbatim_end_date in drug_exposure
    :param date_cols_list: list of date/datetime columns
    :return: dictionary with mappings for START_DATE, START_DATETIME, END_DATE, END_DATETIME
        or DATE, DATETIME
    """
    date_cols_dict = {}
    for field in date_cols_list:
        if field.endswith(consts.START_DATETIME):
            date_cols_dict[consts.START_DATETIME] = field
        elif field.endswith(consts.END_DATETIME):
            date_cols_dict[consts.END_DATETIME] = field
        elif field.endswith(consts.DATETIME):
            date_cols_dict[consts.DATETIME] = field
    for field in date_cols_list:
        if field.endswith(consts.START_DATE):
            if date_cols_dict.get(consts.START_DATETIME, '').startswith(field):
                date_cols_dict[consts.START_DATE] = field
        elif field.endswith(consts.END_DATE):
            if date_cols_dict.get(consts.END_DATETIME, '').startswith(field):
                date_cols_dict[consts.END_DATE] = field
        elif field.endswith(consts.DATE):
            if date_cols_dict.get(consts.DATETIME, '').startswith(field):
                date_cols_dict[consts.DATE] = field
    return date_cols_dict


def get_table_cols_df(client, project_id, dataset_id):
    """
    Returns a df of dataset's INFORMATION_SCHEMA.COLUMNS

    :param project_id: bq name of project_id
    :param dataset_id: ba name of dataset_id
    :param client: bq client object
    :return: dataframe of columns from INFORMATION_SCHEMA
    """
    table_cols_query = TABLE_INFORMATION_SCHEMA.render(project=project_id,
                                                       dataset=dataset_id)
    table_cols_df = client.query(table_cols_query).to_dataframe()
    return table_cols_df


def get_table_dates_info(table_cols_df):
    """
    Returns a dict with tables containing pids and date columns

    :param table_cols_df: dataframe of columns from INFORMATION_SCHEMA
    :return: dataframe with key table and date columns as values
    """
    pids_tables = table_cols_df[table_cols_df['column_name'] ==
                                'person_id']['table_name']
    date_tables_df = table_cols_df[table_cols_df['column_name'].str.contains(
        "date")]

    dates_info = {}
    for table in pids_tables:
        date_cols = date_tables_df[date_tables_df['table_name'] ==
                                   table]['column_name']
        # exclude person since it does not contain EHR data
        if date_cols.any() and table != 'person':
            dates_info[table] = date_cols.to_list()

    return dates_info


def generate_queries(client,
                     project_id,
                     dataset_id,
                     sandbox_dataset_id,
                     deact_pids_table_ref,
                     pid_rid_table_ref=None):
    """
    Creates queries for sandboxing and deleting records

    :param client: BigQuery client
    :param project_id: Identifies the project
    :param dataset_id: Identifies the dataset to retract deactivated participants from
    :param sandbox_dataset_id: Identifies the dataset to store records to delete
    :param deact_pids_table_ref: BigQuery table reference to dataset containing deactivated participants
    :param pid_rid_table_ref: BigQuery table reference to dataset containing pid-rid mappings
    :return: List of query dicts
    :raises:
        RuntimeError: 1. If retracting from deid dataset, pid_rid table must be specified
                      2. If mapping or ext table does not exist, EHR data cannot be identified
    """
    table_cols_df = get_table_cols_df(client, project_id, dataset_id)
    table_dates_info = get_table_dates_info(table_cols_df)
    tables = table_cols_df['table_name'].to_list()
    is_deid = ru.is_deid_label_or_id(client, project_id, dataset_id)
    if is_deid and pid_rid_table_ref is None:
        raise RuntimeError(
            f"PID-RID mapping table must be specified for deid dataset {dataset_id}"
        )
    sandbox_queries = []
    clean_queries = []
    for table in table_dates_info:
        table_ref = gbq.TableReference.from_string(
            f"{project_id}.{dataset_id}.{table}")
        mapping_table = f'_mapping_{table}'
        ext_table = f'{table}_ext'
        has_mapping = mapping_table in tables
        has_ext = ext_table in tables
        if has_mapping:
            mapping_ext_ref = gbq.TableReference.from_string(
                f'{project_id}.{dataset_id}.{mapping_table}')
        elif has_ext:
            mapping_ext_ref = gbq.TableReference.from_string(
                f'{project_id}.{dataset_id}.{ext_table}')
        elif table == 'death':
            mapping_ext_ref = None
        else:
            raise RuntimeError(
                f"No mapping or ext tables for {table}, cannot identify EHR data"
            )
        sandbox_table = f"{'_'.join(ISSUE_NUMBERS).lower().replace('-', '_')}_{table}"
        sandbox_ref = gbq.TableReference.from_string(
            f"{project_id}.{sandbox_dataset_id}.{sandbox_table}")
        date_cols = get_date_cols_dict(table_dates_info[table])
        has_start_date = consts.START_DATE in date_cols
        sandbox_queries.append({
            cdr_consts.QUERY:
                SANDBOX_QUERY.render(table_ref=table_ref,
                                     mapping_ext_ref=mapping_ext_ref,
                                     has_mapping=has_mapping,
                                     has_ext=has_ext,
                                     table_id=f'{table}_id',
                                     sandbox_ref=sandbox_ref,
                                     pid_rid_table=pid_rid_table_ref,
                                     deact_pids_table=deact_pids_table_ref,
                                     is_deid=is_deid,
                                     has_start_date=has_start_date,
                                     **date_cols)
        })
        clean_queries.append({
            cdr_consts.QUERY:
                CLEAN_QUERY.render(table_ref=table_ref,
                                   sandbox_ref=sandbox_ref),
            cdr_consts.DESTINATION_TABLE:
                table,
            cdr_consts.DESTINATION_DATASET:
                dataset_id,
            cdr_consts.DISPOSITION:
                bq_consts.WRITE_TRUNCATE
        })
    return sandbox_queries + clean_queries


def query_runner(client, query_dict):
    """
    Runs the query specified via query_dict

    :param client: BigQuery client
    :param query_dict: Query dictionary as used by cleaning rules
    :return: job_id: BigQuery job id for the query job
    :raises: RuntimeError, GoogleCloudError, TOError: If BigQuery job fails
    """
    job_config = gbq.job.QueryJobConfig()

    if query_dict.get(cdr_consts.DESTINATION_TABLE) is not None:
        destination_table = gbq.TableReference.from_string(
            f'{client.project}.'
            f'{query_dict[cdr_consts.DESTINATION_DATASET]}.'
            f'{query_dict[cdr_consts.DESTINATION_TABLE]}')
        job_config.destination = destination_table
        job_config.write_disposition = query_dict.get(cdr_consts.DISPOSITION,
                                                      bq_consts.WRITE_EMPTY)

    try:
        query_job = client.query(query=query_dict.get(cdr_consts.QUERY),
                                 job_config=job_config,
                                 job_id_prefix='deact_')
        job_id = query_job.job_id
        LOGGER.info(f'Running deactivation job {job_id}')

        query_job.result()
        if query_job.errors:
            raise RuntimeError(
                f"Job {query_job.job_id} failed with error {query_job.errors} for query"
                f"{query_dict[cdr_consts.QUERY]}")
    except (GoogleCloudError, TOError) as exp:
        LOGGER.exception(f"Error {exp} while running query"
                         f"{query_dict[cdr_consts.QUERY]}")
        raise exp
    return job_id


def fq_table_name_verification(fq_table_name):
    """
    Ensures fq_table_name is of the format 'project.dataset.table'

    :param fq_table_name: fully qualified BQ table name
    :return: fq_table_name if valid
    :raises: ArgumentTypeError if invalid
    """
    if fq_table_name.count('.') == 2:
        return fq_table_name
    message = f"{fq_table_name} should be of the form 'project.dataset.table'"
    raise argparse.ArgumentTypeError(message)


def fq_deactivated_table_verification(fq_table_name):
    """
    Ensures fq_table_name is of the format 'project.dataset.table'

    :param fq_table_name: fully qualified BQ table name
    :return: fq_table_name if valid
    :raises: ArgumentTypeError if invalid
    """
    fq_table_name = fq_table_name_verification(fq_table_name)
    if fq_table_name.split('.')[-1] == consts.DEACTIVATED_PARTICIPANTS:
        return fq_table_name
    message = f"{fq_table_name} should be of the form 'project.dataset.{consts.DEACTIVATED_PARTICIPANTS}'"
    raise argparse.ArgumentTypeError(message)


def get_base_parser():
    parser = argparse.ArgumentParser(
        description='Retracts deactivated participants',
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        '-p',
        '--project_id',
        action='store',
        dest='project_id',
        help=
        'Identifies the project containing the dataset(s) to retract data from',
        required=True)
    parser.add_argument('-s',
                        '--console_log',
                        dest='console_log',
                        action='store_true',
                        help='Send logs to console')
    return parser


def get_parser():
    parser = get_base_parser()
    parser.add_argument('-d',
                        '--dataset_ids',
                        action='store',
                        nargs='+',
                        dest='dataset_ids',
                        help='Identifies datasets to target. Set to '
                        '"all_datasets" to target all datasets in project '
                        'or specific datasets as -d dataset_1 dataset_2 etc.',
                        required=True)
    parser.add_argument('-a',
                        '--fq_deact_table',
                        action='store',
                        dest='fq_deact_table',
                        type=fq_table_name_verification,
                        help='Specify fully qualified deactivated table '
                        'as "project.dataset.table"',
                        required=True)
    parser.add_argument('-r',
                        '--fq_pid_rid_table',
                        action='store',
                        dest='fq_pid_rid_table',
                        type=fq_table_name_verification,
                        help='Specify fully qualified pid-rid mapping table '
                        'as "project.dataset.table"')
    return parser


def run_deactivation(client,
                     project_id,
                     dataset_ids,
                     fq_deact_table,
                     fq_pid_rid_table=None):
    """
    Runs the deactivation retraction pipeline for a dataset

    :param client: BigQuery client
    :param project_id: Identifies the BigQuery project
    :param dataset_ids: Identifies the datasets to retract deactivated participants from
    :param fq_deact_table: Fully qualified table containing deactivated participants
        and deactivated dates as 'project.dataset.table'
    :param fq_pid_rid_table: Fully qualified table containing mappings from person_ids
        to research_ids as 'project.dataset.table'
    :return:job_ids: List of BigQuery job ids to perform the retraction as strings
    """
    pid_rid_table_ref = gbq.TableReference.from_string(
        fq_pid_rid_table) if fq_pid_rid_table else None
    deact_table_ref = gbq.TableReference.from_string(fq_deact_table)
    job_ids = {}
    for dataset_id in dataset_ids:
        job_ids[dataset_id] = []
        LOGGER.info(f"Retracting deactivated participants from '{dataset_id}'")
        sandbox_dataset_id = sb.check_and_create_sandbox_dataset(
            project_id, dataset_id)
        LOGGER.info(
            f"Using sandbox dataset '{sandbox_dataset_id}' for '{dataset_id}'")
        queries = generate_queries(client, project_id, dataset_id,
                                   sandbox_dataset_id, deact_table_ref,
                                   pid_rid_table_ref)
        for query in queries:
            job_id = query_runner(client, query)
            job_ids[dataset_id].append(job_id)
        LOGGER.info(
            f"Successfully retracted from {dataset_id} via jobs {job_ids[dataset_id]}"
        )
    return job_ids


def main(args=None):
    pipeline_logging.configure(logging.DEBUG, add_console_handler=True)
    parser = get_parser()
    args = parser.parse_args(args)
    client = bq.get_client(args.project_id)
    dataset_ids = ru.get_datasets_list(args.project_id, args.dataset_ids)
    LOGGER.info(
        f"Datasets to retract deactivated participants from: {dataset_ids}")
    run_deactivation(client, args.project_id, dataset_ids, args.fq_deact_table,
                     args.fq_pid_rid_table)
    LOGGER.info(
        f"Retraction of deactivated participants from {dataset_ids} complete")


if __name__ == '__main__':
    main()
