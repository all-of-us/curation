# Python imports
import argparse
import logging
from concurrent.futures import TimeoutError as TOError

# Third party imports
import google.cloud.bigquery as gbq
from google.cloud.exceptions import GoogleCloudError

# Project imports
from utils import bq, pipeline_logging
import retraction.retract_utils as ru
import sandbox as sb
import constants.retraction.retract_deactivated_pids as consts
from constants import bq_utils as bq_consts
from constants.cdr_cleaner import clean_cdr as cdr_consts
from common import JINJA_ENV

LOGGER = logging.getLogger(__name__)

ISSUE_NUMBERS = ["DC-1184"]

TABLE_INFORMATION_SCHEMA = JINJA_ENV.from_string("""
SELECT *
FROM `{{project}}.{{dataset}}.INFORMATION_SCHEMA.COLUMNS`
""")

# Queries to create tables in associated sandbox with rows that will be removed per cleaning rule
SANDBOX_QUERY = JINJA_ENV.from_string("""
CREATE TABLE `{{sandbox_ref.project}}.{{sandbox_ref.dataset_id}}.{{sandbox_ref.table_id}}` AS (
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
AND COALESCE({{end_date}}, EXTRACT(DATE FROM {{end_datetime}}),
    {{start_date}}, EXTRACT(DATE FROM {{start_datetime}})) >= d.deactivated_date
{% elif table_ref.table_id == 'death' %}
WHERE COALESCE(death_date, EXTRACT(DATE FROM death_datetime)) >= d.deactivated_date
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
    date_cols_dict = {}
    i = 0
    while i < len(date_cols_list):
        if consts.START_DATETIME in date_cols_list[i]:
            date_cols_dict[consts.START_DATETIME] = date_cols_list.pop(i)
        elif consts.END_DATETIME in date_cols_list[i]:
            date_cols_dict[consts.END_DATETIME] = date_cols_list.pop(i)
        elif consts.DATETIME in date_cols_list[i]:
            date_cols_dict[consts.DATETIME] = date_cols_list.pop(i)
        else:
            i += 1
    i = 0
    while i < len(date_cols_list):
        if consts.START_DATE in date_cols_list[i]:
            start_date = date_cols_list.pop(i)
            if start_date in date_cols_dict.get(consts.START_DATETIME, ''):
                date_cols_dict[consts.START_DATE] = start_date
        elif consts.END_DATE in date_cols_list[i]:
            end_date = date_cols_list.pop(i)
            if end_date in date_cols_dict.get(consts.END_DATETIME, ''):
                date_cols_dict[consts.END_DATE] = end_date
        elif consts.DATE in date_cols_list[i]:
            date = date_cols_list.pop(i)
            if date in date_cols_dict.get(consts.DATETIME, ''):
                date_cols_dict[consts.DATE] = date
        else:
            i += 1
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
    table_cols_df = get_table_cols_df(client, project_id, dataset_id)
    table_dates_info = get_table_dates_info(table_cols_df)
    tables = table_cols_df['table_name'].to_list()
    is_deid = ru.is_deid_dataset(dataset_id)
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
    job_config = gbq.job.QueryJobConfig()

    if query_dict.get(cdr_consts.DESTINATION_TABLE) is not None:
        destination_table = gbq.TableReference.from_string(
            f'{client.project}.{query_dict[cdr_consts.DESTINATION_DATASET]}.{query_dict[cdr_consts.DESTINATION_TABLE]}'
        )
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


def get_parser():
    parser = argparse.ArgumentParser(
        description='Retracts deactivated participants',
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('-p',
                        '--project_id',
                        action='store',
                        dest='project_id',
                        help='Identifies the project to retract data from',
                        required=True)
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
                        help='Specify fully qualified deactivated table '
                        'as "project.dataset.table"',
                        required=True)
    parser.add_argument('-r',
                        '--fq_pid_rid_table',
                        action='store',
                        dest='fq_pid_rid_table',
                        help='Specify fully qualified pid-rid mapping table '
                        'as "project.dataset.table"',
                        required=False)
    return parser


def run_deactivation(client,
                     project_id,
                     dataset_ids,
                     fq_deact_table,
                     fq_pid_rid_table=None):
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


def main():
    pipeline_logging.configure(logging.DEBUG, add_console_handler=True)
    parser = get_parser()
    args = parser.parse_args()
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
