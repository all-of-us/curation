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
import logging

# Third party imports
from google.api_core.exceptions import BadRequest
import pandas as pd
from jinja2 import Template

# Project imports
from utils import bq
import common
from retraction import retract_utils as ru
from constants.retraction import retract_utils as ru_consts
from constants.retraction import participant_row_counts as consts


def get_combined_deid_query(project_id,
                            dataset_id,
                            pid_source,
                            table_df,
                            for_deid=False):
    """
    Get query to determine all row counts and ehr row counts from combined, deid and release datasets
    Please specify person_ids for combined and research_ids for deid and release datasets
    # TODO: Use deid_map once the dataset naming convention is complete

    :param project_id: identifies the project
    :param dataset_id: identifies the dataset
    :param pid_source: identifies the source of pids
    :param table_df: dataframe from BQ INFORMATION_SCHEMA.COLUMNS
    :param for_deid: indicates whether query is for deid dataset (as opposed to combined)
    :return: query:
    """
    query_list = []
    pid = ru_consts.PERSON_ID
    if for_deid:
        pid = ru_consts.RESEARCH_ID
    pid_sql_expr = ru.get_pid_sql_expr(pid_source, pid)
    tables = ru.get_tables(table_df)
    tables_with_pid = ru.get_pid_tables(table_df)
    mapping_type = ru.get_mapping_type(tables)
    mapping_tables = ru.get_mapping_tables(mapping_type, tables)
    cdm_and_mapping_tables = ru.get_cdm_and_mapping_tables(
        mapping_tables, tables_with_pid)

    # Combined
    for table in tables_with_pid:
        if table in cdm_and_mapping_tables:
            tmpl = Template(consts.CDM_MAPPING_TABLE_COUNT).render(
                project=project_id,
                dataset=dataset_id,
                table=table,
                table_id=ru.get_table_id(table),
                src_id=ru.get_src_id(mapping_type),
                pids_expr=pid_sql_expr,
                mapping_table=cdm_and_mapping_tables[table],
                ID_CONST=common.ID_CONSTANT_FACTOR + common.RDR_ID_CONSTANT)
        else:
            # person table comes from RDR, so not counted for EHR counts
            ehr_count = 0
            # death table does not have mapping and could have come from EHR or RDR, needs investigation
            if table == common.DEATH:
                ehr_count = "COUNT(*)"
            tmpl = Template(consts.PID_TABLE_COUNT).render(
                project=project_id,
                dataset=dataset_id,
                table=table,
                pids_expr=pid_sql_expr,
                ehr_count=ehr_count)
        query_list.append(tmpl)
    query = consts.UNION_ALL.join(query_list)
    return query


def get_dataset_query(project_id,
                      dataset_id,
                      pid_source,
                      table_df,
                      for_rdr=False):
    """
    Get query to determine all row counts and ehr row counts from unioned, rdr and nonconforming datasets

    :param project_id: identifies the project
    :param dataset_id: identifies the dataset
    :param pid_source: identifies the source of pids
    :param table_df: dataframe from BQ INFORMATION_SCHEMA.COLUMNS
    :param for_rdr: indicates if query is for an RDR dataset. If True, set ehr_counts = 0,
        else set ehr_counts to "COUNT(*)" for unioned_ehr and other datasets that need investigation
    :return: query:
    """
    query_list = []
    pid_sql_expr = ru.get_pid_sql_expr(pid_source)
    tables_with_pid = ru.get_pid_tables(table_df)

    # Unioned EHR or generic dataset
    for table in tables_with_pid:
        ehr_count = 0 if for_rdr else "COUNT(*)"
        tmpl = Template(consts.PID_TABLE_COUNT).render(project=project_id,
                                                       dataset=dataset_id,
                                                       table=table,
                                                       pids_expr=pid_sql_expr,
                                                       ehr_count=ehr_count)
        query_list.append(tmpl)
    query = consts.UNION_ALL.join(query_list)
    return query


def get_ehr_query(project_id, dataset_id, pid_source, hpo_id, table_df):
    """
    Get query to determine all row counts and ehr row counts from ehr datasets,
    returns unioned_ehr query if hpo_id is set to 'none', since ehr datasets also contain unioned tables

    :param project_id: identifies the project
    :param dataset_id: identifies the dataset
    :param pid_source: identifies the source of pids
    :param hpo_id: identifies the hpo site that submitted the pids, can be set to 'none' for unioned tables query
    :param table_df: dataframe from BQ INFORMATION_SCHEMA.COLUMNS
    :return: query:
    """
    query_list = []
    pid_sql_expr = ru.get_pid_sql_expr(pid_source)
    tables_with_pid = ru.get_pid_tables(table_df)

    # EHR
    unioned_tables = [
        table for table in tables_with_pid if common.UNIONED_EHR in table
    ]

    tables_to_consider = unioned_tables

    if hpo_id != 'none':
        hpo_tables_with_pid = [
            table for table in tables_with_pid if hpo_id in table
        ]
        tables_to_consider += hpo_tables_with_pid

    for table in tables_to_consider:
        ehr_count = "COUNT(*)"
        tmpl = Template(consts.PID_TABLE_COUNT).render(project=project_id,
                                                       dataset=dataset_id,
                                                       table=table,
                                                       pids_expr=pid_sql_expr,
                                                       ehr_count=ehr_count)
        query_list.append(tmpl)
    query = consts.UNION_ALL.join(query_list)
    return query


def count_pid_rows_in_dataset(project_id, dataset_id, hpo_id, pid_source):
    """
    Returns df containing tables which have non-zero counts of participant rows for pids in pids_source
    If a dataset/table does not appear in the logs, it does not contain any relevant pid rows

    :param project_id: identifies the project
    :param dataset_id: identifies the dataset
    :param hpo_id: Identifies the hpo site that submitted the pids
    :param pid_source: string containing query or list containing pids
    :return: df with headers table_id, all_counts, all_ehr_counts, and map_ehr_counts
    """
    dataset_type = ru.get_dataset_type(dataset_id)
    counts_df = pd.DataFrame(columns=[
        ru_consts.TABLE_ID, consts.ALL_COUNT, consts.ALL_EHR_COUNT,
        consts.MAP_EHR_COUNT
    ])
    table_df = bq.get_table_info_for_dataset(project_id, dataset_id)

    if dataset_type == common.COMBINED:
        query = get_combined_deid_query(project_id, dataset_id, pid_source,
                                        table_df)
    elif dataset_type == common.DEID or dataset_type == common.RELEASE:
        query = get_combined_deid_query(project_id,
                                        dataset_id,
                                        pid_source,
                                        table_df,
                                        for_deid=True)
    elif dataset_type == common.EHR:
        query = get_ehr_query(project_id, dataset_id, pid_source, hpo_id,
                              table_df)
    elif dataset_type == common.RDR:
        query = get_dataset_query(project_id,
                                  dataset_id,
                                  pid_source,
                                  table_df,
                                  for_rdr=True)
    else:
        query = get_dataset_query(project_id, dataset_id, pid_source, table_df)

    if query:
        counts_df = bq.query(query, project_id)
        # ignore zero counts
        counts_df = counts_df[counts_df[consts.ALL_COUNT] > 0]
    return counts_df


def log_total_rows(df, dataset_id):
    """
    Logs rows from counts dataframe

    :param df: dataframe containing counts
    :param dataset_id: Identifies the dataset under consideration
    :return:
    """
    rows = df.get_values()
    if rows.size > 0:
        for count_row in rows:
            logging.info('{}, {}, {}, {}, {}'.format(dataset_id, *count_row))


def count_pid_rows_in_project(project_id, hpo_id, pid_source, dataset_ids=None):
    """
    Logs dataset_name, table_id, all_count, all_ehr_count and map_ehr_count to count rows pertaining to pids

    :param project_id: identifies the project
    :param hpo_id: Identifies the hpo site that submitted the pids
    :param pid_source: string containing query or list containing pids
    :param dataset_ids: list identifying datasets to retract from or None to retract from all datasets
    :return:
    """
    dataset_ids = ru.get_dataset_ids_to_target(project_id, dataset_ids)
    for dataset_id in dataset_ids:
        try:
            # We do not fetch queries for each dataset here and union them since it exceeds BQ query length limits
            counts_df = count_pid_rows_in_dataset(project_id, dataset_id,
                                                  hpo_id, pid_source)
            log_total_rows(counts_df, dataset_id)
        except BadRequest:
            # log non-conforming datasets and continue
            logging.exception(f'Dataset {dataset_id} could not be analyzed')


if __name__ == '__main__':
    parser = ru.fetch_parser()
    args = parser.parse_args()

    count_pid_rows_in_project(args.project_id, args.hpo_id, args.pid_source,
                              args.dataset_ids)
