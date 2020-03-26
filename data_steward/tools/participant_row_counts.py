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
from jinja2 import Template

# Project imports
from utils import bq
import common
from constants.tools import participant_row_counts as consts
from tools.retract_data_bq import is_deid_dataset


def get_table_information_for_dataset(project_id, dataset_id):
    """
    Get df of table_ids(first column) and table names that have a person_id column as the second column

    :param project_id: identifies the project
    :param dataset_id: identifies the dataset
    :return df containing table column information
    """
    person_table_query = consts.TABLE_INFO_QUERY.format(project=project_id,
                                                        dataset=dataset_id)
    result_df = bq.query(person_table_query, project_id)
    return result_df


def get_combined_deid_query(project_id, dataset_id, pid_sql_expr, table_df):
    """
    Get query to determine all row counts and ehr row counts from combined, deid and release datasets
    Please specify person_ids for combined and research_ids for deid and release datasets
    # TODO: Use deid_map once the dataset naming convention is complete

    :param project_id: identifies the project
    :param dataset_id: identifies the dataset
    :param pid_sql_expr: string containing sql_expression identifying pids
    :param table_df: dataframe from BQ INFORMATION_SCHEMA.COLUMNS
    :return: query: 
    """
    query_list = []
    tables = table_df.get(consts.TABLE_NAME).to_list()
    tables_with_pid = table_df[table_df.get(consts.COLUMN_NAME) ==
                               consts.PERSON_ID].get(
                                   consts.TABLE_NAME).to_list()
    mapping_ext_tables = [
        table for table in tables
        if common.MAPPING_PREFIX in table or common.EXT_SUFFIX in table
    ]

    # filters tables which do not exist, also ensures table is valid cdm_table
    cdm_tables_with_mapping = dict((get_cdm_table(table), table)
                                   for table in mapping_ext_tables
                                   if get_cdm_table(table) in tables_with_pid)

    # Combined, DEID, Release
    for table in tables_with_pid:
        if table in cdm_tables_with_mapping:
            table_id = table + '_id'
            tmpl = Template(consts.CDM_MAPPING_TABLE_COUNT).render(
                project=project_id,
                dataset=dataset_id,
                table=table,
                table_id=table_id,
                pids_expr=pid_sql_expr,
                mapping_table=cdm_tables_with_mapping[table])
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


def get_dataset_query(project_id, dataset_id, pid_sql_expr, table_df):
    """
    Get query to determine all row counts and ehr row counts from unioned, rdr and nonconforming datasets
    
    :param project_id: identifies the project
    :param dataset_id: identifies the dataset
    :param pid_sql_expr: string containing sql_expression identifying pids
    :param table_df: dataframe from BQ INFORMATION_SCHEMA.COLUMNS
    :return: 
    """
    query_list = []
    tables_with_pid = table_df[table_df.get(consts.COLUMN_NAME) ==
                               consts.PERSON_ID].get(
                                   consts.TABLE_NAME).unique()
    # Unioned EHR or generic dataset
    for table in tables_with_pid:
        ehr_count = "COUNT(*)"
        tmpl = Template(consts.PID_TABLE_COUNT).render(project=project_id,
                                                       dataset=dataset_id,
                                                       table=table,
                                                       pids_expr=pid_sql_expr,
                                                       ehr_count=ehr_count)
        query_list.append(tmpl)
    query = consts.UNION_ALL.join(query_list)
    return query


def get_ehr_query(project_id, dataset_id, pid_sql_expr, hpo_id, table_df):
    """
    Get query to determine all row counts and ehr row counts from ehr datasets,
    returns unioned_ehr query if hpo_id is set to 'none', since ehr datasets also contain unioned tables

    :param project_id: identifies the project
    :param dataset_id: identifies the dataset
    :param pid_sql_expr: string containing sql_expression identifying pids
    :param hpo_id: identifies the hpo site that submitted the pids, can be set to 'none' for unioned tables query
    :param table_df: dataframe from BQ INFORMATION_SCHEMA.COLUMNS
    :return: 
    """
    query_list = []
    tables_with_pid = table_df[table_df.get(consts.COLUMN_NAME) ==
                               consts.PERSON_ID].get(
                                   consts.TABLE_NAME).unique()
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


def get_cdm_table(mapping_ext_table):
    if common.MAPPING_PREFIX in mapping_ext_table:
        return mapping_ext_table.replace(common.MAPPING_PREFIX, '')
    return mapping_ext_table.replace(common.EXT_SUFFIX, '')


def get_dataset_type(dataset_id):
    if common.COMBINED in dataset_id and common.DEID not in dataset_id:
        return common.COMBINED
    if common.UNIONED_EHR in dataset_id:
        return common.UNIONED_EHR
    if common.EHR in dataset_id and common.UNIONED_EHR not in dataset_id:
        return common.EHR
    if common.DEID in dataset_id or is_deid_dataset(dataset_id):
        return common.DEID
    return common.OTHER


def get_pid_list_to_sql_expr(pid_source):
    """
    Converts list of ints into BQ compatible string of the form '(int_1, int_2, ...)'
    
    :param pid_source: list of pids to consider as ints
    :return: BQ compatible string of ints
    """
    return str(tuple(pid_source))


def get_pid_table_to_sql_expr(pid_source):
    """
    Converts pid table string into BQ statement selecting pids from input table
    
    :param pid_source: string of the form 'project.dataset.table' where table contains pids to consider
    :return: BQ statement selecting pids
    """
    return consts.PID_QUERY.format(pid_source=pid_source)


def get_pid_sql_expr(pid_source):
    """
    Converts a list of integer pids into a bq-compatible sql expression containing the pids as values
    or a string of the form 'project.dataset.table' into a SELECT query that selects pids from the table

    :param pid_source: can be a list of pids or string of the form 'project.dataset.table', where table contains pids
    :return: bq-compatible string expression of pids or SELECT query that selects pids from table
    :raises ValueError if pid_source type is incorrect or pid_table string is not specified correctly
    """
    if type(pid_source) == list:
        return get_pid_list_to_sql_expr(pid_source)
    if type(pid_source) == str and pid_source.count('.') == 2:
        return get_pid_table_to_sql_expr(pid_source)
    raise ValueError(
        'Please specify pid_table parameters as "project.dataset.table"')


def count_pid_rows_in_dataset(project_id, dataset_id, hpo_id, pid_source):
    """
    Returns df containing tables which have non-zero counts of participant rows for pids in pids_string

    :param project_id: identifies the project
    :param dataset_id: identifies the dataset
    :param hpo_id: Identifies the hpo site that submitted the pids
    :param pid_source: string containing query or list containing pids
    :return: df with headers table_id, all_counts and ehr_counts
    """
    pid_sql_expr = get_pid_sql_expr(pid_source)
    dataset_type = get_dataset_type(dataset_id)
    counts_df = pd.DataFrame(
        columns=[consts.TABLE_ID, consts.ALL_COUNT, consts.EHR_COUNT])
    table_df = get_table_information_for_dataset(project_id, dataset_id)

    if dataset_type == common.COMBINED or dataset_type == common.DEID or dataset_type == common.RELEASE:
        query = get_combined_deid_query(project_id, dataset_id, pid_sql_expr,
                                        table_df)
    elif dataset_type == common.EHR:
        query = get_ehr_query(project_id, dataset_id, pid_sql_expr, hpo_id,
                              table_df)
    else:
        query = get_dataset_query(project_id, dataset_id, pid_sql_expr,
                                  table_df)

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
            logging.info(
                'DATASET_ID: {}\tTABLE_ID: {}\tALL_COUNT: {}\tEHR_COUNT: {}'.
                format(dataset_id, *count_row))


def count_pid_rows_in_project(project_id, hpo_id, pid_source):
    """
    Logs dataset_name, table_id, non_cdm_count and ehr_count to count rows pertaining to pids

    :param project_id: identifies the project
    :param hpo_id: Identifies the hpo site that submitted the pids
    :param pid_source: string containing query or list containing pids
    :return: dataframe containing 
    """
    datasets = bq.list_datasets(project_id)
    dataset_ids = [dataset.dataset_id for dataset in datasets]
    for dataset_id in dataset_ids:
        try:
            counts_df = count_pid_rows_in_dataset(project_id, dataset_id,
                                                  hpo_id, pid_source)
            log_total_rows(counts_df, dataset_id)
        except BadRequest:
            logging.exception(f'Dataset {dataset_id} could not be analyzed')


def fetch_parser():
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
                        help='Identifies the site submitting the person_ids, '
                        'can be "none" if not targeting ehr datasets',
                        required=True)
    subparsers = parser.add_subparsers()

    subparser_pid_list = subparsers.add_parser(
        name='pid_list',
        help='Specifies the source of pids in list of int form')
    subparser_pid_list.add_argument(
        dest='pid_source',
        nargs='+',
        type=int,
        help='person/research ids to consider separated by spaces')

    subparser_pid_table = subparsers.add_parser(
        name='pid_table', help='Specifies the source of pids in BQ table form')
    subparser_pid_table.add_argument(
        dest='pid_source', help='Specify table as "project.dataset.table"')
    return parser


if __name__ == '__main__':
    parser = fetch_parser()
    args = parser.parse_args()

    count_pid_rows_in_project(args.project_id, args.hpo_id, args.pid_source)
