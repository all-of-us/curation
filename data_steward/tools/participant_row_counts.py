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


def get_table_id(table):
    """
    Returns id column of the cdm table

    :param table: cdm table name
    :return: id column name for the table
    """
    return table + '_id'


def get_src_id(mapping_type):
    """
    Returns source id column name for mapping or ext tables

    :param mapping_type: common.MAPPING or common.EXT
    :return: src_id or src_hpo_id
    """
    src_id = 'src_id'
    if mapping_type == common.MAPPING:
        src_id = 'src_hpo_id'
    return src_id


def get_tables(table_df):
    """
    returns all tables in dataset using information_schema.columns dataframe

    :param table_df: dataframe from information_schema.columns
    :return: list containing all tables in dataset
    """
    tables = table_df.get(consts.TABLE_NAME).to_list()
    return tables


def get_pid_tables(table_df):
    """
    returns tables containing person_id column in dataset using information_schema.columns dataframe

    :param table_df: dataframe from information_schema.columns
    :return: list containing tables with person_id in dataset
    """
    tables_with_pid = table_df[table_df.get(consts.COLUMN_NAME) ==
                               consts.PERSON_ID].get(
                                   consts.TABLE_NAME).to_list()
    return tables_with_pid


def get_mapping_type(tables):
    """
    Returns whether mapping or ext tables exist within a dataset using list of tables as input

    :param tables: list of tables within the dataset
    :return: common.EXT or common.MAPPING
    """
    mapping_tables = [
        table for table in tables if common.MAPPING_PREFIX in table
    ]
    ext_tables = [table for table in tables if common.EXT_SUFFIX in table]

    if len(mapping_tables) >= len(ext_tables):
        return common.MAPPING
    return common.EXT


def get_mapping_tables(mapping_type, tables):
    """
    returns mapping tables in dataset using mapping type and list of tables in the dataset

    :param mapping_type: common.EXT or common.MAPPING
    :param tables: list of tables in dataset
    :return: list of mapping tables (or ext tables)
    """
    if mapping_type == common.MAPPING:
        mapping_tables = [
            table for table in tables if common.MAPPING_PREFIX in table
        ]
        return mapping_tables
    mapping_tables = [table for table in tables if common.EXT_SUFFIX in table]
    return mapping_tables


def get_cdm_table(mapping_ext_table):
    if common.MAPPING_PREFIX in mapping_ext_table:
        return mapping_ext_table.replace(common.MAPPING_PREFIX, '')
    return mapping_ext_table.replace(common.EXT_SUFFIX, '')


def get_cdm_and_mapping_tables(mapping_tables, tables_with_pid):
    """
    Returns dict containing cdm tables and corresponding mapping tables as key value pairs

    :param mapping_tables: list of mapping tables in dataset
    :param tables_with_pid: list of tables containing person_id
    :return: dict containing cdm_table, mapping_table as key, value pairs
    """
    # filters tables which do not exist, also ensures table is valid cdm_table
    cdm_and_mapping_tables = dict((get_cdm_table(table), table)
                                  for table in mapping_tables
                                  if get_cdm_table(table) in tables_with_pid)
    return cdm_and_mapping_tables


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
    pid = consts.PERSON_ID
    if for_deid:
        pid = consts.RESEARCH_ID
    pid_sql_expr = get_pid_sql_expr(pid_source, pid)
    tables = get_tables(table_df)
    tables_with_pid = get_pid_tables(table_df)
    mapping_type = get_mapping_type(tables)
    mapping_tables = get_mapping_tables(mapping_type, tables)
    cdm_and_mapping_tables = get_cdm_and_mapping_tables(mapping_tables,
                                                        tables_with_pid)

    # Combined
    for table in tables_with_pid:
        if table in cdm_and_mapping_tables:
            tmpl = Template(consts.CDM_MAPPING_TABLE_COUNT).render(
                project=project_id,
                dataset=dataset_id,
                table=table,
                table_id=get_table_id(table),
                src_id=get_src_id(mapping_type),
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


def get_dataset_query(project_id, dataset_id, pid_source, table_df):
    """
    Get query to determine all row counts and ehr row counts from unioned, rdr and nonconforming datasets

    :param project_id: identifies the project
    :param dataset_id: identifies the dataset
    :param pid_source: identifies the source of pids
    :param table_df: dataframe from BQ INFORMATION_SCHEMA.COLUMNS
    :return: query:
    """
    query_list = []
    pid_sql_expr = get_pid_sql_expr(pid_source)
    tables_with_pid = get_pid_tables(table_df)

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
    pid_sql_expr = get_pid_sql_expr(pid_source)
    tables_with_pid = get_pid_tables(table_df)

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


def get_pid_table_to_sql_expr(pid_source, pid):
    """
    Converts pid table string into BQ statement selecting pids from input table

    :param pid: person_id or research_id
    :param pid_source: string of the form 'project.dataset.table' where table contains pids to consider
    :return: BQ statement selecting pids
    """
    return consts.PID_QUERY.format(pid=pid, pid_source=pid_source)


def get_pid_sql_expr(pid_source, pid=consts.PERSON_ID):
    """
    Converts a list of integer pids into a bq-compatible sql expression containing the pids as values
    or a string of the form 'project.dataset.table' into a SELECT query that selects pids from the table

    :param pid_source: can be a list of pids or string of the form 'project.dataset.table', where table contains pids
    :param pid: person_id or research_id, required for table sql expr, 'person_id' by default
    :return: bq-compatible string expression of pids or SELECT query that selects pids from table
    :raises ValueError if pid_source type is incorrect or pid_table string is not specified correctly
    """
    if type(pid_source) == list:
        return get_pid_list_to_sql_expr(pid_source)
    if type(pid_source) == str and pid_source.count('.') == 2:
        return get_pid_table_to_sql_expr(pid_source, pid)
    raise ValueError(
        'Please specify pid_table parameters as "project.dataset.table"')


def count_pid_rows_in_dataset(project_id, dataset_id, hpo_id, pid_source):
    """
    Returns df containing tables which have non-zero counts of participant rows for pids in pids_source

    :param project_id: identifies the project
    :param dataset_id: identifies the dataset
    :param hpo_id: Identifies the hpo site that submitted the pids
    :param pid_source: string containing query or list containing pids
    :return: df with headers table_id, all_counts, all_ehr_counts, and map_ehr_counts
    """
    dataset_type = get_dataset_type(dataset_id)
    counts_df = pd.DataFrame(columns=[
        consts.TABLE_ID, consts.ALL_COUNT, consts.ALL_EHR_COUNT,
        consts.MAP_EHR_COUNT
    ])
    table_df = get_table_information_for_dataset(project_id, dataset_id)

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


def count_pid_rows_in_project(project_id, hpo_id, pid_source):
    """
    Logs dataset_name, table_id, all_count, all_ehr_count and map_ehr_count to count rows pertaining to pids

    :param project_id: identifies the project
    :param hpo_id: Identifies the hpo site that submitted the pids
    :param pid_source: string containing query or list containing pids
    :return:
    """
    datasets = bq.list_datasets(project_id)
    dataset_ids = [dataset.dataset_id for dataset in datasets]
    for dataset_id in dataset_ids:
        try:
            # We do not fetch queries for each dataset here and union them since it exceeds BQ query length limits
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
