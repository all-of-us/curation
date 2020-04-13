import re
import argparse
import logging

import common
from utils import bq
from constants.retraction import retract_utils as consts
from constants.utils import bq as bq_consts
from retraction import retract_data_bq as rbq  # TODO refactor and remove

UNIONED_REGEX = re.compile('unioned_ehr_?\d{6}')
COMBINED_REGEX = re.compile('combined\d{6}')
DEID_REGEX = re.compile('.*deid.*')
EHR_REGEX = re.compile('ehr_?\d{6}')
RELEASE_REGEX = re.compile('R\d{4}Q\dR\d')


def get_table_id(table):
    """
    Returns id column of the cdm table

    :param table: cdm table name
    :return: id column name for the table
    """
    return table + '_id'


def get_tables(table_df):
    """
    returns all tables in dataset using information_schema.columns dataframe

    :param table_df: dataframe from information_schema.columns
    :return: list containing all tables in dataset
    """
    tables = table_df.get(bq_consts.TABLE_NAME).to_list()
    return tables


def get_pid_tables(table_df):
    """
    returns tables containing person_id column in dataset using information_schema.columns dataframe

    :param table_df: dataframe from information_schema.columns
    :return: list containing tables with person_id in dataset
    """
    tables_with_pid = table_df[table_df.get(bq_consts.COLUMN_NAME) ==
                               consts.PERSON_ID].get(
                                   bq_consts.TABLE_NAME).to_list()
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


def get_dataset_type(dataset_id):
    if common.COMBINED in dataset_id and common.DEID not in dataset_id:
        return common.COMBINED
    if common.UNIONED_EHR in dataset_id:
        return common.UNIONED_EHR
    if common.RDR in dataset_id:
        return common.RDR
    if common.EHR in dataset_id and common.UNIONED_EHR not in dataset_id:
        return common.EHR
    if common.DEID in dataset_id or rbq.is_deid_dataset(dataset_id):
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


def get_dataset_ids_to_target(project_id, parsed_dataset_ids):
    """
    Returns dataset_ids of interest

    :param project_id: Identifies the project to target
    :param parsed_dataset_ids: List of dataset_ids input by the user separated by spaces, or
        "all_datasets" to target all datasets in project
    :return: List of dataset_ids in the project to target
    """
    dataset_ids = []
    all_datasets = bq.list_datasets(project_id)
    all_dataset_ids = [dataset.dataset_id for dataset in all_datasets]
    if parsed_dataset_ids == [consts.ALL_DATASETS]:
        dataset_ids = all_dataset_ids
    else:
        for dataset_id in parsed_dataset_ids:
            if dataset_id == consts.ALL_DATASETS:
                raise ValueError(
                    "Please enter 'all_datasets' to target all datasets "
                    "or specific datasets without using 'all_datasets'")
            if dataset_id not in all_dataset_ids:
                logging.info(
                    f"Dataset {dataset_id} not found in project {project_id}, skipping"
                )
            else:
                dataset_ids.append(dataset_id)
    return dataset_ids


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
    parser.add_argument('-d',
                        '--dataset_ids',
                        action='store',
                        nargs='+',
                        dest='dataset_ids',
                        help='Identifies datasets to target. Set to'
                        ' "all_datasets" to target all datasets in project '
                        'or specific datasets as -d dataset_1 dataset_2 etc.',
                        required=True)
    parser.add_argument('-o',
                        '--hpo_id',
                        action='store',
                        dest='hpo_id',
                        help='Identifies the site submitting the person_ids, '
                        'can be "none" if not targeting ehr datasets',
                        required=True)
    subparsers = parser.add_subparsers()

    subparser_pid_source = subparsers.add_parser(
        name='pid_source', help='Specifies the source of pids')
    subparser_pid_source.add_argument(
        '-l',
        '--pid_list',
        dest='pid_source',
        nargs='+',
        type=int,
        help='person/research ids to consider separated by spaces')
    subparser_pid_source.add_argument(
        '-t',
        '--pid_table',
        dest='pid_source',
        help='Specify table as "project.dataset.table"')
    return parser
