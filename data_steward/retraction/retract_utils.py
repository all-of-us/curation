# Python imports
import re
import argparse
import logging

# Project imports
from common import (COMBINED, DEID, EHR, EXT, EXT_SUFFIX, FITBIT, MAPPING,
                    MAPPING_PREFIX, OTHER, RDR, SANDBOX, UNIONED_EHR,
                    DE_IDENTIFIED)
from gcloud.bq import BigQueryClient
from constants.retraction import retract_utils as consts
from constants.utils import bq as bq_consts

LOGGER = logging.getLogger(__name__)

DEID_REGEX = re.compile(r'.*deid.*')
STAGING_REGEX = re.compile(r'.*staging.*')


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
    mapping_tables = [table for table in tables if MAPPING_PREFIX in table]
    ext_tables = [table for table in tables if EXT_SUFFIX in table]

    if len(mapping_tables) >= len(ext_tables):
        return MAPPING
    return EXT


def get_src_id(mapping_type):
    """
    Returns source id column name for mapping or ext tables

    :param mapping_type: common.MAPPING or common.EXT
    :return: src_id or src_hpo_id
    """
    src_id = 'src_id'
    if mapping_type == MAPPING:
        src_id = 'src_hpo_id'
    return src_id


def get_datasets_list(client, dataset_ids_list):
    """
    Returns list of dataset_ids on which to perform retraction
    :param client: a BigQueryClient which has project containing datasets to retract from
    :param dataset_ids_list: list of datasets to retract from. If set to 'all_datasets',
        retracts from all datasets. If set to 'none', skips retraction from BigQuery datasets
    :return: List of dataset_ids to retract from

    NOTE sandbox datasets and OTHER datasets are excluded from retraction

    """
    all_dataset_ids = [
        dataset.dataset_id for dataset in list(client.list_datasets())
    ]

    if not dataset_ids_list or dataset_ids_list == [consts.NONE]:
        dataset_ids = []
        LOGGER.warning(
            "No datasets specified. Defaulting to empty list. Expect bucket only retraction."
        )
    elif dataset_ids_list == [consts.ALL_DATASETS]:
        dataset_ids = all_dataset_ids
        LOGGER.info(
            f"All datasets are specified. Setting dataset_ids to all datasets in project: {client.project}."
        )
    else:
        # only consider datasets that exist in the project
        dataset_ids = [
            dataset_id for dataset_id in dataset_ids_list
            if dataset_id in all_dataset_ids
        ]
        LOGGER.info(
            f"Datasets specified and existing in project {client.project}: {dataset_ids}"
        )

    # NOTE Excludes sandbox and OTHER datasets
    dataset_ids = [
        dataset_id for dataset_id in dataset_ids
        if get_dataset_type(dataset_id) != OTHER and
        not is_sandbox_dataset(dataset_id)
    ]

    LOGGER.info(f"Found datasets to retract from: {', '.join(dataset_ids)}")

    return dataset_ids


def is_deid_label_or_id(client, project_id, dataset_id):
    """
    Validates if a dataset is labeled deid or contains deid in the dataset_id

    :param client: BigQuery client
    :param project_id: project containing the dataset
    :param dataset_id: dataset to identify
    :return: Boolean indicating if a dataset is a deid dataset
    """
    label = _is_labeled_deid(client, project_id, dataset_id)
    if label is None:
        return is_deid_dataset(dataset_id)
    return label


def _is_labeled_deid(client, project_id, dataset_id):
    """
    Returns boolean indicating if a dataset is a deid dataset using the label 'de_identified'

    :param client: BigQuery client object
    :param project_id: Identifies the project
    :param dataset_id: Identifies the dataset
    :return: Boolean indicating if the dataset is labeled a deid dataset or None if unlabeled
    """
    # when called by is_deid_label_or_id, return None so name can be checked.
    if not client:
        LOGGER.debug("Client not available.  Labels can't be checked.")
        return None

    dataset = client.get_dataset(f'{project_id}.{dataset_id}')
    if dataset.labels and DE_IDENTIFIED in dataset.labels:
        return dataset.labels[DE_IDENTIFIED] == consts.TRUE
    return None


def is_deid_dataset(dataset_id):
    """
    Returns boolean indicating if a dataset is a deid dataset using the dataset_id
    :param dataset_id: Identifies the dataset
    :return: Boolean indicating if the dataset is a deid dataset
    NOTE It returns True for deid_xyz datasets too (e.g. deid_base_sandbox)
    """
    return DEID in dataset_id or is_deid_fitbit_dataset(dataset_id)


def is_combined_dataset(dataset_id):
    """
    Returns boolean indicating if a dataset is a combined dataset using the dataset_id
    :param dataset_id: Identifies the dataset
    :return: Boolean indicating if the dataset is a combined dataset
    NOTE It returns True for combined_xyz datasets too (e.g. combined_staging)
    """
    return COMBINED in dataset_id


def is_combined_release_dataset(dataset_id):
    """
    Returns boolean indicating if a dataset is a combined release dataset using the dataset_id
    :param dataset_id: Identifies the dataset
    :return: Boolean indicating if the dataset is a combined release dataset or not
    """
    return COMBINED in dataset_id and 'release' in dataset_id


def is_deid_fitbit_dataset(dataset_id: str):
    """
    Returns boolean indicating if a dataset is a deid fitbit dataset using the dataset_id
    :param dataset_id: Identifies the dataset
    :return: Boolean indicating if the dataset is a deid fitbit dataset
    """
    return FITBIT in dataset_id and (dataset_id.startswith('C') or
                                     dataset_id.startswith('R'))


def is_fitbit_dataset(dataset_id):
    """
    Returns boolean indicating if a dataset is a fitbit dataset using the dataset_id
    :param dataset_id: Identifies the dataset
    :return: Boolean indicating if the dataset is a fitbit dataset
    NOTE It returns True for fitbit_xyz datasets too (e.g. fitbit_sandbox)
    """
    return FITBIT in dataset_id


def is_unioned_dataset(dataset_id):
    """
    Returns boolean indicating if a dataset is a unioned dataset using the dataset_id
    :param dataset_id: Identifies the dataset
    :return: Boolean indicating if the dataset is a unioned dataset
    NOTE It returns True for unioned_ehr_xyz datasets too (e.g. unioned_ehr_sandbox)
    """
    return UNIONED_EHR in dataset_id


def is_ehr_dataset(dataset_id):
    """
    Returns boolean indicating if a dataset is an ehr dataset using the dataset_id
    :param dataset_id: Identifies the dataset
    :return: Boolean indicating if the dataset is an ehr dataset
    NOTE It returns True for ehr_xyz datasets too (e.g. ehr_sandbox)
    """
    return EHR in dataset_id and not UNIONED_EHR in dataset_id


def is_rdr_dataset(dataset_id):
    """
    Returns boolean indicating if a dataset is a rdr dataset using the dataset_id
    :param dataset_id: Identifies the dataset
    :return: Boolean indicating if the dataset is an ehr dataset
    NOTE It returns True for rdr_xyz datasets too (e.g. rdr_sandbox)
    """
    return RDR in dataset_id


def is_sandbox_dataset(dataset_id):
    """
    Returns boolean indicating if a dataset is a sandbox dataset using the dataset_id
    :param dataset_id: Identifies the dataset
    :return: Boolean indicating if the dataset is a sandbox dataset
    """
    return SANDBOX in dataset_id


def is_staging_dataset(dataset_id):
    """
    # NOTE This function is not referenced anywhere.

    Returns boolean indicating if a dataset is a staging dataset using the dataset_id
    :param dataset_id: Identifies the dataset
    :return: Boolean indicating if the dataset is a staging dataset
    """
    return bool(re.match(STAGING_REGEX, dataset_id))


def get_dataset_type(dataset_id):
    if COMBINED in dataset_id and DEID not in dataset_id:
        return COMBINED
    if UNIONED_EHR in dataset_id:
        return UNIONED_EHR
    if RDR in dataset_id:
        return RDR
    if EHR in dataset_id and UNIONED_EHR not in dataset_id:
        return EHR
    if FITBIT in dataset_id:
        return FITBIT
    if DEID in dataset_id or is_deid_dataset(dataset_id):
        return DEID
    return OTHER


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
    if mapping_type == MAPPING:
        mapping_tables = [table for table in tables if MAPPING_PREFIX in table]
        return mapping_tables
    mapping_tables = [table for table in tables if EXT_SUFFIX in table]
    return mapping_tables


def get_cdm_table(mapping_ext_table):
    if MAPPING_PREFIX in mapping_ext_table:
        return mapping_ext_table.replace(MAPPING_PREFIX, '')
    return mapping_ext_table.replace(EXT_SUFFIX, '')


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


def get_dataset_ids_to_target(project_id, dataset_ids=None):
    """
    Return dataset_ids that are found in the project based on BQ metadata

    :param project_id: Identifies the project to target
    :param dataset_ids: list identifying datasets or None for all datasets
    :return: List of dataset_ids in the project to target
    """
    client = BigQueryClient(project_id)
    all_datasets = list(client.list_datasets())
    all_dataset_ids = [dataset.dataset_id for dataset in all_datasets]
    result_dataset_ids = []
    if dataset_ids is None:
        result_dataset_ids = all_dataset_ids
    else:
        for dataset_id in dataset_ids:
            if dataset_id not in all_dataset_ids:
                LOGGER.info(
                    f"Dataset {dataset_id} not found in project {project_id}, skipping"
                )
            else:
                result_dataset_ids.append(dataset_id)
    return result_dataset_ids


def check_dataset_ids_for_sentinel(dataset_ids):
    """
    Checks if sentinel value "all_datasets" is the only value in the list dataset_ids
    If so, returns None. If not, raises error if "all_datasets" is in the list of dataset_ids

    :param dataset_ids: list of dataset_ids
    :return: dataset_ids: list of dataset_ids
    :raises ValueError
    """
    if len(dataset_ids) == 1 and dataset_ids[0] == consts.ALL_DATASETS:
        return None
    for dataset_id in dataset_ids:
        if dataset_id == consts.ALL_DATASETS:
            raise ValueError(
                "Please enter 'all_datasets' to target all datasets "
                "or specific datasets without using 'all_datasets'")
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
                        help='Identifies datasets to target. Set to '
                        '-d all_datasets to target all datasets in project '
                        'or specific datasets as -d dataset_1 dataset_2 etc.',
                        required=True)
    parser.add_argument('-o',
                        '--hpo_id',
                        action='store',
                        dest='hpo_id',
                        help='Identifies the site submitting the person_ids, '
                        'can be "none" if not targeting ehr datasets',
                        required=True)
    pid_source_group = parser.add_mutually_exclusive_group(required=True)

    pid_source_group.add_argument(
        '-l',
        '--pid_list',
        dest='pid_source',
        nargs='+',
        type=int,
        help='person/research ids to consider separated by spaces')
    pid_source_group.add_argument(
        '-t',
        '--pid_table',
        dest='pid_source',
        help='Specify table as "project.dataset.table"')
    return parser
