"""
Remove observation records associated with a provided list of person_ids
"""
import csv

import constants.bq_utils as bq_consts
from constants.cdr_cleaner import clean_cdr as cdr_consts

OBSERVATION = 'observation'

SELECT_OBSERVATION_RECORDS_EXCLUDE_PIDS = (
    'select * '
    'FROM `{project_id}.{dataset_id}.observation` '
    'where person_id not in ({records}) '
)


def read_pids_csv(filepath):
    """
    Extract pids from csv file

    :param filepath: Path to the file containing the pids
    :return: list of int
    """
    pids = []
    with open(filepath, 'rb') as f:
        reader = csv.reader(f)
        reader.next()
        for row in reader:
            pid = int(row[0])
            pids.append(pid)
    return pids


def pids2sql(pids):
    """
    Convert list of pids to SQL sequence expression

    :param pids: list of person_id
    :return: str representation of SQL expression
    """
    str_pids = map(str, pids)
    return ', '.join(str_pids)


def get_exclude_pids_query(project_id, dataset_id, pids):
    """
    Get query to wipe observation records associated with person_ids

    :param project_id: identifies the project containing the dataset
    :param dataset_id: identifies the dataset containing the OMOP data
    :param pids: list of person_ids
    :return: a query dict representing the query to run
    """
    query = dict()
    pids_sql = pids2sql(pids)
    query[cdr_consts.QUERY] = SELECT_OBSERVATION_RECORDS_EXCLUDE_PIDS.format(project_id=project_id,
                                                                             dataset_id=dataset_id,
                                                                             records=pids_sql)
    query[cdr_consts.DESTINATION_TABLE] = OBSERVATION
    query[cdr_consts.DISPOSITION] = bq_consts.WRITE_TRUNCATE
    query[cdr_consts.DESTINATION_DATASET] = dataset_id

    return query


def main(project_id, dataset_id, file_path):
    """
    Get list of queries to remove observation records associated with pids in a specified file

    :param project_id: identifies the project containing the dataset
    :param dataset_id: identifies the dataset containing the OMOP data
    :param file_path: path to csv file containing pids
    :return: a list of query dict
    """
    queries = []
    file_path = read_pids_csv(file_path)
    query_dict = get_exclude_pids_query(project_id, dataset_id, file_path)
    queries.append(query_dict)
    return queries


def parse_args():
    """
    Add file_path to the default cdr_cleaner.args_parser argument list

    :return: an expanded argument list object
    """
    import cdr_cleaner.args_parser as parser
    help_text = 'path to csv file (with header row) containing pids whose observation records are to be removed'
    additional_argument_1 = {parser.SHORT_ARGUMENT: '-f',
                             parser.LONG_ARGUMENT: '--file_path',
                             parser.ACTION: 'store',
                             parser.DEST: 'file_path',
                             parser.HELP: help_text,
                             parser.REQUIRED: True}

    args = parser.default_parse_args([additional_argument_1])
    return args


if __name__ == '__main__':
    import cdr_cleaner.clean_cdr_engine as clean_engine

    ARGS = parse_args()

    clean_engine.add_console_logging(ARGS.console_log)
    query_list = main(ARGS.project_id, ARGS.dataset_id, ARGS.file_path)
    clean_engine.clean_dataset(ARGS.project_id, query_list)
