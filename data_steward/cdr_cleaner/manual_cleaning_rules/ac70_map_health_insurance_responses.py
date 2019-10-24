"""
For all survey answers for a particular survey question and certain pids,
    1. Mark answers as invalid for all participants
    2. Use a second survey to generate valid answers for a subset of pids who took the second survey
"""
import csv

import constants.bq_utils as bq_consts
from constants.cdr_cleaner import clean_cdr as cdr_consts


def get_queries_health_insurance(project_id, dataset_id, file_path):
    """
    Queries to run for updating health insurance information
    :param project_id: project id associated with the dataset to run the queries on
    :param dataset_id: dataset id to run the queries on
    :param file_path: path to file containing the relevant pids
    :return: list of query dicts
    """
    queries = []
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
    query_list = get_queries_health_insurance(ARGS.project_id, ARGS.dataset_id, ARGS.file_path)
    clean_engine.clean_dataset(ARGS.project_id, ARGS.dataset_id, query_list)
