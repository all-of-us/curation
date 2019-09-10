"""
Removes  duplicate, fake, ghost, withdrawn, and test participants from observation table the CDR

TODO Get the pids of duplicate participants(AC-69)
     Get the pids of fake, ghost and withdrawn participants(AC-68) from the team drive before running this script.
"""
import csv

import constants.bq_utils as bq_consts
from constants.cdr_cleaner import clean_cdr as cdr_consts

OBSERVATION = 'observation'

DELETE_OBSERVATION_RECORDS = (
    'select * '
    'FROM `{project_id}.{dataset_id}.observation` '
    'where person_id not in ({records}) '
)


def extract_pids_from_csv(filepath):
    """
    takes path to csv file which contains pids as input, extracts pids from csv and adds to list of pids.
    returns the string generated from joining the ids in the list.
    :param filepath: Path to the file containing the pids
    :return: string
    """
    pids = []
    with open(filepath, 'rb') as f:
        reader = csv.reader(f)
        reader.next()
        for row in reader:
            pids.append(row[0])
    pids = ', '.join(pids)
    return pids


def get_delete_ghost_fake_participants_query(project_id, dataset_id, pids):
    """
    gets the query for deleting the ghost, fake and withdrawn participants from the CDR.
    :param project_id: identifies the project containing the dataset
    :param dataset_id: identifies the dataset containing the OMOP data
    :param pids: pids of ghost, fake, test and withdrawn participants extracted from a external file
    :return:
    """
    query = dict()
    query[cdr_consts.QUERY] = DELETE_OBSERVATION_RECORDS.format(project_id=project_id,
                                                                dataset_id=dataset_id,
                                                                records=pids,
                                                                )
    query[cdr_consts.DESTINATION_TABLE] = OBSERVATION
    query[cdr_consts.DISPOSITION] = bq_consts.WRITE_TRUNCATE
    query[cdr_consts.DESTINATION_DATASET] = dataset_id

    return query


def get_delete_duplicate_participants_query(project_id, dataset_id, pids):
    """
    gets the query for deleting the ghost, fake and withdrawn participants from the CDR.
    :param project_id: identifies the project containing the dataset
    :param dataset_id: identifies the dataset containing the OMOP data
    :param pids: pids of duplicate participants extracted from a external file
    :return:
    """
    query = dict()
    query[cdr_consts.QUERY] = DELETE_OBSERVATION_RECORDS.format(project_id=project_id,
                                                                dataset_id=dataset_id,
                                                                records=pids,
                                                                )
    query[cdr_consts.DESTINATION_TABLE] = OBSERVATION
    query[cdr_consts.DISPOSITION] = bq_consts.WRITE_TRUNCATE
    query[cdr_consts.DESTINATION_DATASET] = dataset_id

    return query


def main(project_id, dataset_id, fake_ghost_ids, duplicate_participants_ids):
    """
    takes in the file path and gets the person ids. gets queries to delete the records with the pids collected
    from the external file.
    :param project_id: identifies the project containing the dataset
    :param dataset_id: identifies the dataset containing the OMOP data
    :param fake_ghost_ids: path to csv file containing pids of ghost, fake, test and withdrawn participants
    :param duplicate_participants_ids: path to csv file containing pids of duplicate participants
    :return: a list of query dict to remove the duplicate, fake, ghost, test participants
    """
    queries = []
    fake_ghost_ids = extract_pids_from_csv(fake_ghost_ids)
    duplicate_participants_ids = extract_pids_from_csv(duplicate_participants_ids)
    queries.append(get_delete_duplicate_participants_query(project_id, dataset_id, duplicate_participants_ids))
    queries.append(get_delete_ghost_fake_participants_query(project_id, dataset_id, fake_ghost_ids))
    return queries


def parse_args():
    """
    this function expands the default argument list defined in cdr_cleaner.args_parser
    :return: an expanded argument list object
    """
    import cdr_cleaner.args_parser as parser

    additional_argument_1 = {parser.SHORT_ARGUMENT: '-ghost_ids',
                             parser.LONG_ARGUMENT: '--fake_ghost_ids',
                             parser.ACTION: 'store',
                             parser.DEST: 'fake_ghost_ids',
                             parser.HELP: 'path to csv file containing fake, ghost and withdrawn participants',
                             parser.REQUIRED: True}
    additional_argument_2 = {parser.SHORT_ARGUMENT: '-dup_ids',
                             parser.LONG_ARGUMENT: '--duplicate_participants_ids',
                             parser.ACTION: 'store',
                             parser.DEST: 'duplicate_participants_ids',
                             parser.HELP: 'path to csv file containing duplicate participants',
                             parser.REQUIRED: True}

    args = parser.default_parse_args([additional_argument_1, additional_argument_2])
    return args


if __name__ == '__main__':
    import cdr_cleaner.clean_cdr_engine as clean_engine

    ARGS = parse_args()

    clean_engine.add_console_logging(ARGS.console_log)
    query_list = main(ARGS.project_id, ARGS.dataset_id, ARGS.fake_ghost_ids, ARGS.duplicate_participants_ids)
    clean_engine.clean_dataset(ARGS.project_id, ARGS.dataset_id, query_list)
