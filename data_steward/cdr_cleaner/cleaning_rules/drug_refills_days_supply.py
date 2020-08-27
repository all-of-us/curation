"""
Ensure rug refills < 10 and days_supply < 180
"""
import logging

# Project imports
import common
from constants import bq_utils as bq_consts
from constants.cdr_cleaner import clean_cdr as cdr_consts

LOGGER = logging.getLogger(__name__)

MAX_DAYS_SUPPLY = 180
MAX_REFILLS = 10
drug_exposure = common.DRUG_EXPOSURE

MAX_DAYS_SUPPLY_AND_REFILLS_QUERY = (
    'SELECT * '
    'FROM `{project_id}.{dataset_id}.drug_exposure` '
    'WHERE ((days_supply <= {MAX_DAYS_SUPPLY} or days_supply is null) '
    '       AND (REFILLS <= {MAX_REFILLS} or REFILLS IS NULL))')


def get_days_supply_refills_queries(project_id, dataset_id):
    """
    This function gets the queries required to remove table records which are prior
    to the person's birth date or 150 years past the birth date from a dataset

    :param project_id: Project name
    :param dataset_id: Name of the dataset where a rule should be applied
    :return: a list of queries.
    """
    queries = []
    query = dict()
    query[cdr_consts.QUERY] = MAX_DAYS_SUPPLY_AND_REFILLS_QUERY.format(
        project_id=project_id,
        dataset_id=dataset_id,
        MAX_DAYS_SUPPLY=MAX_DAYS_SUPPLY,
        MAX_REFILLS=MAX_REFILLS)
    query[cdr_consts.DESTINATION_TABLE] = drug_exposure
    query[cdr_consts.DISPOSITION] = bq_consts.WRITE_TRUNCATE
    query[cdr_consts.DESTINATION_DATASET] = dataset_id
    queries.append(query)
    return queries


if __name__ == '__main__':
    import cdr_cleaner.args_parser as parser
    import cdr_cleaner.clean_cdr_engine as clean_engine

    ARGS = parser.parse_args()

    if ARGS.list_queries:
        clean_engine.add_console_logging()
        query_list = clean_engine.get_query_list(
            ARGS.project_id, ARGS.dataset_id,
            [(get_days_supply_refills_queries,)])
        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(ARGS.project_id, ARGS.dataset_id,
                                   [(get_days_supply_refills_queries,)])
