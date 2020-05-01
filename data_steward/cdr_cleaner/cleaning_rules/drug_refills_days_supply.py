"""
Ensure drug refills < 10 and days_supply < 180
"""

# Project imports
import common
from constants import bq_utils as bq_consts
from constants.cdr_cleaner import clean_cdr as cdr_consts
from utils import bq

MAX_DAYS_SUPPLY = 180
MAX_REFILLS = 10
INITIAL_ROW_COUNT = 'initial_row_count'
FINAL_ROW_COUNT = 'final_row_count'
SANDBOX_ROW_COUNT = 'sandbox_row_count'
drug_exposure = common.DRUG_EXPOSURE
tables_affected = [common.DRUG_EXPOSURE]
issue_numbers = ['DC-403']
TABLE_COUNT_QUERY = '''
SELECT COALESCE(COUNT(*), 0) AS row_count FROM `{dataset}.{table}`
'''

MAX_DAYS_SUPPLY_AND_REFILLS_QUERY = (
    'SELECT * '
    'FROM `{dataset_id}.{table}` '
    'WHERE ((days_supply <= {MAX_DAYS_SUPPLY} or days_supply is null) '
    '       AND (REFILLS <= {MAX_REFILLS} or REFILLS IS NULL))')

NEGATIVE_MAX_DAYS_SUPPLY_AND_REFILLS_QUERY = (
    'SELECT * '
    'FROM `{dataset_id}.{table}` '
    'WHERE (days_supply > {MAX_DAYS_SUPPLY} '
    '       AND REFILLS > {MAX_REFILLS})')


def get_table_counts(tables, dataset):
    counts_dict = dict()
    for table in tables:
        query = TABLE_COUNT_QUERY.format(dataset=dataset, table=table)
        count = client.query(query).to_dataframe()
        counts_dict[table] = count['row_count'][0]
    return counts_dict


def validate_cleaning_rule(dataset, sandbox_dataset, sandbox_tables,
                           initial_counts):
    final_row_counts = get_table_counts(tables_affected, dataset)
    sandbox_row_counts = get_table_counts(list(sandbox_tables.values()),
                                          sandbox_dataset)

    for k, v in initial_counts.items():
        if v == final_row_counts[k] + sandbox_row_counts[sandbox_tables[k]]:
            print(
                f'{issue_numbers[0]} cleaning rule has run successfully on {dataset}.{k} table.'
            )
        else:
            print(
                f'{issue_numbers[0]} cleaning rule is failed on {dataset}.{k} table.\
                 There is a discrepancy in no.of records that\'s been deleted')


def get_sandbox_tablenames(tables, issue_numbers):
    table_names = {}
    for table in tables:
        table_name = issue_numbers[0].replace('-', '_') + '_' + table
        table_names[table] = table_name
    return table_names


def get_days_supply_refills_queries(project_id, dataset_id, sandbox_dataset,
                                    sandbox_table_names):
    """
    This function gets the queries required to remove table records which are prior
    to the person's birth date or 150 years past the birth date from a dataset

    :param project_id: Project name
    :param dataset_id: Name of the dataset where a rule should be applied
    :param sandbox_dataset:
    :param sandbox_table_names:
    :return: a list of queries.
    """
    # Store records to be deleted in sandbox dataset
    queries = []
    for table in tables_affected:
        query = dict()
        query[cdr_consts.
              QUERY] = NEGATIVE_MAX_DAYS_SUPPLY_AND_REFILLS_QUERY.format(
                  project_id=project_id,
                  dataset_id=dataset_id,
                  table=table,
                  MAX_DAYS_SUPPLY=MAX_DAYS_SUPPLY,
                  MAX_REFILLS=MAX_REFILLS)
        query[cdr_consts.DESTINATION_TABLE] = sandbox_table_names[table]
        query[cdr_consts.DISPOSITION] = bq_consts.WRITE_EMPTY
        query[cdr_consts.DESTINATION_DATASET] = sandbox_dataset
        queries.append(query)

    for table in tables_affected:
        query = dict()
        query[cdr_consts.QUERY] = MAX_DAYS_SUPPLY_AND_REFILLS_QUERY.format(
            project_id=project_id,
            dataset_id=dataset_id,
            table=table,
            MAX_DAYS_SUPPLY=MAX_DAYS_SUPPLY,
            MAX_REFILLS=MAX_REFILLS)
        query[cdr_consts.DESTINATION_TABLE] = table
        query[cdr_consts.DISPOSITION] = bq_consts.WRITE_TRUNCATE
        query[cdr_consts.DESTINATION_DATASET] = dataset_id
        queries.append(query)
    return queries


if __name__ == '__main__':
    import cdr_cleaner.args_parser as parser
    import cdr_cleaner.clean_cdr_engine as clean_engine

    ARGS = parser.parse_args()
    clean_engine.add_console_logging(ARGS.console_log)
    client = bq.get_client(project_id=ARGS.project_id)
    initial_row_counts = get_table_counts(tables_affected, ARGS.dataset_id)
    sandbox_tables = get_sandbox_tablenames(tables_affected, issue_numbers)
    query_list = get_days_supply_refills_queries(ARGS.project_id,
                                                 ARGS.dataset_id,
                                                 ARGS.sandbox_dataset_id,
                                                 sandbox_tables)
    clean_engine.clean_dataset(ARGS.project_id, query_list)
    validate_cleaning_rule(ARGS.dataset_id,
                           ARGS.sandbox_dataset_id,
                           sandbox_tables,
                           initial_counts=initial_row_counts)
