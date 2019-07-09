"""
A death date is considered "valid" if it is after the program start date and before the current date.
Allowing for more flexibility, we choose Jan 1, 2017 as the program start date.
"""

# Project imports
import constants.bq_utils as bq_consts
import constants.cdr_cleaner.clean_cdr as cdr_consts

death = 'death'
program_start_date = '2017-01-01'
current_date = 'CURRENT_DATE()'

KEEP_VALID_DEATH_TABLE_ROWS = (
    "SELECT * "
    "FROM `{project_id}.{dataset_id}.{table}` "
    "WHERE {table}_date > '{program_start_date}' AND {table}_date < {current_date} "
)


def get_valid_death_date_queries(project_id, dataset_id):
    """
    This function gets the queries required to keep table records
    associated with a person whose death date is "valid" described above

    :param project_id: Project name
    :param dataset_id: Name of the dataset where a rule should be applied
    :return a list of queries.
    """
    queries = []
    query = dict()
    query[cdr_consts.QUERY] = KEEP_VALID_DEATH_TABLE_ROWS.format(project_id=project_id,
                                                                 dataset_id=dataset_id,
                                                                 table=death,
                                                                 program_start_date=program_start_date,
                                                                 current_date=current_date)
    query[cdr_consts.DESTINATION_TABLE] = death
    query[cdr_consts.DISPOSITION] = bq_consts.WRITE_TRUNCATE
    query[cdr_consts.DESTINATION_DATASET] = dataset_id
    queries.append(query)
    return queries


if __name__ == '__main__':
    import cdr_cleaner.args_parser as parser
    import cdr_cleaner.clean_cdr_engine as clean_engine

    ARGS = parser.parse_args()
    clean_engine.add_console_logging(ARGS.console_log)
    query_list = get_valid_death_date_queries(ARGS.project_id, ARGS.dataset_id)
    clean_engine.clean_dataset(ARGS.project_id, ARGS.dataset_id, query_list)
