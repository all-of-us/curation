import field_mapping
import constants.bq_utils as bq_consts
import constants.cdr_cleaner.clean_cdr as cdr_consts

OBSERVATION_TABLE = 'observation'

DOMAIN_TABLES_EXCEPT_OBSERVATION = [
    'condition_occurrence',
    'visit_occurrence',
    'drug_exposure',
    'measurement',
    'procedure_occurrence',
    'observation_period',
    'device_exposure',
    'specimen'
]

OBSERVATION_DEFAULT_YEAR_THRESHOLD = 1900

DEFAULT_YEAR_THRESHOLD = 1980

DATE_FIELD_KEY_WORD = 'date'

REMOVE_RECORDS_WITH_WRONG_DATE_FIELD_TEMPLATE = '''
SELECT
    *
FROM `{project_id}.{dataset_id}.{table_id}`
WHERE {where_clause}
'''

WHERE_CLAUSE_REQUIRED_FIELD = '''
(EXTRACT(YEAR FROM {date_field_name}) > {year_threshold} AND CAST({date_field_name} AS DATE) <= current_date())
'''

WHERE_CLAUSE_NULLABLE_FIELD = '''
IF({date_field_name} IS NULL, TRUE, EXTRACT(YEAR FROM {date_field_name}) >= {year_threshold} AND CAST({date_field_name} AS DATE) <= current_date())
'''

AND = ' AND '


def generate_where_clause(table_id, date_field_name, year_threshold):
    """
    This function generates a where clause for the date field based on the given year threshold
    :param table_id: the table id
    :param date_field_name: the date field
    :param year_threshold: the year threshold for the date field
    :return:
    """

    if field_mapping.is_field_required(table_id, date_field_name):
        where_clause = WHERE_CLAUSE_REQUIRED_FIELD.format(date_field_name=date_field_name,
                                                          year_threshold=year_threshold)
    else:
        where_clause = WHERE_CLAUSE_NULLABLE_FIELD.format(date_field_name=date_field_name,
                                                          year_threshold=year_threshold)

    return where_clause


def parse_remove_records_with_wrong_date_query(project_id, dataset_id, table_id, year_threshold):
    """
    This query generates the query to keep the records whose date fields are larger than and equal to the year_threshold
    :param project_id: the project id
    :param dataset_id: the dataset id
    :param table_id: the table id
    :param year_threshold: the year threshold for removing the records
    :return: a query that keep the records qualifying for the year threshold
    """
    date_field_names = [field for field in field_mapping.get_domain_fields(table_id) if DATE_FIELD_KEY_WORD in field]

    where_clause = ''

    for date_field_name in date_field_names:

        if where_clause != '':
            where_clause += AND

        where_clause += generate_where_clause(table_id, date_field_name, year_threshold)

    return REMOVE_RECORDS_WITH_WRONG_DATE_FIELD_TEMPLATE.format(project_id=project_id,
                                                                dataset_id=dataset_id,
                                                                table_id=table_id,
                                                                where_clause=where_clause)


def get_remove_records_with_wrong_date_queries(project_id,
                                               dataset_id,
                                               year_threshold,
                                               observation_year_threshold):
    """
    This function generates a list of query dicts for removing the records with wrong date in the corresponding destination table.
    :param project_id: the project_id in which the query is run
    :param dataset_id: the dataset_id in which the query is run
    :param year_threshold: the year threshold applied to the pre-defined list of domain tables except observation
    :param observation_year_threshold: the year threshold applied to observation
    :return: a list of query dicts for removing the records with wrong date in the corresponding destination table
    """

    queries = []

    query = dict()
    query[cdr_consts.QUERY] = parse_remove_records_with_wrong_date_query(project_id,
                                                                         dataset_id,
                                                                         OBSERVATION_TABLE,
                                                                         observation_year_threshold)
    query[cdr_consts.DESTINATION_TABLE] = OBSERVATION_TABLE
    query[cdr_consts.DISPOSITION] = bq_consts.WRITE_TRUNCATE
    query[cdr_consts.DESTINATION_DATASET] = dataset_id
    query[cdr_consts.BATCH] = True
    queries.append(query)

    for domain_table in DOMAIN_TABLES_EXCEPT_OBSERVATION:
        query = dict()
        query[cdr_consts.QUERY] = parse_remove_records_with_wrong_date_query(project_id,
                                                                             dataset_id,
                                                                             domain_table,
                                                                             year_threshold)
        query[cdr_consts.DESTINATION_TABLE] = domain_table
        query[cdr_consts.DISPOSITION] = bq_consts.WRITE_TRUNCATE
        query[cdr_consts.DESTINATION_DATASET] = dataset_id
        query[cdr_consts.BATCH] = True
        queries.append(query)

    return queries


def parse_args():
    """
    This function expands the default argument list defined in cdr_cleaner.args_parser
    :return: an expanded argument list object
    """
    import cdr_cleaner.args_parser as parser

    argument_parser = parser.get_argument_parser()

    argument_parser.add_argument('-y',
                                 '--year_threshold',
                                 dest='year_threshold',
                                 action='store',
                                 help='The year threshold applied to domain tables except observation',
                                 required=False,
                                 default=DEFAULT_YEAR_THRESHOLD)

    argument_parser.add_argument('-o',
                                 '--observation_year_threshold',
                                 dest='observation_year_threshold',
                                 action='store',
                                 help='The threshold applied to observation',
                                 required=False,
                                 default=OBSERVATION_DEFAULT_YEAR_THRESHOLD)

    return argument_parser.parse_args()


if __name__ == '__main__':
    import cdr_cleaner.clean_cdr_engine as clean_engine

    ARGS = parse_args()

    clean_engine.add_console_logging(ARGS.console_log)
    query_list = get_remove_records_with_wrong_date_queries(ARGS.project_id,
                                                            ARGS.dataset_id,
                                                            ARGS.year_threshold,
                                                            ARGS.observation_year_threshold)
    clean_engine.clean_dataset(ARGS.project_id, ARGS.dataset_id, query_list)