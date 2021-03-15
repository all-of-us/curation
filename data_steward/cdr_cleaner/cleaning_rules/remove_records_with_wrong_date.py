import logging
from datetime import datetime

from cdr_cleaner.cleaning_rules import field_mapping
import constants.bq_utils as bq_consts
import constants.cdr_cleaner.clean_cdr as cdr_consts

LOGGER = logging.getLogger(__name__)

OBSERVATION_TABLE = 'observation'

DOMAIN_TABLES_EXCEPT_OBSERVATION = [
    'condition_occurrence', 'visit_occurrence', 'drug_exposure', 'measurement',
    'procedure_occurrence', 'observation_period', 'device_exposure', 'specimen'
]

OBSERVATION_DEFAULT_YEAR_THRESHOLD = 1900

DEFAULT_YEAR_THRESHOLD = 1980

DATE_FIELD_KEY_WORD = 'date'

REMOVE_RECORDS_WITH_WRONG_DATE_FIELD_TEMPLATE = '''
SELECT
    {col_expr}
FROM `{project_id}.{dataset_id}.{table_id}`
WHERE {where_clause}
'''

WHERE_CLAUSE_REQUIRED_FIELD = (
    '(EXTRACT(YEAR FROM {date_field_name}) > {year_threshold} AND CAST({date_field_name} AS DATE) <= DATE("{cutoff_date}"))'
)

NULLABLE_DATE_FIELD_EXPRESSION = (
    'IF(EXTRACT(YEAR FROM {date_field_name}) <= {year_threshold} OR CAST({date_field_name} AS DATE) > DATE("{cutoff_date}"), NULL, {date_field_name}) AS {date_field_name}'
)

AND = ' AND '


def get_date_fields(table_id):
    """
    The function retrieves a list of date related fields for the given table
    :param table_id:
    :return:
    """
    return [
        field for field in field_mapping.get_domain_fields(table_id)
        if DATE_FIELD_KEY_WORD in field
    ]


def generate_field_expr(table_id, year_threshold, cutoff_date):
    """
    This function generates the select statements for the table. For the nullable date fields, it sets the value to NULL
    if the nullable date field fails the threshold criteria
    :param table_id:
    :param year_threshold:
    :param cutoff_date:
    :return:
    """
    col_expression_list = []

    nullable_date_field_names = [
        field for field in get_date_fields(table_id)
        if not field_mapping.is_field_required(table_id, field)
    ]

    for field_name in field_mapping.get_domain_fields(table_id):

        if field_name in nullable_date_field_names:
            col_expression_list.append(
                NULLABLE_DATE_FIELD_EXPRESSION.format(
                    date_field_name=field_name,
                    year_threshold=year_threshold,
                    cutoff_date=cutoff_date))
        else:
            col_expression_list.append(field_name)

    return ','.join(col_expression_list)


def parse_remove_records_with_wrong_date_query(project_id, dataset_id, table_id,
                                               year_threshold, cutoff_date):
    """
    This query generates the query to keep the records whose date fields are larger than and equal to the year_threshold
    :param project_id: the project id
    :param dataset_id: the dataset id
    :param table_id: the table id
    :param year_threshold: the year threshold for removing the records
    :param cutoff_date: EHR/RDR date cutoff of format YYYY-MM-DD
    :return: a query that keep the records qualifying for the year threshold
    """

    required_date_field_names = [
        field for field in get_date_fields(table_id)
        if field_mapping.is_field_required(table_id, field)
    ]
    where_clause = ''

    for date_field_name in required_date_field_names:

        if where_clause != '':
            where_clause += AND

        where_clause += WHERE_CLAUSE_REQUIRED_FIELD.format(
            date_field_name=date_field_name,
            year_threshold=year_threshold,
            cutoff_date=cutoff_date)

    col_expr = generate_field_expr(table_id, year_threshold, cutoff_date)

    return REMOVE_RECORDS_WITH_WRONG_DATE_FIELD_TEMPLATE.format(
        project_id=project_id,
        dataset_id=dataset_id,
        table_id=table_id,
        col_expr=col_expr,
        where_clause=where_clause)


def get_remove_records_with_wrong_date_queries(
    project_id,
    dataset_id,
    sandbox_dataset_id,
    cutoff_date=None,
    year_threshold=DEFAULT_YEAR_THRESHOLD,
    observation_year_threshold=OBSERVATION_DEFAULT_YEAR_THRESHOLD):
    """
    This function generates a list of query dicts for removing the records with wrong date in the corresponding destination table.
    :param project_id: the project_id in which the query is run
    :param dataset_id: the dataset_id in which the query is run
    :param cutoff_date: EHR/RDR date cutoff of format YYYY-MM-DD
    :param sandbox_dataset_id: Identifies the sandbox dataset to store rows
    #TODO use sandbox_dataset_id for CR
    :param year_threshold: the year threshold applied to the pre-defined list of domain tables except observation
    :param observation_year_threshold: the year threshold applied to observation
    :return: a list of query dicts for removing the records with wrong date in the corresponding destination table
    """
    if not cutoff_date:
        cutoff_date = str(datetime.now().date())
    queries = []

    query = dict()
    query[cdr_consts.QUERY] = parse_remove_records_with_wrong_date_query(
        project_id, dataset_id, OBSERVATION_TABLE, observation_year_threshold,
        cutoff_date)
    query[cdr_consts.DESTINATION_TABLE] = OBSERVATION_TABLE
    query[cdr_consts.DISPOSITION] = bq_consts.WRITE_TRUNCATE
    query[cdr_consts.DESTINATION_DATASET] = dataset_id
    query[cdr_consts.BATCH] = True
    queries.append(query)

    for domain_table in DOMAIN_TABLES_EXCEPT_OBSERVATION:
        query = dict()
        query[cdr_consts.QUERY] = parse_remove_records_with_wrong_date_query(
            project_id, dataset_id, domain_table, year_threshold, cutoff_date)
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

    argument_parser.add_argument(
        '-y',
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

    argument_parser.add_argument(
        '-c',
        '--cutoff_date ',
        dest='cutoff_date',
        action='store',
        help='EHR/RDR date cutoff of format YYYY-MM-DD',
        required=True)

    return argument_parser.parse_args()


if __name__ == '__main__':
    import cdr_cleaner.clean_cdr_engine as clean_engine

    ARGS = parse_args()

    if ARGS.list_queries:
        clean_engine.add_console_logging()
        query_list = clean_engine.get_query_list(
            ARGS.project_id,
            ARGS.dataset_id,
            ARGS.sandbox_dataset_id,
            [(get_remove_records_with_wrong_date_queries,)],
            cutoff_date=ARGS.cutoff_date)
        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(
            ARGS.project_id,
            ARGS.dataset_id,
            ARGS.sandbox_dataset_id,
            [(get_remove_records_with_wrong_date_queries,)],
            cutoff_date=ARGS.cutoff_date)
