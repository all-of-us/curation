"""
Year of birth should not be in the future (as of writing this, 2019) or before 1800
Using rule 18, 19 in Achilles Heel for reference
"""
import logging

# Project imports
import constants.bq_utils as bq_consts
import constants.cdr_cleaner.clean_cdr as cdr_consts
import common
import resources

LOGGER = logging.getLogger(__name__)

person = common.PERSON
MIN_YEAR_OF_BIRTH = 1800
MAX_YEAR_OF_BIRTH = '(EXTRACT(YEAR FROM CURRENT_DATE()) - 17)'

KEEP_YEAR_OF_BIRTH_TABLE_ROWS = (
    'SELECT * '
    'FROM `{project_id}.{dataset_id}.{table}` '
    'WHERE person_id '
    'IN '
    '(SELECT person_id '
    'FROM `{project_id}.{dataset_id}.{person_table}` p '
    'WHERE p.year_of_birth > {MIN_YEAR_OF_BIRTH} '
    'AND p.year_of_birth < {MAX_YEAR_OF_BIRTH}) ')

KEEP_YEAR_OF_BIRTH_PERSON_ROWS = (
    'SELECT * '
    'FROM `{project_id}.{dataset_id}.{person_table}` p '
    'WHERE p.year_of_birth > {MIN_YEAR_OF_BIRTH} '
    'AND p.year_of_birth < {MAX_YEAR_OF_BIRTH} ')


def has_person_id_key(table):
    """
    Determines if a CDM table contains person_id field except for person table

    :param table: name of a CDM table
    :return: True if the CDM table contains a person_id field, False otherwise
    """
    if 'person' in table:
        return False
    fields = resources.fields_for(table)
    person_id_field = 'person_id'
    return any(
        field for field in fields
        if field['type'] == 'integer' and field['name'] == person_id_field)


def get_year_of_birth_queries(project_id, dataset_id):
    """
    This function gets the queries required to remove table records
    associated with a person whose birth year is before 1800 or after 2019

    :param project_id: Project name
    :param dataset_id: Name of the dataset where a rule should be applied
    :return a list of queries.
    """
    queries = []
    for table in resources.CDM_TABLES:
        if has_person_id_key(table):
            query = dict()
            query[cdr_consts.QUERY] = KEEP_YEAR_OF_BIRTH_TABLE_ROWS.format(
                project_id=project_id,
                dataset_id=dataset_id,
                table=table,
                person_table=person,
                MIN_YEAR_OF_BIRTH=MIN_YEAR_OF_BIRTH,
                MAX_YEAR_OF_BIRTH=MAX_YEAR_OF_BIRTH)
            query[cdr_consts.DESTINATION_TABLE] = table
            query[cdr_consts.DISPOSITION] = bq_consts.WRITE_TRUNCATE
            query[cdr_consts.DESTINATION_DATASET] = dataset_id
            queries.append(query)
    person_query = dict()
    person_query[cdr_consts.QUERY] = KEEP_YEAR_OF_BIRTH_PERSON_ROWS.format(
        project_id=project_id,
        dataset_id=dataset_id,
        person_table=person,
        MIN_YEAR_OF_BIRTH=MIN_YEAR_OF_BIRTH,
        MAX_YEAR_OF_BIRTH=MAX_YEAR_OF_BIRTH)
    person_query[cdr_consts.DESTINATION_TABLE] = person
    person_query[cdr_consts.DISPOSITION] = bq_consts.WRITE_TRUNCATE
    person_query[cdr_consts.DESTINATION_DATASET] = dataset_id
    queries.append(person_query)
    return queries


if __name__ == '__main__':
    import cdr_cleaner.args_parser as parser
    import cdr_cleaner.clean_cdr_engine as clean_engine

    ARGS = parser.parse_args()

    if ARGS.list_queries:
        clean_engine.add_console_logging()
        query_list = clean_engine.get_query_list(ARGS.project_id,
                                                 ARGS.dataset_id,
                                                 [(get_year_of_birth_queries,)])
        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(ARGS.project_id, ARGS.dataset_id,
                                   [(get_year_of_birth_queries,)])
