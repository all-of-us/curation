"""
Age should not be negative for the person at any dates/start dates.
Using rule 20, 21 in Achilles Heel for reference.
Also ensure ages are not beyond 150.
"""
import logging

# Project imports
import common
from constants import bq_utils as bq_consts
from constants.cdr_cleaner import clean_cdr as cdr_consts

LOGGER = logging.getLogger(__name__)

# tables to consider, along with their date/start date fields
date_fields = {
    common.OBSERVATION_PERIOD: 'observation_period_start_date',
    common.VISIT_OCCURRENCE: 'visit_start_date',
    common.CONDITION_OCCURRENCE: 'condition_start_date',
    common.PROCEDURE_OCCURRENCE: 'procedure_date',
    common.DRUG_EXPOSURE: 'drug_exposure_start_date',
    common.OBSERVATION: 'observation_date',
    common.DRUG_ERA: 'drug_era_start_date',
    common.CONDITION_ERA: 'condition_era_start_date',
    common.MEASUREMENT: 'measurement_date',
    common.DEVICE_EXPOSURE: 'device_exposure_start_date'
}

person = common.PERSON
MAX_AGE = 150

# negative age at recorded time in table
NEGATIVE_AGES_QUERY = ('SELECT * '
                       'FROM `{project_id}.{dataset_id}.{table}` '
                       'WHERE {table}_id NOT IN '
                       '(SELECT t.{table}_id '
                       'FROM `{project_id}.{dataset_id}.{table}` t '
                       'JOIN `{project_id}.{dataset_id}.{person_table}` p '
                       'ON t.person_id = p.person_id '
                       'WHERE t.{table_date} < DATE(p.birth_datetime)) ')

# age > MAX_AGE (=150) at recorded time in table
MAX_AGE_QUERY = (
    'SELECT * '
    'FROM `{project_id}.{dataset_id}.{table}` '
    'WHERE {table}_id NOT IN '
    '(SELECT t.{table}_id '
    'FROM `{project_id}.{dataset_id}.{table}` t '
    'JOIN `{project_id}.{dataset_id}.{person_table}` p '
    'ON t.person_id = p.person_id '
    'WHERE EXTRACT(YEAR FROM t.{table_date}) - EXTRACT(YEAR FROM p.birth_datetime) > {MAX_AGE}) '
)

# negative age at death
NEGATIVE_AGE_DEATH_QUERY = ('SELECT * '
                            'FROM `{project_id}.{dataset_id}.{table}` '
                            'WHERE person_id NOT IN '
                            '(SELECT d.person_id '
                            'FROM `{project_id}.{dataset_id}.{table}` d '
                            'JOIN `{project_id}.{dataset_id}.{person_table}` p '
                            'ON d.person_id = p.person_id '
                            'WHERE d.death_date < DATE(p.birth_datetime)) ')


def get_negative_ages_queries(project_id, dataset_id):
    """
    This function gets the queries required to remove table records which are prior
    to the person's birth date or 150 years past the birth date from a dataset

    :param project_id: Project name
    :param dataset_id: Name of the dataset where a rule should be applied
    :return: a list of queries.
    """
    queries = []
    for table in date_fields:
        query_na = dict()
        query_ma = dict()
        person_table = person
        query_na[cdr_consts.QUERY] = NEGATIVE_AGES_QUERY.format(
            project_id=project_id,
            dataset_id=dataset_id,
            table=table,
            person_table=person_table,
            table_date=date_fields[table])
        query_na[cdr_consts.DESTINATION_TABLE] = table
        query_na[cdr_consts.DISPOSITION] = bq_consts.WRITE_TRUNCATE
        query_na[cdr_consts.DESTINATION_DATASET] = dataset_id
        query_ma[cdr_consts.QUERY] = MAX_AGE_QUERY.format(
            project_id=project_id,
            dataset_id=dataset_id,
            table=table,
            person_table=person_table,
            table_date=date_fields[table],
            MAX_AGE=MAX_AGE)
        query_ma[cdr_consts.DESTINATION_TABLE] = table
        query_ma[cdr_consts.DISPOSITION] = bq_consts.WRITE_TRUNCATE
        query_ma[cdr_consts.DESTINATION_DATASET] = dataset_id
        queries.extend([query_na, query_ma])

    # query for death before birthdate
    death = common.DEATH
    query = dict()
    person_table = person
    query[cdr_consts.QUERY] = NEGATIVE_AGE_DEATH_QUERY.format(
        project_id=project_id,
        dataset_id=dataset_id,
        table=death,
        person_table=person_table)
    query[cdr_consts.DESTINATION_TABLE] = death
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
        query_list = clean_engine.get_query_list(ARGS.project_id,
                                                 ARGS.dataset_id,
                                                 [(get_negative_ages_queries,)])
        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(ARGS.project_id, ARGS.dataset_id,
                                   [(get_negative_ages_queries,)])
