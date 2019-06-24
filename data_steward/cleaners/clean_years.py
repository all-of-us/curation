"""
Year of birth should not be in the future (as of writing this, 2019) or before 1800
Using rule 18, 19 in Achilles Heel for reference
"""

# Project imports
import constants.cleaners.clean_cdr as cdr_consts
import resources

person = 'person'
MIN_YEAR_OF_BIRTH = 1800
MAX_YEAR_OF_BIRTH = 2019

DELETE_YEAR_OF_BIRTH_TABLE_ROWS = (
    'DELETE '
    'FROM `{project_id}.{dataset_id}.{table}` '
    'WHERE person_id '
    'IN '
    '(SELECT person_id '
    'FROM `{project_id}.{dataset_id}.{person_table}` p '
    'WHERE p.year_of_birth < {MIN_YEAR_OF_BIRTH} '
    'OR p.year_of_birth > {MAX_YEAR_OF_BIRTH}) '
)

DELETE_YEAR_OF_BIRTH_PERSON_ROWS = (
    'DELETE '
    'FROM `{project_id}.{dataset_id}.{person_table}` p '
    'WHERE p.year_of_birth < {MIN_YEAR_OF_BIRTH} '
    'OR p.year_of_birth > {MAX_YEAR_OF_BIRTH}) '
)


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
    return any(field for field in fields if field['type'] == 'integer' and field['name'] == person_id_field)


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
            query[cdr_consts.QUERY] = DELETE_YEAR_OF_BIRTH_TABLE_ROWS.format(project_id=project_id,
                                                                             dataset_id=dataset_id,
                                                                             table=table,
                                                                             person_table=person,
                                                                             MIN_YEAR_OF_BIRTH=MIN_YEAR_OF_BIRTH,
                                                                             MAX_YEAR_OF_BIRTH=MAX_YEAR_OF_BIRTH)
            queries.append(query)
    person_query = dict()
    person_query[cdr_consts.QUERY] = DELETE_YEAR_OF_BIRTH_PERSON_ROWS.format(project_id=project_id,
                                                                      dataset_id=dataset_id,
                                                                      person_table=person,
                                                                      MIN_YEAR_OF_BIRTH=MIN_YEAR_OF_BIRTH,
                                                                      MAX_YEAR_OF_BIRTH=MAX_YEAR_OF_BIRTH)
    queries.append(person_query)
    return queries


if __name__ == '__main__':
    import argparse
    import clean_cdr_engine

    parser = argparse.ArgumentParser(description='Parse project_id and dataset_id',
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('-p', '--project_id',
                        action='store', dest='project_id',
                        help='Project associated with the input and output datasets')
    parser.add_argument('-d', '--dataset_id',
                        action='store', dest='dataset_id',
                        help='Dataset where cleaning rules are to be applied')
    args = parser.parse_args()
    if args.dataset_id:
        query_list = get_year_of_birth_queries(args.project_id, args.dataset_id)
        clean_cdr_engine.clean_dataset(args.project_id, args.dataset_id, query_list)
