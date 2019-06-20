"""
End dates should not be prior to start dates in any table
* If end date is nullable, it will be nulled
* If end date is required,
    * If visit type is ER(id 9203)/Outpatient(id 9202), end date = start date
    * If visit type is inpatient(id 9201)
        * If other tables have dates for that visit, end date = max(all dates from other tables for that visit)
        * Else, end date = start date.
Using rule 18, 19 in Achilles Heel for reference
Drug era, condition era, cohort are excluded
"""

# Project imports
import bq_utils
import resources

concepts = {'visit_occurrence': ['visit_start_date', 'visit_end_date'],
            'condition_occurrence': ['condition_start_date', 'condition_end_date'],
            'drug_exposure': ['drug_exposure_start_date', 'drug_exposure_end_date'],
            'device_exposure': ['device_exposure_start_date', 'device_exposure_start_date']}

person = 'person'
MIN_YEAR_OF_BIRTH = 1800
MAX_YEAR_OF_BIRTH = 2019

NULL_BAD_END_DATES = '''
    SELECT l.*
    FROM `{project_id}.{dataset_id}.{table}` l
    LEFT JOIN (SELECT *
              FROM `{project_id}.{dataset_id}.{table}`
              WHERE NOT {table_end_date} < {table_start_date}) r
    ON l.{table}_id = r.table_id
    '''

DELETE_YEAR_OF_BIRTH_PERSON_ROWS = '''
    DELETE
    FROM `{project_id}.{dataset_id}.{person_table}` p
    WHERE p.year_of_birth < {MIN_YEAR_OF_BIRTH}
    OR p.year_of_birth > {MAX_YEAR_OF_BIRTH})
    '''


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
            if bq_utils.table_exists(table, dataset_id):
                query = DELETE_YEAR_OF_BIRTH_TABLE_ROWS.format(project_id=project_id,
                                                               dataset_id=dataset_id,
                                                               table=table,
                                                               person_table=person,
                                                               MIN_YEAR_OF_BIRTH=MIN_YEAR_OF_BIRTH,
                                                               MAX_YEAR_OF_BIRTH=MAX_YEAR_OF_BIRTH)
                queries.append(query)
    query = DELETE_YEAR_OF_BIRTH_PERSON_ROWS.format(project_id=project_id,
                                                    dataset_id=dataset_id,
                                                    person_table=person,
                                                    MIN_YEAR_OF_BIRTH=MIN_YEAR_OF_BIRTH,
                                                    MAX_YEAR_OF_BIRTH=MAX_YEAR_OF_BIRTH)
    queries.append(query)
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