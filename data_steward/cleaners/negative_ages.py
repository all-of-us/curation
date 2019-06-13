"""
Age should not be negative for the person at any dates/start dates.
Using rule 20, 21 in Achilles Heel for reference.
Also ensure ages are not beyond 150.
"""

# Project imports
import bq_utils

# tables to consider, along with their date/start date fields
date_fields = {'observation_period': 'observation_period_start_date',
               'visit_occurrence': 'visit_start_date',
               'condition_occurrence': 'condition_start_date',
               'procedure_occurrence': 'procedure_date',
               'drug_exposure': 'drug_exposure_start_date',
               'observation': 'observation_date',
               'drug_era': 'drug_era_start_date',
               'condition_era': 'condition_era_start_date',
               'measurement': 'measurement_date',
               'device_exposure': 'device_exposure_date'}

person = 'person'
MAX_AGE = 150

# negative age at recorded time in table
NEGATIVE_AGES_QUERY = '''
DELETE
FROM `{project_id}.{dataset_id}.{table_name}`
WHERE {table}_id IN
(SELECT t.{table}_id
FROM `{project_id}.{dataset_id}.{table_name}` t
JOIN `{project_id}.{dataset_id}.{person_table_name}` p
ON t.person_id = p.person_id
WHERE t.{table_date} < DATE(p.birth_datetime))
'''

# age > MAX_AGE (=150) at recorded time in table
MAX_AGE_QUERY = '''
DELETE
FROM `{project_id}.{dataset_id}.{table_name}`
WHERE {table}_id IN
(SELECT t.{table}_id
FROM `{project_id}.{dataset_id}.{table_name}` t
JOIN `{project_id}.{dataset_id}.{person_table_name}` p
ON t.person_id = p.person_id
WHERE EXTRACT(YEAR FROM t.{table_date}) - EXTRACT(YEAR FROM p.birth_datetime) > {MAX_AGE})
'''

# negative age at death
NEGATIVE_AGE_DEATH_QUERY = '''
DELETE
FROM `{project_id}.{dataset_id}.{table_name}`
WHERE person_id IN
(SELECT d.person_id
FROM `{project_id}.{dataset_id}.{table_name}` d
JOIN `{project_id}.{dataset_id}.{person_table_name}` p
ON d.person_id = p.person_id
WHERE d.death_date < DATE(p.birth_datetime))
'''


def get_negative_ages_queries(project_id, dataset_id):
    """
    This function gets the queries required to remove table records which are prior
    to the person's birth date or 150 years past the birth date from a dataset

    :param project_id: Project name
    :param dataset_id: Name of the dataset where a rule should be applied
    :return: a list of queries.
    """
    queries = []
    for table in date_fields.keys():
        if 'unioned' in dataset_id:
            table_name = 'unioned_ehr_{table}'.format(table=table)
            person_table_name = 'unioned_ehr_{table}'.format(table=person)
        else:
            table_name = table
            person_table_name = person
        if bq_utils.table_exists(table_name, dataset_id):
            query_na = NEGATIVE_AGES_QUERY.format(project_id=project_id,
                                                  dataset_id=dataset_id,
                                                  table_name=table_name,
                                                  table=table,
                                                  person_table_name=person_table_name,
                                                  table_date=date_fields[table])
            query_ma = MAX_AGE_QUERY.format(project_id=project_id,
                                            dataset_id=dataset_id,
                                            table_name=table_name,
                                            table=table,
                                            person_table_name=person_table_name,
                                            table_date=date_fields[table],
                                            MAX_AGE=MAX_AGE)
            queries.extend([query_na, query_ma])

    # query for death before birthdate
    table = 'death'
    if 'unioned' in dataset_id:
        table_name = 'unioned_ehr_{table}'.format(table=table)
        person_table_name = 'unioned_ehr_{table}'.format(table=person)
    else:
        table_name = table
        person_table_name = person
    query = NEGATIVE_AGE_DEATH_QUERY.format(project_id=project_id,
                                            dataset_id=dataset_id,
                                            table_name=table_name,
                                            person_table_name=person_table_name)
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
        query_list = get_negative_ages_queries(args.project_id, args.dataset_id)
        clean_cdr_engine.clean_dataset(args.project_id, args.dataset_id, query_list)
