"""
If there is a death_date listed for a person_id, ensure that no temporal fields
(see the CDR cleaning spreadsheet tab labeled all temporal here) for that person_id exist more than
30 days after the death_date.
"""

import constants.bq_utils as bq_consts
import constants.cleaners.clean_cdr as cdr_consts

# add table names as keys and temporal representations as values into a dictionary
TEMPORAL_TABLES_WITH_END_DATES = {'visit_occurrence': 'visit',
                                  'condition_occurrence': 'condition',
                                  'drug_exposure': 'drug_exposure',
                                  'device_exposure': 'device_exposure'}

TEMPORAL_TABLES_WITH_NO_END_DATES = {'person': 'birth',
                                     'measurement': 'measurement',
                                     'procedure_occurrence': 'procedure',
                                     'observation': 'observation',
                                     'specimen': 'specimen'}

# Join Death to domain_table ON person_id
# check date field is not more than 30 days after the death date
# select domain_table_id from the result
# use the above generated domain_table_ids as a list
# select rows in a domain_table where the domain_table_ids not in above generated list of ids

REMOVE_DEATH_DATE_QUERY_WITH_END_DATES = (
    "SELECT * "
    "FROM `{dataset}.{table_name}` "
    "WHERE {table_name}_id NOT IN ("
    "  SELECT ma.{table_name}_id "
    "  FROM `{dataset}.{table_name}` ma"
    "  JOIN `{dataset}.death` d"
    "  ON ma.person_id = d.person_id"
    "  WHERE (date_diff(ma.{start_date},d.death_date, DAY) > 30"
    "  and date_diff({end_date}, d.death_date, DAY) > 30))"
)

REMOVE_DEATH_DATE_QUERY = (
    "SELECT * "
    "FROM `{project_id}.{dataset}.{table_name}` "
    "WHERE {table_name}_id NOT IN ("
    "  SELECT ma.{table_name}_id "
    "  FROM `{dataset}.{table_name}` ma"
    "  JOIN `{dataset}.death` d"
    "  ON ma.person_id = d.person_id"
    "  WHERE (date_diff({date_column}, death_date, DAY) > 30))"
)


def no_data_30_days_after_death(project_id, dataset_id):
    """
    Returns a list of queries which will remove data for each person if the data is 30 days after the death date.

    :param project_id: Project associated with the input and output datasets
    :param dataset_id: Dataset where cleaning rules are to be applied
    :return: a list of queries
    """
    queries = []
    for table in TEMPORAL_TABLES_WITH_NO_END_DATES:
        if table == cdr_consts.PERSON_TABLE_NAME:
            date_column = 'DATE({person_table_column}_datetime)'.format(
                person_table_column=TEMPORAL_TABLES_WITH_NO_END_DATES[table])
            query = dict()
            query[cdr_consts.QUERY] = REMOVE_DEATH_DATE_QUERY.format(project_id=project_id,
                                                                     dataset=dataset_id,
                                                                     table_name=table,
                                                                     date_column=date_column)
            query[cdr_consts.DESTINATION_TABLE] = table
            query[cdr_consts.DISPOSITION] = bq_consts.WRITE_TRUNCATE
            query[cdr_consts.DESTINATION_DATASET] = dataset_id

            queries.append(query)
        else:
            query = dict()
            date_column = '{table_column}_date'.format(table_column=TEMPORAL_TABLES_WITH_NO_END_DATES[table])
            query[cdr_consts.QUERY] = REMOVE_DEATH_DATE_QUERY.format(project_id=project_id,
                                                                     dataset=dataset_id,
                                                                     table_name=table,
                                                                     date_column=date_column)
            query[cdr_consts.DESTINATION_TABLE] = table
            query[cdr_consts.DISPOSITION] = bq_consts.WRITE_TRUNCATE
            query[cdr_consts.DESTINATION_DATASET] = dataset_id

            queries.append(query)

    for table in TEMPORAL_TABLES_WITH_END_DATES:
        query = dict()
        start_date = '{table_column}_start_date'.format(table_column=TEMPORAL_TABLES_WITH_END_DATES[table])
        end_date = '{table_column}_end_date'.format(table_column=TEMPORAL_TABLES_WITH_END_DATES[table])
        query[cdr_consts.QUERY] = REMOVE_DEATH_DATE_QUERY_WITH_END_DATES.format(project_id=project_id,
                                                                                dataset=dataset_id,
                                                                                table_name=table,
                                                                                start_date=start_date,
                                                                                end_date=end_date)
        query[cdr_consts.DESTINATION_TABLE] = table
        query[cdr_consts.DISPOSITION] = bq_consts.WRITE_TRUNCATE
        query[cdr_consts.DESTINATION_DATASET] = dataset_id

        queries.append(query)
    return queries


if __name__ == '__main__':
    import args_parser as parser

    if parser.args.dataset_id:
        query_list = no_data_30_days_after_death(parser.args.project_id, parser.args.dataset_id)
        parser.clean_engine.clean_dataset(parser.args.project_id, parser.args.dataset_id, query_list)
