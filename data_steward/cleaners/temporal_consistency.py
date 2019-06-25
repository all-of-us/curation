"""
Bad end dates:
End dates should not be prior to start dates in any table
* If end date is nullable, it will be nulled
* If end date is required,
    * If visit type is inpatient(id 9201)
        * If other tables have dates for that visit, end date = max(all dates from other tables for that visit)
        * Else, end date = start date.
    * Else, If visit type is ER(id 9203)/Outpatient(id 9202), end date = start date
"""

# Project imports
import bq_utils
import constants.cleaners.clean_cdr as cdr_consts
import constants.bq_utils as bq_consts

table_dates = {'condition_occurrence': ['condition_start_date', 'condition_end_date'],
               'drug_exposure': ['drug_exposure_start_date', 'drug_exposure_end_date'],
               'device_exposure': ['device_exposure_start_date', 'device_exposure_start_date']}

visit_occurrence = 'visit_occurrence'
placeholder_date = '1900-01-01'

NULL_BAD_END_DATES = (
    'SELECT l.* '
    'FROM `{project_id}.{dataset_id}.{table}` l '
    'LEFT JOIN (SELECT * '
    'FROM `{project_id}.{dataset_id}.{table}` '
    'WHERE NOT {table_end_date} < {table_start_date}) r '
    'ON l.{table}_id = r.{table}_id '
)

POPULATE_VISIT_END_DATES = (
    'SELECT '
    'visit_occurrence_id, '
    'person_id, '
    'visit_concept_id, '
    'visit_start_date, '
    'visit_start_datetime, '
    'CASE '
    'WHEN visit_concept_id = 9201 AND max_end_date != "{placeholder_date}" THEN max_end_date '
    'ELSE visit_start_date '
    'END AS visit_end_date, '
    'visit_end_datetime, '
    'visit_type_concept_id, '
    'provider_id, '
    'care_site_id, '
    'visit_source_value, '
    'visit_source_concept_id, '
    'admitting_source_concept_id, '
    'admitting_source_value, '
    'discharge_to_concept_id, '
    'discharge_to_source_value, '
    'preceding_visit_occurrence_id '
    'FROM '
    '(SELECT '
    'GREATEST( '
    'CASE WHEN MAX(co.condition_end_date) IS NULL THEN "{placeholder_date}" '
    'ELSE MAX(co.condition_end_date) END, '
    'CASE WHEN MAX(dre.drug_exposure_end_date) IS NULL THEN "{placeholder_date}" '
    'ELSE MAX(dre.drug_exposure_end_date) END, '
    'CASE WHEN MAX(dve.device_exposure_end_date) IS NULL THEN "{placeholder_date}" '
    'ELSE MAX(dve.device_exposure_end_date) END) as max_end_date, '
    'vo.* '
    'FROM `{project_id}.{dataset_id}.visit_occurrence` vo '
    'LEFT JOIN `{project_id}.{dataset_id}.condition_occurrence` co '
    'ON vo.visit_occurrence_id = co.visit_occurrence_id '
    'LEFT JOIN `{project_id}.{dataset_id}.drug_exposure` dre '
    'ON vo.visit_occurrence_id = dre.visit_occurrence_id '
    'LEFT JOIN `{project_id}.{dataset_id}.device_exposure` dve '
    'ON vo.visit_occurrence_id = dve.visit_occurrence_id '
    'WHERE vo.visit_end_date < vo.visit_start_date '
    'GROUP BY '
    'visit_occurrence_id, '
    'person_id, visit_concept_id, '
    'visit_start_date, '
    'visit_start_datetime, '
    'visit_end_date, '
    'visit_end_datetime, '
    'visit_type_concept_id, '
    'provider_id, care_site_id, '
    'visit_source_value, '
    'visit_source_concept_id, '
    'admitting_source_concept_id, '
    'admitting_source_value, '
    'discharge_to_concept_id, '
    'discharge_to_source_value, '
    'preceding_visit_occurrence_id) '
    'UNION ALL '
    'SELECT * '
    'FROM `{project_id}.{dataset_id}.visit_occurrence` '
    'WHERE visit_start_date <= visit_end_date '
)


def get_bad_end_date_queries(project_id, dataset_id):
    """
    This function gets the queries required to update end dates as described at the top

    :param project_id: Project name
    :param dataset_id: Name of the dataset where a rule should be applied
    :return a list of queries.
    """
    queries = []
    for table in table_dates:
        if bq_utils.table_exists(table, dataset_id):
            query = dict()
            query[cdr_consts.QUERY] = NULL_BAD_END_DATES.format(project_id=project_id,
                                                                dataset_id=dataset_id,
                                                                table=table,
                                                                table_start_date=table_dates[table][0],
                                                                table_end_date=table_dates[table][1])
            query[cdr_consts.DESTINATION_TABLE] = table
            query[cdr_consts.DISPOSITION] = bq_consts.WRITE_TRUNCATE
            query[cdr_consts.DESTINATION_DATASET] = dataset_id
            queries.append(query)
    query = dict()
    query[cdr_consts.QUERY] = POPULATE_VISIT_END_DATES.format(project_id=project_id,
                                                              dataset_id=dataset_id,
                                                              placeholder_date=placeholder_date)
    query[cdr_consts.DESTINATION_TABLE] = visit_occurrence
    query[cdr_consts.DISPOSITION] = bq_consts.WRITE_TRUNCATE
    query[cdr_consts.DESTINATION_DATASET] = dataset_id
    queries.append(query)
    return queries


if __name__ == '__main__':
    import argparse
    import clean_cdr_engine

    parser = argparse.ArgumentParser(description='Parse project_id and dataset_id',
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('-p', '--project_id',
                        action='store', dest='project_id',
                        help='Project associated with the input and output datasets',
                        required=True)
    parser.add_argument('-d', '--dataset_id',
                        action='store', dest='dataset_id',
                        help='Dataset where cleaning rules are to be applied',
                        required=True)
    args = parser.parse_args()
    if args.dataset_id:
        query_list = get_bad_end_date_queries(args.project_id, args.dataset_id)
        print(query_list)
        clean_cdr_engine.clean_dataset(args.project_id, args.dataset_id, query_list)
