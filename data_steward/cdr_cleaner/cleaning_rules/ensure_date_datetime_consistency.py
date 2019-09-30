import field_mapping
import constants.bq_utils as bq_consts
import constants.cdr_cleaner.clean_cdr as cdr_consts
import common

TABLE_DATES = {
    common.CONDITION_OCCURRENCE: {'condition_start_datetime': 'condition_start_date',
                                  'condition_end_datetime': 'condition_end_date'},
    common.DRUG_EXPOSURE: {'drug_exposure_start_datetime': 'drug_exposure_start_date',
                           'drug_exposure_end_datetime': 'drug_exposure_end_date'},
    common.DEVICE_EXPOSURE: {'device_exposure_start_datetime': 'device_exposure_start_date',
                             'device_exposure_end_datetime': 'device_exposure_end_date'},
    common.MEASUREMENT: {'measurement_datetime': 'measurement_date'},
    common.OBSERVATION: {'observation_datetime': 'observation_date'},
    common.PROCEDURE_OCCURRENCE: {'procedure_datetime': 'procedure_date'},
    common.DEATH: {'death_datetime': 'death_date'},
    common.SPECIMEN: {'specimen_datetime': 'specimen_date'},
    common.OBSERVATION_PERIOD: {'observation_period_start_datetime': 'observation_period_start_date',
                                'observation_period_end_datetime': 'observation_period_end_date'},
    common.VISIT_OCCURRENCE: {'visit_start_datetime': 'visit_start_date',
                              'visit_end_datetime': 'visit_end_date'}
}

FIX_DATETIME_QUERY = """
SELECT {cols}
FROM `{project_id}.{dataset_id}.{table_id}`
"""


def get_cols(table_id):
    """
    Generates the fields to choose along with case statements to generate datetime

    :param table_id: table for which the fields
    :return:
    """
    table_fields = field_mapping.get_domain_fields(table_id)
    col_exprs = []
    for field in table_fields:
        if field in TABLE_DATES[table_id]:
            if field_mapping.is_field_required(table_id, field):
                col_expr = (' CASE'
                            ' WHEN EXTRACT(DATE FROM {field}) = {date_field}'
                            ' THEN {field}'
                            ' ELSE CAST(DATETIME({date_field}, EXTRACT(TIME FROM {field})) AS TIMESTAMP)'
                            ' END AS {field}').format(field=field, date_field=TABLE_DATES[table_id][field])
            else:
                col_expr = (' CASE'
                            ' WHEN EXTRACT(DATE FROM {field}) = {date_field}'
                            ' THEN {field}'
                            ' ELSE NULL'
                            ' END AS {field}').format(field=field, date_field=TABLE_DATES[table_id][field])
        else:
            col_expr = field
        col_exprs.append(col_expr)
    cols = ', '.join(col_exprs)
    return cols


def get_fix_incorrect_datetime_to_date_queries(project_id, dataset_id):
    """
    This function generates a list of query dicts for ensuring the dates and datetimes are consistent

    :param project_id: the project_id in which the query is run
    :param dataset_id: the dataset_id in which the query is run
    :return: a list of query dicts for ensuring the dates and datetimes are consistent
    """
    queries = []
    for table in TABLE_DATES:
        query = dict()
        query[cdr_consts.QUERY] = FIX_DATETIME_QUERY.format(project_id=project_id,
                                                            dataset_id=dataset_id,
                                                            table_id=table,
                                                            cols=get_cols(table))
        query[cdr_consts.DESTINATION_TABLE] = table
        query[cdr_consts.DISPOSITION] = bq_consts.WRITE_TRUNCATE
        query[cdr_consts.DESTINATION_DATASET] = dataset_id
        queries.append(query)
    return queries


if __name__ == '__main__':
    import cdr_cleaner.clean_cdr_engine as clean_engine
    import cdr_cleaner.args_parser as parser

    ARGS = parser.parse_args()

    clean_engine.add_console_logging(ARGS.console_log)
    query_list = get_fix_incorrect_datetime_to_date_queries(ARGS.project_id, ARGS.dataset_id)
    clean_engine.clean_dataset(ARGS.project_id, ARGS.dataset_id, query_list)
