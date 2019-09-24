import field_mapping
import constants.bq_utils as bq_consts
import constants.cdr_cleaner.clean_cdr as cdr_consts
import common

table_dates = {'condition_start_date': 'condition_start_datetime',
               'condition_end_date': 'condition_end_datetime',
               'drug_exposure_start_date': 'drug_exposure_start_datetime',
               'drug_exposure_end_date': 'drug_exposure_end_datetime',
               'device_exposure_start_date': 'device_exposure_start_datetime',
               'device_exposure_end_date': 'device_exposure_end_datetime',
               'measurement_date': 'measurement_datetime',
               'observation_date': 'observation_datetime',
               'procedure_date': 'procedure_datetime',
               'death_date': 'death_datetime',
               'specimen_date': 'specimen_datetime',
               'observation_period_start_date': 'observation_period_start_datetime',
               'observation_period_end_date': 'observation_period_end_datetime',
               'visit_start_date': 'visit_start_datetime',
               'visit_end_date': 'visit_end_datetime'}


def get_remove_records_with_wrong_datetime_queries(project_id, dataset_id):
    """
    This function generates a list of query dicts for ensuring the dates and datetimes are consistent

    :param project_id: the project_id in which the query is run
    :param dataset_id: the dataset_id in which the query is run
    :return: a list of query dicts for ensuring the dates and datetimes are consistent
    """
    queries = []

    return queries


if __name__ == '__main__':
    import cdr_cleaner.clean_cdr_engine as clean_engine
    import cdr_cleaner.args_parser as parser

    ARGS = parser.parse_args()

    clean_engine.add_console_logging(ARGS.console_log)
    query_list = get_remove_records_with_wrong_datetime_queries(ARGS.project_id,
                                                                ARGS.dataset_id)
    clean_engine.clean_dataset(ARGS.project_id, ARGS.dataset_id, query_list)
