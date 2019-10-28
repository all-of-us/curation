"""
Update answers where the participant skipped answering but the answer was registered as -1
"""
from constants.cdr_cleaner import clean_cdr as cdr_consts

UPDATE_NEGATIVE_PPI_QUERY = """
UPDATE
  `{project_id}.{dataset_id}.observation`
SET value_as_number = NULL,
value_source_concept_id = 903096,
value_as_concept_id = 903096,
value_as_string = 'PMI_Skip',
value_source_value = 'PMI_Skip'
WHERE value_as_number = -1
AND observation_source_concept_id = 1585747
"""


def get_update_ppi_queries(project_id, dataset_id):
    """
    Generate the query that updates negative ppi answers to pmi_skip where obs_src_concept_id is 1585747

    :param project_id: the project_id in which the query is run
    :param dataset_id: the dataset_id in which the query is run
    :return: a list of query dicts for updating records
    """
    queries = []

    query = dict()
    query[cdr_consts.QUERY] = UPDATE_NEGATIVE_PPI_QUERY.format(project_id=project_id,
                                                               dataset_id=dataset_id)
    queries.append(query)

    return queries


if __name__ == '__main__':
    import cdr_cleaner.args_parser as parser
    import cdr_cleaner.clean_cdr_engine as clean_engine

    ARGS = parser.parse_args()

    clean_engine.add_console_logging(ARGS.console_log)
    query_list = get_update_ppi_queries(ARGS.project_id, ARGS.dataset_id)
    clean_engine.clean_dataset(ARGS.project_id, ARGS.dataset_id, query_list)
