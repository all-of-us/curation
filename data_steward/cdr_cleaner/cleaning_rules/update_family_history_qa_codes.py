import constants.bq_utils as bq_consts
import constants.cdr_cleaner.clean_cdr as cdr_consts

UPDATE_FAMILY_HISTORY_QUERY = """
UPDATE `{project_id}.{dataset_id}.observation`
SET observation_source_concept_id = 43529655,
    value_source_concept_id = 43529090
WHERE observation_source_concept_id = 43529632
AND value_source_concept_id = 43529091
"""


def get_update_family_history_qa_queries(project_id, dataset_id):
    """
    Collect queries for updating family history questions and answers

    :param project_id: Name of the project
    :param dataset_id: Name of the dataset where the queries should be run

    :return:
    """
    queries_list = []

    query = dict()
    query[cdr_consts.QUERY] = UPDATE_FAMILY_HISTORY_QUERY.format(dataset=dataset_id,
                                                                 project=project_id)
    queries_list.append(query)

    return queries_list


if __name__ == '__main__':
    import cdr_cleaner.args_parser as parser
    import cdr_cleaner.clean_cdr_engine as clean_engine

    ARGS = parser.parse_args()
    clean_engine.add_console_logging(ARGS.console_log)
    query_list = get_update_family_history_qa_queries(ARGS.project_id, ARGS.dataset_id)
    clean_engine.clean_dataset(ARGS.project_id, query_list)
