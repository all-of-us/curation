import logging

import constants.cdr_cleaner.clean_cdr as cdr_consts

LOGGER = logging.getLogger(__name__)

REMOVE_ADDITIONAL_RESPONSES_OTHER_THAN_NOT = """
DELETE
FROM
  `{project}.{dataset}.observation`
WHERE
  person_id IN (
  SELECT
    person_id
  FROM
    `{project}.{dataset}.observation`
  WHERE
    observation_concept_id = 1586140
    AND value_source_concept_id = 1586148)
  AND (observation_concept_id = 1586140
    AND value_source_concept_id != 1586148)
"""


def get_remove_multiple_race_ethnicity_answers_queries(project_id, dataset_id):
    """
    runs the query which removes the other than None of these identify me answers for gender/sex
    if a person answers None of these identify me as an option from observation table
    :param project_id: Name of the project
    :param dataset_id: Name of the dataset where the queries should be run
    :return:
    """
    queries_list = []

    query = dict()
    query[cdr_consts.QUERY] = REMOVE_ADDITIONAL_RESPONSES_OTHER_THAN_NOT.format(
        dataset=dataset_id,
        project=project_id,
    )
    queries_list.append(query)

    return queries_list


if __name__ == '__main__':
    import cdr_cleaner.args_parser as parser
    import cdr_cleaner.clean_cdr_engine as clean_engine

    ARGS = parser.parse_args()

    if ARGS.list_queries:
        clean_engine.add_console_logging()
        query_list = clean_engine.get_query_list(
            ARGS.project_id, ARGS.dataset_id,
            [(get_remove_multiple_race_ethnicity_answers_queries,)])
        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(
            ARGS.project_id, ARGS.dataset_id,
            [(get_remove_multiple_race_ethnicity_answers_queries,)])
