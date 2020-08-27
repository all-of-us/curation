"""
Ticket: DC-564
This cleaning rule is meant to run on RDR datasets
This rule updates old Questions and Answers with the corresponding new ones.
"""
import logging

import constants.cdr_cleaner.clean_cdr as cdr_consts

LOGGER = logging.getLogger(__name__)

UPDATE_FAMILY_HISTORY_QUERY = """
UPDATE `{project_id}.{dataset_id}.observation`
SET
observation_source_concept_id = CASE
    WHEN (observation_source_concept_id = 43529632 AND value_source_concept_id = 43529091) THEN 43529655
    WHEN (observation_source_concept_id = 43529637 AND value_source_concept_id = 43529094) THEN 43529660
    WHEN (observation_source_concept_id = 43529636 AND value_source_concept_id = 702787) THEN 43529659
END,
value_source_concept_id = CASE
    WHEN (observation_source_concept_id = 43529632 AND value_source_concept_id = 43529091) THEN 43529090
    WHEN (observation_source_concept_id = 43529637 AND value_source_concept_id = 43529094) THEN 43529093
    WHEN (observation_source_concept_id = 43529636 AND value_source_concept_id = 702787) THEN 43529088
END
WHERE observation_source_concept_id IN (43529632, 43529637, 43529636)
AND value_source_concept_id IN (43529091, 43529094, 702787)
"""


def get_update_family_history_qa_queries(project_id, dataset_id):
    """
    Collect queries for updating family history questions and answers

    :param project_id: Name of the project
    :param dataset_id: Name of the dataset where the queries should be run

    :return: list of query dicts
    """
    query_list = []

    query = dict()
    query[cdr_consts.QUERY] = UPDATE_FAMILY_HISTORY_QUERY.format(
        dataset_id=dataset_id, project_id=project_id)
    query_list.append(query)

    return query_list


if __name__ == '__main__':
    import cdr_cleaner.args_parser as parser
    import cdr_cleaner.clean_cdr_engine as clean_engine

    ARGS = parser.parse_args()

    if ARGS.list_queries:
        clean_engine.add_console_logging()
        query_list = clean_engine.get_query_list(
            ARGS.project_id, ARGS.dataset_id,
            [(get_update_family_history_qa_queries,)])
        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(ARGS.project_id, ARGS.dataset_id,
                                   [(get_update_family_history_qa_queries,)])
