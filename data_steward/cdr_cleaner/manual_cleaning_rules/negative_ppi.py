"""
Update answers where the participant skipped answering but the answer was registered as -1
"""
import logging

from constants.cdr_cleaner import clean_cdr as cdr_consts
import constants.bq_utils as bq_consts

LOGGER = logging.getLogger(__name__)
CLEANING_RULE_NAME = 'update_ppi_negative_pain_level'

SELECT_NEGATIVE_PPI_QUERY = """
SELECT
  *
FROM
  `{project_id}.{dataset_id}.observation`
WHERE value_as_number = -1
AND observation_source_concept_id = 1585747
"""

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


def get_update_ppi_queries(project_id, dataset_id, sandbox_dataset_id):
    """
    Generate the update_query that updates negative ppi answers to pmi_skip where obs_src_concept_id is 1585747

    :param project_id: the project_id in which the update_query is run
    :param dataset_id: the dataset_id in which the update_query is run
    :param sandbox_dataset_id: the dataset_id in which the delete_query is run
    :return: a list of update_query dicts for updating records
    """
    queries = []

    select_query = dict()
    select_query[cdr_consts.QUERY] = SELECT_NEGATIVE_PPI_QUERY.format(
        project_id=project_id, dataset_id=dataset_id)
    select_query[cdr_consts.DESTINATION_TABLE] = CLEANING_RULE_NAME
    select_query[cdr_consts.DISPOSITION] = bq_consts.WRITE_TRUNCATE
    select_query[cdr_consts.DESTINATION_DATASET] = sandbox_dataset_id
    queries.append(select_query)

    update_query = dict()
    update_query[cdr_consts.QUERY] = UPDATE_NEGATIVE_PPI_QUERY.format(
        project_id=project_id, dataset_id=dataset_id)
    queries.append(update_query)

    return queries


if __name__ == '__main__':
    import cdr_cleaner.args_parser as parser
    import cdr_cleaner.clean_cdr_engine as clean_engine

    ARGS = parser.get_argument_parser().parse_args()

    if ARGS.list_queries:
        clean_engine.add_console_logging()
        query_list = clean_engine.get_query_list(ARGS.project_id,
                                                 ARGS.dataset_id,
                                                 ARGS.sandbox_dataset_id,
                                                 [(get_update_ppi_queries,)])
        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(ARGS.project_id, ARGS.dataset_id,
                                   ARGS.sandbox_dataset_id,
                                   [(get_update_ppi_queries,)])
