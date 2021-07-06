"""
Background

DC-685 - removing participants who identified as AI/AN in PPI.

There is a requirement from the NIH that participants who identified as AI/AN in PPI cannot have ANY of their data
included in released datasets for the Data Browser or Workbench/Cohort Builder. These participants have had their data
retracted non-programmatically previously. However, since this is expected to be a long-term policy, this should be a
cleaning rule applied to combined so this can be automated.
We will remove ALL DATA in combined associated with PIDs who have rows with the following in the observation table:
observation_source_concept_id = 1586140
value_source_concept_id = 1586141
"""
import logging

# Project imports
import utils.bq
from cdr_cleaner.cleaning_rules import sandbox_and_remove_pids

LOGGER = logging.getLogger(__name__)

TICKET_NUMBER = 'DC685'

PIDS_QUERY = """
SELECT person_id
FROM `{project}.{dataset}.observation`
WHERE observation_source_concept_id IN (1586140) AND value_source_concept_id IN (1586141)
"""


def get_pids_list(project_id, dataset_id, pids_query):
    """
    takes a query based on the cleaning rule and returns a list of person_ids
    :param project_id: bq name of project_id
    :param dataset_id: bq name of dataset_id
    :param pids_query: query that grabs all person_ids based on the cleaning rule
    :return: list of person_ids
    """

    pid_list = utils.bq.query(
        pids_query.format(project=project_id,
                          dataset=dataset_id))['person_id'].tolist()

    return pid_list


def get_queries(project_id, dataset_id, sandbox_dataset_id):
    """
    return a list of queries to remove AIAN participant rows
    :param project_id: Name of the project
    :param dataset_id: Name of the dataset where the queries should be run
    :param sandbox_dataset_id: Identifies the sandbox dataset to store rows
    :return: A list of string queries that can be executed to delete AIAN participants and
    all corresponding rows from the dataset with the associated PID.
    """

    queries_list = []

    queries_list.extend(
        sandbox_and_remove_pids.get_sandbox_queries(
            project_id, dataset_id,
            get_pids_list(project_id, dataset_id, PIDS_QUERY), TICKET_NUMBER,
            sandbox_dataset_id))
    queries_list.extend(
        sandbox_and_remove_pids.get_remove_pids_queries(
            project_id, dataset_id,
            get_pids_list(project_id, dataset_id, PIDS_QUERY)))
    return queries_list


if __name__ == '__main__':
    import cdr_cleaner.args_parser as parser
    import cdr_cleaner.clean_cdr_engine as clean_engine

    ARGS = parser.parse_args()

    if ARGS.list_queries:
        clean_engine.add_console_logging()
        query_list = clean_engine.get_query_list(ARGS.project_id,
                                                 ARGS.dataset_id,
                                                 ARGS.sandbox_dataset_id,
                                                 [(get_queries,)])
        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(ARGS.project_id, ARGS.dataset_id,
                                   ARGS.sandbox_dataset_id, [(get_queries,)])
