"""In PPI surveys, the purpose of questionnaire_response_id is to group all responses from the same survey together.
Some PPI questions allowed participants to provide multiple answers, which be will connected via the same
questionnaire_response_id. However, a participant may submit the responses multiple times for the same questions,
therefore creating duplicates. We need to use the combination of person_id, observation_source_concept_id,
observation_source_value, and questionnaire_response_id to identify multiple sets of responses. We only want to keep
the most recent set of responses and remove previous sets of responses. When identifying the most recent responses,
we can't use questionnaire_response_id alone because a larger questionnaire_response_id doesn't mean it's created at
a later time therefore we need to create ranks (lowest rank = most recent responses) based on observation_date_time.
However, we also need to add questionnaire_response_id for assigning unique ranks to different sets of responses
because there are cases where the multiple sets of responses for the same question were submitted at exactly the same
timestamp but with different answers. In addition, we also need to check whether one of the duplicate responses is
PMI_Skip because there are cases where the most recent response is a skip and the legitimate response was submitted
earlier, we want to keep the actual response instead of PMI_Skip regardless of the timestamps of those responses."""

from constants.cdr_cleaner import clean_cdr as cdr_consts
import constants.bq_utils as bq_consts
import argparse

CLEANING_RULE_NAME = 'remove_multiple_responses'

SELECT_DUPLICATE_TEMPLATE = """
SELECT
  *
FROM
  `{project_id}.{dataset_id}.observation`
WHERE
  observation_id IN 
  (
    {duplicate_id_query}
  )
"""

REMOVE_DUPLICATE_TEMPLATE = """
DELETE
FROM
  `{project_id}.{dataset_id}.observation`
WHERE
  observation_id IN 
  (
    {duplicate_id_query}
  )
"""

IDENTIFY_DUPLICATE_ID_TEMPLATE = """
    SELECT
      observation_id
    FROM (
      SELECT
        *,
        DENSE_RANK() OVER(
              PARTITION BY person_id, 
              observation_source_concept_id, 
              observation_source_value 
              ORDER BY is_pmi_skip ASC, max_observation_datetime DESC, questionnaire_response_id DESC) AS rank_order
      FROM (
        SELECT
          observation_id,
          person_id,
          observation_source_concept_id,
          observation_source_value,
          questionnaire_response_id,
          IF (value_source_value = 'PMI_Skip', 1, 0) AS is_pmi_skip,
          MAX(observation_datetime) OVER(
              PARTITION BY person_id, 
              observation_source_concept_id, 
              observation_source_value, 
              questionnaire_response_id) AS max_observation_datetime
        FROM `{project_id}.{dataset_id}.observation` 
        WHERE observation_source_concept_id != 1586099  -- exclude EHRConsentPII_ConsentPermission
      ) o 
    ) o
    WHERE o.rank_order != 1
"""


def get_select_statement(project_id, dataset_id):
    duplicate_id_query = IDENTIFY_DUPLICATE_ID_TEMPLATE.format(project_id=project_id, dataset_id=dataset_id)
    return SELECT_DUPLICATE_TEMPLATE.format(project_id=project_id,
                                            dataset_id=dataset_id,
                                            duplicate_id_query=duplicate_id_query)


def get_delete_statement(project_id, dataset_id):
    duplicate_id_query = IDENTIFY_DUPLICATE_ID_TEMPLATE.format(project_id=project_id, dataset_id=dataset_id)
    return REMOVE_DUPLICATE_TEMPLATE.format(project_id=project_id,
                                            dataset_id=dataset_id,
                                            duplicate_id_query=duplicate_id_query)


def create_sandbox_table_name(dataset_id, cleaning_rule_name):
    """
    creates the sandbox table name
    :param dataset_id: Dataset id to which the cleaning rule is applied
    :param cleaning_rule_name: Name of the cleaning rule
    :return: the table name in the sandbox
    """
    sandbox_table_name = dataset_id + '_' + cleaning_rule_name
    return sandbox_table_name


def get_remove_duplicate_set_of_responses_to_same_questions_queries(project_id, dataset_id, sandbox_dataset_id):
    """
    Generate the delete_query that remove the duplicate sets of responses to the same questions.

    :param project_id: the project_id in which the delete_query is run
    :param dataset_id: the dataset_id in which the delete_query is run
    :param sandbox_dataset_id: the dataset_id in which the delete_query is run
    :return: a list of delete_query dicts for rerouting the records to the corresponding destination table
    """

    queries = []

    select_query = dict()
    select_query[cdr_consts.QUERY] = get_select_statement(project_id, dataset_id)
    select_query[cdr_consts.DESTINATION_TABLE] = create_sandbox_table_name(dataset_id, CLEANING_RULE_NAME)
    select_query[cdr_consts.DISPOSITION] = bq_consts.WRITE_TRUNCATE
    select_query[cdr_consts.DESTINATION_DATASET] = sandbox_dataset_id
    queries.append(select_query)

    delete_query = dict()
    delete_query[cdr_consts.QUERY] = get_delete_statement(project_id, dataset_id)
    delete_query[cdr_consts.BATCH] = True
    queries.append(delete_query)

    return queries


if __name__ == '__main__':
    import cdr_cleaner.clean_cdr_engine as clean_engine

    parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('-p', '--project_id',
                        action='store', dest='project_id',
                        help='Identifies the project to fix the data in.',
                        required=True)
    parser.add_argument('-d', '--dataset_id',
                        action='store', dest='dataset_id',
                        help='Identifies the dataset to apply the fix on.',
                        required=True)
    parser.add_argument('-s', '--sandbox_dataset_id',
                        action='store', dest='sandbox_dataset_id',
                        help='Identifies the sandbox dataset.',
                        required=True)

    ARGS = parser.parse_args()

    clean_engine.add_console_logging(ARGS.console_log)
    query_list = get_remove_duplicate_set_of_responses_to_same_questions_queries(ARGS.project_id,
                                                                                 ARGS.dataset_id,
                                                                                 ARGS.sandbox_dataset_id)
    clean_engine.clean_dataset(ARGS.project_id, ARGS.dataset_id, query_list)
