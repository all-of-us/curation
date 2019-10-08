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
timestamp but with different answers. """

from constants.cdr_cleaner import clean_cdr as cdr_consts

REMOVE_DUPLICATE_TEMPLATE = """
DELETE
FROM
  `{project_id}.{dataset_id}.observation`
WHERE
  observation_id IN 
  (
    SELECT
      observation_id
    FROM (
      SELECT
        *,
        DENSE_RANK() OVER(
              PARTITION BY person_id, 
              observation_source_concept_id, 
              observation_source_value 
              ORDER BY max_observation_datetime DESC, questionnaire_response_id DESC) AS rank_order
      FROM (
        SELECT
          observation_id,
          person_id,
          observation_source_concept_id,
          observation_source_value,
          questionnaire_response_id,
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
)
"""


def get_remove_duplicate_set_of_responses_to_same_questions_queries(project_id, dataset_id):
    """
    Generate the query that remove the duplicate sets of responses to the same questions.

    :param project_id: the project_id in which the query is run
    :param dataset_id: the dataset_id in which the query is run
    :return: a list of query dicts for rerouting the records to the corresponding destination table
    """

    queries = []

    query = dict()
    query[cdr_consts.QUERY] = REMOVE_DUPLICATE_TEMPLATE.format(project_id=project_id, dataset_id=dataset_id)
    query[cdr_consts.BATCH] = True
    queries.append(query)

    return queries


if __name__ == '__main__':
    import cdr_cleaner.args_parser as parser
    import cdr_cleaner.clean_cdr_engine as clean_engine

    ARGS = parser.parse_args()

    clean_engine.add_console_logging(ARGS.console_log)
    query_list = get_remove_duplicate_set_of_responses_to_same_questions_queries(ARGS.project_id, ARGS.dataset_id)
    clean_engine.clean_dataset(ARGS.project_id, ARGS.dataset_id, query_list)
