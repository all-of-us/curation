from constants.cdr_cleaner import clean_cdr as cdr_consts
import constants.bq_utils as bq_consts

OBSERVATION_TABLE = 'observation'

REMOVE_DUPLICATE_TEMPLATE = """
SELECT
  o.*
FROM
  `{project_id}.{dataset_id}.observation` AS o
JOIN 
(
  SELECT
    observation_id
  FROM (
    SELECT
      DENSE_RANK() OVER(PARTITION BY person_id, 
        observation_source_concept_id, 
        observation_source_value, 
        value_source_concept_id, 
        CAST(value_as_number AS STRING)
      ORDER BY
        observation_datetime DESC,
        observation_id DESC) AS rank_order,
      observation_id
    FROM
      `{project_id}.{dataset_id}.observation` ) o
  WHERE
    o.rank_order = 1
) unique_observation_ids 
ON o.observation_id = unique_observation_ids.observation_id
"""


def get_remove_duplicate_queries(project_id, dataset_id):
    """
    Generate the query that remove the duplicate rows in the observation table

    :param project_id: the project_id in which the query is run
    :param dataset_id: the dataset_id in which the query is run
    :return: a list of query dicts for rerouting the records to the corresponding destination table
    """

    queries = []

    query = dict()
    query[cdr_consts.QUERY] = REMOVE_DUPLICATE_TEMPLATE.format(project_id=project_id, dataset_id=dataset_id)
    query[cdr_consts.DESTINATION_TABLE] = OBSERVATION_TABLE
    query[cdr_consts.DISPOSITION] = bq_consts.WRITE_TRUNCATE
    query[cdr_consts.DESTINATION_DATASET] = dataset_id
    queries.append(query)

    return queries


if __name__ == '__main__':
    import cdr_cleaner.args_parser as parser
    import cdr_cleaner.clean_cdr_engine as clean_engine

    ARGS = parser.parse_args()

    clean_engine.add_console_logging(ARGS.console_log)
    query_list = get_remove_duplicate_queries(ARGS.project_id, ARGS.dataset_id)
    clean_engine.clean_dataset(ARGS.project_id, ARGS.snapshot_dataset_id, query_list)
