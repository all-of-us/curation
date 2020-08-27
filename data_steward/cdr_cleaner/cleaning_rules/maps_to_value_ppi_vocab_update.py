import logging

import constants.cdr_cleaner.clean_cdr as cdr_consts

LOGGER = logging.getLogger(__name__)

UPDATE_PPI_QUERY = """
UPDATE
  `{project}.{dataset}.observation` a
SET
  a.value_as_concept_id = b.concept_id
FROM (
  SELECT
    *
  FROM (
    SELECT
      c2.concept_name,
      c2.concept_id,
      o.*,
      RANK() OVER (PARTITION BY o.observation_id, o.value_source_concept_id ORDER BY c2.concept_id ASC) AS rank
    FROM
      `{project}.{dataset}.observation` o
    JOIN
      `{project}.{dataset}.concept` c
    ON
      o.value_source_concept_id = c.concept_id
    JOIN
      `{project}.{dataset}.concept_relationship` cr
    ON
      c.concept_id = cr.concept_id_1
      AND cr.relationship_id = 'Maps to value'
    JOIN
      `{project}.{dataset}.concept` c2
    ON
      c2.concept_id = cr.concept_id_2
    WHERE
      o.observation_concept_id = o.value_as_concept_id
      AND o.observation_concept_id != 0 )
  WHERE
    rank=1 ) AS b
WHERE
  a.observation_id = b.observation_id
"""


def get_maps_to_value_ppi_vocab_update_queries(project_id, dataset_id):
    """
    runs the query which updates the ppi vocabulary in observation table

    :param project_id: Name of the project
    :param dataset_id: Name of the dataset where the queries should be run
    :return:
    """
    queries_list = []

    query = dict()
    query[cdr_consts.QUERY] = UPDATE_PPI_QUERY.format(dataset=dataset_id,
                                                      project=project_id)
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
            [(get_maps_to_value_ppi_vocab_update_queries,)])
        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(
            ARGS.project_id, ARGS.dataset_id,
            [(get_maps_to_value_ppi_vocab_update_queries,)])
