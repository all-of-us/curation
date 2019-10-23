import constants.cdr_cleaner.clean_cdr as cdr_consts

CLEAN_PPI_NUMERIC_FIELDS = """
UPDATE
  {project}.{dataset}.observation u1
SET
  u1.value_as_number = NULL,
  u1.value_as_concept_id = 2000000010
FROM
  (
  SELECT
  *
FROM
  {project}.{dataset}.observation
WHERE
  observation_concept_id = 1585889 AND (value_as_number < 0 OR value_as_number > 20)

UNION ALL

SELECT
  *
FROM
  {project}.{dataset}.observation
WHERE
  observation_concept_id = 1585890 AND (value_as_number < 0 OR value_as_number > 20)

UNION ALL

SELECT
  *
FROM
  {project}.{dataset}.observation
WHERE
  observation_concept_id = 1585795 AND (value_as_number < 0 OR value_as_number > 99)

UNION ALL

SELECT
  *
FROM
  {project}.{dataset}.observation
WHERE
  observation_concept_id = 1585802 AND (value_as_number < 0 OR value_as_number > 99)

UNION ALL

SELECT
  *
FROM
  {project}.{dataset}.observation
WHERE
  observation_concept_id = 1585820 AND (value_as_number < 0 OR value_as_number > 255)

UNION ALL

SELECT
  *
FROM
  {project}.{dataset}.observation
WHERE
  observation_concept_id = 1585864 AND (value_as_number < 0 OR value_as_number > 99)

UNION ALL

SELECT
  *
FROM
  {project}.{dataset}.observation
WHERE
  observation_concept_id = 1585870 AND (value_as_number < 0 OR value_as_number > 99)

UNION ALL

SELECT
  *
FROM
  {project}.{dataset}.observation
WHERE
  observation_concept_id = 1585873 AND (value_as_number < 0 OR value_as_number > 99)

UNION ALL

SELECT
  *
FROM
  {project}.{dataset}.observation
WHERE
  observation_concept_id = 1586159 AND (value_as_number < 0 OR value_as_number > 99)

UNION ALL

SELECT
  *
FROM
  {project}.{dataset}.observation
WHERE
  observation_concept_id = 1586162 AND (value_as_number < 0 OR value_as_number > 99) ) a
WHERE
  u1.observation_id = a.observation_id
"""


def get_clean_ppi_num_fields_using_parameters_queries(project_id, dataset_id):
    """
    runs the query which updates the ppi numeric fields in observation table based on the 
    upper and lower bounds specified.
    :param project_id: Name of the project
    :param dataset_id: Name of the dataset where the queries should be run
    :return:
    """
    queries_list = []

    query = dict()
    query[cdr_consts.QUERY] = CLEAN_PPI_NUMERIC_FIELDS.format(dataset=dataset_id,
                                                              project=project_id,
                                                              )
    queries_list.append(query)

    return queries_list


if __name__ == '__main__':
    import cdr_cleaner.args_parser as parser
    import cdr_cleaner.clean_cdr_engine as clean_engine

    ARGS = parser.parse_args()
    clean_engine.add_console_logging(ARGS.console_log)
    query_list = get_clean_ppi_num_fields_using_parameters_queries(ARGS.project_id, ARGS.dataset_id)
    clean_engine.clean_dataset(ARGS.project_id, ARGS.dataset_id, query_list)
