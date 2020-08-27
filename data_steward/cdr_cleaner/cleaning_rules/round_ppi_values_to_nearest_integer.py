"""
Numeric PPI free text questions should only have integer values, but there are some non-integer values present that
needs to be updated.
For the following observation_source_concept_ids, we have to make sure if the value_as_number field is an integer.
If it is not, It should be rounded to the nearest integer:

1585889
1585890
1585795
1585802
1585820
1585864
1585870
1585873
1586159
1586162

"""
import logging

import constants.cdr_cleaner.clean_cdr as cdr_consts

LOGGER = logging.getLogger(__name__)

ROUND_PPI_VALUES_QUERY = """
UPDATE
  `{project}.{dataset}.observation`
SET
  value_as_number = CAST(ROUND(value_as_number) AS INT64)
WHERE
  observation_source_concept_id IN (1585889,
    1585890,
    1585795,
    1585802,
    1585820,
    1585864,
    1585870,
    1585873,
    1586159,
    1586162)
  AND value_as_number IS NOT NULL
"""


def get_round_ppi_values_queries(project_id, dataset_id):
    """

    This function parser the query required to round the PPI numeric values to nearest integer

    :param project_id: Name of the project
    :param dataset_id: Name of the dataset where the queries should be run
    :return:
    """
    queries_list = []
    query = dict()

    query[cdr_consts.QUERY] = ROUND_PPI_VALUES_QUERY.format(dataset=dataset_id,
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
            ARGS.project_id, ARGS.dataset_id, [(get_round_ppi_values_queries,)])
        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(ARGS.project_id, ARGS.dataset_id,
                                   [(get_round_ppi_values_queries,)])
