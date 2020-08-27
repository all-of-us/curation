import logging

import constants.cdr_cleaner.clean_cdr as cdr_consts

LOGGER = logging.getLogger(__name__)

GENERALIZED_CONCEPT_ID_QUERY_TEMPLATE = '''
UPDATE
  `{project_id}.{dataset_id}.observation`
SET
  value_as_concept_id = {generalized_gender_concept_id},
  value_source_concept_id = {generalized_gender_concept_id}
WHERE
  observation_source_concept_id = 1585838 -- the concept for gender identity
  AND value_source_concept_id = {gender_value_source_concept_id}
  AND person_id IN (
  SELECT
    person_id
  FROM
    `{project_id}.{dataset_id}.observation`
  WHERE
    observation_source_concept_id = 1585845 -- the concept for biological sex at birth
      AND value_source_concept_id = {biological_sex_birth_concept_id})
'''

GENERALIZE_GENDER_CONCEPT_ID = 2000000002

WOMAN_CONCEPT_ID = 1585840

MAN_CONCEPT_ID = 1585839

SEX_AT_BIRTH_MALE_CONCEPT_ID = 1585846

SEX_AT_BIRTH_FEMALE_CONCEPT_ID = 1585847


def parse_query_for_updating_woman_to_generalized_concept_id(
    project_id, dataset_id):
    """
    This function returns an update query to update the gender to the generalized gender concept_id for the cases
    where the biological sex is reported as male and gender is reported as woman.
    :param project_id: the project id
    :param dataset_id: the dataset id
    :return:an update query to update the gender from woman to the generalized concept id
    """
    return GENERALIZED_CONCEPT_ID_QUERY_TEMPLATE.format(
        project_id=project_id,
        dataset_id=dataset_id,
        gender_value_source_concept_id=WOMAN_CONCEPT_ID,
        biological_sex_birth_concept_id=SEX_AT_BIRTH_MALE_CONCEPT_ID,
        generalized_gender_concept_id=GENERALIZE_GENDER_CONCEPT_ID)


def parse_query_for_updating_man_to_generalized_concept_id(
    project_id, dataset_id):
    """
    This function returns an update query to update the gender to the generalized gender concept_id for the cases
    where the biological sex is reported as female and gender is reported as man.
    :param project_id: the project id
    :param dataset_id: the dataset id
    :return:an update query to update the gender from man to the generalized concept id
    """
    return GENERALIZED_CONCEPT_ID_QUERY_TEMPLATE.format(
        project_id=project_id,
        dataset_id=dataset_id,
        gender_value_source_concept_id=MAN_CONCEPT_ID,
        biological_sex_birth_concept_id=SEX_AT_BIRTH_FEMALE_CONCEPT_ID,
        generalized_gender_concept_id=GENERALIZE_GENDER_CONCEPT_ID)


def get_generalized_concept_id_queries(project_id, dataset_id):
    """
    This function generates a list of query dicts for updating the records for which we need to the generalize gender
    in both of value_as_concept_id and value_source_concept_id
    :param project_id: the project_id in which the query is run
    :param dataset_id: the dataset_id in which the query is run
    :return: a list of query dicts for updating the gender concept ids
    """

    queries = []

    query = dict()
    query[cdr_consts.
          QUERY] = parse_query_for_updating_woman_to_generalized_concept_id(
              project_id, dataset_id)
    query[cdr_consts.BATCH] = True
    queries.append(query)

    query = dict()
    query[cdr_consts.
          QUERY] = parse_query_for_updating_man_to_generalized_concept_id(
              project_id, dataset_id)
    query[cdr_consts.BATCH] = True
    queries.append(query)

    return queries


if __name__ == '__main__':
    import cdr_cleaner.clean_cdr_engine as clean_engine
    import cdr_cleaner.args_parser as parser

    ARGS = parser.parse_args()

    if ARGS.list_queries:
        clean_engine.add_console_logging()
        query_list = clean_engine.get_query_list(
            ARGS.project_id, ARGS.dataset_id,
            [(get_generalized_concept_id_queries,)])
        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(ARGS.project_id, ARGS.dataset_id,
                                   [(get_generalized_concept_id_queries,)])
