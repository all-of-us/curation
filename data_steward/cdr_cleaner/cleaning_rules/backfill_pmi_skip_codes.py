import logging

import constants.bq_utils as bq_consts
import constants.cdr_cleaner.clean_cdr as cdr_consts

LOGGER = logging.getLogger(__name__)

OBSERVATION_TABLE_NAME = 'observation'

PMI_SKIP_FIX_QUERY = """
    SELECT 
       coalesce(obs.observation_id,
         ques.observation_id) AS observation_id,
       coalesce(obs.person_id,
         ques.person_id) AS person_id,
       coalesce(obs.observation_concept_id,
         ques.observation_concept_id) AS observation_concept_id,
       coalesce(obs.observation_date,
         ques.default_observation_date) AS observation_date,
       coalesce(obs.observation_datetime,
         ques.default_observation_datetime) AS observation_datetime,
       coalesce(obs.observation_type_concept_id,
         ques.observation_type_concept_id) AS observation_type_concept_id,
       value_as_number,
       value_as_string,
       coalesce(obs.value_as_concept_id,
         903096) AS value_as_concept_id,
       coalesce(obs.qualifier_concept_id,
         0) AS qualifier_concept_id,
       coalesce(obs.unit_concept_id,
         0) AS unit_concept_id,
       provider_id,
       visit_occurrence_id,
       coalesce(obs.observation_source_value,
         ques.observation_source_value) AS observation_source_value,
       coalesce(obs.observation_source_concept_id,
         ques.observation_source_concept_id) AS observation_source_concept_id,
       unit_source_value,
       qualifier_source_value,
       coalesce(obs.value_source_concept_id,
         903096) AS value_source_concept_id,
     case 
     when obs.value_source_concept_id = 903096 Then 'PMI_Skip' 
     when value_source_concept_id = 903096 Then 'PMI_Skip' 
     else obs.value_source_value 
     end as value_source_value, 
       questionnaire_response_id 
     FROM
       `{project}.{dataset}.observation` AS obs
      FULL OUTER JOIN (
       WITH
         per AS (
         SELECT
           DISTINCT obs.person_id,
           per.gender_concept_id
         FROM
           `{project}.{dataset}.observation` obs
         JOIN
           `{project}.{dataset}.person` AS per
         ON
           obs.person_id = per.person_id
         WHERE
           observation_source_concept_id IN (1586135, 1586140, 1585838, 1585899, 1585940, 1585892, 1585889,
          1585890, 1585386, 1585389, 1585952, 1585375, 1585370, 1585879, 1585886, 1585857, 1586166, 1586174,
          1586182, 1586190, 1586198, 1585636, 1585766, 1585772, 1585778, 1585711, 1585717, 1585723, 1585729,
          1585735, 1585741, 1585747, 1585748, 1585754, 1585760, 1585803, 1585815, 1585784)
          ),
         obs AS (
         SELECT
           DISTINCT observation_concept_id,
           observation_source_concept_id,
           observation_source_value,
           observation_type_concept_id
         FROM
           `{project}.{dataset}.observation`
         WHERE
           observation_source_concept_id IN (1586135, 1586140, 1585838, 1585899, 1585940, 1585892, 1585889,
          1585890, 1585386, 1585389, 1585952, 1585375, 1585370, 1585879, 1585886, 1585857, 1586166, 1586174,
          1586182, 1586190, 1586198, 1585636, 1585766, 1585772, 1585778, 1585711, 1585717, 1585723, 1585729,
          1585735, 1585741, 1585747, 1585748, 1585754, 1585760, 1585803, 1585815, 1585784)
          ),
         dte AS (
         SELECT
           person_id,
           MAX(observation_date) AS default_observation_date,
           MAX(observation_datetime) AS default_observation_datetime
         FROM
           `{project}.{dataset}.observation`
         WHERE
           observation_source_concept_id IN (1586135, 1586140, 1585838, 1585899, 1585940, 1585892, 1585889,
          1585890, 1585386, 1585389, 1585952, 1585375, 1585370, 1585879, 1585886, 1585857, 1586166, 1586174,
          1586182, 1586190, 1586198, 1585636, 1585766, 1585772, 1585778, 1585711, 1585717, 1585723, 1585729,
          1585735, 1585741, 1585747, 1585748, 1585754, 1585760, 1585803, 1585815, 1585784)
         GROUP BY
           person_id)
       SELECT
         ROW_NUMBER() OVER() + 1000000000000 AS observation_id,
         cartesian.*,
         dte.default_observation_date,
         dte.default_observation_datetime
       FROM (
         SELECT
           per.person_id,
           observation_concept_id,
           observation_source_concept_id,
           observation_source_value,
           observation_type_concept_id
         FROM
           per,
           obs
         WHERE
           (observation_source_concept_id != 1585784)
           OR (per.gender_concept_id = 8532
             AND observation_source_concept_id = 1585784) 
           ) cartesian
       JOIN
         dte
       ON
         cartesian.person_id = dte.person_id
       ORDER BY
         cartesian.person_id ) AS ques
      ON
       obs.person_id = ques.person_id
       AND obs.observation_concept_id = ques.observation_concept_id
"""


def get_run_pmi_fix_queries(project_id, dataset_id):
    """

    runs the query which adds skipped rows in survey before 2019-04-10 as PMI_Skip

    :param project_id: Name of the project
    :param dataset_id: Name of the dataset where the queries should be run

    :return:
    """
    queries_list = []

    query = dict()
    query[cdr_consts.QUERY] = PMI_SKIP_FIX_QUERY.format(
        dataset=dataset_id,
        project=project_id,
    )
    query[cdr_consts.DESTINATION_TABLE] = OBSERVATION_TABLE_NAME
    query[cdr_consts.DISPOSITION] = bq_consts.WRITE_TRUNCATE
    query[cdr_consts.DESTINATION_DATASET] = dataset_id
    queries_list.append(query)

    return queries_list


if __name__ == '__main__':
    import cdr_cleaner.args_parser as parser
    import cdr_cleaner.clean_cdr_engine as clean_engine

    ARGS = parser.parse_args()
    if ARGS.list_queries:
        clean_engine.add_console_logging()
        query_list = clean_engine.get_query_list(ARGS.project_id,
                                                 ARGS.dataset_id,
                                                 [(get_run_pmi_fix_queries,)])
        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(ARGS.project_id, ARGS.dataset_id,
                                   [(get_run_pmi_fix_queries,)])
