"""
Several answers to smoking questions were incorrectly coded as questions
This rule generates corrected rows and deletes incorrect rows
"""

import csv

import constants.bq_utils as bq_consts
from constants.cdr_cleaner import clean_cdr as cdr_consts


SANDBOX_CREATE_QUERY = """
CREATE TABLE `{project_id}.{sandbox_dataset_id}.{temp_new_smoking_rows}`
AS
SELECT
    observation_id,
    person_id,
    observation_concept_id,
    observation_date,
    observation_datetime,
    observation_type_concept_id,
    value_as_number,
    value_as_string,
    value_as_concept_id,
    qualifier_concept_id,
    unit_concept_id,
    provider_id,
    visit_occurrence_id,
    observation_source_value,
    observation_source_concept_id,
    unit_source_value,
    qualifier_source_value,
    value_source_concept_id,
    value_source_value,
    questionnaire_response_id
FROM 
(SELECT
    observation_id,
    person_id,
    new_observation_concept_id as observation_concept_id, 
    observation_date,
    observation_datetime,
    observation_type_concept_id,
    value_as_number,
    value_as_string,
    new_value_as_concept_id as value_as_concept_id,
    qualifier_concept_id,
    unit_concept_id,
    provider_id,
    visit_occurrence_id,
    observation_source_value,
    new_observation_source_concept_id as observation_source_concept_id,
    unit_source_value,
    qualifier_source_value,
    new_value_source_concept_id as value_source_concept_id,
    value_source_value,
    questionnaire_response_id, 
    ROW_NUMBER() OVER(PARTITION BY person_id, new_observation_source_concept_id ORDER BY rank ASC) AS this_row
FROM
    `{project_id}.{sandbox_dataset_id}.{smoking_lookup_table}`
JOIN `{project_id}.{combined_dataset_id}.observation` 
    USING (observation_source_concept_id, value_as_concept_id)
)
WHERE this_row=1
"""

DELETE_INCORRECT_RECORDS = """
DELETE
FROM `{project_id}.{combined_dataset_id}.observation`
WHERE observation_source_concept_id IN
(SELECT
  observation_source_concept_id
FROM `{project_id}.{sandbox_dataset_id}.{smoking_lookup_table}`
)
"""

INSERT_CORRECTED_RECORDS = """
INSERT INTO `{project_id}.{combined_dataset_id}.observation` 
    (observation_id,
    person_id,
    observation_concept_id,
    observation_date,
    observation_datetime,
    observation_type_concept_id,
    value_as_number,
    value_as_string,
    value_as_concept_id,
    qualifier_concept_id,
    unit_concept_id,
    provider_id,
    visit_occurrence_id,
    observation_source_value,
    observation_source_concept_id,
    unit_source_value,
    qualifier_source_value,
    value_source_concept_id,
    value_source_value,
    questionnaire_response_id)
SELECT
    *
FROM `{project_id}.{sandbox_dataset_id}.{temp_new_smoking_rows}`
"""


def get_queries_clean_smoking(project_id, dataset_id):
    """
    Queries to run for deleting incorrect smoking rows and inserting corrected rows
    :param project_id: project id associated with the dataset to run the queries on
    :param dataset_id: dataset id to run the queries on
    :return: list of query dicts
    """
    queries = []
    return queries


if __name__ == '__main__':
    import cdr_cleaner.args_parser as parser
    import cdr_cleaner.clean_cdr_engine as clean_engine

    ARGS = parser.parse_args()

    clean_engine.add_console_logging(ARGS.console_log)
    query_list = get_queries_clean_smoking(ARGS.project_id, ARGS.dataset_id)
    clean_engine.clean_dataset(ARGS.project_id, ARGS.dataset_id, query_list)
