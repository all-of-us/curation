"""
For all answers for the survey question (43528428) and given pids,
    1. Mark answers as invalid for all participants
    2. Use the second survey (1384450) to generate valid answers for a subset of pids who took the second survey
"""

import csv

import constants.bq_utils as bq_consts
from constants.cdr_cleaner import clean_cdr as cdr_consts


ORIGINAL_OBSERVATION_SOURCE_CONCEPT_ID = 43528428
HCAU_OBSERVATION_SOURCE_CONCEPT_ID = 1384450
INVALID_CONCEPT_ID = 46237613


SANDBOX_CREATE_QUERY = """
CREATE TABLE `{project_id}.{sandbox_dataset_id}.{temp_table_id}`
AS
SELECT
  *
FROM
(SELECT 
  ob.observation_id, 
  ob.person_id, 
  ob.observation_concept_id, 
  ob.observation_date, 
  ob.observation_datetime, 
  ob.observation_type_concept_id, 
  ob.value_as_number, 
  new_value_source_value AS value_as_string, 
  new_value_as_concept_id AS value_as_concept_id, 
  ob.qualifier_concept_id, 
  ob.unit_concept_id, 
  ob.provider_id, 
  ob.visit_occurrence_id, 
  'HealthInsurance_InsuranceTypeUpdate' AS observation_source_value, 
  {ORIGINAL_OBSERVATION_SOURCE_CONCEPT_ID} AS observation_source_concept_id, 
  ob.unit_source_value, 
  ob.qualifier_source_value, 
  new_value_source_concept_id AS value_source_concept_id, 
  new_value_source_value AS value_source_value, 
  ob.questionnaire_response_id
FROM `{project_id}.{combined_dataset_id}.observation` ob
JOIN (
  SELECT DISTINCT
    hcau_value_source_concept_id,
    source_c.concept_code AS new_value_source_value, 
    source_c.concept_id AS new_value_source_concept_id, 
    FIRST_VALUE(standard_c.concept_id) OVER (PARTITION BY source_c.concept_id ORDER BY c_r.relationship_id DESC)
    AS new_value_as_concept_id
  FROM 
    `{project_id}.{sandbox_dataset_id}.{hcau_answer_map_table}`
  JOIN `{project_id}.{combined_dataset_id}.concept` source_c ON (basics_value_source_concept_id=source_c.concept_id)
  JOIN `{project_id}.{combined_dataset_id}.concept_relationship` c_r ON (source_c.concept_id=c_r.concept_id_1)
  JOIN `{project_id}.{combined_dataset_id}.concept` standard_c ON (standard_c.concept_id=c_r.concept_id_2)
  WHERE source_c.vocabulary_id='PPI' 
  AND c_r.relationship_id LIKE 'Maps to%'  --prefers the 'maps to value', but will take 'maps to' if necessary
)
ON ob.value_source_concept_id = hcau_value_source_concept_id 
WHERE observation_source_concept_id IN ({HCAU_OBSERVATION_SOURCE_CONCEPT_ID})
AND person_id IN ({pids})
)
"""


UPDATE_INVALID_QUERY = """
UPDATE 
 `{project_id}.{combined_dataset_id}.observation` ob
SET
  ob.value_as_concept_id = c.concept_id,
  ob.value_as_string = c.concept_code,
  ob.value_source_concept_id = c.concept_id,
  ob.value_source_value = c.concept_code
FROM `{project_id}.{combined_dataset_id}.concept` c
WHERE ob.observation_source_concept_id IN ({ORIGINAL_OBSERVATION_SOURCE_CONCEPT_ID})
AND c.concept_id = {INVALID_CONCEPT_ID}
AND ob.person_id IN ({pids})
"""


DELETE_ORIGINAL_FOR_HCAU_PARTICIPANTS = """
DELETE
FROM `{project_id}.{combined_dataset_id}.observation` ob
WHERE ob.observation_source_concept_id IN ({ORIGINAL_OBSERVATION_SOURCE_CONCEPT_ID})
AND ob.person_id IN 
(SELECT DISTINCT
  person_id
FROM `{project_id}.{sandbox_dataset_id}.{temp_table_id}`)
"""


INSERT_ANSWERS_FOR_HCAU_PARTICIPANTS = """
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
FROM `{project_id}.{sandbox_dataset_id}.{temp_table_id}`
"""


def get_queries_health_insurance(project_id, dataset_id, file_path):
    """
    Queries to run for updating health insurance information
    :param project_id: project id associated with the dataset to run the queries on
    :param dataset_id: dataset id to run the queries on
    :param file_path: path to file containing the relevant pids
    :return: list of query dicts
    """
    queries = []
    return queries


def parse_args():
    """
    Add file_path to the default cdr_cleaner.args_parser argument list

    :return: an expanded argument list object
    """
    import cdr_cleaner.args_parser as parser
    help_text = 'path to csv file (with header row) containing pids whose observation records are to be removed'
    additional_argument_1 = {parser.SHORT_ARGUMENT: '-f',
                             parser.LONG_ARGUMENT: '--file_path',
                             parser.ACTION: 'store',
                             parser.DEST: 'file_path',
                             parser.HELP: help_text,
                             parser.REQUIRED: True}

    args = parser.default_parse_args([additional_argument_1])
    return args


if __name__ == '__main__':
    import cdr_cleaner.clean_cdr_engine as clean_engine

    ARGS = parse_args()

    clean_engine.add_console_logging(ARGS.console_log)
    query_list = get_queries_health_insurance(ARGS.project_id, ARGS.dataset_id, ARGS.file_path)
    clean_engine.clean_dataset(ARGS.project_id, ARGS.dataset_id, query_list)
