"""

Original Issues: DC-2617

"""

import logging

# Project imports
import constants.cdr_cleaner.clean_cdr as cdr_consts
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule, query_spec_list
from cdr_cleaner.cleaning_rules.set_unmapped_question_answer_survey_concepts import (
    SetConceptIdsForSurveyQuestionsAnswers)
from constants.bq_utils import WRITE_TRUNCATE
from common import OBSERVATION, JINJA_ENV

LOGGER = logging.getLogger(__name__)

JIRA_ISSUE_NUMBERS = ['DC2617']

# update this logic so that it actually has the combination of the new ids, not the count
MAPPING_QUERY_1 = JINJA_ENV.from_string("""
CREATE OR REPLACE TABLE `{{project}}.{{sandbox_dataset}}.{{sandbox_table}}_mapping` AS
WITH non_standard_concept AS (
    SELECT 
        o.value_source_value, 
        c1.concept_id, 
        cr.relationship_id, 
        COUNT(DISTINCT cr.concept_id_2) AS count_
    FROM `{{project}}.{{dataset}}.observation` o
    JOIN `{{project}}.{{dataset}}.concept` c1
    ON o.value_source_concept_id = c1.concept_id
    JOIN `{{project}}.{{dataset}}.concept_relationship` cr
    ON c1.concept_id = cr.concept_id_1
    JOIN `{{project}}.{{dataset}}.concept` c2
    ON o.value_source_concept_id = c2.concept_id
    WHERE c2.standard_concept IS NULL
    AND cr.relationship_id IN ('Maps to', 'Maps to value')
    GROUP BY 1, 2, 3
),
maps_to AS (
    SELECT * FROM non_standard_concept WHERE relationship_id = 'Maps to'
),
maps_to_value AS (
    SELECT * FROM non_standard_concept WHERE relationship_id = 'Maps to value'
)
SELECT 
    COALESCE(m1.concept_id, m2.concept_id) AS concept_id,
    COALESCE(m1.value_source_value, m2.value_source_value) AS value_source_value,
    COALESCE(m1.count_ * m2.count_, m1.count_, m2.count_) AS num_new_rows
FROM maps_to AS m1
FULL OUTER JOIN maps_to_value AS m2
ON m1.concept_id = m2.concept_id
""")

SANDBOX_QUERY = JINJA_ENV.from_string("""
CREATE OR REPLACE TABLE `{{project}}.{{sandbox_dataset}}.{{sandbox_table}}` AS
SELECT * FROM `{{project}}.{{dataset}}.observation` o
JOIN `{{project}}.{{dataset}}.concept` c1
ON o.value_source_concept_id = c1.concept_id
JOIN `{{project}}.{{dataset}}.concept` c2
ON o.value_source_value = c2.concept_id
JOIN `{{project}}.{{sandbox_dataset}}.concept_relationship` cr1
ON c2.concept_id = cr1.concept_id_1
WHERE c1.standard_concept IS NULL
AND cr1.relationship_id = 'Maps to value'
""")

DELETE_QUERY = JINJA_ENV.from_string("""
DELETE FROM `{{project}}.{{dataset}}.observation`
WHERE observation_id IN (
    SELECT observation_id FROM `{{project}}.{{sandbox_dataset}}.{{sandbox_table}}`
)
""")

INSERT_1_RECORD = JINJA_ENV.from_string("""
INSERT INTO `{{project}}.{{dataset}}.observation`                             
  ({{observation_fields}})
  SELECT 
    o.observation_id, --> make it a new id?
    o.person_id,
    cr1.concept_id_2 AS observation_concept_id,
    o.observation_date,
    o.observation_datetime,
    o.observation_type_concept_id,
    o.value_as_number,
    o.value_as_string,
    cr2.concept_id_2 AS value_as_concept_id,
    o.qualifier_concept_id,
    o.unit_concept_id,
    o.provider_id,
    o.visit_occurrence_id,
    o.visit_detail_id,
    o.observation_source_value,
    o.observation_source_concept_id,
    o.unit_source_value,
    o.qualifier_source_value,
    o.value_source_concept_id,
    o.value_source_value, 
    o.questionnaire_response_id
FROM `{{project}}.{{sandbox_dataset}}.{{sandbox_table}}` o
JOIN `{{project}}.{{sandbox_dataset}}.{{sandbox_table}}_mapping` m
ON o.value_source_concept_id = m.concept_id
JOIN `{{project}}.{{sandbox_dataset}}.concept` c1
ON o.value_source_value = c1.concept_code
JOIN `{{project}}.{{sandbox_dataset}}.concept_relationship` cr1
ON c1.concept_id = cr1.concept_id_1 AND cr1.relationship_id = 'Maps to'
JOIN `{{project}}.{{sandbox_dataset}}.concept_relationship` cr2
ON c1.concept_id = cr2.concept_id_1 AND cr2.relationship_id = 'Maps to value'
WHERE m.num_new_rows = 1
""")

# TODO kokokara dousuru?
INSERT_2_RECORDS_1 = JINJA_ENV.from_string("""
INSERT INTO `{{project}}.{{dataset}}.observation`                             
  ({{observation_fields}})
  SELECT 
    o.observation_id,
    o.person_id,
    cr1.concept_id_2 AS observation_concept_id,
    o.observation_date,
    o.observation_datetime,
    o.observation_type_concept_id,
    o.value_as_number,
    o.value_as_string,
    cr2.concept_id_2 AS value_as_concept_id,
    o.qualifier_concept_id,
    o.unit_concept_id,
    o.provider_id,
    o.visit_occurrence_id,
    o.visit_detail_id,
    o.observation_source_value,
    o.observation_source_concept_id,
    o.unit_source_value,
    o.qualifier_source_value,
    o.value_source_concept_id,
    o.value_source_value, 
    o.questionnaire_response_id
FROM `{{project}}.{{sandbox_dataset}}.{{sandbox_table}}` o
JOIN `{{project}}.{{sandbox_dataset}}.{{sandbox_table}}_mapping` m
ON o.value_source_concept_id = m.concept_id
JOIN `{{project}}.{{sandbox_dataset}}.concept` c1
ON o.value_source_value = c1.concept_code
JOIN `{{project}}.{{sandbox_dataset}}.concept_relationship` cr1
ON c1.concept_id = cr1.concept_id_1 AND cr1.relationship_id = 'Maps to'
JOIN `{{project}}.{{sandbox_dataset}}.concept_relationship` cr2
ON c1.concept_id = cr2.concept_id_1 AND cr2.relationship_id = 'Maps to value'
WHERE m.num_new_rows = 1
""")

INSERT_2_RECORDS_2 = JINJA_ENV("""
                            """)

INSERT_4_RECORDS = JINJA_ENV("""
                            """)


class UpdatePfhhConcepts(BaseCleaningRule):

    def __init__(self, project_id, dataset_id, sandbox_dataset_id):
        """
        Initialize the class with proper information.

        Set the issue numbers, description and affected datasets. As other tickets may affect
        this SQL, append them to the list of Jira Issues.
        DO NOT REMOVE ORIGINAL JIRA ISSUE NUMBERS!
        """
        desc = ('   ' '   ')

        super().__init__(issue_numbers=JIRA_ISSUE_NUMBERS,
                         description=desc,
                         affected_datasets=[cdr_consts.RDR],
                         affected_tables=[OBSERVATION],
                         project_id=project_id,
                         dataset_id=dataset_id,
                         sandbox_dataset_id=sandbox_dataset_id,
                         depends_on=[SetConceptIdsForSurveyQuestionsAnswers])

    def get_query_specs(self, *args, **keyword_args) -> query_spec_list:
        """
        Return a list of dictionary query specifications.

        :return:  A list of dictionaries. Each dictionary contains a single query
            and a specification for how to execute that query. The specifications
            are optional but the query is required.
        """
        sandbox_query_dict = {
            cdr_consts.QUERY:
                SANDBOX_FIX_UNMAPPED_SURVEY_ANSWERS_QUERY.render(
                    project=self.project_id,
                    sandbox_dataset=self.sandbox_dataset_id,
                    sandbox_table=self.sandbox_table_for(OBSERVATION),
                    dataset=self.dataset_id)
        }

        update_query_dict = {
            cdr_consts.QUERY:
                UPDATE_FIX_UNMAPPED_SURVEY_ANSWERS_QUERY.render(
                    project=self.project_id,
                    sandbox_dataset=self.sandbox_dataset_id,
                    sandbox_table=self.sandbox_table_for(OBSERVATION),
                    dataset=self.dataset_id),
            cdr_consts.DESTINATION_TABLE:
                OBSERVATION,
            cdr_consts.DESTINATION_DATASET:
                self.dataset_id,
            cdr_consts.DISPOSITION:
                WRITE_TRUNCATE
        }

        return [sandbox_query_dict, update_query_dict]

    def setup_rule(self, client, *args, **keyword_args):
        pass

    def setup_validation(self, client):
        """
        Run required steps for validation setup
        """
        raise NotImplementedError("Please fix me.")

    def validate_rule(self, client):
        """
        Validates the cleaning rule which deletes or updates the data from the tables
        """
        raise NotImplementedError("Please fix me.")

    def get_sandbox_tablenames(self):
        return [
            self.sandbox_table_for(affected_table)
            for affected_table in self._affected_tables
        ]


if __name__ == '__main__':
    import cdr_cleaner.args_parser as parser
    import cdr_cleaner.clean_cdr_engine as clean_engine

    ARGS = parser.parse_args()

    if ARGS.list_queries:
        clean_engine.add_console_logging()
        query_list = clean_engine.get_query_list(ARGS.project_id,
                                                 ARGS.dataset_id,
                                                 ARGS.sandbox_dataset_id,
                                                 [(UpdatePfhhConcepts,)])
        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(ARGS.project_id, ARGS.dataset_id,
                                   ARGS.sandbox_dataset_id,
                                   [(UpdatePfhhConcepts,)])
