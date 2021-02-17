import logging

import resources
from common import JINJA_ENV
import constants.cdr_cleaner.clean_cdr as cdr_consts
from cdr_cleaner.cleaning_rules.controlled_tier.concept_suppression import \
    AbstractBqLookupTableConceptSuppression

LOGGER = logging.getLogger(__name__)

ISSUE_NUMBERS = ['DC1367']

SUPPRESSION_RULE_CONCEPT_TABLE = 'motor_vehicle_accident_suppression_concept'

MOTOR_VEHICLE_ACCIDENT_CONCEPT_QUERY = JINJA_ENV.from_string("""
CREATE OR REPLACE TABLE {{project}}.{{dataset}}.{{concept_suppression_lookup}}
AS
WITH icd_vehicle_list AS (
  SELECT 
    *
  FROM `{{project}}.{{dataset}}.concept`
  WHERE
  # get all possible codes from E800 - E849
  REGEXP_CONTAINS(concept_code, r"^E8[0-4][0-9]")
  # cut out codes from E8000 and up
  AND NOT REGEXP_CONTAINS(concept_code, r"E8[0-4][0-9][\d]")
    AND vocabulary_id = 'ICD9CM'

  UNION ALL 

  SELECT 
    *
  FROM `{{project}}.{{dataset}}.concept`
  WHERE REGEXP_CONTAINS(concept_code, r"^V")
  # vocabulary identification is important
  AND REGEXP_CONTAINS(vocabulary_id, r"^ICD10")

  UNION ALL

  SELECT #This list contains the concepts that can not be captured by the ICD hierarchy
    *
  FROM `{{project}}.{{dataset}}.concept`
  WHERE concept_id in (1575835, 44833005, 44831866)

),
snomed_vehicle_list AS (
  SELECT DISTINCT 
    concept.* 
  FROM `{{project}}.{{dataset}}.concept` AS concept 
  JOIN `{{project}}.{{dataset}}.concept_ancestor` 
    ON descendant_concept_id = concept_id
  WHERE ancestor_concept_id in (435713, 444074, 440931, 440902, 4057838, 438329, 4105376, 4317651) 
    AND vocabulary_id = 'SNOMED'
),
expand_snomed_using_has_due_to AS (
  SELECT DISTINCT
    c.*
  FROM snomed_vehicle_list AS i
  JOIN `{{project}}.{{dataset}}.concept_relationship` AS cr
    ON cr.concept_id_2 = i.concept_id AND cr.relationship_id = 'Has due to'
  JOIN `{{project}}.{{dataset}}.concept_relationship` AS cr2
    ON cr.concept_id_1 = cr2.concept_id_1 AND cr2.relationship_id = 'Maps to'
  JOIN `{{project}}.{{dataset}}.concept_ancestor` AS ca
    ON cr2.concept_id_2 = ca.ancestor_concept_id 
  JOIN `{{project}}.{{dataset}}.concept` AS c
    ON ca.descendant_concept_id = c.concept_id AND c.vocabulary_id = 'SNOMED'
),
expanded_snomed_vehicle_list AS (
  SELECT 
    * 
  FROM snomed_vehicle_list

  UNION DISTINCT

  SELECT 
    * 
  FROM expand_snomed_using_has_due_to
),
non_standard_vehicle_list AS (
  SELECT DISTINCT
      c.*
  FROM expanded_snomed_vehicle_list AS v
  JOIN `{{project}}.{{dataset}}.concept_relationship` AS cr
    ON v.concept_id = cr.concept_id_2 AND cr.relationship_id = 'Maps to'
  JOIN `{{project}}.{{dataset}}.concept` AS c
    ON cr.concept_id_1 = c.concept_id AND c.vocabulary_id = 'SNOMED'
)

SELECT
  *
FROM expanded_snomed_vehicle_list

UNION DISTINCT

SELECT
  *
FROM non_standard_vehicle_list

UNION DISTINCT

SELECT 
  * 
FROM icd_vehicle_list 
""")


class MotorVehicleAccidentSuppression(AbstractBqLookupTableConceptSuppression):

    def __init__(self, project_id, dataset_id, sandbox_dataset_id):
        """
        Initialize the class with proper info.

        Set the issue numbers, description and affected datasets.  As other
        tickets may affect this SQL, append them to the list of Jira Issues.
        DO NOT REMOVE ORIGINAL JIRA ISSUE NUMBERS!
        """
        desc = (
            'Sandbox and record suppress all records with a concept_id or concept_code '
            'relating to a motor vehicle accident. ')
        super().__init__(
            issue_numbers=ISSUE_NUMBERS,
            description=desc,
            affected_datasets=[cdr_consts.CONTROLLED_TIER_DEID],
            project_id=project_id,
            dataset_id=dataset_id,
            sandbox_dataset_id=sandbox_dataset_id,
            affected_tables=resources.CDM_TABLES,
            concept_suppression_lookup_table=SUPPRESSION_RULE_CONCEPT_TABLE)

    def create_suppression_lookup_table(self, client):
        concept_suppression_lookup_query = MOTOR_VEHICLE_ACCIDENT_CONCEPT_QUERY.render(
            project=self.project_id,
            dataset=self.dataset_id,
            concept_suppression_lookup=self.get_suppression_concept_table_name(
            ))
        query_job = client.query(concept_suppression_lookup_query)
        query_job.result()


if __name__ == '__main__':
    import cdr_cleaner.args_parser as parser
    import cdr_cleaner.clean_cdr_engine as clean_engine

    ARGS = parser.default_parse_args()

    if ARGS.list_queries:
        clean_engine.add_console_logging()
        query_list = clean_engine.get_query_list(
            ARGS.project_id, ARGS.dataset_id, ARGS.sandbox_dataset_id,
            [(MotorVehicleAccidentSuppression,)])
        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(ARGS.project_id, ARGS.dataset_id,
                                   ARGS.sandbox_dataset_id,
                                   [(MotorVehicleAccidentSuppression,)])
