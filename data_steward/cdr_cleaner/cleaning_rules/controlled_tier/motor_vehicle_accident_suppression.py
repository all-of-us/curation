import logging
from typing import Dict, List
from google.cloud.bigquery.client import Client

import resources
from common import JINJA_ENV
from constants import bq_utils as bq_consts
import constants.cdr_cleaner.clean_cdr as cdr_consts
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule, query_spec_list

LOGGER = logging.getLogger(__name__)

ISSUE_NUMBERS = ['DC1367']

SUPPRESSION_RULE_CONCEPT_TABLE = 'motor_vehicle_accident_suppression_concept'

TABLE_ID = 'table_id'

GET_ALL_TABLES_QUERY_TEMPLATE = JINJA_ENV.from_string("""
SELECT
  table_id
FROM `{{project}}.{{dataset}}.__TABLES__`
WHERE table_id IN (
{% for table_name in table_names %}
    {% if loop.previtem is defined %}, {% else %}  {% endif %} '{{table_name}}'
{% endfor %}
)
""")

MOTOR_VEHICLE_ACCIDENT_CONCEPT_QUERY = JINJA_ENV.from_string("""
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

SUPPRESION_RECORD_SANDBOX_QUERY_TEMPLATE = JINJA_ENV.from_string("""
SELECT
  d.*
FROM `{{project}}.{{dataset}}.{{domain_table}}` AS d
{% for concept_field in concept_fields %}
LEFT JOIN `{{project}}.{{sandbox_dataset}}.{{suppression_concept}}` AS s{{loop.index}}
  ON d.{{concept_field}} = s{{loop.index}}.concept_id 
{% endfor %}
WHERE COALESCE(
{% for concept_field in concept_fields %}
    {% if loop.previtem is defined %}, {% else %}  {% endif %} s{{loop.index}}.concept_id
{% endfor %}) IS NOT NULL
""")

SUPPRESION_RECORD_QUERY_TEMPLATE = JINJA_ENV.from_string("""
SELECT
  d.*
FROM `{{project}}.{{dataset}}.{{domain_table}}` AS d
LEFT JOIN `{{project}}.{{sandbox_dataset}}.{{sandbox_table}}` AS s
    ON d.{{domain_table}}_id = s.{{domain_table}}_id
WHERE s.{{domain_table}}_id IS NULL
""")


class MotorVehicleAccidentSuppression(BaseCleaningRule):

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
        super().__init__(issue_numbers=ISSUE_NUMBERS,
                         description=desc,
                         affected_datasets=[cdr_consts.CONTROLLED_TIER_DEID],
                         project_id=project_id,
                         dataset_id=dataset_id,
                         sandbox_dataset_id=sandbox_dataset_id,
                         affected_tables=resources.CDM_TABLES)

    def setup_rule(self, client: Client, *args, **keyword_args):
        query_job = client.query(
            GET_ALL_TABLES_QUERY_TEMPLATE.render(
                project=self.project_id,
                dataset=self.dataset_id,
                table_names=self.affected_tables))
        result = query_job.result()
        self.affected_tables = [dict(row.items())[TABLE_ID] for row in result]

    def get_suppression_concept_table_name(self) -> str:
        """
        Get the suppression concept table name specific to this suppression rule
        :return:
        """
        return SUPPRESSION_RULE_CONCEPT_TABLE

    def get_concept_suppression_query(self) -> Dict[str, str]:
        """
        Get a dictionary that contains the query for generating the suppression concept table
        :return:
        """
        suppression_concept_query = MOTOR_VEHICLE_ACCIDENT_CONCEPT_QUERY.render(
            project=self.project_id, dataset=self.dataset_id)
        return {
            cdr_consts.QUERY:
                suppression_concept_query,
            cdr_consts.DESTINATION_TABLE:
                self.get_suppression_concept_table_name(),
            cdr_consts.DISPOSITION:
                bq_consts.WRITE_TRUNCATE,
            cdr_consts.DESTINATION_DATASET:
                self.sandbox_dataset_id
        }

    def get_sandbox_tablenames(self):
        return [
            self.sandbox_table_for(affected_table)
            for affected_table in self._affected_tables
        ]

    def setup_validation(self, client, *args, **keyword_args):
        pass

    def validate_rule(self, client, *args, **keyword_args):
        pass

    def concept_id_fields(self, table_name) -> List[str]:
        """
        Determine if column is a concept_id column

        :param table_name: 
        :return: True if column is a concept_id column, False otherwise
        """
        return [
            field_name['name']
            for field_name in resources.fields_for(table_name)
            if field_name['name'].endswith('concept_id')
        ]

    def table_contains_concept_id(self, table_name) -> bool:
        return len(self.concept_id_fields(table_name)) > 0

    def get_sandbox_query(self, table_name):
        """
        Sandbox records in the given table whose concept id fields contain any concepts in the 
        suppression concept table 
        
        :param table_name: 
        :return: 
        """
        suppression_record_sandbox_query = SUPPRESION_RECORD_SANDBOX_QUERY_TEMPLATE.render(
            project=self.project_id,
            dataset=self.dataset_id,
            sandbox_dataset=self.sandbox_dataset_id,
            domain_table=table_name,
            concept_fields=self.concept_id_fields(table_name),
            suppression_concept=self.get_suppression_concept_table_name())

        return {
            cdr_consts.QUERY: suppression_record_sandbox_query,
            cdr_consts.DESTINATION_TABLE: self.sandbox_table_for(table_name),
            cdr_consts.DISPOSITION: bq_consts.WRITE_TRUNCATE,
            cdr_consts.DESTINATION_DATASET: self.sandbox_dataset_id
        }

    def get_suppression_query(self, table_name):
        """
        Get the suppression query that deletes records that are in the corresponding sandbox table
        
        :param table_name: 
        :return: 
        """
        suppression_record_query = SUPPRESION_RECORD_QUERY_TEMPLATE.render(
            project=self.project_id,
            dataset=self.dataset_id,
            sandbox_dataset=self.sandbox_dataset_id,
            domain_table=table_name,
            sandbox_table=self.sandbox_table_for(table_name))

        return {
            cdr_consts.QUERY: suppression_record_query,
            cdr_consts.DESTINATION_DATASET: self.dataset_id
        }

    def get_query_specs(self, *args, **keyword_args) -> query_spec_list:
        sandbox_queries = [
            self.get_sandbox_query(table_name)
            for table_name in self.affected_tables
            if self.table_contains_concept_id(table_name)
        ]

        queries = [
            self.get_suppression_query(table_name)
            for table_name in self.affected_tables
            if self.table_contains_concept_id(table_name)
        ]

        return [self.get_concept_suppression_query()
               ] + sandbox_queries + queries


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
