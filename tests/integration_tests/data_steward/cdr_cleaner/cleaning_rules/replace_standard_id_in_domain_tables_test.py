"""
Integration test for replace_standard_id_in_domain_tables module

Original Issues: DC-808, DC-1170

The intent is to map the unmapped survey answers (value_as_concept_ids=0) using 
value_source_concept_id through 'Maps to' relationship 
"""

# Python Imports
import os

# Project Imports
from common import CONDITION_OCCURRENCE, CONCEPT, CONCEPT_RELATIONSHIP
from common import JINJA_ENV
from app_identity import PROJECT_ID
from cdr_cleaner.cleaning_rules.replace_standard_id_in_domain_tables import \
    ReplaceWithStandardConceptId, SRC_CONCEPT_ID_TABLE_NAME
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import \
    BaseTest

# The existing table is created and partitioned on the pseudo column _PARTITIONTIME, partitioning
# by _PARTITIONTIME doesn't work using a query_statement for creating a table, therefore CREATE
# OR REPLACE TABLE doesn' work and we need to DROP the table first. The cleaning rule generates
# queries that explicitly list out all the columns associated with the domain table in SELECT,
# due to this reason, we have to create those columns as well in the test table.
CONDITION_OCCURRENCE_DATA_TEMPLATE = JINJA_ENV.from_string("""
DROP TABLE IF EXISTS `{{project_id}}.{{dataset_id}}.condition_occurrence`;
CREATE TABLE `{{project_id}}.{{dataset_id}}.condition_occurrence`
AS (
WITH w AS (
  SELECT ARRAY<STRUCT<
        condition_occurrence_id int64, 
        person_id int64, 
        condition_concept_id int64, 
        condition_start_date date,
        condition_start_datetime timestamp,
        condition_end_date date,
        condition_end_datetime timestamp,
        condition_type_concept_id int64,
        stop_reason string,
        provider_id int64,
        visit_occurrence_id int64,
        condition_source_value string,
        condition_source_concept_id int64,
        condition_status_source_value string,
        condition_status_concept_id int64
        >>
      [(1, 1, 319835, null, null, null, null, null, null, null, null, null, 45567179, null, null),
       (2, 1, 0, null, null, null, null, null, null, null, null, null, 45567179, null, null),
       (3, 1, 45567179, null, null, null, null, null, null, null, null, null, 0, null, null),
       (4, 1, 45567179, null, null, null, null, null, null, null, null, null, 45567179, null, null), 
       (5, 1, 40398862, null, null, null, null, null, null, null, null, null, 40398862, null, null),
       (6, 1, 45587397, null, null, null, null, null, null, null, null, null, 45587397, null, null)] col
)
SELECT 
    condition_occurrence_id, 
    person_id, 
    condition_concept_id, 
    condition_start_date,
    condition_start_datetime,
    condition_end_date,
    condition_end_datetime,
    condition_type_concept_id,
    stop_reason,
    provider_id,
    visit_occurrence_id,
    condition_source_value,
    condition_source_concept_id,
    condition_status_source_value,
    condition_status_concept_id 
FROM w, UNNEST(w.col))
""")

MAPPING_CONDITION_OCCURRENCE_DATA_TEMPLATE = JINJA_ENV.from_string("""
DROP TABLE IF EXISTS `{{project_id}}.{{dataset_id}}._mapping_condition_occurrence`;
CREATE TABLE `{{project_id}}.{{dataset_id}}._mapping_condition_occurrence`
AS (
WITH w AS (
  SELECT ARRAY<STRUCT<condition_occurrence_id int64, src_table_id string, src_dataset_id string, src_condition_occurrence_id int64, src_hpo_id string>>
      [(1, 'hpo_condition_occurrence', 'hpo_dataset', 10, 'test_hpo'),
       (2, 'hpo_condition_occurrence', 'hpo_dataset', 20, 'test_hpo'),
       (3, 'hpo_condition_occurrence', 'hpo_dataset', 30, 'test_hpo'),
       (4, 'hpo_condition_occurrence', 'hpo_dataset', 40, 'test_hpo'), 
       (5, 'hpo_condition_occurrence', 'hpo_dataset', 50, 'test_hpo'),
       (6, 'hpo_condition_occurrence', 'hpo_dataset', 60, 'test_hpo')] col
)
SELECT condition_occurrence_id, src_table_id, src_dataset_id, src_condition_occurrence_id, src_hpo_id FROM w, UNNEST(w.col))
""")

CONCEPT_DATA_TEMPLATE = JINJA_ENV.from_string("""
DROP TABLE IF EXISTS `{{project_id}}.{{dataset_id}}.concept`;
CREATE TABLE `{{project_id}}.{{dataset_id}}.concept`
AS (
WITH w AS (
  SELECT ARRAY<STRUCT<concept_id int64, concept_name string, standard_concept string>>
      [(319835, 'Congested Heart Failure', 'S'),
       (45567179, 'Congested Heart Failure', null),
       (45587397, 'Anaemia complicating pregnancy, childbirth and the puerperium', null),
       (434701, 'Anemia in mother complicating pregnancy, childbirth AND/OR puerperium', 'S'),
       (444094, 'Finding related to pregnancy', 'S'), 
       (40398862, 'Ischemic chest pain', null)] col
)
SELECT concept_id, concept_name, standard_concept FROM w, UNNEST(w.col))
""")

CONCEPT_RELATIONSHIP_DATA_TEMPLATE = JINJA_ENV.from_string("""
DROP TABLE IF EXISTS `{{project_id}}.{{dataset_id}}.concept_relationship`;
CREATE TABLE `{{project_id}}.{{dataset_id}}.concept_relationship`
AS (
WITH w AS (
  SELECT ARRAY<STRUCT<concept_id_1 int64, concept_id_2 int64, relationship_id string>>
      [(319835, 319835, 'Maps to'),
       (45567179, 319835, 'Maps to'),
       (45587397, 434701, 'Maps to'),
       (45587397, 444094, 'Maps to')] col
)
SELECT concept_id_1, concept_id_2, relationship_id FROM w, UNNEST(w.col))
""")


class ReplaceWithStandardConceptIdTest(BaseTest.CleaningRulesTestBase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

        super().initialize_class_vars()

        # Set the test project identifier
        cls.project_id = os.environ.get(PROJECT_ID)

        # Set the expected test datasets
        cls.dataset_id = os.environ.get('COMBINED_DATASET_ID')
        cls.sandbox_id = cls.dataset_id + '_sandbox'

        cls.rule_instance = ReplaceWithStandardConceptId(
            cls.project_id, cls.dataset_id, cls.sandbox_id)

        # Generates list of fully qualified table names and their corresponding sandbox table names
        for table_name in cls.rule_instance.affected_tables:
            cls.fq_table_names.append(
                f'{cls.project_id}.{cls.dataset_id}.{table_name}')
            cls.fq_table_names.append(
                f'{cls.project_id}.{cls.dataset_id}._mapping_{table_name}')
            sandbox_table_name = cls.rule_instance.sandbox_table_for(table_name)
            cls.fq_sandbox_table_names.append(
                f'{cls.project_id}.{cls.sandbox_id}.{sandbox_table_name}')

        cls.fq_table_names.extend([
            f'{cls.project_id}.{cls.dataset_id}.{table_name}' for table_name in
            [SRC_CONCEPT_ID_TABLE_NAME, CONCEPT, CONCEPT_RELATIONSHIP]
        ])

        # call super to set up the client, create datasets
        cls.up_class = super().setUpClass()

    def setUp(self):
        """
        Create empty tables for the rule to run on
        """
        # Create tables required for the test
        super().setUp()

        concept_data_query = CONCEPT_DATA_TEMPLATE.render(
            project_id=self.project_id, dataset_id=self.dataset_id)

        concept_relationship_data_query = CONCEPT_RELATIONSHIP_DATA_TEMPLATE.render(
            project_id=self.project_id, dataset_id=self.dataset_id)

        condition_occurrence_data_query = CONDITION_OCCURRENCE_DATA_TEMPLATE.render(
            project_id=self.project_id, dataset_id=self.dataset_id)

        mapping_condition_occurrence_data_query = MAPPING_CONDITION_OCCURRENCE_DATA_TEMPLATE.render(
            project_id=self.project_id, dataset_id=self.dataset_id)

        # Load test data
        self.load_test_data([
            f'''{concept_data_query};
                {concept_relationship_data_query};
                {condition_occurrence_data_query};
                {mapping_condition_occurrence_data_query}'''
        ])

    def test_replace_standard_id_in_domain_tables(self):
        """
        condition_occurrence 
        
        record 1: condition_concept_id is already standard, so do nothing 
        
        record 2: condition_concept_id is 0, but condition_source_concept_id 45567179 can be 
        mapped to the standard concept 319835. So condition_concept_id should be replaced with 
        319835 and this record is sandboxed.
        
        record 3: condition_concept_id is the non-standard 45567179 and 
        condition_source_concept_id 0. 45567179 can be mapped to the standard concept 319835. So 
        condition_concept_id should be replaced with 319835 and condition_source_concept_id 
        should be replaced by the original condition_concept_id 45567179 and this record is 
        sandboxed. 
        
        record 4: condition_concept_id is the non-standard 45567179 and 
        condition_source_concept_id the same non-standard 45567179. 45567179 can be mapped to the 
        standard concept 319835. So condition_concept_id should be replaced with 319835 and 
        condition_source_concept_id remains the same and this record is sandboxed. 
        
        record 5: condition_concept_id is the non-standard 40398862 and 
        condition_source_concept_id the same non-standard 40398862, but there is no standard 
        concept mapping for 40398862. Although condition_concept_id and 
        condition_source_concept_id remain the same, this record is still sandboxe because this 
        record is flagged as being non-standard. 
        
        record 6: condition_concept_id is the non-standard 45587397 and 
        condition_source_concept_id the same non-standard 45587397. 45567179 can be mapped to two 
        standard concepts 434701 and 444094. So this record is split into two new records, 
        the new ids are generated using this logic MAX(condition_occurrence_id) + ROW_NUMBER(). 
        Two new records with condition_occurrence_id 7 and 8 are generated and the corresponding 
        mapping table _mapping_condition_occurrence is updated as well. This record is sandboxed. 
     
        :return: 
        """
        # Expected results list
        tables_and_counts = [{
            'fq_table_name':
                f'{self.project_id}.{self.dataset_id}.{CONDITION_OCCURRENCE}',
            'fq_sandbox_table_name':
                f'{self.project_id}.{self.sandbox_id}.{self.rule_instance.sandbox_table_for(CONDITION_OCCURRENCE)}',
            'loaded_ids': [1, 2, 3, 4, 5, 6],
            'sandboxed_ids': [2, 3, 4, 5, 6],
            'fields': [
                'condition_occurrence_id', 'person_id', 'condition_concept_id',
                'condition_source_concept_id'
            ],
            'cleaned_values': [
                (1, 1, 319835, 45567179), (2, 1, 319835, 45567179),
                (3, 1, 319835, 45567179), (4, 1, 319835, 45567179),
                (5, 1, 40398862, 40398862), (7, 1, 434701, 45587397),
                (8, 1, 444094, 45587397)
            ]
        }, {
            'fq_table_name':
                f'{self.project_id}.{self.dataset_id}._mapping_{CONDITION_OCCURRENCE}',
            'loaded_ids': [1, 2, 3, 4, 5, 6],
            'fields': [
                'condition_occurrence_id', 'src_condition_occurrence_id'
            ],
            'cleaned_values': [(1, 10), (2, 20), (3, 30), (4, 40), (5, 50),
                               (7, 60), (8, 60)]
        }]

        self.default_test(tables_and_counts)
