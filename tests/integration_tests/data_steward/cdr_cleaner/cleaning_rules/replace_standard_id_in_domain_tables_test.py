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
            f'{cls.project_id}.{cls.dataset_id}.{table_name}'
            for table_name in [CONCEPT, CONCEPT_RELATIONSHIP]
        ])

        # Add _logging_standard_concept_id_replacement to fq_sandbox_table_names for cleanup in
        # teardown
        cls.fq_sandbox_table_names.append(
            f'{cls.project_id}.{cls.sandbox_id}.{SRC_CONCEPT_ID_TABLE_NAME}')

        # call super to set up the client, create datasets
        cls.up_class = super().setUpClass()

    def setUp(self):
        """
        Create empty tables for the rule to run on
        """
        # Create tables required for the test
        super().setUp()

        condition_occurrence_data_template = JINJA_ENV.from_string("""
        INSERT INTO `{{project_id}}.{{dataset_id}}.condition_occurrence` (
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
                condition_status_concept_id)
        VALUES
              (1, 1, 319835, '2020-05-10', timestamp('2020-05-10 00:00:00 UTC'), null, null, 0, null, null, null, null, 45567179, null, null),
               (2, 1, 0, '2020-05-10', timestamp('2020-05-10 00:00:00 UTC'), null, null, 0, null, null, null, null, 45567179, null, null),
               (3, 1, 45567179, '2020-05-10', timestamp('2020-05-10 00:00:00 UTC'), null, null, 0, null, null, null, null, 0, null, null),
               (4, 1, 45567179, '2020-05-10', timestamp('2020-05-10 00:00:00 UTC'), null, null, 0, null, null, null, null, 45567179, null, null), 
               (5, 1, 40398862, '2020-05-10', timestamp('2020-05-10 00:00:00 UTC'), null, null, 0, null, null, null, null, 40398862, null, null),
               (6, 1, 45587397, '2020-05-10', timestamp('2020-05-10 00:00:00 UTC'), null, null, 0, null, null, null, null, 45587397, null, null)
        """)

        mapping_condition_occurrence_data_template = JINJA_ENV.from_string("""
        INSERT INTO `{{project_id}}.{{dataset_id}}._mapping_condition_occurrence`
        (
          condition_occurrence_id, src_table_id, src_dataset_id, src_condition_occurrence_id, src_hpo_id)
        VALUES
               (1, 'hpo_condition_occurrence', 'hpo_dataset', 10, 'test_hpo'),
               (2, 'hpo_condition_occurrence', 'hpo_dataset', 20, 'test_hpo'),
               (3, 'hpo_condition_occurrence', 'hpo_dataset', 30, 'test_hpo'),
               (4, 'hpo_condition_occurrence', 'hpo_dataset', 40, 'test_hpo'), 
               (5, 'hpo_condition_occurrence', 'hpo_dataset', 50, 'test_hpo'),
               (6, 'hpo_condition_occurrence', 'hpo_dataset', 60, 'test_hpo')
        """)

        concept_data_template = JINJA_ENV.from_string("""
        INSERT INTO `{{project_id}}.{{dataset_id}}.concept`
        (
          concept_id, concept_name, standard_concept, domain_id, vocabulary_id, concept_class_id, concept_code, valid_start_date, valid_end_date)
        VALUES
               (319835, 'Congested Heart Failure', 'S', 'foo', 'bar', 'baz', 'alpha', '2020-01-01', '2099-12-31'),
               (45567179, 'Congested Heart Failure', 'C', 'foo', 'bar', 'baz', 'beta', '2020-01-01', '2099-12-31'),
               (45587397, 'Anaemia complicating pregnancy, childbirth and the puerperium', 'C', 'foo', 'bar', 'baz', 'gamma', '2020-01-01', '2099-12-31'),
               (434701, 'Anemia in mother complicating pregnancy, childbirth AND/OR puerperium', 'S', 'foo', 'bar', 'baz', 'rho', '2020-01-01', '2099-12-31'),
               (444094, 'Finding related to pregnancy', 'S', 'foo', 'bar', 'baz', 'phi', '2020-01-01', '2099-12-31'), 
               (40398862, 'Ischemic chest pain', 'C', 'foo', 'bar', 'baz', 'zed', '2020-01-01', '2099-12-31')
        """)

        concept_relationship_data_template = JINJA_ENV.from_string("""
        INSERT INTO `{{project_id}}.{{dataset_id}}.concept_relationship`
        (
          concept_id_1, concept_id_2, relationship_id, valid_start_date, valid_end_date)
        VALUES
               (319835, 319835, 'Maps to', '2020-01-01', '2099-12-31'),
               (45567179, 319835, 'Maps to', '2020-01-01', '2099-12-31'),
               (45587397, 434701, 'Maps to', '2020-01-01', '2099-12-31'),
               (45587397, 444094, 'Maps to', '2020-01-01', '2099-12-31')
        """)

        concept_data_query = concept_data_template.render(
            project_id=self.project_id, dataset_id=self.dataset_id)

        concept_relationship_data_query = concept_relationship_data_template.render(
            project_id=self.project_id, dataset_id=self.dataset_id)

        condition_occurrence_data_query = condition_occurrence_data_template.render(
            project_id=self.project_id, dataset_id=self.dataset_id)

        mapping_condition_occurrence_data_query = mapping_condition_occurrence_data_template.render(
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
