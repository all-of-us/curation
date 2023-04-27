"""
Integration test for missing_concept_record_suppression module

Original Issue: DC1601
"""

# Python Imports
import os

#Project imports
from app_identity import PROJECT_ID
from cdr_cleaner.cleaning_rules.missing_concept_record_suppression import MissingConceptRecordSuppression
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import BaseTest
from common import AOU_DEATH, DEATH, OBSERVATION, CONCEPT

# Third party imports
from dateutil import parser


class MissingConceptRecordSuppressionTest(BaseTest.CleaningRulesTestBase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

        super().initialize_class_vars()

        # set the test project identifier
        project_id = os.environ.get(PROJECT_ID)
        cls.project_id = project_id

        # set the expected test datasets
        dataset_id = os.environ.get('COMBINED_DATASET_ID')
        cls.dataset_id = dataset_id
        sandbox_id = f'{dataset_id}_sandbox'
        cls.sandbox_id = sandbox_id

        cls.rule_instance = MissingConceptRecordSuppression(
            project_id, dataset_id, sandbox_id)

        cls.fq_table_names = [
            f'{project_id}.{dataset_id}.{AOU_DEATH}',
            f'{project_id}.{dataset_id}.{DEATH}',
            f'{project_id}.{dataset_id}.{OBSERVATION}',
            f'{project_id}.{dataset_id}.{CONCEPT}',
        ]

        cls.fq_sandbox_table_names = [
            f'{cls.project_id}.{cls.sandbox_id}.{cls.rule_instance.sandbox_table_for(AOU_DEATH)}',
            f'{cls.project_id}.{cls.sandbox_id}.{cls.rule_instance.sandbox_table_for(DEATH)}',
            f'{cls.project_id}.{cls.sandbox_id}.{cls.rule_instance.sandbox_table_for(OBSERVATION)}',
        ]

        super().setUpClass()

    def setUp(self):
        fq_dataset_name = self.fq_table_names[0].split('.')
        self.fq_dataset_name = '.'.join(fq_dataset_name[:-1])

        self.date = parser.parse('2020-05-05').date()

        super().setUp()

    def test_missing_concept_record_suppression_cleaning(self):
        """
        Tests that concepts missing from vocabulary are suppressed.

        Validates pre conditions, tests execution, and post conditions based on the load
        statements and the tables_and_counts variable.        
        """
        queries = []

        insert_concept = self.jinja_env.from_string("""
            INSERT INTO `{{fq_dataset_name}}.concept`
                (concept_id, concept_name, domain_id, vocabulary_id, concept_class_id, concept_code, valid_start_date, valid_end_date)
            VALUES
                (4225432, "Blue color", "Condition", "SNOMED", "Clinical Finding", "84614006", date('2020-05-05'), date('2020-05-05')),
                (4297377, "Red color" , "Condition" ,"SNOMED" ,"Clinical Finding", "386713009" , date('2020-05-05'), date('2020-05-05')),
                (441840, "Clinical finding", "Condition", "SNOMED", "Clinical Finding", "404684003", date('2020-05-05'), date('2020-05-05'))

        """).render(fq_dataset_name=self.fq_dataset_name)

        insert_death = self.jinja_env.from_string("""
            INSERT INTO `{{fq_dataset_name}}.death`
                (person_id, death_date, death_type_concept_id, cause_concept_id, cause_source_concept_id)
            VALUES
                (1, date('2020-05-05'), 441840, 441840, 441840),
                -- Dropped due to unknown death_type_concept_id --
                (2, date('2020-05-05'), 50000000, 441840, 441840),
                -- Dropped due to unknown cause_concept_id --
                (3, date('2020-05-05'), 441840, 50000000, 441840),
                -- NOT dropped. `_source_concept_id` columns can have unknown concepts --
                (4, date('2020-05-05'), 441840, 441840, 50000000),
                (5, date('2020-05-05'), 441840, 441840, 50000001)
            """).render(fq_dataset_name=self.fq_dataset_name)

        insert_aou_death = self.jinja_env.from_string("""
            INSERT INTO `{{fq_dataset_name}}.aou_death`
                (aou_death_id, person_id, death_date, death_type_concept_id, cause_concept_id, cause_source_concept_id, src_id, primary_death_record)
            VALUES
                ('a', 1, date('2020-05-05'), 441840, 441840, 441840, 'rdr', False),
                -- Dropped due to unknown death_type_concept_id --
                ('b', 1, date('2020-05-05'), 50000000, 441840, 441840, 'hpo_b', True),
                -- Dropped due to unknown cause_concept_id --
                ('c', 1, date('2020-05-05'), 441840, 50000000, 441840, 'hpo_c', False),
                -- NOT dropped. `_source_concept_id` columns can have unknown concepts --
                ('d', 1, date('2020-05-05'), 441840, 441840, 50000000, 'hpo_d', False),
                ('e', 1, date('2020-05-05'), 441840, 441840, 50000001, 'hpo_e', False)
            """).render(fq_dataset_name=self.fq_dataset_name)

        insert_obs = self.jinja_env.from_string("""
            INSERT INTO `{{fq_dataset_name}}.observation`
                (observation_id, person_id, observation_concept_id, observation_date, 
                observation_type_concept_id, value_as_concept_id, qualifier_concept_id, unit_concept_id, 
                observation_source_concept_id, value_source_concept_id)
            VALUES
                (1, 1, 4225432, date('2020-05-05'), 441840, 441840, 441840, 441840, 441840, 441840),
                (2, 2, 4297377, date('2020-05-05'), 441840, 441840, 441840, 441840, 441840, 441840),
                -- Dropped due to unknown observation_concept_id --
                (3, 3, 50000000, date('2020-05-05'), 441840, 441840, 441840, 441840, 441840, 441840),
                -- Dropped due to unknown observation_type_concept_id --
                (4, 4, 4297377, date('2020-05-05'), 19, 441840, 441840, 441840, 441840, 441840),
                -- NOT dropped. `_source_concept_id` columns can have unknown concepts --
                (5, 5, 4225432, date('2020-05-05'), 441840, 441840, 441840, 441840, 50000000, 441840),
                (6, 6, 4225432, date('2020-05-05'), 441840, 441840, 441840, 441840, 50000001, 441840)
            """).render(fq_dataset_name=self.fq_dataset_name)

        queries = [insert_concept, insert_death, insert_aou_death, insert_obs]

        self.load_test_data(queries)

        tables_and_counts = [{
            'fq_table_name': f'{self.fq_dataset_name}.aou_death',
            'fq_sandbox_table_name': self.fq_sandbox_table_names[0],
            'loaded_ids': ['a', 'b', 'c', 'd', 'e'],
            'sandboxed_ids': ['b', 'c'],
            'fields': ['aou_death_id'],
            'cleaned_values': [('a',), ('d',), ('e',)]
        }, {
            'fq_table_name': f'{self.fq_dataset_name}.death',
            'fq_sandbox_table_name': self.fq_sandbox_table_names[1],
            'loaded_ids': [1, 2, 3, 4, 5],
            'sandboxed_ids': [2, 3],
            'fields': ['person_id'],
            'cleaned_values': [(1,), (4,), (5,)]
        }, {
            'fq_table_name': f'{self.fq_dataset_name}.observation',
            'fq_sandbox_table_name': self.fq_sandbox_table_names[2],
            'loaded_ids': [1, 2, 3, 4, 5, 6],
            'sandboxed_ids': [3, 4],
            'fields': ['observation_id'],
            'cleaned_values': [(1,), (2,), (5,), (6,)]
        }]

        self.default_test(tables_and_counts)