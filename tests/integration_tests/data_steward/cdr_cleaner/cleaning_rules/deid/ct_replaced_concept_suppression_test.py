"""
Integration test for ControlledTierReplacedConceptSuppression
"""
# Python Imports
import os

# Project Imports
from common import AOU_DEATH
from app_identity import PROJECT_ID
from cdr_cleaner.cleaning_rules.deid.ct_replaced_concept_suppression import (
    ControlledTierReplacedConceptSuppression, SUPPRESSION_RULE_CONCEPT_TABLE)
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import BaseTest


class ControlledTierReplacedConceptSuppressionTest(
        BaseTest.CleaningRulesTestBase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

        super().initialize_class_vars()

        cls.project_id = os.environ.get(PROJECT_ID)
        cls.dataset_id = os.environ.get('COMBINED_DATASET_ID')
        cls.sandbox_id = f"{cls.dataset_id}_sandbox"

        cls.rule_instance = ControlledTierReplacedConceptSuppression(
            cls.project_id, cls.dataset_id, cls.sandbox_id)

        cls.fq_table_names = [f'{cls.project_id}.{cls.dataset_id}.{AOU_DEATH}']
        cls.fq_sandbox_table_names = [
            f'{cls.project_id}.{cls.sandbox_id}.{cls.rule_instance.sandbox_table_for(AOU_DEATH)}',
            f'{cls.project_id}.{cls.sandbox_id}.{SUPPRESSION_RULE_CONCEPT_TABLE}'
        ]

        super().setUpClass()

    def setUp(self):
        super().setUp()

        insert_aou_death = self.jinja_env.from_string("""
        INSERT INTO `{{project_id}}.{{dataset_id}}.aou_death`
            (aou_death_id, person_id, death_date, death_type_concept_id, cause_concept_id, cause_source_concept_id, src_id, primary_death_record)
        VALUES
            ('a1', 1, date('2020-05-05'), 4053421, 0, 0, 'rdr', False),
            ('b1', 1, date('2021-05-05'), 0, 4053421, 0, 'hpo_b', True),
            ('c1', 1, date('2021-05-05'), 0, 0, 4053421, 'hpo_c', False),
            ('d1', 1, date('2021-05-05'), 0, 0, 0, 'hpo_d', False)
        """).render(project_id=self.project_id, dataset_id=self.dataset_id)

        self.load_test_data([insert_aou_death])

    def test_ct_replaced_concept_suppression(self):
        tables_and_counts = [{
            'fq_table_name': f'{self.project_id}.{self.dataset_id}.{AOU_DEATH}',
            'fq_sandbox_table_name':
                f'{self.project_id}.{self.sandbox_id}.'
                f'{self.rule_instance.sandbox_table_for(AOU_DEATH)}',
            'loaded_ids': ['a1', 'b1', 'c1', 'd1'],
            'sandboxed_ids': ['a1', 'b1', 'c1'],
            'fields': ['aou_death_id'],
            'cleaned_values': [('d1',)]
        }]

        self.default_test(tables_and_counts)
