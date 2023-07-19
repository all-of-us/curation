"""
Integration test for CalculatePrimaryDeathRecord
"""
# Python Imports
import os

# Project imports
import cdr_cleaner.clean_cdr_engine as clean_engine
from app_identity import PROJECT_ID
from cdr_cleaner.cleaning_rules.calculate_primary_death_record import CalculatePrimaryDeathRecord
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import BaseTest
from common import AOU_DEATH


class CalculatePrimaryDeathRecordTest(BaseTest.CleaningRulesTestBase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

        super().initialize_class_vars()

        cls.project_id = os.environ.get(PROJECT_ID)
        cls.dataset_id = os.environ.get('COMBINED_DATASET_ID')
        cls.sandbox_id = f"{cls.dataset_id}_sandbox"

        cls.rule_instance = CalculatePrimaryDeathRecord(cls.project_id,
                                                        cls.dataset_id,
                                                        cls.sandbox_id)

        cls.fq_table_names = [
            f'{cls.project_id}.{cls.dataset_id}.{AOU_DEATH}',
        ]

        cls.fq_sandbox_table_names = [
            f'{cls.project_id}.{cls.sandbox_id}.{cls.rule_instance.sandbox_table_for(AOU_DEATH)}'
        ]

        super().setUpClass()

    def setUp(self):
        super().setUp()

        insert_aou_death = self.jinja_env.from_string("""
        INSERT INTO `{{project}}.{{dataset}}.aou_death`
            (aou_death_id, person_id, death_date, death_datetime, death_type_concept_id, 
             src_id, primary_death_record)
        VALUES
            ('a1', 1, '2020-01-01', NULL, 0, 'hpo_a', False),
            ('h1', 1, '2010-01-01', '2010-01-01 00:00:00', 0, 'healthpro', True),
            ('a2', 2, '2020-01-01', '2020-01-01 00:00:00', 0, 'hpo_a', False),
            ('b2', 2, '2020-01-01', '2020-01-01 00:00:00', 0, 'hpo_b', True),
            ('h2', 2, '2010-01-01', '2010-01-01 00:00:00', 0, 'healthpro', False),
            ('a3', 3, '2020-01-01', NULL, 0, 'hpo_a', False),
            ('b3', 3, '2020-01-01', '2020-01-01 00:00:00', 0, 'hpo_b', False),
            ('h3', 3, '2010-01-01', '2010-01-01 00:00:00', 0, 'healthpro', False),
            ('a4', 4, '2020-01-01', '2020-01-01 00:00:00', 0, 'hpo_a', False),
            ('c4', 4, '2019-12-31', '2019-12-31 00:00:00', 0, 'hpo_c', False),
            ('h4', 4, '2010-01-01', '2010-01-01 00:00:00', 0, 'Staff Portal: HealthPro', False),
            ('a5', 5, '2020-01-01', '2020-01-01 12:00:00', 0, 'hpo_a', False),
            ('c5', 5, '2020-01-01', '2020-01-01 00:00:00', 0, 'hpo_c', False),
            ('h5', 5, '2010-01-01', '2010-01-01 00:00:00', 0, 'Staff Portal: HealthPro', False),
            ('h6-1', 6, '2010-01-02', '2010-01-02 00:00:00', 0, 'Staff Portal: HealthPro', False),
            ('h6-2', 6, '2010-01-01', '2010-01-01 00:00:00', 0, 'Staff Portal: HealthPro', False),
            ('h6-3', 6, NULL, NULL, 0, 'Staff Portal: HealthPro', False),
            ('h7-1', 7, NULL, NULL, 0, 'Staff Portal: HealthPro', False),
            ('h7-2', 7, NULL, NULL, 0, 'Staff Portal: HealthPro', False),
            ('h7-3', 7, NULL, NULL, 0, 'Staff Portal: HealthPro', False)
        """).render(project=self.project_id, dataset=self.dataset_id)

        self.load_test_data([insert_aou_death])

    def test_calculate_primary_death_record(self):
        """
        Test cases for AOU_DEATH data:
        person_id = 1: hpo_a. An EHR record is chosen over a HealthPro record.
        person_id = 2: Exactly same records exist in hpo_a and hpo_b. hpo_a alphabetically becomes primary.
        person_id = 3: The only difference between hpo_a and hpo_b is hpo_b has non-NULL death_datetime. hpo_b becomes primary.
        person_id = 4: hpo_c's death_date is earlier than hpo_a. hpo_c becomes primary.
        person_id = 5: hpo_c's death_datetime is earlier than hpo_a. hpo_c becomes primary.
        person_id = 6: Multiple recoreds from HealthPro. The one with the earliest death_date becomes primary.
        person_id = 7: Multiple records from HealthPro but they all have NULL death_date. No record becomes primary.
        """

        tables_and_counts = [{
            'fq_table_name':
                self.fq_table_names[0],
            'fq_sandbox_table_name':
                self.fq_sandbox_table_names[0],
            'loaded_ids': [
                'a1', 'h1', 'a2', 'b2', 'h2', 'a3', 'b3', 'h3', 'a4', 'c4',
                'h4', 'a5', 'c5', 'h5', 'h6-1', 'h6-2', 'h6-3', 'h7-1', 'h7-2',
                'h7-3'
            ],
            'sandboxed_ids': ['a1', 'h1', 'a2', 'b2', 'b3', 'c4', 'c5', 'h6-2'],
            'fields': ['aou_death_id', 'person_id', 'primary_death_record'],
            'cleaned_values': [('a1', 1, True), ('h1', 1, False),
                               ('a2', 2, True), ('b2', 2, False),
                               ('h2', 2, False), ('a3', 3, False),
                               ('b3', 3, True), ('h3', 3, False),
                               ('a4', 4, False), ('c4', 4, True),
                               ('h4', 4, False), ('a5', 5, False),
                               ('c5', 5, True), ('h5', 5, False),
                               ('h6-1', 6, False), ('h6-2', 6, True),
                               ('h6-3', 6, False), ('h7-1', 7, False),
                               ('h7-2', 7, False), ('h7-3', 7, False)]
        }]

        self.default_test(tables_and_counts)
