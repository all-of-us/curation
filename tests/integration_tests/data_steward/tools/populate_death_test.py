"""
Integration test for populate_death
"""
# Python imports
import os

# Project imports
from app_identity import get_application_id, PROJECT_ID
from common import AOU_DEATH, DEATH
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import BaseTest
from tools.populate_death import populate_death


class PopulateDeathTest(BaseTest.BigQueryTestBase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

        super().initialize_class_vars()

        cls.project_id = os.environ.get(PROJECT_ID)
        cls.dataset_id = os.environ.get('COMBINED_DATASET_ID')

        cls.fq_table_names = [
            f'{cls.project_id}.{cls.dataset_id}.{AOU_DEATH}',
            f'{cls.project_id}.{cls.dataset_id}.{DEATH}',
        ]

        super().setUpClass()

    def setUp(self):
        super().setUp()

        insert_aou_death = self.jinja_env.from_string("""
            INSERT INTO `{{project}}.{{dataset}}.{{aou_death}}`
            (aou_death_id, person_id, death_date, death_type_concept_id, src_id, primary_death_record)
            VALUES
            ('h01', 1, '2022-01-01', 11, 'healthpro', False),
            ('a01', 1, '2022-01-01', 12, 'hpo_a', True),
            ('h02', 2, '2022-01-01', 21, 'healthpro', True),
            ('a03', 3, '2022-01-01', 31, 'hpo_a', False)
            """).render(project=self.project_id,
                        dataset=self.dataset_id,
                        aou_death=AOU_DEATH)

        self.load_test_data([insert_aou_death])

    def test_populate_death(self):
        """
        Test death table are populated using aou_death table as expected.
        """
        populate_death(self.client, self.project_id, self.dataset_id)

        self.assertTableValuesMatch(
            f'{self.project_id}.{self.dataset_id}.{DEATH}',
            ['person_id', 'death_type_concept_id'], [(1, 12), (2, 21)])

    def test_death_is_not_empty(self):
        """
        Test populate_death throws an error if death table is not empty.
        """

        insert_death = self.jinja_env.from_string("""
            INSERT INTO `{{project}}.{{dataset}}.{{death}}`
            (person_id, death_date, death_type_concept_id)
            VALUES (1, '2022-01-01', 1)
            """).render(project=self.project_id,
                        dataset=self.dataset_id,
                        death=DEATH)

        self.load_test_data([insert_death])

        with self.assertRaises(AssertionError):
            populate_death(self.client, self.project_id, self.dataset_id)
