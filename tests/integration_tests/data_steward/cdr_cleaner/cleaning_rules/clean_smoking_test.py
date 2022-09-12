"""
Integration test for CleanSmokingPpi
"""

# Python imports
import os

# Project imports
from app_identity import PROJECT_ID
from cdr_cleaner.cleaning_rules.clean_smoking_ppi import CleanSmokingPpi
from common import JINJA_ENV, OBSERVATION
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import BaseTest

LOAD_QUERY = JINJA_ENV.from_string("""
    INSERT INTO `{{project_id}}.{{dataset_id}}.observation` (
        observation_id,
        person_id,
        observation_concept_id, 
        observation_source_concept_id,
        value_as_concept_id,
        value_source_concept_id
    )
    VALUES
        (1, 11, 1, 1585866, 45877994, 1),
        (2, 12, 2, 903062, 45877994, 2),
        (3, 13, 40770349, 1585873, 1177221, 903079),
        (4, 14, 4, 1585865, 45877994, 4),
        (5, 15, 5, 1585871, 45877994, 5),
        (6, 16, 40766333, 1585864, NULL, NULL),
        (7, 17, 7, 1585864, 903096, 7),
        (8, 18, 8, 1585870, 903096, 8),
        (9, 19, 40770349, 1585873, 903096, 903096),
        (10, 20, 40766333, 1585864, 45876636, 903087)
""")


class CleanSmokingPpiTest(BaseTest.CleaningRulesTestBase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

        super().initialize_class_vars()

        cls.project_id = os.environ.get(PROJECT_ID)
        cls.dataset_id = os.environ.get('RDR_DATASET_ID')
        cls.sandbox_id = cls.dataset_id + '_sandbox'

        cls.rule_instance = CleanSmokingPpi(cls.project_id,
                                            cls.dataset_id,
                                            cls.sandbox_id)

        cls.fq_table_names = [f'{cls.project_id}.{cls.dataset_id}.{OBSERVATION}']

        cls.fq_sandbox_table_names = [
            f'{cls.project_id}.{cls.sandbox_id}.{cls.rule_instance.sandbox_table_for(OBSERVATION)}'
        ]

        super().setUpClass()

    def setUp(self):

        super().setUp()

        self.load_test_data([
            LOAD_QUERY.render(project_id=self.project_id,
                              dataset_id=self.dataset_id)
        ])

    def test_setup_rule(self):
        # run setup_rule and see if the affected_table is updated
        self.rule_instance.setup_rule(self.client)

        # sees that setup worked and reset affected_tables as expected
        self.assertEqual(set(OBSERVATION),
                         set(self.rule_instance.affected_tables))

    def test_clean_smoking_ppi(self):
        """
        Tests that the specifications for the queries perform as designed.
        """

        tables_and_counts = [{
            'fq_table_name':
                f'{self.project_id}.{self.dataset_id}.{OBSERVATION}',
            'fq_sandbox_table_name':
                self.fq_sandbox_table_names[0],
            'loaded_ids': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            'sandboxed_ids': [1, 2, 4, 5, 7, 8],
            'fields': [
                'observation_id', 'person_id', 'observation_concept_id',
                'observation_source_concept_id', 'value_as_concept_id',
                'value_source_concept_id'],
            'cleaned_values': [
                (1, 11, 40766333, 1585864, 1177221, 903079),
                (2, 12, 1585870, 1585870, 1177221, 903079),
                (3, 13, 40770349, 1585873, 1177221, 903079),
                (4, 14, 40766333, 1585864, 45876636, 903087),
                (5, 15, 1585870, 1585870, 45876636, 903087),
                (6, 16, 40766333, 1585864, '', ''),
                (7, 17, 40766333, 1585864, 903096, 903096),
                (8, 18, 1585870, 1585870, 903096, 903096),
                (9, 19, 40770349, 1585873, 903096, 903096),
                (10, 20, 40766333, 1585864, 45876636, 903087)]
        }]

        self.default_test(tables_and_counts)
