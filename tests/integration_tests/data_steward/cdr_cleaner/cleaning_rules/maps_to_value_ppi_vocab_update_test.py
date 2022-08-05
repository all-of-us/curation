"""
Integration test for MapsToValuePpiVocabUpdate.
"""

# Python imports
import os

# Third party imports
from dateutil import parser

# Project imports
from app_identity import PROJECT_ID
from cdr_cleaner.cleaning_rules.maps_to_value_ppi_vocab_update import MapsToValuePpiVocabUpdate
from common import JINJA_ENV, OBSERVATION, VOCABULARY_TABLES
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import BaseTest

LOAD_QUERY = JINJA_ENV.from_string("""
    INSERT INTO `{{project_id}}.{{dataset_id}}.observation` (
        observation_id,
        person_id,
        value_source_concept_id,
        value_as_concept_id,
        observation_concept_id, 
        observation_date,
        observation_type_concept_id
    )
    VALUES
        (1, 11, 1586202, 45876662, 40771103, date('2000-01-01'), 0),
        (2, 12, 1586203, 40771103, 40771103, date('2000-01-01'), 0),
        (3, 13, 1586204, 40771103, 40771103, date('2000-01-01'), 0),
        (4, 14, 1585730, 45881924, 40764341, date('2000-01-01'), 0),
        (5, 15, 1585731, 40764341, 40764341, date('2000-01-01'), 0),
        (6, 16, 1585733, 40764341, 40764341, date('2000-01-01'), 0)
""")


class MapsToValuePpiVocabUpdateTest(BaseTest.CleaningRulesTestBase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

        super().initialize_class_vars()

        cls.project_id = os.environ.get(PROJECT_ID)
        cls.dataset_id = os.environ.get('RDR_DATASET_ID')
        cls.sandbox_id = cls.dataset_id + '_sandbox'
        cls.vocabulary_id = os.environ.get('VOCABULARY_DATASET')

        cls.rule_instance = MapsToValuePpiVocabUpdate(cls.project_id,
                                                      cls.dataset_id,
                                                      cls.sandbox_id)

        cls.fq_table_names = [
            f'{cls.project_id}.{cls.dataset_id}.{table}'
            for table in [OBSERVATION] + VOCABULARY_TABLES
        ]

        cls.fq_sandbox_table_names = [
            f'{cls.project_id}.{cls.sandbox_id}.{cls.rule_instance.sandbox_table_for(OBSERVATION)}'
        ]

        cls.up_class = super().setUpClass()

    def setUp(self):

        super().setUp()

        self.load_test_data([
            LOAD_QUERY.render(project_id=self.project_id,
                              dataset_id=self.dataset_id)
        ])
        self.copy_vocab_tables(self.vocabulary_id)

    def test_maps_to_value_ppi_vocab_update(self):
        """
        Tests that the specifications for the queries perform as designed.
        """

        self.maxDiff = None
        test_date = parser.parse('2000-01-01').date()

        tables_and_counts = [{
            'fq_table_name':
                f'{self.project_id}.{self.dataset_id}.{OBSERVATION}',
            'fq_sandbox_table_name':
                self.fq_sandbox_table_names[0],
            'loaded_ids': [1, 2, 3, 4, 5, 6],
            'sandboxed_ids': [2, 3, 5, 6],
            'fields': [
                'observation_id', 'person_id', 'value_source_concept_id',
                'value_as_concept_id', 'observation_concept_id',
                'observation_date', 'observation_type_concept_id'
            ],
            'cleaned_values': [
                (1, 11, 1586202, 45876662, 40771103, test_date, 0),
                (2, 12, 1586203, 45879058, 40771103, test_date, 0),
                (3, 13, 1586204, 45885058, 40771103, test_date, 0),
                (4, 14, 1585730, 45881924, 40764341, test_date, 0),
                (5, 15, 1585731, 45884457, 40764341, test_date, 0),
                (6, 16, 1585733, 45876387, 40764341, test_date, 0)
            ]
        }]

        self.default_test(tables_and_counts)
