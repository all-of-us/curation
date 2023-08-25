"""
Integration test for ConvertPrePostCoordinatedConcepts.
"""

# Python Imports
import os

# Project Imports
from app_identity import PROJECT_ID
from common import JINJA_ENV, OBSERVATION, VOCABULARY_TABLES
from cdr_cleaner.cleaning_rules.convert_pre_post_coordinated_concepts import ConvertPrePostCoordinatedConcepts
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import BaseTest

LOAD_QUERY = JINJA_ENV.from_string("""
    INSERT INTO `{{project_id}}.{{dataset_id}}.observation` (
        observation_id,
        person_id,
        value_source_concept_id,
        observation_concept_id, 
        value_as_concept_id,
        observation_date,
        observation_type_concept_id,
        value_source_value
    )
    VALUES
        (1, 11, 40192464, 40192464, 40770206, date('2000-01-01'), 0, 'SDOH_28'),
        (2, 11, 1585793, 4058563, 40767339, date('2000-01-01'), 0, 'HysterectomyHistory_Yes'),
        (3, 11, 43530574, 43528514, 4180793, date('2000-01-01'), 0, 'SonCancerCondition_PancreaticCancer'),
        (4, 14, 43529989, 43530381, 141095, date('2000-01-01'), 0, 'AcneCurrently_Yes'),
        (5, 14, 43529625, 43528515, 443392, date('2000-01-01'), 0, 'CancerCondition_OtherCancer'),
        (6, 16, 43528574, 43528630, 45883358, date('2000-01-01'), 0, 'GrandparentDigestiveCondition_ColonPolyps'),
        (7, 17, 43528355, 1740608, 141095, date('2000-01-01'), 0, 'OtherConditions_Acne')
""")


class ConvertPrePostCoordinatedConceptsTest(BaseTest.CleaningRulesTestBase):

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
        cls.rule_instance = ConvertPrePostCoordinatedConcepts(
            cls.project_id, cls.dataset_id, cls.sandbox_id)

        cls.fq_table_names = [
            f'{cls.project_id}.{cls.dataset_id}.{table}'
            for table in [OBSERVATION] + VOCABULARY_TABLES
        ]

        cls.fq_sandbox_table_names = [
            f'{cls.project_id}.{cls.sandbox_id}.{cls.rule_instance.sandbox_table_for(OBSERVATION)}'
        ]

        cls.up_class = super().setUpClass()

    def setUp(self):
        """
        Create empty tables for the rule to run on
        """
        super().setUp()

        self.load_test_data([
            LOAD_QUERY.render(project_id=self.project_id,
                              dataset_id=self.dataset_id)
        ])
        self.copy_vocab_tables(self.vocabulary_id)

    def test_convert_pre_post_coordinated_concepts(self):
        """
        Test cases for each observation ID:
        1: 40192464... Standard concept. This CR does not affect it.
        2:  1585793... Non-standard concept, but no "Maps to value" relationship. This CR does not affect it.
        3: 43530574... Non-standard concept, and it has 1 "Maps to" and 1 "Maps to value" relationships.
                       This CR removes the record and create 1 new record with new observation_id.
        4: 43529989... Non-standard concept, and it has 1 "Maps to" and 2 "Maps to value" relationships.
                       This CR removes the record and create 2 new records with new observation_ids.
        5: 43529625... Non-standard concept, and it has 2 "Maps to" and 1 "Maps to value" relationships.
                       This CR removes the record and create 2 new records with new observation_ids.
        6: 43528574... Non-standard concept, and it has 2 "Maps to" and 2 "Maps to value" relationships.
                       This CR removes the record and create 4 new records with new observation_ids.
        7:  43528355... Non-standard concept, Only "Maps to value" relationship. This CR does not affect it.
        """

        tables_and_counts = [{
            'fq_table_name':
                f'{self.project_id}.{self.dataset_id}.observation',
            'fq_sandbox_table_name':
                self.fq_sandbox_table_names[0],
            'loaded_ids': [1, 2, 3, 4, 5, 6, 7],
            'sandboxed_ids': [3, 4, 5, 6],
            'fields': [
                'observation_id', 'value_source_concept_id',
                'observation_concept_id', 'value_as_concept_id'
            ],
            'cleaned_values': [
                (1, 40192464, 40192464, 40770206),
                (2, 1585793, 4058563, 40767339),
                (100000000003, 43530574, 4052795, 4180793),
                (100000000004, 43529989, 43530381, 141095),
                (200000000004, 43529989, 43530381, 45877994),
                (100000000005, 43529625, 4171594, 443392),
                (200000000005, 43529625, 43528515, 443392),
                (100000000006, 43528574, 713134, 4285898),
                (200000000006, 43528574, 713134, 45883358),
                (300000000006, 43528574, 43528630, 4285898),
                (400000000006, 43528574, 43528630, 45883358),
                (7, 43528355, 1740608, 141095),
            ]
        }]

        self.default_test(tables_and_counts)
