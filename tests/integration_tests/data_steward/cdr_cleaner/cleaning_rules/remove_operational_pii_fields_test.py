"""
Integration test for RemoveOperationalPiiFields
"""

# Python imports
import os

# Project imports
from app_identity import PROJECT_ID
from cdr_cleaner.cleaning_rules.remove_operational_pii_fields import (
    RemoveOperationalPiiFields, OPERATIONAL_PII_FIELDS_TABLE,
    INTERMEDIARY_TABLE)
from common import JINJA_ENV, OBSERVATION
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import BaseTest

LOAD_QUERY = JINJA_ENV.from_string("""
    INSERT INTO `{{project_id}}.{{dataset_id}}.observation` (
        observation_id,
        person_id,
        observation_concept_id, 
        observation_source_value,
        observation_date,
        observation_type_concept_id
    )
    VALUES
        (1, 11, 1,"ArizonaSitePairing_AZWalgreensSitepairingLocation",date('2000-01-01'), 0),
        (2, 12, 1,"CaliforniaSitePairing_CaliforniaPairing",date('2000-01-01'), 0),
        (3, 13, 0,"DiagnosedHealthCondition_DaughterCirculatoryCondit",date('2000-01-01'), 0),
        (4, 14, 0,"OrganTransplant_BoneTransplantDate",date('2000-01-01'), 0),
        (5, 15, 1,"Snap_MusicTech",date('2000-01-01'), 0),
        (6, 16, 1,"Snap_Frequency",date('2000-01-01'), 0)
""")


class RemoveOperationalPiiFieldsTest(BaseTest.CleaningRulesTestBase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

        super().initialize_class_vars()

        cls.project_id = os.environ.get(PROJECT_ID)
        cls.dataset_id = os.environ.get('RDR_DATASET_ID')
        cls.sandbox_id = f'{cls.dataset_id}_sandbox'

        cls.rule_instance = RemoveOperationalPiiFields(cls.project_id,
                                                       cls.dataset_id,
                                                       cls.sandbox_id)

        cls.fq_table_names = [
            f'{cls.project_id}.{cls.dataset_id}.{OBSERVATION}'
        ]

        cls.fq_sandbox_table_names = [
            f'{cls.project_id}.{cls.sandbox_id}.{OPERATIONAL_PII_FIELDS_TABLE}',
            f'{cls.project_id}.{cls.sandbox_id}.{INTERMEDIARY_TABLE}'
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
        self.assertEqual(set([OBSERVATION]),
                         set(self.rule_instance.affected_tables))

    def test_remove_operational_pii_fields(self):
        """
        Tests that the specifications for the queries perform as designed.
        """

        tables_and_counts = [{
            'fq_table_name':
                f'{self.project_id}.{self.dataset_id}.{OBSERVATION}',
            'fq_sandbox_table_name':
                f'{self.project_id}.{self.sandbox_id}.{INTERMEDIARY_TABLE}',
            'loaded_ids': [1, 2, 3, 4, 5, 6],
            'sandboxed_ids': [1, 2, 5, 6],
            'fields': [
                'observation_id', 'person_id', 'observation_concept_id',
                'observation_source_value'
            ],
            'cleaned_values': [
                (3, 13, 0,
                 "DiagnosedHealthCondition_DaughterCirculatoryCondit"),
                (4, 14, 0, "OrganTransplant_BoneTransplantDate")
            ]
        }]

        self.default_test(tables_and_counts)
