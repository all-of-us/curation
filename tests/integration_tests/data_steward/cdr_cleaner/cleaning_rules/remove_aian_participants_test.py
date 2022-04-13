"""
Integration test for remove_aian_participants module

Original Issues: DC-850, DC-1199

(description comes here).

"""

# Python Imports
import os

# Project Imports
from app_identity import PROJECT_ID
import bq_utils
from cdr_cleaner.cleaning_rules.remove_aian_participants import RemoveAianParticipants
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import BaseTest


class RemoveAianParticipantsTest(BaseTest.CleaningRulesTestBase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

        super().initialize_class_vars()

        # Set the test project identifier
        cls.project_id = os.environ.get(PROJECT_ID)

        # Set the expected test datasets
        cls.dataset_id = bq_utils.get_combined_dataset_id()
        cls.sandbox_id = cls.dataset_id + '_sandbox'

        cls.rule_instance = RemoveAianParticipants(cls.project_id,
                                                   cls.dataset_id,
                                                   cls.sandbox_id)

        sb_table_names = cls.rule_instance.get_sandbox_tablenames()

        cls.fq_sandbox_table_names = [
            f'{cls.project_id}.{cls.sandbox_id}.{table_name}'
            for table_name in sb_table_names
        ]

        cls.fq_table_names = [
            f'{cls.project_id}.{cls.dataset_id}.observation',
        ]

        # call super to set up the client, create datasets, and create empty test tables
        # NOTE: It does not create empty sandbox tables.
        super().setUpClass()

    def setUp(self):
        """
        Create common information for tests.

        Creates common expected parameter types from cleaned tables and a common
        fully qualified (fq) dataset name string to load the data.
        """
        fq_dataset_name = self.fq_table_names[0].split('.')
        self.fq_dataset_name = '.'.join(fq_dataset_name[:-1])

        super().setUp()

    def test_remove_aian_participants(self):
        """
        (Description comes here)
        """

        tmpl = self.jinja_env.from_string("""
            INSERT INTO `{{fq_dataset_name}}.observation` (
                observation_id, person_id, observation_concept_id, observation_date,
                observation_datetime, observation_type_concept_id, observation_source_concept_id,
                observation_source_value, value_source_value, questionnaire_response_id
            )
            VALUES
            (123, 1111111, 1384662, DATE('2015-09-15'), TIMESTAMP('2015-09-15'), 45905771, 1384662,
            'InfectiousDiseases_InfectiousDiseaseCondition', 'InfectiousDiseaseCondition_Chickenpox', 111111111),
            (234, 1111111, 1384662, DATE('2015-07-15'), TIMESTAMP('2015-07-15'),45905771, 1384662,
            'InfectiousDiseases_InfectiousDiseaseCondition', 'InfectiousDiseaseCondition_NoInfectiousDisease', 222222222),
            (345, 2222222, 3044964, DATE('2020-07-15'), TIMESTAMP('2020-07-15'), 45905771, 1333276,
            'phq_9_4', 'COPE_A_75', 333333333),
            (456, 2222222, 3044098, DATE('2020-08-15'), TIMESTAMP('2020-08-15'), 45905771, 1333276,
            'phq_9_5', 'COPE_A_161', 444444444),
            (567, 2222222, 3044964, DATE('2020-07-15'), TIMESTAMP('2020-07-15'), 45905771, 1333276,
            'phq_9_4', 'COPE_A_75', 555555555),
            -- 567 is sandboxed and 678 is retained since it has higher observation_id --
            (678, 2222222, 3044964, DATE('2020-07-15'), TIMESTAMP('2020-07-15'), 45905771, 1333276,
            'phq_9_4', 'COPE_A_75', 555555555),
            (789, 2222222, 3044964, DATE('2020-07-15'), TIMESTAMP('2020-07-15'), 45905771, 1333276,
            'phq_9_4', 'COPE_A_161', 555555555),
            (890, 2222222, 3044098, DATE('2020-12-15'), TIMESTAMP('2020-12-15'), 45905771, 1333276,
            'phq_9_5', 'COPE_A_161', 666666666)""")

        tmp2 = self.jinja_env.from_string("""
            INSERT INTO `{{fq_dataset_name}}.cope_survey_semantic_version_map` (
                participant_id, questionnaire_response_id,
                semantic_version,
                cope_month)
            VALUES (2222222, 333333333, '4', 'jul' ),
                (2222222, 555555555, '4', 'jul' ),
                (2222222, 444444444, '6', 'aug' ),
                (2222222, 666666666, '14','dec' )
                    """)

        query = tmpl.render(fq_dataset_name=self.fq_dataset_name)
        self.load_test_data([query])

        query = tmp2.render(fq_dataset_name=self.fq_dataset_name)
        self.load_test_data([query])

        # Expected results list
        tables_and_counts = [{
            'fq_table_name':
                '.'.join([self.fq_dataset_name, 'observation']),
            'fq_sandbox_table_name':
                self.fq_sandbox_table_names[0],
            'loaded_ids': [123, 234, 345, 456, 567, 678, 789, 890],
            'sandboxed_ids': [345, 567],
            'fields': ['observation_id', 'questionnaire_response_id'],
            'cleaned_values': [(123, 111111111), (234, 222222222),
                               (456, 444444444), (678, 555555555),
                               (789, 555555555), (890, 666666666)]
        }]

        self.default_test(tables_and_counts)
