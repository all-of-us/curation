"""Integration test for xyz
"""
# Python imports
import os

# Third party imports
from dateutil import parser

# Project imports
from app_identity import PROJECT_ID
from cdr_cleaner.cleaning_rules.drop_race_ethnicity_gender_observation import DropRaceEthnicityGenderObservation
from common import OBSERVATION, UNIONED_EHR
from resources import fields_for
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import BaseTest


class DropRaceEthnicityGenderObservationTest(BaseTest.CleaningRulesTestBase):

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
        dataset_id = os.environ.get('BIGQUERY_DATASET_ID')
        cls.dataset_id = dataset_id
        sandbox_id = dataset_id + '_sandbox'
        cls.sandbox_id = sandbox_id

        cls.rule_instance = DropRaceEthnicityGenderObservation(
            project_id, dataset_id, sandbox_id)

        cls.fq_sandbox_table_names = [
            f'{cls.project_id}.{cls.sandbox_id}.{sb_table_name}'
            for sb_table_name in cls.rule_instance.get_sandbox_tablenames()
        ]

        cls.fq_table_names = [
            f'{project_id}.{dataset_id}.{table_name}'
            for table_name in cls.rule_instance.affected_tables
        ]

        super().setUpClass()

    def setUp(self):
        """
        """
        self.date = parser.parse('2020-05-05').date()
        self.client.create_tables(self.fq_table_names,
                                  exists_ok=True,
                                  fields=fields_for(OBSERVATION))

    def test_drop_race_ethnicity_gender_observation(self):
        """
        """
        insert_observation = self.jinja_env.from_string("""
            INSERT INTO `{{fq_table_names}}`
                (observation_id, person_id, observation_date, observation_concept_id, observation_type_concept_id)
            VALUES
                (1, 1, date('2020-05-05'), 4013886, 0),
                (2, 1, date('2020-05-05'), 4135376, 0),
                (3, 1, date('2020-05-05'), 4271761, 0),
                (4, 1, date('2020-05-05'), 9999999, 0)
        """).render(fq_table_names=self.fq_table_names[0])

        queries = [insert_observation]
        self.load_test_data(queries)

        tables_and_counts = [
            {
                'fq_table_name':
                    f'{self.project_id}.{self.dataset_id}.{UNIONED_EHR}_{OBSERVATION}',
                'fq_sandbox_table_name':
                    f'{self.fq_sandbox_table_names[0]}',
                'fields': [
                    'observation_id',
                    'person_id',
                    'observation_date',
                    'observation_concept_id',
                    'observation_type_concept_id',
                ],
                'loaded_ids': [1, 2, 3, 4],
                'sandboxed_ids': [1, 2, 3],
                'cleaned_values': [(4, 1, self.date, 9999999, 0),],
            },
        ]

        self.default_test(tables_and_counts)
