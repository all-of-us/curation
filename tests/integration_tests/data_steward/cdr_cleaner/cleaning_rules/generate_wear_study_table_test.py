"""
integration test for generate_wear_study_table
"""

# Python imports
import os

# Third party imports
from dateutil import parser

# Project imports
from common import WEAR_STUDY, OBSERVATION
from app_identity import PROJECT_ID
from cdr_cleaner.cleaning_rules.generate_wear_study_table import GenerateWearStudyTable
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import BaseTest


class GenerateWearStudyTableTest(BaseTest.CleaningRulesTestBase):

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
        sandbox_id = dataset_id + '_sandbox'
        cls.sandbox_id = sandbox_id

        cls.rule_instance = GenerateWearStudyTable(cls.project_id,
                                                   cls.dataset_id,
                                                   cls.sandbox_id)

        cls.affected_tables = [OBSERVATION, WEAR_STUDY]

        # Generates list of fully qualified table names and their corresponding sandbox table names
        cls.fq_table_names = [
            f"{cls.project_id}.{cls.dataset_id}.{table}"
            for table in cls.affected_tables
        ]

        cls.fq_sandbox_table_names = []

        # call super to set up the client, create datasets
        cls.up_class = super().setUpClass()

    def setUp(self):
        # create tables
        super().setUp()

        self.date_one = parser.parse('2021-01-01').date()
        self.date_two = parser.parse('2022-01-01').date()
        self.date_three = parser.parse('2023-01-01').date()


        observation_query = self.jinja_env.from_string("""
              INSERT INTO `{{project_id}}.{{dataset_id}}.observation` (
                  observation_id,
                  person_id,
                  observation_concept_id,
                  observation_source_concept_id,
                  value_source_concept_id,
                  observation_date,
                  observation_type_concept_id
                  )
                VALUES
                  -- one yes record --
                  (1,1,1,2100000010,2100000009,date('2021-01-01'),1),
                  -- two yes records --
                  (1,2,1,2100000010,2100000009,date('2021-01-01'),1),
                  (1,2,1,2100000010,2100000009,date('2022-01-01'),1),
                  -- one no record --
                  (1,3,1,2100000010,2100000008,date('2021-01-01'),1),
                  -- two no records then a yes --
                  (1,4,1,2100000010,2100000008,date('2021-01-01'),1),
                  (1,4,1,2100000010,2100000008,date('2022-01-01'),1),
                  (1,4,1,2100000010,2100000009,date('2023-01-01'),1),
                  -- two yes records then a no --
                  (1,5,1,2100000010,2100000009,date('2021-01-01'),1),
                  (1,5,1,2100000010,2100000009,date('2022-01-01'),1),
                  (1,5,1,2100000010,2100000008,date('2023-01-01'),1),
                  -- yes then no then yes --
                  (1,6,1,2100000010,2100000009,date('2021-01-01'),1),
                  (1,6,1,2100000010,2100000008,date('2022-01-01'),1),
                  (1,6,1,2100000010,2100000009,date('2023-01-01'),1)

            """).render(project_id=self.project_id, dataset_id=self.dataset_id)

        # load the test data
        self.load_test_data([observation_query])

    def test_queries(self):
        """
        Validates pre-conditions, test execution and post conditions based on the tables_and_counts variable.

        """
        tables_and_counts = [{
            'name':
                WEAR_STUDY,
            'fq_table_name':
                self.fq_table_names[1],
            'fields': [
                  'person_id',
                  'resultsconsent_wear',
                  'wear_consent_start_date',
                  'wear_consent_end_date'
            ],
            'loaded_ids': [],
            'sandboxed_ids': [],
            'cleaned_values': [
                # one yes record.
                (1, 'Yes', self.date_one, None),
                # two yes records. earliest recorded
                (2, 'Yes', self.date_one, None),
                # two yes records and one no. earliest dates recorded
                (5, 'Yes', self.date_one, self.date_three),
                # yes, then no, then yes. earliest dates recorded
                (6, 'Yes', self.date_one, self.date_two)
            ]
        }]

        self.default_test(tables_and_counts)
