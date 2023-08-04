# Python imports
import os
from datetime import datetime, timezone

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
        # intended to be run on the rdr dataset
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
                  (1,1,1,2100000010,2100000009,'2020-01-01',1),
                  -- two yes records --
                  (2,1,1,2100000010,2100000009,'2020-01-01',1),
                  (3,1,1,2100000010,2100000009,'2020-01-01',1),
                  -- one no record --
                  (4,1,1,2100000010,2100000009,'2020-01-01',1),
                  -- two no records --
                  (5,1,1,2100000010,2100000009,'2020-01-01',1),
                  (6,1,1,2100000010,2100000009,'2020-01-01',1),
                  -- no then yes --
                  (7,1,1,2100000010,2100000009,'2020-01-01',1),
                  (8,1,1,2100000010,2100000009,'2020-01-01',1),
                  -- yes then no --
                  (9,1,1,2100000010,2100000009,'2020-01-01',1),
                  (10,1,1,2100000010,2100000009,'2020-01-01',1),
                  -- yes then no then yes --
                  (11,1,1,2100000010,2100000009,'2020-01-01',1),
                  (12,1,1,2100000010,2100000009,'2020-01-01',1),
                  (13,1,1,2100000010,2100000009,'2020-01-01',1),
                  -- no then yes then no --
                  (14,1,1,2100000010,2100000009,'2020-01-01',1),
                  (15,1,1,2100000010,2100000009,'2020-01-01',1),
                  (16,1,1,2100000010,2100000009,'2020-01-01',1)
            """).render(project_id=self.project_id, dataset_id=self.dataset_id)

        # load the test data
        self.load_test_data([observation_query])

    def test_queries(self):
        """
        Validates pre-conditions, test execution and post conditions based on the tables_and_counts variable.

        Tests that new research_device_ids were created for only new person_id/device_id pairs.
        """
        tables_and_counts = [{
            'name':
                WEAR_STUDY,
            'fq_table_name':
                self.fq_table_names[0],
            'fields': [
                 'observation_id',
                  'person_id',
                  'observation_source_concept_id',
                  'value_source_concept_id',
                  'observation_date'
            ],
            'loaded_ids': list(range(1,17)),
            'sandboxed_ids': [],
            'cleaned_values': [
                (1,1,2100000010,2100000009, datetime.now(timezone.utc).date()),
                (2,1,2100000010,2100000009, datetime.now(timezone.utc).date()),
                (3,1,2100000010,2100000009, datetime.strptime('2022-01-01',
                                                       '%Y-%m-%d').date()),
                (4,1,2100000010,2100000009, datetime.now(timezone.utc).date())
            ]
        }]

        self.default_test(tables_and_counts)
