"""
Integration test for clean by birth year cleaning rule.
Original Issue: DC-392
"""
# Python imports
import os

# Third party imports
from google.cloud import bigquery

# Project Imports
from app_identity import PROJECT_ID
from cdr_cleaner.cleaning_rules.clean_by_birth_year import (CleanByBirthYear)
from common import JINJA_ENV, AOU_REQUIRED, OBSERVATION, PERSON
from resources import get_person_id_tables
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import (
    BaseTest)

INSERT_RAW_DATA = JINJA_ENV.from_string("""
  INSERT INTO `{{project_id}}.{{dataset_id}}.person` (
      person_id
      ,gender_concept_id
      ,year_of_birth
      ,race_concept_id
      ,ethnicity_concept_id
  ) 
  VALUES
  -- records to sandbox --
  (1,0,1799,0,0),
  (4,0,2020,0,0),
  -- records to keep --
  (2,0,1800,0,0),
  (3,0,1975,0,0);

  INSERT INTO `{{project_id}}.{{dataset_id}}.observation` (
      observation_id,
      person_id,
      observation_concept_id,
      observation_date,
      observation_type_concept_id
      )
    VALUES
      -- records to sandbox --
      (1,1,0,'2020-01-01',0),
      (2,4,0,'2020-01-01',0),
      -- records to keep --
      (3,2,0,'2020-01-01',0),
      (4,3,0,'2020-01-01',0)
""")


class CleanByBirthYearTest(BaseTest.CleaningRulesTestBase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

        super().initialize_class_vars()

        # Set the test project identifier
        cls.project_id = os.environ.get(PROJECT_ID)

        # Set the expected test datasets
        cls.dataset_id = os.environ.get('RDR_DATASET_ID')
        cls.sandbox_id = cls.dataset_id + '_sandbox'

        cls.rule_instance = CleanByBirthYear(cls.project_id, cls.dataset_id,
                                             cls.sandbox_id)
        # Generates list of fully qualified table names and their corresponding sandbox table names
        cls.fq_table_names.append(
            f'{cls.project_id}.{cls.dataset_id}.{OBSERVATION}')
        cls.fq_table_names.append(f'{cls.project_id}.{cls.dataset_id}.{PERSON}')

        # fq_sandbox_table_names to delete after the test
        for table in cls.rule_instance.get_sandbox_tablenames():
            cls.fq_sandbox_table_names.append(
                f'{cls.project_id}.{cls.sandbox_id}.{table}')

        # call super to set up the client, create datasets
        cls.up_class = super().setUpClass()

    def setUp(self):
        # Set the test project identifier
        super().setUp()
        raw_data_load_query = INSERT_RAW_DATA.render(project_id=self.project_id,
                                                     dataset_id=self.dataset_id)

        # Load test data
        self.load_test_data([f'{raw_data_load_query}'])

    def test_setup_rule(self):

        has_person_id = get_person_id_tables(AOU_REQUIRED)
        self.assertEqual(set(has_person_id),
                         set(self.rule_instance.affected_tables))

        # run setup_rule and see if the affected_tables is updated
        self.rule_instance.setup_rule(self.client)

        # sees that setup worked and reset affected_tables as expected
        self.assertEqual(set([PERSON, OBSERVATION]),
                         set(self.rule_instance.affected_tables))

    def test_setting_concept_identifiers(self):
        """
        Tests for the loaded test data
        """
        # Expected results list
        tables_and_counts = [{
            'fq_table_name':
                f'{self.project_id}.{self.dataset_id}.{OBSERVATION}',
            'loaded_ids': [1, 2, 3, 4],
            'sandboxed_ids': [3, 4],
            'fields': [
                'observation_id', 'person_id', 'observation_concept_id',
                'observation_type_concept_id'
            ],
            'cleaned_values': [(3, 2, 0, 0), (4, 3, 0, 0)]
        }, {
            'fq_table_name': f'{self.project_id}.{self.dataset_id}.{PERSON}',
            'loaded_ids': [1, 2, 3, 4],
            'sandboxed_ids': [1, 4],
            'fields': [
                'person_id', 'gender_concept_id', 'year_of_birth',
                'race_concept_id', 'ethnicity_concept_id'
            ],
            'cleaned_values': [(2, 0, 1800, 0, 0), (3, 0, 1975, 0, 0)]
        }]

        self.default_test(tables_and_counts)
