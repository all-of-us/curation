"""
Integration test for updating answers where the participant skipped answering but the answer was registered as -1.

Original Issue: DC-536
"""
# Python imports
import os

# Third party imports
from dateutil.parser import parse

# Project Imports
from app_identity import PROJECT_ID
from cdr_cleaner.cleaning_rules.update_ppi_negative_pain_level import UpdatePpiNegativePainLevel
from common import JINJA_ENV, OBSERVATION, VOCABULARY_TABLES
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import BaseTest

test_query = JINJA_ENV.from_string("""select * from `{{intermediary_table}}`""")

INSERT_RAW_DATA = JINJA_ENV.from_string("""
  INSERT INTO `{{project_id}}.{{dataset_id}}.observation` (
      observation_id,
      person_id,
      observation_concept_id,
      observation_date,
      observation_type_concept_id,
      value_as_number,
      value_as_string,
      value_as_concept_id,
      observation_source_concept_id,
      value_source_concept_id,
      value_source_value
      )
    VALUES
      (1,1,0,date('2020-01-01'),1,-1,'',0,1585747,1,'Test Value'),
      (2,1,0,date('2020-01-01'),1,-1,'',0,1585747,1,'Test Value'),
      (3,1,0,date('2020-01-01'),1,3,'',0,1585747,1,'Test Value'),
      (4,1,0,date('2020-01-01'),1,4,'',0,1585747,1,'Test Value')
""")


class UpdatePpiNegativePainLevelTest(BaseTest.CleaningRulesTestBase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

        super().initialize_class_vars()

        # Set the test project identifier
        cls.project_id = os.environ.get(PROJECT_ID)

        # Set the expected test datasets
        cls.dataset_id = os.environ.get('COMBINED_DATASET_ID')
        cls.sandbox_id = f"{cls.dataset_id}_sandbox"
        cls.vocabulary_id = os.environ.get('VOCABULARY_DATASET')

        cls.rule_instance = UpdatePpiNegativePainLevel(cls.project_id,
                                                       cls.dataset_id,
                                                       cls.sandbox_id)
        # Generates list of fully qualified table names and their corresponding sandbox table names
        cls.fq_table_names.append(
            f'{cls.project_id}.{cls.dataset_id}.{OBSERVATION}')
        for table in VOCABULARY_TABLES:
            cls.fq_table_names.append(
                f'{cls.project_id}.{cls.dataset_id}.{table}')

        for table in cls.rule_instance.get_sandbox_tablenames():
            cls.fq_sandbox_table_names.append(
                f'{cls.project_id}.{cls.sandbox_id}.{table}')

        # call super to set up the client, create datasets
        super().setUpClass()

    def setUp(self):
        # Set the test project identifier
        super().setUp()
        self.copy_vocab_tables(self.vocabulary_id)
        raw_data_load_query = INSERT_RAW_DATA.render(project_id=self.project_id,
                                                     dataset_id=self.dataset_id)

        self.date = parse('2020-01-01').date()
        # Load test data
        self.load_test_data([f'{raw_data_load_query}'])

    def test_setting_concept_identifiers(self):
        """
        Tests concept_identifiers are updated or unchanged for the loaded test data
        """
        # Expected results list
        tables_and_counts = [{
            'fq_table_name':
                f'{self.project_id}.{self.dataset_id}.{OBSERVATION}',
            'loaded_ids': [1, 2, 3, 4],
            'sandboxed_ids': [1, 3],
            'fields': [
                'observation_id', 'person_id', 'observation_concept_id',
                'observation_date', 'observation_type_concept_id',
                'value_as_number', 'value_as_string', 'value_as_concept_id',
                'observation_source_concept_id', 'value_source_concept_id',
                'value_source_value'
            ],
            'cleaned_values': [
                (1, 1, 0, self.date, 1, None, 'PMI Skip', 903096, 1585747,
                 903096, 'PMI_Skip'),
                (2, 1, 0, self.date, 1, None, 'PMI Skip', 903096, 1585747,
                 903096, 'PMI_Skip'),
                (3, 1, 0, self.date, 1, 3, '', 0, 1585747, 1, 'Test Value'),
                (4, 1, 0, self.date, 1, 4, '', 0, 1585747, 1, 'Test Value')
            ]
        }]

        self.default_test(tables_and_counts)
