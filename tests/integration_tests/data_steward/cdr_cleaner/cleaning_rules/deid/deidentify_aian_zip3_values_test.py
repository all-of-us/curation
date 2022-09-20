"""
Integration test for deidentify_aian_zip3_values.py

Original Issue: DC-2706
"""
# Python Imports
import os

# Project Imports
from common import OBSERVATION
from app_identity import PROJECT_ID
from cdr_cleaner.cleaning_rules.deid.deidentify_aian_zip3_values import DeidentifyAIANZip3Values
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import \
    BaseTest


class DeidentifyAIANZip3ValuesTest(BaseTest.CleaningRulesTestBase):

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
        cls.sandbox_id = cls.dataset_id + '_sandbox'

        cls.rule_instance = DeidentifyAIANZip3Values(cls.project_id,
                                                     cls.dataset_id,
                                                     cls.sandbox_id)

        # Generates list of fully qualified table names
        cls.fq_table_names.append(
            f'{cls.project_id}.{cls.dataset_id}.{OBSERVATION}')
        sandbox_table_name = cls.rule_instance.sandbox_table_for(OBSERVATION)
        cls.fq_sandbox_table_names.append(
            f'{cls.project_id}.{cls.sandbox_id}.{sandbox_table_name}')

        # call super to set up the client, create datasets
        cls.up_class = super().setUpClass()

    def setUp(self):
        """
        Create empty tables for the rule to run on
        """
        # Create domain tables required for the test
        super().setUp()

        # Load the test data
        observation_data_template = self.jinja_env.from_string("""
            INSERT INTO `{{project_id}}.{{dataset_id}}.observation`
            (
                observation_id,
                person_id,
                observation_concept_id,
                observation_type_concept_id,
                observation_date,
                value_as_string,
                value_as_concept_id,
                observation_source_concept_id,
                value_source_concept_id
            )
            VALUES
              -- First oarticipant to be identified as AIAN as --
              -- observation_source_concept_id =1586140 and value_source_concept_id = 1586141 -- 
              (1, 1, 0, 0, '2020-01-01', '', 0, 1586140, 1586141),
              -- Second participant doesnot identifies as AIAN as --
              -- observation_source_concept_id = 0 and value_source_concept_id = 0 -- 
              (2, 2, 0, 0, '2020-01-01', '', 0, 0, 0),
              -- Zip3 and state data for participant 1 where the value_as_string is set to 354** for --
              -- observation_source_concept_id = 1585250. and state row i.e observation_source_concept_id = 1585249 --
              -- is set to value_as_concept = 1234234 and value_as_string is set to '', --
              -- which sholuld be updated by the cleaning rule and sets the value_as_string to null and --
              -- value_As_concept_id to  2000000011 -- 
              (3, 1, 0, 0, '2020-01-01', '354**', 0, 1585250, 0),
              (4, 1, 0, 0, '2020-01-01', '', 1234234, 1585249, 0),
              -- Zip3 and state rows for 2nd participant where the value_as_string is set to 123** for --
              -- observation_source_concept_id = 1585250 and state row i.e observation_source_concept_id = 1585249 --
              -- is set to value_as_concept = 1234567 and value_as_string is set to ''. --
              -- which will not be modified by the cleaning rule. --
              (5, 2, 0, 0, '2020-01-01', '', 1234567, 1585249, 0),
              (6, 2, 0, 0, '2020-01-01', '123**', 0, 1585250, 0),
              -- Adding second AIAN participant --
              (7, 3, 0, 0, '2020-01-01', '', 0, 1586140, 1586141),
              (8, 3, 0, 0, '2020-01-01', '123**', 0, 1585250, 0),
              (9, 3, 0, 0, '2020-01-01', '', 1234234, 1585249, 0)
            """)

        insert_observation_query = observation_data_template.render(
            project_id=self.project_id, dataset_id=self.dataset_id)

        # Load test data
        self.load_test_data([f'''{insert_observation_query};'''])

    def test_deidentify_aian_zip3_values(self):
        # Expected results list
        tables_and_counts = [{
            'fq_table_name':
                f'{self.project_id}.{self.dataset_id}.observation',
            'fq_sandbox_table_name':
                f'{self.project_id}.{self.sandbox_id}.'
                f'{self.rule_instance.sandbox_table_for("observation")}',
            'loaded_ids': [1, 2, 3, 4, 5, 6, 7, 8, 9],
            'sandboxed_ids': [3, 4, 8, 9],
            'fields': [
                'observation_id',
                'person_id',
                'value_as_string',
                'value_as_concept_id',
                'observation_source_concept_id',
                'value_source_concept_id',
            ],
            'cleaned_values': [(1, 1, '', 0, 1586140, 1586141),
                               (2, 2, '', 0, 0, 0),
                               (3, 1, '000**', 0, 1585250, 0),
                               (4, 1, None, 2000000011, 1585249, 0),
                               (5, 2, '', 1234567, 1585249, 0),
                               (6, 2, '123**', 0, 1585250, 0),
                               (7, 3, '', 0, 1586140, 1586141),
                               (8, 3, '000**', 0, 1585250, 0),
                               (9, 3, None, 2000000011, 1585249, 0)]
        }]

        self.default_test(tables_and_counts)
