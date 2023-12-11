"""
Integration test for drop_row_duplicates.py

Original Issues: DC-3630

"""

# Python Imports
import os

# Project Imports
from app_identity import PROJECT_ID
from cdr_cleaner.cleaning_rules.drop_row_duplicates import DropRowDuplicates
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import BaseTest


class DropRowDuplicatesTest(BaseTest.CleaningRulesTestBase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

        super().initialize_class_vars()

        # Set the test project identifier
        project_id = os.environ.get(PROJECT_ID)
        cls.project_id = project_id

        # Set the expected test datasets
        dataset_id = os.environ.get('RDR_DATASET_ID')
        cls.dataset_id = dataset_id
        sandbox_id = dataset_id + '_sandbox'
        cls.sandbox_id = sandbox_id

        cls.rule_instance = DropRowDuplicates(project_id, dataset_id,
                                              sandbox_id)

        sb_table_names = cls.rule_instance.get_sandbox_tablenames()
        for table_name in sb_table_names:
            cls.fq_sandbox_table_names.append(
                f'{project_id}.{sandbox_id}.{table_name}')

        cls.fq_table_names = [f'{project_id}.{dataset_id}.observation']

        # call super to set up the client, create datasets, and create
        # empty test tables
        # NOTE:  does not create empty sandbox tables.
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

    def test_field_cleaning(self):
        """
        Tests that the specifications for the CR perform as designed.

        Validates pre conditions, tests execution, and post conditions based on the load
        statements and the tables_and_counts variable.
        """

        tmpl = self.jinja_env.from_string("""
INSERT INTO `{{fq_dataset_name}}.observation` (
    observation_id, person_id, value_source_value, questionnaire_response_id,
    observation_concept_id, observation_date, observation_datetime,
    observation_type_concept_id, value_as_concept_id,
    observation_source_value,observation_source_concept_id,
    value_source_concept_id, value_as_string, visit_occurrence_id
    )
VALUES
-- duplicates on all except observation_id. larger observation_ids are dropped  --   
  (2, 222, 'value_source_value', 222,222, date('2024-01-01'), 
  timestamp('2024-01-01 00:00:00 UTC'), 222, 222,
  "observation_source_value", 222, 222, "value_as_string", 222),
  (3, 222, 'value_source_value', 222,222, date('2024-01-01'), 
  timestamp('2024-01-01 00:00:00 UTC'), 222, 222,
  "observation_source_value", 222, 222, "value_as_string", 222),
  (4, 222, 'value_source_value', 222,222, date('2024-01-01'), 
  timestamp('2024-01-01 00:00:00 UTC'), 222, 222,
  "observation_source_value", 222, 222, "value_as_string", 222),
-- similar rows on all except observation_id and value_as_concept_id. not duplicates. not affected.  --   
  (5, 333, 'value_source_value', 333,333, date('2024-01-01'), 
  timestamp('2024-01-01 00:00:00 UTC'), 333, 333,
  "observation_source_value", 333, 333, "value_as_string", 333),
  (6, 333, 'value_source_value', 333,333, date('2024-01-01'), 
  timestamp('2024-01-01 00:00:00 UTC'), 333, 000,
  "observation_source_value", 333, 333, "value_as_string", 333),
-- duplicates on all except observation_id and value_as_string is null. larger observation_ids are dropped.  --   
  (7, 444, 'value_source_value', 444,444, date('2024-01-01'), 
  timestamp('2024-01-01 00:00:00 UTC'), 444, NULL,
  "observation_source_value", 444, 444, "value_as_string", 444),
  (8, 444, 'value_source_value', 444,444, date('2024-01-01'), 
  timestamp('2024-01-01 00:00:00 UTC'), 444, NULL,
  "observation_source_value", 444, 444, "value_as_string", 444)
""")

        query = tmpl.render(fq_dataset_name=self.fq_dataset_name)
        self.load_test_data([query])

        # Expected results list
        tables_and_counts = [{
            'fq_table_name': self.fq_table_names[0],
            'fq_sandbox_table_name': self.fq_sandbox_table_names[0],
            'loaded_ids': [2, 3, 4, 5, 6, 7, 8],
            'sandboxed_ids': [3, 4, 8],
            'fields': ['observation_id', 'person_id'],
            'cleaned_values': [(2, 222), (5, 333), (6, 333), (7, 444)]
        }]

        self.default_test(tables_and_counts)
