"""
Integration test for the null_concept_ids_for_numeric_ppi module.

Nullify concept ids for numeric PPIs from the RDR observation dataset

Original Issues: DC-537, DC-703

The intent is to null concept ids (value_source_concept_id, value_as_concept_id, value_source_value,
value_as_string) from the RDR observation dataset. The changed records should be archived in the
dataset sandbox.
"""

# Python Imports
import os

# Project Imports
from app_identity import PROJECT_ID
from cdr_cleaner.cleaning_rules.null_concept_ids_for_numeric_ppi import NullConceptIDForNumericPPI
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import BaseTest


class NullConceptIDForNumericPPITest(BaseTest.CleaningRulesTestBase):

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

        cls.rule_instance = NullConceptIDForNumericPPI(project_id, dataset_id,
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
        fully qualified (fq) dataset name string used to load the data.
        """
        self.value_source_concept_id = 'NULL'
        self.value_as_concept_id = 'NULL'
        self.value_source_value = 'NULL'
        self.value_as_string = 'NULL'

        fq_dataset_name = self.fq_table_names[0].split('.')
        self.fq_dataset_name = '.'.join(fq_dataset_name[:-1])

        super().setUp()

    def test_field_cleaning(self):
        """
        Tests that the specifications for the SANDBOX_QUERY and CLEAN_NUMERIC_PPI_QUERY
        perform as designed.

        Validates pre conditions, tests execution, and post conditions based on the load
        statements and the tables_and_counts variable.
        """

        tmpl = self.jinja_env.from_string("""
        INSERT INTO `{{fq_dataset_name}}.observation`
        (observation_id, person_id, observation_concept_id, observation_date, 
         observation_type_concept_id, questionnaire_response_id, value_as_number,
         value_source_concept_id, value_as_concept_id)
        VALUES
            (123, 111111, 0, date('2015-07-15'), 0, 111, 111, 111, 111),
            (345, 222222, 0, date('2015-07-15'), 0, 222, 222, 222, 222),
            (567, 333333, 0, date('2015-07-15'), 0, 333, 333, 333, 333),
            (789, 444444, 0, date('2015-07-15'), 0, 444, 444, 444, 444)""")

        query = tmpl.render(fq_dataset_name=self.fq_dataset_name)
        self.load_test_data([query])

        # Expected results list
        tables_and_counts = [{
            'fq_table_name':
                '.'.join([self.fq_dataset_name, 'observation']),
            'fq_sandbox_table_name':
                self.fq_sandbox_table_names[0],
            'loaded_ids': [123, 345, 567, 789],
            'sandboxed_ids': [123, 345, 567, 789],
            'fields': [
                'value_source_concept_id', 'value_as_concept_id',
                'value_source_value', 'value_as_string'
            ],
            'cleaned_values': [
                (123, self.value_source_concept_id, self.value_as_concept_id,
                 self.value_source_value, self.value_as_string),
                (345, self.value_source_concept_id, self.value_as_concept_id,
                 self.value_source_value, self.value_as_string),
                (567, self.value_source_concept_id, self.value_as_concept_id,
                 self.value_source_value, self.value_as_string),
                (789, self.value_source_concept_id, self.value_as_concept_id,
                 self.value_source_value, self.value_as_string)
            ]
        }]

        self.default_test(tables_and_counts)
