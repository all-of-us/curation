"""
Integration test for clean_ppi_numeric_fields_using_parameters module

Apply value ranges to ensure that values are reasonable and to minimize the likelihood
of sensitive information (like phone numbers) within the free text fields.

Original Issues: DC-827, DC-502, DC-487

The intent is to ensure that numeric free-text fields that are not manipulated by de-id
have value range restrictions applied to the value_as_number field across the entire dataset.
"""

# Python Imports
import os

# Project Imports
from app_identity import PROJECT_ID
from cdr_cleaner.cleaning_rules.clean_ppi_numeric_fields_using_parameters import CleanPPINumericFieldsUsingParameters
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import BaseTest


class CleanPPINumericFieldsUsingParameterTest(BaseTest.CleaningRulesTestBase):

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

        cls.rule_instance = CleanPPINumericFieldsUsingParameters(
            project_id, dataset_id, sandbox_id)

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
        self.value_as_number = None
        self.value_as_concept_id = 2000000010

        fq_dataset_name = self.fq_table_names[0].split('.')
        self.fq_dataset_name = '.'.join(fq_dataset_name[:-1])

        super().setUp()

    def test_field_cleaning(self):
        """
        Tests that the specifications for the SANDBOX_QUERY and CLEAN_PPI_NUMERIC_FIELDS_QUERY
        perform as designed.

        Validates pre conditions, tests execution, and post conditions based on the load
        statements and the tables_and_counts variable.
        """

        tmpl = self.jinja_env.from_string("""
            INSERT INTO `{{fq_dataset_name}}.observation`
            (observation_id, person_id, observation_concept_id, observation_date, 
             observation_type_concept_id, value_as_number, value_as_concept_id)
            VALUES
                (123, 111111, 1585889, date('2015-07-15'), 0, 21, 111),
                (345, 222222, 1585890, date('2015-07-15'), 0, -21, 222),
                (567, 333333, 1585795, date('2015-07-15'), 0, 100, 333),
                (789, 444444, 1585802, date('2015-07-15'), 0, -100, 444),
                (555, 555555, 1585820, date('2015-07-15'), 0, 256, 111),
                (121, 121212, 1585820, date('2015-07-15'), 0, -256, 111),
                (666, 666666, 1585864, date('2015-07-15'), 0, 100, 222),
                (777, 777777, 1585870, date('2015-07-15'), 0, -100, 333),
                (888, 888888, 1585873, date('2015-07-15'), 0, 15, 444),
                (999, 999999, 1586159, date('2015-07-15'), 0, 16, 444),
                (321, 000000, 1586162, date('2015-07-15'), 0, 17, 444)""")

        query = tmpl.render(fq_dataset_name=self.fq_dataset_name)
        self.load_test_data([query])

        # Expected results list
        tables_and_counts = [{
            'fq_table_name':
                '.'.join([self.fq_dataset_name, 'observation']),
            'fq_sandbox_table_name':
                self.fq_sandbox_table_names[0],
            'loaded_ids': [
                123, 345, 567, 789, 555, 121, 666, 777, 888, 999, 321
            ],
            'sandboxed_ids': [123, 345, 567, 789, 555, 121, 666, 777],
            'fields': [
                'observation_id', 'observation_concept_id', 'value_as_number',
                'value_as_concept_id'
            ],
            'cleaned_values': [
                (123, 1585889, self.value_as_number, self.value_as_concept_id),
                (345, 1585890, self.value_as_number, self.value_as_concept_id),
                (567, 1585795, self.value_as_number, self.value_as_concept_id),
                (789, 1585802, self.value_as_number, self.value_as_concept_id),
                (555, 1585820, self.value_as_number, self.value_as_concept_id),
                (121, 1585820, self.value_as_number, self.value_as_concept_id),
                (666, 1585864, self.value_as_number, self.value_as_concept_id),
                (777, 1585870, self.value_as_number, self.value_as_concept_id),
                (888, 1585873, 15, 444), (999, 1586159, 16, 444),
                (321, 1586162, 17, 444)
            ]
        }]

        self.default_test(tables_and_counts)
