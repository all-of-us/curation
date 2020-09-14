"""
Integration test for clean_ppi_numeric_fields_using_parameters module

Apply value ranges to ensure that values are reasonable and to minimize the likelihood
of sensitive information (like phone numbers) within the free text fields.

Original Issues: DC-1058, DC-1061, DC-827, DC-502, DC-487

The intent is to ensure that numeric free-text fields that are not manipulated by de-id
have value range restrictions applied to the value_as_number field across the entire dataset.
"""

# Python Imports
import os

# Project Imports
from app_identity import PROJECT_ID
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import BaseTest
import cdr_cleaner.cleaning_rules.clean_ppi_numeric_fields_using_parameters as clean_ppi


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

        cls.rule_instance = clean_ppi.CleanPPINumericFieldsUsingParameters(
            project_id, dataset_id, sandbox_id)

        sb_table_names = cls.rule_instance.get_sandbox_tablenames()
        cls.fq_sandbox_table_names = [
            f'{project_id}.{sandbox_id}.{sb_table_names}'
        ]

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
        self.dc_1061_value_as_concept_id = 2000000013
        self.dc_1058_value_as_concept_id = 2000000012

        fq_dataset_name = self.fq_table_names[0].split('.')
        self.fq_dataset_name = '.'.join(fq_dataset_name[:-1])

        super().setUp()

    def test_invalid_values_fields_cleaning(self):
        """
        Tests that the specifications for the INVALID_VALUES_SANDBOX_QUERY and CLEAN_INVALID_VALUES_QUERY
        perform as designed.

        Validates pre conditions, tests execution, and post conditions based on the load
        statements and the tables_and_counts variable.
        """
        tmpl = self.jinja_env.from_string("""
            INSERT INTO `{{fq_dataset_name}}.observation`
            (observation_id, person_id, observation_concept_id, observation_date,
             observation_type_concept_id, value_as_number, value_as_concept_id,
             observation_source_concept_id)
            VALUES
                (123, 111111, 1585889, date('2015-07-15'), 0, 21, 111, 0),
                (345, 222222, 1585890, date('2015-07-15'), 0, -21, 222, 0),
                (567, 333333, 1585795, date('2015-07-15'), 0, 100, 333, 0),
                (789, 444444, 1585802, date('2015-07-15'), 0, -100, 444, 0),
                (555, 555555, 1585820, date('2015-07-15'), 0, 256, 111, 0),
                (121, 121212, 1585820, date('2015-07-15'), 0, -256, 111, 0),
                (666, 666666, 1585864, date('2015-07-15'), 0, 100, 222, 0),
                (777, 777777, 1585870, date('2015-07-15'), 0, -100, 333, 0),
                (888, 888888, 1585873, date('2015-07-15'), 0, 15, 444, 0),
                (999, 999999, 1586159, date('2015-07-15'), 0, 16, 444, 0),
                (321, 000000, 1586162, date('2015-07-15'), 0, 17, 444, 0),
                -- 11+ generalization test setup --
                (198, 111, 0, date('2020-09-06'), 0, 21, 111, 1585889),
                (987, 222, 0, date('2020-09-06'), 0, 12, 222, 1333015),
                (876, 333, 0, date('2020-09-06'), 0, 4, 111, 1585889),
                (765, 444, 0, date('2020-09-06'), 0, 6, 222, 1333015),
                (654, 444, 0, date('2020-09-06'), 0, 11, 222, 1333015),
                (6543, 444, 0, date('2020-09-06'), 0, 10, 222, 1333015),
                -- 6+ generalization test setup --
                (111, 555, 0, date('2020-09-11'), 0, 7, 333, 1333023),
                (222, 666, 0, date('2020-09-11'), 0, 5, 444, 1333023),
                (333, 777, 0, date('2020-09-11'), 0, 7, 333, 1585890),
                (444, 888, 0, date('2020-09-11'), 0, 5, 444, 1585890),
                (543, 999, 0, date('2020-09-11'), 0, 6, 543, 1585890)""")

        query = tmpl.render(fq_dataset_name=self.fq_dataset_name)
        self.load_test_data([query])

        # Expected results list
        tables_and_counts = [{
            'fq_table_name':
                '.'.join([self.fq_dataset_name, 'observation']),
            'fq_sandbox_table_name':
                self.fq_sandbox_table_names[0],
            'loaded_ids': [
                123, 345, 567, 789, 555, 121, 666, 777, 888, 999, 321, 198, 987,
                876, 765, 111, 222, 333, 444, 543, 654, 6543
            ],
            'sandboxed_ids': [
                123, 345, 567, 789, 555, 121, 666, 777, 198, 987, 111, 333
            ],
            'fields': [
                'observation_id', 'observation_concept_id', 'value_as_number',
                'value_as_concept_id', 'observation_source_concept_id'
            ],
            'cleaned_values': [(123, 1585889, 21.0, 111, 0),
                               (345, 1585890, self.value_as_number,
                                self.value_as_concept_id, 0),
                               (567, 1585795, self.value_as_number,
                                self.value_as_concept_id, 0),
                               (789, 1585802, self.value_as_number,
                                self.value_as_concept_id, 0),
                               (555, 1585820, self.value_as_number,
                                self.value_as_concept_id, 0),
                               (121, 1585820, self.value_as_number,
                                self.value_as_concept_id, 0),
                               (666, 1585864, self.value_as_number,
                                self.value_as_concept_id, 0),
                               (777, 1585870, self.value_as_number,
                                self.value_as_concept_id, 0),
                               (888, 1585873, 15, 444, 0),
                               (999, 1586159, 16, 444, 0),
                               (321, 1586162, 17, 444, 0),
                               (198, 0, self.value_as_number,
                                self.dc_1061_value_as_concept_id, 1585889),
                               (987, 0, self.value_as_number,
                                self.dc_1061_value_as_concept_id, 1333015),
                               (876, 0, 4, 111, 1585889),
                               (765, 0, 6, 222, 1333015),
                               (654, 0, 11, 222, 1333015),
                               (6543, 0, 10, 222, 1333015),
                               (111, 0, self.value_as_number,
                                self.dc_1058_value_as_concept_id, 1333023),
                               (222, 0, 5, 444, 1333023),
                               (333, 0, self.value_as_number,
                                self.dc_1058_value_as_concept_id, 1585890),
                               (444, 0, 5, 444, 1585890),
                               (543, 0, 6, 543, 1585890)],
        }]

        self.default_test(tables_and_counts)
