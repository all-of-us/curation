"""
Integration test for update_fields_numbers_as_strings cleaning rule

Original Issues: DC-1052

Background
It has been discovered that the field type for some PPI survey answers is incorrect: there are several instances of
numeric answers being saved as ‘string’ field types. The expected long-term fix is for PTSC to correct the field type
on their end; however, there is no anticipated timeline for the completion of this work. As a result, the Curation team
will need to create a cleaning rule to correct these errors.

Cleaning rule to fill null values in value_as_number with values in value_as_string,
EXCEPT when it’s a ‘PMI Skip’ for each of the observation_source_value

Rule should be applied to the RDR export
"""

# Python Imports
import os

# Project Imports
from app_identity import PROJECT_ID
from cdr_cleaner.cleaning_rules.update_fields_numbers_as_strings import UpdateFieldsNumbersAsStrings
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import BaseTest


class UpdateFieldsNumbersAsStringsTest(BaseTest.CleaningRulesTestBase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

        super().initialize_class_vars()

        cls.insert_fake_participant_tmpls = [
            cls.jinja_env.from_string("""
            INSERT INTO
            `{{fq_table_name}}` 
            (observation_id,
            person_id,
            observation_concept_id,
            observation_date,
            observation_type_concept_id,
            value_as_number,
            value_as_string, 
            observation_source_value)
            VALUES
            (12345, 1111, 8621, DATE('2018-09-20'), 45905771, NULL, '29', 'ipaq_3_cope_a_24'),
            (123456, 2222, 715713, DATE('2019-09-20'), 45905771, NULL, '1', 'lifestyle_2_xx12_cope_a_198'),
            (1234567, 3333, 715723, DATE('2020-01-20'), 45905771, NULL, '10', 'lifestyle_2_xx12_cope_a_152'),
            (12345678, 4444, 1333015, DATE('2017-11-20'), 45905771, 30, 'test', 'basics_xx'),
            (123456789, 5555, 1333118, DATE('2015-10-13'), 45905771, NULL, '40', 'FAKE'),
            (123456781, 6666, 1333118, DATE('2015-10-13'), 45905771, NULL, 'PMI Skip', 'cdc_covid_19_n_a2')
            """)
        ]

        # Set the test project identifier
        project_id = os.environ.get(PROJECT_ID)
        cls.project_id = project_id

        # Set the expected test datasets
        dataset_id = os.environ.get('RDR_DATASET_ID')
        sandbox_id = dataset_id + '_sandbox'

        cls.query_class = UpdateFieldsNumbersAsStrings(project_id, dataset_id,
                                                       sandbox_id)

        sb_table_names = cls.query_class.get_sandbox_tablenames()
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
        Add data to the tables for the rule to run on
        """
        self.load_statements = []

        for tmpl in self.insert_fake_participant_tmpls:
            query = tmpl.render(fq_table_name=self.fq_table_names[0])
        self.load_statements.append(query)

        super().setUp()

    def test(self):
        """
        Tests that the specifications for the SANDBOX_QUERY and NUMBERS_AS_STRINGS_QUERY
        perform as designed.

        Validates pre conditions, tests execution, and post conditions based on the load
        statements and the tables_and_counts variable.
        """

        self.load_test_data(self.load_statements)

        # Expected results list

        # observation_ids: [12345, 123456, 1234567] should have value_as_number populated with what was
        # initially in value_as_string
        # observation_ds: [12345678, 123456789, 1234567891] should remain unchanged
        tables_and_counts = [{
            'fq_table_name':
                self.fq_table_names[0],
            'fq_sandbox_table_name':
                self.fq_sandbox_table_names[0],
            'loaded_ids': [
                12345, 123456, 1234567, 12345678, 123456789, 1234567891
            ],
            'sandboxed_ids': [
                12345, 123456, 1234567, 12345678, 123456789, 1234567891
            ],
            'fields': ['observation_id', 'value_as_number', 'value_as_string'],
            'cleaned_values': [(12345, 29, None), (123456, 1, None),
                               (1234567, 10, None), (12345678, 30, 'test'),
                               (123456789, None, '40'),
                               (1234567891, None, 'PMI Skip')]
        }]

        self.default_test(tables_and_counts)