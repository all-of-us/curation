"""
Integration test for clean_ppi_numeric_fields_using_parameters module

Apply value ranges to ensure that values are reasonable and to minimize the likelihood
of sensitive information (like phone numbers) within the free text fields.

Original Issues: DC-1058, DC-1061, DC-827, DC-502, DC-487, DC-2475, DC-2649

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
        self.invalid_values_value_as_concept_id = 2000000010
        self.eleven_plus_value_as_concept_id = 2000000013
        self.six_plus_value_as_concept_id = 2000000012

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

        queries = []

        invalid_values_tmpl = self.jinja_env.from_string(
            """
            INSERT INTO `{{fq_dataset_name}}.observation`
            (observation_id, person_id, observation_concept_id, observation_source_concept_id, observation_date,
             observation_type_concept_id, value_as_number, value_as_string, value_as_concept_id, value_source_concept_id)
            VALUES
                -- invalid values test setup --
                (103, 3, 1585795,1585795, date('2015-07-15'), 0, 100, NULL, 1234567, 1234567),
                (104, 4, 1585802,1585802, date('2015-07-15'), 0, -100, NULL, 1234567, 1234567),
                (105, 5, 1585820,1585820, date('2015-07-15'), 0, 256, NULL, 1234567, 1234567),
                (106, 6, 1585820,1585820, date('2015-07-15'), 0, -256, NULL, 1234567, 1234567),
                (107, 7, 40766333,1585864, date('2015-07-15'), 0, 100, NULL, 1234567, 1234567),
                (108, 8, 1585870,1585870, date('2015-07-15'), 0, -100, NULL, 1234567, 1234567),
                (109, 9, 40770349,1585873, date('2015-07-15'), 0, 15, NULL, 7654321, 7654321),
                (110, 10, 40766929,1586159, date('2015-07-15'), 0, 16, NULL, 7654321, 7654321),
                (111, 11, 40766930,1586162, date('2015-07-15'), 0, 17, NULL, 7654321, 7654321),
                (122, 22, 1333023,1333023, date('2015-07-15'), 0, NULL, 'test', 7654321, 7654321)"""
        ).render(fq_dataset_name=self.fq_dataset_name)
        queries.append(invalid_values_tmpl)

        eleven_plus_tmpl = self.jinja_env.from_string(
            """
            INSERT INTO `{{fq_dataset_name}}.observation`
            (observation_id, person_id, observation_concept_id, observation_source_concept_id, observation_date,
             observation_type_concept_id, value_as_number, value_as_concept_id, value_source_concept_id)
            VALUES
                -- 11+ generalization test setup --
                (112, 12, 1333015,1333015, date('2020-09-06'), 0, -12, 1234567, 1234567),
                (113, 13, 1585889,1585889, date('2020-09-06'), 0, 12, 1234567, 1234567),
                (114, 14, 1333015,1333015, date('2020-09-06'), 0, 10, 7654321, 7654321),
                (118, 18, 1585889,1585889, date('2020-09-06'), 0, 20, 7654321, 1234567),
                (121, 21, 1333015,1333015, date('2020-09-06'), 0, 15, 1234567, 1234567)"""
        ).render(fq_dataset_name=self.fq_dataset_name)
        queries.append(eleven_plus_tmpl)

        six_plus_tmpl = self.jinja_env.from_string(
            """
            INSERT INTO `{{fq_dataset_name}}.observation`
            (observation_id, person_id, observation_concept_id, observation_source_concept_id, observation_date,
             observation_type_concept_id, value_as_number, value_as_concept_id, value_source_concept_id)
            VALUES
                -- 6+ generalization test setup --
                (115, 15, 1333023,1333023, date('2020-09-11'), 0, -7, 1234567, 1234567),
                (116, 16, 1585890,1585890, date('2020-09-11'), 0, 7, 1234567, 1234567),
                (117, 17, 1333023,1333023, date('2020-09-11'), 0, 5, 7654321, 7654321),
                (119, 19, 1585890,1585890, date('2020-09-11'), 0, 20, 7654321, 7654321),
                (120, 20, 1333023,1333023, date('2020-09-11'), 0, 6, 1234567, 1234567)"""
        ).render(fq_dataset_name=self.fq_dataset_name)
        queries.append(six_plus_tmpl)

        skips_tmpl = self.jinja_env.from_string("""
            INSERT INTO `{{fq_dataset_name}}.observation`
            (observation_id, person_id, observation_concept_id, observation_source_concept_id, observation_date,
             observation_type_concept_id, value_as_number, value_as_string, value_as_concept_id, value_source_concept_id)
            VALUES
                -- 6+ Skip values should not be invalidated --
                (123, 23, 1333023,1333023, date('2015-07-15'), 0, NULL, '', 0, 903096),
                (124, 24, 1333023,1333023, date('2015-07-15'), 0, NULL, ' ', 0, 903096),
                (125, 25, 1333023,1333023, date('2015-07-15'), 0, NULL, 'PMI_Skip', 0, 903096),
                (126, 26, 1333023,1333023, date('2015-07-15'), 0, NULL, NULL, 0, 903096),
                -- 11+ Skip values should not be invalidated --
                (127, 27, 1333015,1333015, date('2015-07-15'), 0, NULL, '', 903096, 903096),
                (128, 28, 1333015,1333015, date('2015-07-15'), 0, NULL, ' ', 903096, 903096),
                (129, 29, 1333015,1333015, date('2015-07-15'), 0, NULL, 'PMI_Skip', 903096, 903096),
                (130, 30, 1333015,1333015, date('2015-07-15'), 0, NULL, NULL, 903096, 903096)
                """).render(fq_dataset_name=self.fq_dataset_name)
        queries.append(skips_tmpl)

        self.load_test_data(queries)

        # Expected results list
        tables_and_counts = [{
            'fq_table_name':
                '.'.join([self.fq_dataset_name, 'observation']),
            'fq_sandbox_table_name':
                self.fq_sandbox_table_names[0],
            'loaded_ids': [
                103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115,
                116, 117, 118, 119, 120, 121, 122, 123, 124, 125, 126, 127, 128,
                129, 130
            ],
            'sandboxed_ids': [
                103, 104, 105, 106, 107, 108, 112, 113, 115, 116, 118, 119, 120,
                121, 122, 123, 124, 125
            ],
            'fields': [
                'observation_id', 'observation_concept_id',
                'observation_source_concept_id', 'value_as_number',
                'value_as_string', 'value_as_concept_id',
                'value_source_concept_id'
            ],
            'cleaned_values': [
                # invalid values tests
                (103, 1585795, 1585795, None, None,
                 self.invalid_values_value_as_concept_id,
                 self.invalid_values_value_as_concept_id),
                (104, 1585802, 1585802, None, None,
                 self.invalid_values_value_as_concept_id,
                 self.invalid_values_value_as_concept_id),
                (105, 1585820, 1585820, None, None,
                 self.invalid_values_value_as_concept_id,
                 self.invalid_values_value_as_concept_id),
                (106, 1585820, 1585820, None, None,
                 self.invalid_values_value_as_concept_id,
                 self.invalid_values_value_as_concept_id),
                (107, 40766333, 1585864, None, None,
                 self.invalid_values_value_as_concept_id,
                 self.invalid_values_value_as_concept_id),
                (108, 1585870, 1585870, None, None,
                 self.invalid_values_value_as_concept_id,
                 self.invalid_values_value_as_concept_id),
                (109, 40770349, 1585873, 15, None, 7654321, 7654321),
                (110, 40766929, 1586159, 16, None, 7654321, 7654321),
                (111, 40766930, 1586162, 17, None, 7654321, 7654321),
                (122, 1333023, 1333023, None, None,
                 self.invalid_values_value_as_concept_id,
                 self.invalid_values_value_as_concept_id),
                # 11+ values tests
                (112, 1333015, 1333015, None, None,
                 self.invalid_values_value_as_concept_id,
                 self.invalid_values_value_as_concept_id),
                (113, 1585889, 1585889, None, None,
                 self.eleven_plus_value_as_concept_id,
                 self.eleven_plus_value_as_concept_id),
                (114, 1333015, 1333015, 10, None, 7654321, 7654321),
                (118, 1585889, 1585889, None, None,
                 self.invalid_values_value_as_concept_id,
                 self.invalid_values_value_as_concept_id),
                (121, 1333015, 1333015, None, None,
                 self.eleven_plus_value_as_concept_id,
                 self.eleven_plus_value_as_concept_id),
                # 6+ values tests
                (115, 1333023, 1333023, None, None,
                 self.invalid_values_value_as_concept_id,
                 self.invalid_values_value_as_concept_id),
                (116, 1585890, 1585890, None, None,
                 self.six_plus_value_as_concept_id,
                 self.six_plus_value_as_concept_id),
                (117, 1333023, 1333023, 5, None, 7654321, 7654321),
                (119, 1585890, 1585890, None, None,
                 self.invalid_values_value_as_concept_id,
                 self.invalid_values_value_as_concept_id),
                (120, 1333023, 1333023, None, None,
                 self.six_plus_value_as_concept_id,
                 self.six_plus_value_as_concept_id),
                # Test skips
                (123, 1333023, 1333023, None, None, 0, 903096),
                (124, 1333023, 1333023, None, None, 0, 903096),
                (125, 1333023, 1333023, None, None, 0, 903096),
                (126, 1333023, 1333023, None, None, 0, 903096),
                (127, 1333015, 1333015, None, '', 903096, 903096),
                (128, 1333015, 1333015, None, ' ', 903096, 903096),
                (129, 1333015, 1333015, None, 'PMI_Skip', 903096, 903096),
                (130, 1333015, 1333015, None, None, 903096, 903096)
            ]
        }]

        self.default_test(tables_and_counts)
