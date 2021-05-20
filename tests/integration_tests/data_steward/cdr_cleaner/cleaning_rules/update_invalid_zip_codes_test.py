"""
Integration test for drop_invalid_zip_codes.py module.

This cleaning rule sandboxes and updates any invalid zip codes.

Original Issues: DC-1633, DC-1645

Ensures that any invalid zip codes are sandboxed and updated. Invalid zip codes are those that are less than 5 digits
    in length or do not match a zip3 value in the zip3 lookup table. If a zip code is invalid, the value_as_string is
    updated to 'Invalid Zip' and the value_as_number is updated to 0.
"""

# Python imports
import os

# Third party imports
import mock
from dateutil import parser

# Project imports
from common import OBSERVATION, ZIP3_LOOKUP
from app_identity import PROJECT_ID
from cdr_cleaner.cleaning_rules.update_invalid_zip_codes import UpdateInvalidZipCodes
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import BaseTest


class UpdateInvalidZipCodesTest(BaseTest.CleaningRulesTestBase):

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
        # intended to be run on the deid_base dataset.  The combined dataset
        # environment variable should be guaranteed to exist
        dataset_id = os.environ.get('RDR_DATASET_ID')
        cls.dataset_id = dataset_id
        sandbox_id = dataset_id + '_sandbox'
        cls.sandbox_id = sandbox_id

        cls.rule_instance = UpdateInvalidZipCodes(project_id, dataset_id,
                                                  sandbox_id)

        sb_table_names = cls.rule_instance.get_sandbox_tablenames()
        for table_name in sb_table_names:
            cls.fq_sandbox_table_names.append(
                f'{cls.project_id}.{cls.sandbox_id}.{table_name}')

        # This will normally be the PIPELINE_TABLES dataset, but is being
        # mocked for this test
        cls.zip3_lookup_name = f'{cls.project_id}.{cls.dataset_id}.{ZIP3_LOOKUP}'

        cls.fq_table_names = [
            f"{project_id}.{dataset_id}.{OBSERVATION}", cls.zip3_lookup_name
        ]
        # call super to set up the client, create datasets, and create
        # empty test tables
        # NOTE:  does not create empty sandbox tables.
        super().setUpClass()

    def setUp(self):
        """
        Create common information for tests.

        Creates common expected parameter types from cleaned tables and a common fully qualified (fq)
        dataset name string used to load the data.
        """
        fq_dataset_name = self.fq_table_names[0].split('.')
        self.fq_dataset_name = '.'.join(fq_dataset_name[:-1])

        self.date = parser.parse('2019-03-03').date()
        self.invalid_zip_response = 'Response removed due to invalid value'
        self.invalid_zip_concept = 2000000010

        super().setUp()

    def test_update_invalid_zip_codes(self):
        """
        Tests that the specifications for SANDBOX_INVALID_ZIP_CODES and UPDATE_INVALID_ZIP_CODES perform as designed.

        Validates pre conditions, tests execution, and post conditions based on the load
        statements and the tables_and_counts variable.
        """

        zip3_lookup_tmpl = self.jinja_env.from_string("""
            INSERT INTO `{{fq_dataset_name}}.zip3_lookup` (zip3)
            VALUES
                (123), (234)
            """).render(fq_dataset_name=self.fq_dataset_name)

        observation_tmpl = self.jinja_env.from_string("""
            INSERT INTO `{{fq_dataset_name}}.observation` (observation_id, person_id, observation_concept_id, 
                observation_date, observation_type_concept_id, observation_source_concept_id, value_as_number, 
                value_as_string, value_source_concept_id, value_source_value)
            VALUES
                -- valid zip codes, will not be affected --
                (1, 1, 0, date('2019-03-03'), 0, 1585250, null, '12345', null, null), 
                (2, 2, 0, date('2019-03-03'), 0, 1585250, null, '23456-1234', null, null),
                
                -- invalid zip codes, will be sandboxed and updated in observation table --
                (3, 3, 0, date('2019-03-03'), 0, 1585250, null, '1234', null, null), 
                (4, 4, 0, date('2019-03-03'), 0, 1585250, null, '123', null, null),
                (5, 5, 0, date('2019-03-03'), 0, 1585250, null, '12', null, null),
                (6, 6, 0, date('2019-03-03'), 0, 1585250, null, '1', null, null),
                
                -- not zip codes (not zip code concept id), will not be affected --
                (7, 7, 0, date('2019-03-03'), 0, 1111111, null, '111', null, null),
                (8, 8, 0, date('2019-03-03'), 0, 2222222, null, '222', null, null)
            """).render(fq_dataset_name=self.fq_dataset_name)

        self.load_test_data([zip3_lookup_tmpl, observation_tmpl])

        tables_and_counts = [{
            'fq_table_name':
                '.'.join([self.fq_dataset_name, 'observation']),
            'fq_sandbox_table_name':
                self.fq_sandbox_table_names[0],
            'fields': [
                'observation_id', 'person_id', 'observation_concept_id',
                'observation_date', 'observation_type_concept_id',
                'observation_source_concept_id', 'value_as_number',
                'value_as_string', 'value_source_concept_id',
                'value_source_value'
            ],
            'loaded_ids': [1, 2, 3, 4, 5, 6, 7, 8],
            'sandboxed_ids': [3, 4, 5, 6],
            'cleaned_values': [
                (1, 1, 0, self.date, 0, 1585250, None, '12345', None, None),
                (2, 2, 0, self.date, 0, 1585250, None, '23456-1234', None,
                 None),
                (3, 3, 0, self.date, 0, 1585250, 0.0, self.invalid_zip_response,
                 self.invalid_zip_concept, self.invalid_zip_response),
                (4, 4, 0, self.date, 0, 1585250, 0.0, self.invalid_zip_response,
                 self.invalid_zip_concept, self.invalid_zip_response),
                (5, 5, 0, self.date, 0, 1585250, 0.0, self.invalid_zip_response,
                 self.invalid_zip_concept, self.invalid_zip_response),
                (6, 6, 0, self.date, 0, 1585250, 0.0, self.invalid_zip_response,
                 self.invalid_zip_concept, self.invalid_zip_response),
                (7, 7, 0, self.date, 0, 1111111, None, '111', None, None),
                (8, 8, 0, self.date, 0, 2222222, None, '222', None, None)
            ]
        }]

        # mock the PIPELINE_TABLES variable so tests on different branches
        # don't overwrite each other.
        with mock.patch(
                'cdr_cleaner.cleaning_rules.update_invalid_zip_codes.PIPELINE_TABLES',
                self.dataset_id):
            self.default_test(tables_and_counts)
