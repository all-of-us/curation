"""
Integration test for generalize_zip_codes module

This cleaning rule generalizes all PPI concepts containing zip codes (observation_source_concept_id = 1585250)
to their primary three digit representation, (e.g. 35400 → 354**).

This cleaning rule is specific to the controlled tier.


Original Issue: DC1376
"""

# Python Imports
import os
from datetime import datetime

#Project imports
from app_identity import PROJECT_ID
from cdr_cleaner.cleaning_rules.generalize_zip_codes import GeneralizeZipCodes
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import BaseTest
from constants.bq_utils import WRITE_TRUNCATE
from common import OBSERVATION


class GeneralizeZipCodesTest(BaseTest.CleaningRulesTestBase):

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
        dataset_id = os.environ.get('COMBINED_DATASET_ID')
        cls.dataset_id = dataset_id
        sandbox_id = dataset_id + '_sandbox'
        cls.sandbox_id = sandbox_id

        cls.rule_instance = GeneralizeZipCodes(project_id, dataset_id,
                                               sandbox_id)

        cls.fq_table_names = [
            f'{project_id}.{dataset_id}.{OBSERVATION}',
        ]

        # call super to set up the client, create datasets, and create
        # empty test tables
        # NOTE:  does not create empty sandbox tables.
        super().setUpClass()

    def setUp(self):
        fq_dataset_name = self.fq_table_names[0].split('.')
        self.fq_dataset_name = '.'.join(fq_dataset_name[:-1])

        super().setUp()

    def test_generalize_zip_codes_cleaning(self):
        """
        Tests that the sepcifications for GENERALIZE_ZIP_CODES_QUERY perform as designed.

        Validates pre conditions, tests execution, and post conditions based on the load
        statements and the tables_and_counts variable.        
        """
        queries = []

        #Append some queries
        zipcodes_tmpl = self.jinja_env.from_string("""
            INSERT INTO `{{fq_dataset_name}}.observation`
            (observation_id, person_id, observation_concept_id, observation_date, 
             observation_type_concept_id, observation_source_value, observation_source_concept_id,
             value_as_string)
             VALUES
                (1001, 1001, 0, '2020-08-30', 1001, 'StreetAddress_PIIZIP', 1585250, '60512'),
                (1002, 1002, 0, '2020-08-30', 1002, 'StreetAddress_PIIZIP', 1585250, '10832'),
                (1003, 1003, 0, '2020-08-30', 1003, 'StreetAddress_PIIZIP', 1585250, '97261'),
                (1004, 1004, 0, '2020-08-30', 1004, 'StreetAddress_PIIZIP', 1585250, '93431'),
                (1005, 1005, 0, '2020-08-30', 1005, 'StreetAddress_PIIZIP', 1585250, '00589'),
                (1006, 1006, 0, '2020-08-30', 1006, 'StreetAddress_PIIZIP', 1585250, '23591'),
                (1007, 1007, 0, '2020-08-30', 1007, 'StreetAddress_PIIZIP', 1585250, '23512-4'),
                (1008, 1008, 0, '2020-08-30', 1008, 'StreetAddress_PIICity', 1585248, 'New York'),
                (1009, 1009, 0, '2020-08-30', 1009, 'StreetAddress_PIIState', 1585249, 'NY'),
                (1010, 1010, 0, '2020-08-30', 1010, 'StreetAddress_PIIZIP', 1585250, '72321'),
                (1011, 1011, 0, '2020-08-30', 1011, 'StreetAddress_PIIZIP', 1585247, '37218')
        """).render(fq_dataset_name=self.fq_dataset_name)
        queries.append(zipcodes_tmpl)

        self.load_test_data(queries)

        tables_and_counts = [{
            'fq_table_name':
                '.'.join([self.fq_dataset_name, 'observation']),
            'fq_sandbox_table_name':
                '',
            'loaded_ids': [
                1001, 1002, 1003, 1004, 1005, 1006, 1007, 1008, 1009, 1010, 1011
            ],
            'sandboxed_ids': [],
            'fields': [
                'observation_id', 'person_id', 'observation_concept_id',
                'observation_date', 'observation_type_concept_id',
                'observation_source_value', 'observation_source_concept_id',
                'value_as_string'
            ],
            'cleaned_values': [
                (1001, 1001, 0, datetime.strptime('2020-08-30',
                                                  '%Y-%m-%d').date(), 1001,
                 'StreetAddress_PIIZIP', 1585250, '605**'),
                (1002, 1002, 0, datetime.strptime('2020-08-30',
                                                  '%Y-%m-%d').date(), 1002,
                 'StreetAddress_PIIZIP', 1585250, '108**'),
                (1003, 1003, 0, datetime.strptime('2020-08-30',
                                                  '%Y-%m-%d').date(), 1003,
                 'StreetAddress_PIIZIP', 1585250, '972**'),
                (1004, 1004, 0, datetime.strptime('2020-08-30',
                                                  '%Y-%m-%d').date(), 1004,
                 'StreetAddress_PIIZIP', 1585250, '934**'),
                (1005, 1005, 0, datetime.strptime('2020-08-30',
                                                  '%Y-%m-%d').date(), 1005,
                 'StreetAddress_PIIZIP', 1585250, '005**'),
                (1006, 1006, 0, datetime.strptime('2020-08-30',
                                                  '%Y-%m-%d').date(), 1006,
                 'StreetAddress_PIIZIP', 1585250, '235**'),
                (1007, 1007, 0, datetime.strptime('2020-08-30',
                                                  '%Y-%m-%d').date(), 1007,
                 'StreetAddress_PIIZIP', 1585250, '235****'),
                (1008, 1008, 0, datetime.strptime('2020-08-30',
                                                  '%Y-%m-%d').date(), 1008,
                 'StreetAddress_PIICity', 1585248, 'New York'),
                (1009, 1009, 0, datetime.strptime('2020-08-30',
                                                  '%Y-%m-%d').date(), 1009,
                 'StreetAddress_PIIState', 1585249, 'NY'),
                (1010, 1010, 0, datetime.strptime('2020-08-30',
                                                  '%Y-%m-%d').date(), 1010,
                 'StreetAddress_PIIZIP', 1585250, '723**'),
                (1011, 1011, 0, datetime.strptime('2020-08-30',
                                                  '%Y-%m-%d').date(), 1011,
                 'StreetAddress_PIIZIP', 1585247, '37218')
            ]
        }]

        self.default_test(tables_and_counts)
