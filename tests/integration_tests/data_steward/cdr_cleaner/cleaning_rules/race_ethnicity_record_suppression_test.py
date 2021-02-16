"""
Integration test to ensure the records that are properly sandboxed and dropped in the r
ace_ethnicity_record_suppression.py module.

Removes any records that have a observation_source_concept_id as either of these values: 1586151, 1586150, 1586152,
1586153, 1586154, 1586155, 1586156, 1586149)

Original Issue: DC-1365

The intent is to ensure that no records exists that have any of the observation_source_concept_id above by sandboxing
any rows that have those observation_source_concept_id and removing them from the observation table.
"""

# Python imports
import os

# Third party imports
from dateutil import parser

# Project imports
from common import OBSERVATION
from app_identity import PROJECT_ID
from cdr_cleaner.cleaning_rules.race_ethnicity_record_suppression import RaceEthnicityRecordSuppression
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import BaseTest


class RaceEthnicityRecordSuppressionTest(BaseTest.CleaningRulesTestBase):

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

        cls.rule_instance = RaceEthnicityRecordSuppression(
            project_id, dataset_id, sandbox_id)

        sb_table_names = cls.rule_instance.sandbox_table_for(OBSERVATION)

        cls.fq_sandbox_table_names.append(
            f'{cls.project_id}.{cls.sandbox_id}.{sb_table_names}')

        cls.fq_table_names = [f'{project_id}.{dataset_id}.observation']

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

        super().setUp()

    def test_race_ethnicity_record_suppression(self):
        """
        Tests that the specifications for SANDBOX_RECORDS_QUERY and DROP_RECORDS_QUERY perform as designed.

        Validates pre conditions, tests execution, and post conditions based on the load
        statements and the tables_and_counts variable.
        """

        drop_records_query_tmpl = self.jinja_env.from_string(
            """
            INSERT INTO `{{fq_dataset_name}}.observation`
                (observation_id, person_id,  observation_concept_id, observation_date, observation_type_concept_id,
                 observation_source_concept_id)
            VALUES
                (1, 101, 11, date('2019-03-03'), 500, 1111111), (2, 102, 22, date('2019-03-03'), 501, 2222222),
                (3, 103, 33, date('2019-03-03'), 502, 1586151), (4, 103, 44, date('2019-03-03'), 503, 1586151), 
                (5, 104, 55, date('2019-03-03'), 504, 1586152), (6, 105, 66, date('2019-03-03'), 505, 1586153), 
                (7, 106, 77, date('2019-03-03'), 506, 1586154), (8, 107, 88, date('2019-03-03'), 507, 1586155), 
                (9, 108, 99, date('2019-03-03'), 508, 1586156), (10, 109, 100, date('2019-03-03'), 509, 1586149)"""
        ).render(fq_dataset_name=self.fq_dataset_name)

        self.load_test_data([drop_records_query_tmpl])

        tables_and_counts = [{
            'fq_table_name':
                '.'.join([self.fq_dataset_name, 'observation']),
            'fq_sandbox_table_name':
                self.fq_sandbox_table_names[0],
            'fields': [
                'observation_id', 'person_id', 'observation_concept_id',
                'observation_date', 'observation_type_concept_id',
                'observation_source_concept_id'
            ],
            'loaded_ids': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            'sandboxed_ids': [3, 4, 5, 6, 7, 8, 9, 10],
            'cleaned_values': [(1, 101, 11, self.date, 500, 1111111),
                               (2, 102, 22, self.date, 501, 2222222)]
        }]

        self.default_test(tables_and_counts)
