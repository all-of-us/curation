"""
Integration test for generalize_state_by_population module

This cleaning rule will generalize participant states that do not meet a
threshold of participant size.


Original Issue: DC-1614
"""

# Python imports
import os

# Third party imports
import pandas as pd
from dateutil import parser
from google.cloud.bigquery import LoadJobConfig

# Project imports
from app_identity import PROJECT_ID
from cdr_cleaner.cleaning_rules.generalize_state_by_population import GeneralizeStateByPopulation
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import BaseTest
from common import OBSERVATION
from utils.bq import get_table_schema, get_client

PARTICIPANT_THRESH = 200

TEST_STATES = {
    'PIIState_NY': {
        'name': 'PIIState_NY',
        'concept_id': 1585297,
        'participant_count': 1000
    },
    'PIIState_MI': {
        'name': 'PIIState_MI',
        'concept_id': 1585287,
        'participant_count': 200
    },
    'PIIState_OR': {
        'name': 'PIIState_OR',
        'concept_id': 1585303,
        'participant_count': 30
    },
}


class GeneralizeStateByPopulationTest(BaseTest.CleaningRulesTestBase):

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

        cls.rule_instance = GeneralizeStateByPopulation(project_id, dataset_id,
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
        self.date_str = '2020-05-05'
        self.date = parser.parse(self.date_str).date()

        super().setUp()

    def load_test_data(self, df, project_id, dataset_id, table):
        """
        Add data to the tables for the rule to run on.

        :param df: a dataframe containing data to insert
        :param project_id
        :param dataset_id
        :param table
        """
        client = get_client(project_id)
        schema = get_table_schema(table)
        schema = [field for field in schema if field.name in list(df.columns)]
        load_job_config = LoadJobConfig(schema=schema)
        load_job = client.load_table_from_dataframe(df,
                                                    f'{dataset_id}.{table}',
                                                    job_config=load_job_config)
        load_job.result()

    def test_generalize_state_by_population_cleaning(self):
        """
        Tests that the specifications for STATE_GENERALIZATION_QUERY perform as designed.

        Validates pre conditions, tests execution, and post conditions based on the load
        statements and the tables_and_counts variable.        
        """

        #Rows to insert
        inserted_rows = []
        test_obs_id = 1
        for test_state in TEST_STATES.values():
            name = test_state['name']
            concept_id = test_state['concept_id']
            participant_count = test_state['participant_count']

            for i in range(participant_count):
                row = {
                    'observation_id': test_obs_id,
                    'person_id': test_obs_id,
                    'observation_concept_id': 40766229,
                    'observation_date': self.date,
                    'observation_type_concept_id': 45905771,
                    'value_as_concept_id': 1,
                    'observation_source_concept_id': 1585249,
                    'value_source_value': name,
                    'value_source_concept_id': concept_id
                }

                inserted_rows.append(row)
                test_obs_id += 1
        inserted_rows_df = pd.DataFrame(inserted_rows,
                                        columns=list(inserted_rows[0].keys()))

        #Rows to expect
        expected_rows = inserted_rows
        for i, expected_row in enumerate(expected_rows):
            if TEST_STATES[expected_row['value_source_value']][
                    'participant_count'] < PARTICIPANT_THRESH:
                expected_rows[i].update({
                    'value_source_concept_id': 2000000011,
                    'value_as_concept_id': 2000000011
                })
        expected_rows_df = pd.DataFrame(expected_rows,
                                        columns=list(expected_rows[0].keys()))

        #Insert rows
        self.load_test_data(inserted_rows_df, self.project_id, self.dataset_id,
                            OBSERVATION)

        #Check match
        tables_and_counts = [{
            'fq_table_name': '.'.join([self.fq_dataset_name, OBSERVATION]),
            'fq_sandbox_table_name': '',
            'loaded_ids': list(expected_rows_df['observation_id']),
            'sandboxed_ids': [],
            'fields': list(expected_rows_df.columns),
            'cleaned_values': [tuple(row) for row in expected_rows_df.values]
        }]

        self.default_test(tables_and_counts)
