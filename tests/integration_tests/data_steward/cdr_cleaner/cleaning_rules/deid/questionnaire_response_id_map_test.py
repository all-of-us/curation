"""
Integration test for create_deid_questionnaire_response_map module

None

Original Issue: DC2065
"""

# Python Imports
import os

# Third party imports
from dateutil import parser

# Project imports
from app_identity import PROJECT_ID
from cdr_cleaner.cleaning_rules.deid.questionnaire_response_id_map import QRIDtoRID
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import BaseTest
from common import DEID_QUESTIONNAIRE_RESPONSE_MAP, OBSERVATION


class QRIDtoRIDTest(BaseTest.CleaningRulesTestBase):

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

        cls.rule_instance = QRIDtoRID(
            project_id,
            dataset_id,
            sandbox_id,
            deid_questionnaire_response_map_dataset=dataset_id
        )  #store mapping in dataset

        cls.fq_table_names = [
            f'{project_id}.{dataset_id}.{OBSERVATION}',
            f'{project_id}.{dataset_id}.{DEID_QUESTIONNAIRE_RESPONSE_MAP}'
        ]

        cls.kwargs['deid_questionnaire_response_map_dataset'] = dataset_id

        # cls.fq_sandbox_table_names.append(
        #     f'{cls.project_id}.{cls.sandbox_id}.{DEID_QUESTIONNAIRE_RESPONSE_MAP}'
        # )

        # call super to set up the client, create datasets, and create
        # empty test tables
        # NOTE:  does not create empty sandbox tables.
        super().setUpClass()

    def setUp(self):
        fq_dataset_name = self.fq_table_names[0].split('.')
        self.fq_dataset_name = '.'.join(fq_dataset_name[:-1])

        self.date = parser.parse('2020-05-05').date()

        super().setUp()

    def test_qrid_to_rid_cleaning(self):
        """
        Tests that the specifications perform as designed.

        Validates pre conditions, tests execution, and post conditions based on the load
        statements and the tables_and_counts variable.        
        """

        create_observations_query = self.jinja_env.from_string("""
            INSERT INTO `{{fq_dataset_name}}.observation`
                (observation_id, person_id, observation_concept_id, observation_date, 
                observation_type_concept_id, questionnaire_response_id)
            VALUES
                (1, 1, 43529626, date('2020-05-05'), 1, 1),
                (2, 2, 43529099, date('2020-05-05'), 2, 1),
                (3, 3, 43529102, date('2020-05-05'), 3, 2),
                (4, 4, 43529627, date('2020-05-05'), 4, 2),
                (5, 5, 43529625, date('2020-05-05'), 5, 3),
                (6, 6, 43529626, date('2020-05-05'), 1, 99999)
            """).render(fq_dataset_name=self.fq_dataset_name)

        create_mappings_query = self.jinja_env.from_string("""
            INSERT INTO `{{fq_dataset_name}}.{{deid_questionnaire_response_map}}`
                (questionnaire_response_id, research_response_id)
            VALUES
                (1, 5000),
                (2, 8005),
                (3, 9000)
            """).render(
            fq_dataset_name=self.fq_dataset_name,
            deid_questionnaire_response_map=DEID_QUESTIONNAIRE_RESPONSE_MAP)

        queries = [create_observations_query, create_mappings_query]

        self.load_test_data(queries)

        tables_and_counts = [{
            'fq_table_name':
                f'{self.fq_dataset_name}.{OBSERVATION}',
            'fq_sandbox_table_name':
                '',
            'loaded_ids': [1, 2, 3, 4, 5, 6],
            'sandboxed_ids': [],
            'fields': [
                'observation_id', 'person_id', 'observation_concept_id',
                'observation_date', 'observation_type_concept_id',
                'questionnaire_response_id'
            ],
            'cleaned_values': [(1, 1, 43529626, self.date, 1, 5000),
                               (2, 2, 43529099, self.date, 2, 5000),
                               (3, 3, 43529102, self.date, 3, 8005),
                               (4, 4, 43529627, self.date, 4, 8005),
                               (5, 5, 43529625, self.date, 5, 9000),
                               (
                                   6,
                                   6,
                                   43529626,
                                   self.date,
                                   1,
                               )]
        }]

        self.default_test(tables_and_counts)