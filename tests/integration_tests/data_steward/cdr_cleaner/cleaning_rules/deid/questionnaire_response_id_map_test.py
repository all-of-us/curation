"""
Integration test for create_deid_questionnaire_response_map module
"""

# Python Imports
import os

# Third party imports

# Project imports
from app_identity import PROJECT_ID
from cdr_cleaner.cleaning_rules.deid.questionnaire_response_id_map import QRIDtoRID
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import BaseTest
from common import DEID_QUESTIONNAIRE_RESPONSE_MAP, OBSERVATION, SURVEY_CONDUCT


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
            f'{project_id}.{dataset_id}.{SURVEY_CONDUCT}',
            f'{project_id}.{dataset_id}.{DEID_QUESTIONNAIRE_RESPONSE_MAP}'
        ]

        sb_table_name = cls.rule_instance.sandbox_table_for(SURVEY_CONDUCT)
        cls.fq_sandbox_table_names = [
            f'{project_id}.{cls.sandbox_id}.{sb_table_name}'
        ]

        cls.kwargs['deid_questionnaire_response_map_dataset'] = dataset_id

        # call super to set up the client, create datasets, and create
        # empty test tables
        # NOTE:  does not create empty sandbox tables.
        super().setUpClass()

    def setUp(self):
        fq_dataset_name = self.fq_table_names[0].split('.')
        self.fq_dataset_name = '.'.join(fq_dataset_name[:-1])

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

        create_survey_conduct_query = self.jinja_env.from_string("""
            INSERT INTO `{{fq_dataset_name}}.survey_conduct`
                (survey_conduct_id, person_id, survey_concept_id, survey_end_datetime, 
                assisted_concept_id, respondent_type_concept_id, timing_concept_id,
                collection_method_concept_id, survey_source_concept_id,
                validated_survey_concept_id)
            VALUES
                (1, 1, 0, timestamp('2020-05-05 00:00:00'), 0, 0, 0, 0, 0, 0),
                (2, 2, 0, timestamp('2020-05-05 00:00:00'), 0, 0, 0, 0, 0, 0),
                (3, 3, 0, timestamp('2020-05-05 00:00:00'), 0, 0, 0, 0, 0, 0),
                (4, 4, 0, timestamp('2020-05-05 00:00:00'), 0, 0, 0, 0, 0, 0)
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

        queries = [
            create_observations_query, create_survey_conduct_query,
            create_mappings_query
        ]

        self.load_test_data(queries)

        tables_and_counts = [{
            'fq_table_name':
                f'{self.fq_dataset_name}.{OBSERVATION}',
            'fq_sandbox_table_name':
                '',
            'loaded_ids': [1, 2, 3, 4, 5, 6],
            'sandboxed_ids': [],
            'fields': [
                'observation_id', 'person_id', 'questionnaire_response_id'
            ],
            'cleaned_values': [(1, 1, 5000), (2, 2, 5000), (3, 3, 8005),
                               (4, 4, 8005), (5, 5, 9000), (6, 6, None)]
        }, {
            'fq_table_name': f'{self.fq_dataset_name}.{SURVEY_CONDUCT}',
            'fq_sandbox_table_name': self.fq_sandbox_table_names[0],
            'loaded_ids': [1, 2, 3, 4],
            'sandboxed_ids': [4],
            'fields': ['survey_conduct_id', 'person_id'],
            'cleaned_values': [(5000, 1), (8005, 2), (9000, 3)]
        }]

        self.default_test(tables_and_counts)