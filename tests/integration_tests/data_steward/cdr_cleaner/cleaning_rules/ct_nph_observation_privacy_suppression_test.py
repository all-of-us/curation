"""
Integration test for ct_nph_observation_privacy_suppression module

"""

# Python Imports
import os

# Third party imports
from dateutil import parser

#Project imports
from app_identity import PROJECT_ID
from cdr_cleaner.cleaning_rules.deid.ct_nph_observation_privacy_suppression import CTNPHObservationPrivacySuppression, \
ISSUE_NUMBERS
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import BaseTest
from common import CONCEPT, OBSERVATION


class NPHConceptSuppressionTest(BaseTest.CleaningRulesTestBase):

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

        cls.rule_instance = CTNPHObservationPrivacySuppression(project_id, dataset_id,
                                                     sandbox_id)

        sb_table_names = cls.rule_instance.sandbox_table_for(OBSERVATION)

        cls.fq_sandbox_table_names.append(
            f'{cls.project_id}.{cls.sandbox_id}.{sb_table_names}')
        cls.fq_sandbox_table_names.append(
            f'{cls.project_id}.{cls.sandbox_id}.ct_nph_observation_suppressions_{ISSUE_NUMBERS[0]}'
        )

        cls.fq_table_names = [
            f'{project_id}.{dataset_id}.{OBSERVATION}',
            f'{project_id}.{dataset_id}.{CONCEPT}',
        ]

        # call super to set up the client, create datasets, and create
        # empty test tables
        # NOTE:  does not create empty sandbox tables.
        super().setUpClass()

    def setUp(self):
        fq_dataset_name = self.fq_table_names[0].split('.')
        self.fq_dataset_name = '.'.join(fq_dataset_name[:-1])

        self.date = parser.parse('2020-05-05').date()

        super().setUp()

    def test_nph_concept_suppression_cleaning(self):
        """
        Tests that the sepcifications for QUERYNAME perform as designed.

        Validates pre conditions, tests execution, and post conditions based on the load
        statements and the tables_and_counts variable.        
        """

        create_concepts_query_tmpl = self.jinja_env.from_string("""
            INSERT INTO `{{fq_dataset_name}}.concept`
                (concept_id, concept_name, domain_id, vocabulary_id, concept_class_id, concept_code, valid_start_date, valid_end_date)
            VALUES
            -- test suppression on concept_code --
                (111, "some text", "some text", "NPH", "some text", "snsc_sc_instagram_num", date('2020-05-05'), date('2020-05-05')),
            -- test suppression on concept_id --
                (222, "some text", "some text", "PPI", "some text", "snsc_sc_twitter_yn", date('2020-05-05'), date('2020-05-05')),
            -- no suppression --
                (333, "some text", "some text", "FAKE", "some text", "some text", date('2020-05-05'), date('2020-05-05'))
        """).render(fq_dataset_name=self.fq_dataset_name)

        drop_records_query_tmpl = self.jinja_env.from_string("""
            INSERT INTO `{{fq_dataset_name}}.observation`
                (observation_id, person_id, observation_concept_id, observation_source_value, observation_date, 
                observation_type_concept_id)
            VALUES
                (1, 1, 1111, 'snsc_sc_instagram_num', date('2020-05-05'), 1),
                (2, 1, 222, 'text', date('2020-05-05'), 1),
                (3, 1, 333, 'some text', date('2020-05-05'), 1)

            """).render(fq_dataset_name=self.fq_dataset_name)

        queries = [create_concepts_query_tmpl, drop_records_query_tmpl]

        self.load_test_data(queries)

        tables_and_counts = [{
            'fq_table_name':
                '.'.join([self.fq_dataset_name, 'observation']),
            'fq_sandbox_table_name':
                self.fq_sandbox_table_names[0],
            'loaded_ids': [1,2,3],
            'sandboxed_ids': [1,2],
            'fields': [
                'observation_id', 'person_id', 'observation_concept_id', 'observation_source_value',
                'observation_date', 'observation_type_concept_id'
            ],
            'cleaned_values': [(3, 1, 333, 'some text', self.date, 1),
                               ]
        }]

        self.default_test(tables_and_counts)
