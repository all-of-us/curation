"""
Integration test for section_participation_concept_suppression module

Record suppress (drop the rows) responses related to the section_particiption concept (*_concept_id = 903632) in both registered and controlled tiers.


Original Issue: DC-1641
"""

# Python Imports
import os

# Project imports
from app_identity import PROJECT_ID
from cdr_cleaner.cleaning_rules.section_participation_concept_suppression \
    import SectionParticipationConceptSuppression, SUPPRESSION_RULE_CONCEPT_TABLE
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import BaseTest
from common import OBSERVATION, VOCABULARY_TABLES

# Third party imports
from dateutil import parser


class SectionParticipationConceptSuppressionTest(BaseTest.CleaningRulesTestBase
                                                ):

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
        cls.vocabulary_id = os.environ.get('VOCABULARY_DATASET')

        cls.rule_instance = SectionParticipationConceptSuppression(
            project_id, dataset_id, sandbox_id)

        cls.vocab_tables = VOCABULARY_TABLES

        cls.fq_table_names = [
            f'{project_id}.{dataset_id}.{OBSERVATION}',
        ] + [
            f'{project_id}.{dataset_id}.{vocab_table}'
            for vocab_table in cls.vocab_tables
        ]

        cls.fq_sandbox_table_names = [
            f'{project_id}.{sandbox_id}.{cls.rule_instance.sandbox_table_for(OBSERVATION)}',
        ]
        cls.fq_sandbox_table_names.append(
            f'{project_id}.{sandbox_id}.{SUPPRESSION_RULE_CONCEPT_TABLE}')

        # call super to set up the client, create datasets, and create
        # empty test tables
        # NOTE:  does not create empty sandbox tables.
        super().setUpClass()

    def setUp(self):
        fq_dataset_name = self.fq_table_names[0].split('.')
        self.fq_dataset_name = '.'.join(fq_dataset_name[:-1])

        self.date = parser.parse('2020-05-05').date()

        super().setUp()
        self.copy_vocab_tables(self.vocabulary_id)

    def test_section_participation_concept_suppression_cleaning(self):
        """
        Tests that the specifications perform as designed.

        Validates pre conditions, tests execution, and post conditions based on the load
        statements and the tables_and_counts variable.        
        """
        queries = []

        create_observations_query_tmpl = self.jinja_env.from_string("""
            INSERT INTO `{{fq_dataset_name}}.observation`
                (observation_id, person_id, observation_concept_id, observation_date, 
                observation_type_concept_id)
            VALUES
                (1, 1, 1000, date('2020-05-05'), 1),
                (2, 2, 2000, date('2020-05-05'), 2),
                (3, 3, 903632, date('2020-05-05'), 3),
                (4, 4, 4000, date('2020-05-05'), 4)     
        """).render(fq_dataset_name=self.fq_dataset_name)
        queries.append(create_observations_query_tmpl)

        self.load_test_data(queries)

        tables_and_counts = [{
            'fq_table_name':
                '.'.join([self.fq_dataset_name, OBSERVATION]),
            'fq_sandbox_table_name':
                self.fq_sandbox_table_names[0],
            'loaded_ids': [1, 2, 3, 4],
            'sandboxed_ids': [3],
            'fields': [
                'observation_id', 'person_id', 'observation_concept_id',
                'observation_date', 'observation_type_concept_id'
            ],
            'cleaned_values': [(1, 1, 1000, self.date, 1),
                               (2, 2, 2000, self.date, 2),
                               (4, 4, 4000, self.date, 4)]
        }]

        self.default_test(tables_and_counts)
