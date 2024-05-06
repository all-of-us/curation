"""
Integration test for vehicular_accident_concept_suppression module

Original Issues: DC-1959, DC-2212
"""

# Python Imports
import os

#Project imports
from app_identity import PROJECT_ID
from cdr_cleaner.cleaning_rules.vehicular_accident_concept_suppression import VehicularAccidentConceptSuppression, \
SUPPRESSION_RULE_CONCEPT_TABLE
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import BaseTest
from common import JINJA_ENV, CONDITION_OCCURRENCE, CONCEPT, CONCEPT_RELATIONSHIP, CONCEPT_ANCESTOR


class VehicularAccidentConceptSuppressionTest(BaseTest.CleaningRulesTestBase):

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

        cls.rule_instance = VehicularAccidentConceptSuppression(
            project_id, dataset_id, sandbox_id)

        cls.vocab_tables = [CONCEPT, CONCEPT_RELATIONSHIP, CONCEPT_ANCESTOR]

        cls.fq_table_names = [
            f'{project_id}.{dataset_id}.{CONCEPT}',
            f'{project_id}.{dataset_id}.{CONCEPT_RELATIONSHIP}',
            f'{project_id}.{dataset_id}.{CONCEPT_ANCESTOR}',
            f'{project_id}.{dataset_id}.{CONDITION_OCCURRENCE}',
        ]

        cls.fq_sandbox_table_names.append(
            f'{cls.project_id}.{cls.sandbox_id}.{cls.rule_instance.sandbox_table_for(CONDITION_OCCURRENCE)}'
        )
        cls.fq_sandbox_table_names.append(
            f'{cls.project_id}.{cls.sandbox_id}.{SUPPRESSION_RULE_CONCEPT_TABLE}'
        )

        # call super to set up the client, create datasets, and create
        # empty test tables
        # NOTE:  does not create empty sandbox tables.
        super().setUpClass()

    def setUp(self):
        fq_dataset_name = self.fq_table_names[0].split('.')
        self.fq_dataset_name = '.'.join(fq_dataset_name[:-1])

        #Copy all needed vocab tables to the dataset
        for table in self.vocab_tables:
            self.client.copy_table(
                f'{self.project_id}.{self.vocabulary_id}.{table}',
                f'{self.project_id}.{self.dataset_id}.{table}')

        super().setUp()

    def test_vehicular_accident_concept_suppression_cleaning(self):
        """
        Tests that the specifications perform as designed.

        Validates pre conditions, tests execution, and post conditions based on the load
        statements and the tables_and_counts variable.
        """

        condition_insert_template = JINJA_ENV.from_string("""
           INSERT INTO `{{fq_dataset_name}}.condition_occurrence`
                       (condition_occurrence_id, person_id, condition_concept_id, 
                       condition_start_date, condition_start_datetime, condition_type_concept_id, condition_source_concept_id)
                       VALUES
                       (100, 1, 4016214, date('2020-01-01'), timestamp('2020-01-01'), 42894222, 0),
                       --needs to be suppressed concepts, 4054924, 4055777, 44837728 --
                       (101, 2, 4054924, date('2020-01-01'), timestamp('2020-01-01'), 0, 0),
                       (102, 3, 0, date('2020-01-01'), timestamp('2020-01-01'), 4055777, 0),
                       (103, 4, 0, date('2020-01-01'), timestamp('2020-01-01'), 0, 44837728),
                       (104, 5, 44783245, date('2020-01-01'), timestamp('2020-01-01'), 42894222, 0),
                       (105, 6, 44822621, date('2020-01-01'), timestamp('2020-01-01'), 44822621, 0)
                       """).render(fq_dataset_name=self.fq_dataset_name)

        queries = [condition_insert_template]

        self.load_test_data(queries)

        tables_and_counts = [{
            'fq_table_name':
                '.'.join([self.fq_dataset_name, CONDITION_OCCURRENCE]),
            'fq_sandbox_table_name':
                self.fq_sandbox_table_names[0],
            'loaded_ids': [100, 101, 102, 103, 104, 105],
            'sandboxed_ids': [101, 102, 103, 105],
            'fields': [
                'condition_occurrence_id', 'person_id', 'condition_concept_id',
                'condition_type_concept_id', 'condition_source_concept_id'
            ],
            'cleaned_values': [(100, 1, 4016214, 42894222, 0),
                               (104, 5, 44783245, 42894222, 0)]
        }]

        self.default_test(tables_and_counts)