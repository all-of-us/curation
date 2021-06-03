"""
Integration test for covid_ehr_vaccine_concept_suppression module

None

Original Issue: DC1692
"""

# Python Imports
import os

#Project imports
from app_identity import PROJECT_ID
from cdr_cleaner.cleaning_rules.covid_ehr_vaccine_concept_suppression import CovidEHRVaccineConceptSuppression
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import BaseTest
from common import JINJA_ENV, OBSERVATION, CONCEPT, CONCEPT_RELATIONSHIP, CONCEPT_ANCESTOR

# Third party imports
from dateutil.parser import parse


class CovidEHRVaccineConceptSuppressionTest(BaseTest.CleaningRulesTestBase):

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

        cls.rule_instance = CovidEHRVaccineConceptSuppression(
            project_id, dataset_id, sandbox_id)

        cls.vocab_tables = [CONCEPT, CONCEPT_RELATIONSHIP, CONCEPT_ANCESTOR]

        cls.fq_table_names = [
            f'{project_id}.{dataset_id}.{OBSERVATION}',
        ]

        cls.fq_sandbox_table_names.append(
            f'{cls.project_id}.{cls.sandbox_id}.{OBSERVATION}')

        # call super to set up the client, create datasets, and create
        # empty test tables
        # NOTE:  does not create empty sandbox tables.
        super().setUpClass()

    def setUp(self):
        fq_dataset_name = self.fq_table_names[0].split('.')
        self.fq_dataset_name = '.'.join(fq_dataset_name[:-1])

        self.date = parse('2020-05-05').date()

        # #Copy all needed vocab tables to the dataset
        # for table in self.vocab_tables:
        #     self.client.copy_table(
        #         f'{self.project_id}.{self.vocabulary_id}.{table}',
        #         f'{self.project_id}.{self.dataset_id}.{table}')

        super().setUp()

    def test_covid_ehr_vaccine_concept_suppression_cleaning(self):
        """
        Tests that the specifications perform as designed.

        Validates pre conditions, tests execution, and post conditions based on the load
        statements and the tables_and_counts variable.        
        """

        INSERT_CONCEPTS_QUERY = JINJA_ENV.from_string("""
            INSERT INTO `{{fq_dataset_name}}.concept`
            (concept_id, concept_name, domain_id, vocabulary_id, concept_class_id, concept_code, valid_start_date,
            valid_end_date)
            VALUES
            -- Suppressed concepts via name, vocab, and code --
            (724904, 'SARS-COV-2 (COVID-19) vaccine, UNSPECIFIED', 'Drug', 'CVX', 'CVX', 213, '2020-11-16', '2099-12-31')
        """)

        INSERT_OBSERVATIONS_QUERY = JINJA_ENV.from_string("""
            INSERT INTO `{{fq_dataset_name}}.observation`
                (observation_id, person_id, observation_concept_id, observation_source_concept_id, observation_date, 
                observation_type_concept_id)
            VALUES
                -- Records suppressed due to COVID vaccine observation_concept_id --
                (1, 101, 42794278, 0, date('2020-05-05'), 1),
                (2, 102, 37003434, 0, date('2020-05-05'), 2),
                (3, 103, 766239, 0, date('2020-05-05'), 3),
                -- Records suppressed due to COVID vaccine observation_source_concept_id --
                (4, 104, 0, 42794278, date('2020-05-05'), 4),
                (5, 105, 0, 37003434, date('2020-05-05'), 5),
                (6, 106, 0, 766239, date('2020-05-05'), 6),
                -- Records not suppressed due to lack of COVID vaccine concepts --
                (7, 107, 1585250, 0, date('2020-05-05'), 7),
                (8, 108, 1585250, 1585250, date('2020-05-05'), 8),
                (9, 109, 1585248, 1585248, date('2020-05-05'), 0),
                (10, 109, 0, 1585224, date('2020-05-05'), 0)
        """).render(fq_dataset_name=self.fq_dataset_name)

        queries = [INSERT_OBSERVATIONS_QUERY]

        self.load_test_data(queries)

        tables_and_counts = [{
            'fq_table_name':
                '.'.join([self.fq_dataset_name, OBSERVATION]),
            'fq_sandbox_table_name':
                self.fq_sandbox_table_names[0],
            'loaded_ids': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            'sandboxed_ids': [1, 2, 3, 4, 5, 6],
            'fields': [
                'observation_id', 'person_id', 'observation_concept_id',
                'observation_source_concept_id', 'observation_date',
                'observation_type_concept_id'
            ],
            'cleaned_values': [(8, 108, 1585250, 1585250, self.date, 8),
                               (9, 109, 1585248, 1585248, self.date, 0),
                               (10, 109, 0, 1585224, self.date, 0)]
        }]

        self.default_test(tables_and_counts)