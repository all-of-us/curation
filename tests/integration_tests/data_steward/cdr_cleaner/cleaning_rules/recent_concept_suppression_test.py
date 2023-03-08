"""
Integration test for covid_ehr_vaccine_concept_suppression module

None

Original Issue: DC1692
"""

# Python Imports
import os

# Project imports
from app_identity import PROJECT_ID
from cdr_cleaner.cleaning_rules.recent_concept_suppression import RecentConceptSuppression
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import BaseTest
from common import JINJA_ENV, OBSERVATION, CONCEPT, CONCEPT_RELATIONSHIP, CONCEPT_ANCESTOR

# Third party imports
from dateutil.parser import parse
from datetime import date
from dateutil.relativedelta import relativedelta


class RecentConceptSuppressionTest(BaseTest.CleaningRulesTestBase):

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

        # Update cutoff_date if necessary. Cutoff date now set to current date
        cls.cutoff_date = date.today()
        cutoff_date_str = cls.cutoff_date.isoformat()

        cls.kwargs.update({'cutoff_date': cutoff_date_str})

        cls.rule_instance = RecentConceptSuppression(project_id, dataset_id,
                                                     sandbox_id)

        cls.vocab_tables = [CONCEPT]

        cls.fq_table_names = [
            f'{project_id}.{dataset_id}.{CONCEPT}',
            f'{project_id}.{dataset_id}.{OBSERVATION}',
        ]

        cls.fq_sandbox_table_names.append(
            f'{cls.project_id}.{cls.sandbox_id}.{cls.rule_instance.sandbox_table_for(OBSERVATION)}'
        )

        # call super to set up the client, create datasets, and create
        # empty test tables
        # NOTE:  does not create empty sandbox tables.
        super().setUpClass()

    def setUp(self):
        fq_dataset_name = self.fq_table_names[0].split('.')
        self.fq_dataset_name = '.'.join(fq_dataset_name[:-1])

        self.recent_date = self.cutoff_date - relativedelta(months=6)
        self.recent_date_str = self.recent_date.isoformat()

        self.old_date = self.cutoff_date - relativedelta(months=18)
        self.old_date_str = self.old_date.isoformat()

        self.date = parse('2020-05-05').date()

        super().setUp()

    def test_recent_concept_suppression_cleaning(self):
        """
        Tests that the specifications perform as designed.

        Validates pre conditions, tests execution, and post conditions based on the load
        statements and the tables_and_counts variable.        
        """

        INSERT_CONCEPTS_QUERY = JINJA_ENV.from_string("""
            INSERT INTO `{{fq_dataset_name}}.concept`
                (concept_id, concept_name, domain_id, vocabulary_id, concept_class_id, concept_code, valid_start_date, valid_end_date)
            VALUES
                -- New PPI Concepts (Not suppressed: Is PPI) --
                (1310093, 'I have already had COVID-19.', 'Observation', 'PPI', 'Answer', 'cope_a_301', date('{{recent_date}}'), date('3999-01-01')),
                (765936, 'COVID-19 Vaccine Survey', 'Observation', 'PPI', 'Module', 'cope_vaccine3', date('{{recent_date}}'), date('3999-01-01')),

                -- Old Non-PPI Concepts (Not suppressed: valid_start_date < cutoff_date - 1 year) --
                (37310269, 'COVID-19', 'Condition', 'SNOMED', 'Clinical Finding', '1240751000000100', date('{{old_date}}'), date('3999-01-01')),
                (37310268, 'Suspected COVID-19', 'Observation', 'SNOMED', 'Context-dependent', '1240761000000102', date('{{old_date}}'), date('3999-01-01')),

                -- New Non-PPI Concepts (Suppressed: valid_start_date >= cutoff_date - 1 year) --
                (1615361, 'COVID-19 VACCINE MRNA', 'Drug', 'VANDF', 'Drug Product', '4039850', date('{{recent_date}}'), date('3999-01-01')),
                (36661764, 'SARS-CoV-2 (COVID-19) Ag', 'Observation', 'LOINC', 'LOINC Component', 'LP418019-8', date('{{recent_date}}'), date('3999-01-01'))
        """).render(fq_dataset_name=self.fq_dataset_name,
                    recent_date=self.recent_date_str,
                    old_date=self.old_date_str)

        INSERT_OBSERVATIONS_QUERY = JINJA_ENV.from_string("""
            INSERT INTO `{{fq_dataset_name}}.observation`
                (observation_id, person_id, observation_concept_id, observation_source_concept_id, observation_date, 
                observation_type_concept_id)
            VALUES
                -- Not suppressed: Is PPI valid_start_date < cutoff_date - 1 year --                
                (1, 101, 1310093, 0, date('2020-05-05'), 1),
                (2, 115, 765936, 0, date('2020-05-05'), 2),

                -- Not suppressed: valid_start_date < cutoff_date - 1 year --
                (3, 116, 37310268, 98, date('2020-05-05'), 1),
                (4, 116, 0, 37310268, date('2020-05-05'), 1),

                -- Suppressed: Non-PPI and valid_start_date >= cutoff_date - 1 year --
                (5, 102, 0, 1615361, date('2020-05-05'), 2),
                (6, 103, 36661764, 0, date('2020-05-05'), 3)

        """).render(fq_dataset_name=self.fq_dataset_name)

        queries = [INSERT_CONCEPTS_QUERY, INSERT_OBSERVATIONS_QUERY]

        self.load_test_data(queries)

        tables_and_counts = [{
            'fq_table_name':
                '.'.join([self.fq_dataset_name, OBSERVATION]),
            'fq_sandbox_table_name':
                self.fq_sandbox_table_names[0],
            'loaded_ids': [1, 2, 3, 4, 5, 6],
            'sandboxed_ids': [5, 6],
            'fields': [
                'observation_id', 'person_id', 'observation_concept_id',
                'observation_source_concept_id', 'observation_date',
                'observation_type_concept_id'
            ],
            'cleaned_values': [(1, 101, 1310093, 0, self.date, 1),
                               (2, 115, 765936, 0, self.date, 2),
                               (3, 116, 37310268, 98, self.date, 1),
                               (4, 116, 0, 37310268, self.date, 1)]
        }]

        self.default_test(tables_and_counts)
