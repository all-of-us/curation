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
from common import JINJA_ENV, OBSERVATION, DRUG_EXPOSURE, CONCEPT, CONCEPT_RELATIONSHIP, CONCEPT_ANCESTOR

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
        sandbox_id = f"{dataset_id}_sandbox"
        cls.sandbox_id = sandbox_id

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
            f'{project_id}.{dataset_id}.{DRUG_EXPOSURE}',
        ]

        cls.fq_sandbox_table_names.extend([
            f'{cls.project_id}.{cls.sandbox_id}.{cls.rule_instance.sandbox_table_for(OBSERVATION)}',
            f'{cls.project_id}.{cls.sandbox_id}.{cls.rule_instance.sandbox_table_for(DRUG_EXPOSURE)}',
            f'{cls.project_id}.{cls.sandbox_id}.{cls.rule_instance.concept_suppression_lookup_table}'
        ])

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
        self.datetime = parse('2020-05-05 00:00:00 UTC')

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
                (36661764, 'SARS-CoV-2 (COVID-19) Ag', 'Observation', 'LOINC', 'LOINC Component', 'LP418019-8', date('{{recent_date}}'), date('3999-01-01')),

                -- Default Value valid_start_date  --
                (123, 'foo', 'obs', 'bar', 'obs', 'abc', '1970-01-01', '2099-12-31'),
                (456, 'foo2', 'obs2', 'bax', 'obs', 'def', '1970-01-01', '2099-12-31')
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
                (6, 103, 36661764, 0, date('2020-05-05'), 3),

                -- Suppressed:  Default valid_start_date and first usage in less than 12 months --
                (7, 103, 123, 0, '{{recent_date}}', 0),

                -- Not Suppressed:  Default valid_start_date and first usage more than 12 months ago --
                (8, 103, 456, 0, '{{old_date}}', 0)

        """).render(fq_dataset_name=self.fq_dataset_name,
                    recent_date=self.recent_date_str,
                    old_date=self.old_date_str)

        INSERT_DRUG_EXPOSURES_QUERY = JINJA_ENV.from_string("""
            INSERT INTO `{{fq_dataset_name}}.drug_exposure`
                (drug_exposure_id, person_id, drug_concept_id, drug_source_concept_id, drug_exposure_start_date, drug_exposure_start_datetime, drug_type_concept_id)
            VALUES
                -- Not suppressed: Is PPI valid_start_date < cutoff_date - 1 year --
                (1, 101, 1310093, 0, date('2020-05-05'), timestamp('2020-05-05 00:00:00 UTC'), 1),
                (2, 115, 765936, 0, date('2020-05-05'), timestamp('2020-05-05'), 2),

                -- Not suppressed: valid_start_date < cutoff_date - 1 year --
                (3, 116, 37310268, 98, date('2020-05-05'), timestamp('2020-05-05 00:00:00 UTC'), 1),
                (4, 116, 0, 37310268, date('2020-05-05'), timestamp('2020-05-05 00:00:00 UTC'), 1),

                -- Suppressed: Non-PPI and valid_start_date >= cutoff_date - 1 year --
                (5, 102, 0, 1615361, date('2020-05-05'), timestamp('2020-05-05 00:00:00 UTC'), 2),
                (6, 103, 36661764, 0, date('2020-05-05'), timestamp('2020-05-05 00:00:00 UTC'), 3),

                -- Suppressed:  Default valid_start_date and first usage in less than 12 months --
                (7, 103, 123, 0, '{{recent_date}}', timestamp('2020-05-05 00:00:00 UTC'), 0),

                -- Not Suppressed:  Default valid_start_date and first usage more than 12 months ago --
                (8, 103, 456, 0, '{{old_date}}', timestamp('2020-05-05 00:00:00 UTC'), 0)

        """).render(fq_dataset_name=self.fq_dataset_name,
                    recent_date=self.recent_date_str,
                    old_date=self.old_date_str)

        queries = [
            INSERT_CONCEPTS_QUERY, INSERT_OBSERVATIONS_QUERY,
            INSERT_DRUG_EXPOSURES_QUERY
        ]

        self.load_test_data(queries)

        tables_and_counts = [{
            'fq_table_name':
                '.'.join([self.fq_dataset_name, OBSERVATION]),
            'fq_sandbox_table_name':
                self.fq_sandbox_table_names[0],
            'loaded_ids': [1, 2, 3, 4, 5, 6, 7, 8],
            'sandboxed_ids': [5, 6, 7],
            'fields': [
                'observation_id', 'person_id', 'observation_concept_id',
                'observation_source_concept_id', 'observation_date',
                'observation_type_concept_id'
            ],
            'cleaned_values': [(1, 101, 1310093, 0, self.date, 1),
                               (2, 115, 765936, 0, self.date, 2),
                               (3, 116, 37310268, 98, self.date, 1),
                               (4, 116, 0, 37310268, self.date, 1),
                               (8, 103, 456, 0, self.old_date, 0)]
        }, {
            'fq_table_name':
                '.'.join([self.fq_dataset_name, DRUG_EXPOSURE]),
            'fq_sandbox_table_name':
                self.fq_sandbox_table_names[1],
            'loaded_ids': [1, 2, 3, 4, 5, 6, 7, 8],
            'sandboxed_ids': [5, 6, 7],
            'fields': [
                'drug_exposure_id', 'person_id', 'drug_concept_id',
                'drug_source_concept_id', 'drug_exposure_start_date',
                'drug_exposure_start_datetime', 'drug_type_concept_id'
            ],
            'cleaned_values': [
                (1, 101, 1310093, 0, self.date, self.datetime, 1),
                (2, 115, 765936, 0, self.date, self.datetime, 2),
                (3, 116, 37310268, 98, self.date, self.datetime, 1),
                (4, 116, 0, 37310268, self.date, self.datetime, 1),
                (8, 103, 456, 0, self.old_date, self.datetime, 0)
            ]
        }]

        self.default_test(tables_and_counts)
