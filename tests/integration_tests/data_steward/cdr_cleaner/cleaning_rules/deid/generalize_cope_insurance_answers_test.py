"""
Integration test for generalize_cope_insurance_answers module

13332904 and 1333140 have been identified as potentially re-identifying answers to question 1332737.
Privacy has determined these responses must be generalized to 1333127.
This question is a multi-select question.  Potential duplicates will need to be dropped after generalization.

This cleaning rule is specific to the Registered tier.


Original Issue: DC1665
"""

# Python Imports
import os
from dateutil.parser import parse

#Project imports
from app_identity import PROJECT_ID
from cdr_cleaner.cleaning_rules.deid.genaralize_cope_insurance_answers import GeneralizeCopeInsuranceAnswers
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import BaseTest
from common import OBSERVATION


class GeneralizeCopeInsuranceAnswersTest(BaseTest.CleaningRulesTestBase):

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

        cls.rule_instance = GeneralizeCopeInsuranceAnswers(
            project_id, dataset_id, sandbox_id)

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

        super().setUp()

    def test_generalize_cope_insurance_answers(self):
        """
        Tests that the specifications for GeneralizeCopeInsuranceAnswers perform as designed.

        Validates pre conditions, tests execution, and post conditions based on the load
        statements and the tables_and_counts variable.
        """
        queries = []

        # Insert test data
        cope_insurance_answers_tmpl = self.jinja_env.from_string("""
            INSERT INTO `{{fq_dataset_name}}.observation`
            (observation_id, person_id, observation_concept_id, observation_date, observation_type_concept_id,
             observation_source_concept_id, value_source_concept_id,
              value_as_concept_id, value_source_value)
             VALUES
                (1, 1001, 0, '2020-08-30', 0, 1332737, 1332904, 45883720, 'cope_a_68'),
                (2, 1002, 0, '2021-01-30', 0, 1332737, 1333140, 1333140, 'cope_a_109'),
                (3, 1002, 0, '2020-08-30', 0, 1332737, 1332904, 45883720, 'cope_a_68'),
                (4, 1001, 0, '2020-08-30', 0, 1332737, 1333127, 1333127, 'cope_a_33'),
                (5, 1003, 0, '2020-08-30', 0, 1332737, 903096, 903096, 'PMI_Skip'),
                (6, 1004, 0, '2020-08-30', 0, 1332737, 1333147, 1333147, 'cope_a_170'),
                (7, 1005, 0, '2020-08-30', 0, 1007, 0, 0, '')
        """).render(fq_dataset_name=self.fq_dataset_name)
        queries.append(cope_insurance_answers_tmpl)

        self.load_test_data(queries)

        tables_and_counts = [{
            'fq_table_name':
                '.'.join([self.fq_dataset_name, 'observation']),
            'fq_sandbox_table_name':
                '',
            'loaded_ids': [1, 2, 3, 4, 5, 6, 7],
            'sandboxed_ids': [],
            'fields': [
                'observation_id', 'person_id', 'observation_concept_id',
                'observation_date', 'observation_source_concept_id',
                'value_source_concept_id', 'value_as_concept_id',
                'value_source_value'
            ],
            'cleaned_values': [(1, 1001, 0, parse('2020-08-30').date(), 1332737,
                                1333127, 1333127, 'cope_a_33'),
                               (2, 1002, 0, parse('2021-01-30').date(), 1332737,
                                1333127, 1333127, 'cope_a_33'),
                               (5, 1003, 0, parse('2020-08-30').date(), 1332737,
                                903096, 903096, 'PMI_Skip'),
                               (6, 1004, 0, parse('2020-08-30').date(), 1332737,
                                1333147, 1333147, 'cope_a_170'),
                               (7, 1005, 0, parse('2020-08-30').date(), 1007, 0,
                                0, '')]
        }]

        self.default_test(tables_and_counts)
