"""
Integration test for the free_text_survey_response_suppression module

Original Issue: DC-1387

Ensures participant privacy by removing any records containing concepts related to free text responses
"""

# Python imports
import os

# Third party imports
from dateutil import parser

# Project imports
from common import CONCEPT, OBSERVATION
from app_identity import PROJECT_ID
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import BaseTest
from cdr_cleaner.cleaning_rules.free_text_survey_response_suppression import (
    FreeTextSurveyResponseSuppression, SUPPRESSION_RULE_CONCEPT_TABLE)


class FreeTextSurveyResponseSuppressionTest(BaseTest.CleaningRulesTestBase):

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

        cls.rule_instance = FreeTextSurveyResponseSuppression(
            project_id, dataset_id, sandbox_id)

        cls.fq_table_names = [
            f'{project_id}.{dataset_id}.{OBSERVATION}',
            f'{project_id}.{dataset_id}.{CONCEPT}'
        ]

        for table_name in [OBSERVATION]:
            sandbox_table_name = cls.rule_instance.sandbox_table_for(table_name)
            cls.fq_sandbox_table_names.append(
                f'{cls.project_id}.{cls.sandbox_id}.{sandbox_table_name}')

        # Add SUPPRESSION_RULE_CONCEPT_TABLE to fq_sandbox_table_names so it gets deleted after
        # the test
        cls.fq_sandbox_table_names.append(
            f'{cls.project_id}.{cls.sandbox_id}.{SUPPRESSION_RULE_CONCEPT_TABLE}'
        )

        # call super to set up the client, create datasets
        cls.up_class = super().setUpClass()

    def setUp(self):
        """
        Create common information for tests.

        Creates common expected parameter types from cleaned tables and a common
        fully qualified (fq) dataset name string used to load the data.
        """
        self.observation_date = parser.parse('2017-05-02').date()

        fq_dataset_name = self.fq_table_names[0].split('.')
        self.fq_dataset_name = '.'.join(fq_dataset_name[:-1])

        fq_sandbox_name = self.fq_sandbox_table_names[0].split('.')
        self.fq_sandbox_name = '.'.join(fq_sandbox_name[:-1])

        super().setUp()

    def test_free_text_suppression_cleaning(self):
        """
        Validates pre conditions, tests execution, and post conditions based on the load
        statements and the tables_and_counts variable.
        """

        concept_table_tmpl = self.jinja_env.from_string("""
            INSERT INTO `{{fq_dataset_name}}.concept` 
             (
                concept_id,
                concept_name,
                domain_id,
                vocabulary_id,
                concept_class_id,
                concept_code,
                valid_start_date,
                valid_end_date)
            VALUES
            (111, 'Something Text Box', 'Observation', 'SNOWMED', 
             'Context-dependent', 'WhiteFreeText', '2016-05-01', '2016-05-02'),
            (222, 'Text Box', 'Observation', 'SNOWMED', 'Context-dependent', 
             'ATextBox', '2016-05-01', '2016-05-02'),
           (333, 'None Of These', 'Observation', 'SNOWMED', 'Context-dependent',
            'notes', '2016-05-01', '2016-05-02'),
           (444, 'Will Not Be Dropped', 'Observation', 'SNOWMED', 
            'Context-dependent', '0036T', '2016-05-01', '2016-05-02'),
           (555, 'Will Not Be Dropped', 'Observation', 'SNOWMED', 
            'Context-dependent', '46938', '2016-05-01', '2016-05-02'),
            (666, 'Text Box 2', 'Observation', 'SNOWMED', 
            'Context-dependent', 'Whitefreetext', '2016-05-01', '2016-05-02'),
            (777, 'Text Box 3', 'Observation', 'SNOWMED', 
            'Context-dependent', 'TexTBoX', '2016-05-01', '2016-05-02'),
            (888, 'Text Box 3', 'Observation', 'SNOWMED', 
            'Context-dependent', 'FreeTeXT', '2016-05-01', '2016-05-02')
        """)

        observation_table_tmpl = self.jinja_env.from_string("""
            INSERT INTO `{{fq_dataset_name}}.observation`
            (
                observation_id,
                person_id,
                observation_concept_id,
                observation_date,
                observation_type_concept_id,
                value_as_concept_id,
                qualifier_concept_id,
                unit_concept_id,
                observation_source_concept_id,
                value_source_concept_id)
            VALUES
            -- observation_concept_id corresponds to a free text value and record will be dropped --
                (1, 2, 111, date('2017-05-02'), 0, 0, 0, 0, 0, 0),
            -- observation_type_concept_id corresponds to a free text value and record will be dropped --
                (2, 3, 0, date('2017-05-02'), 222, 0, 0, 0, 0, 0),
            -- value_as_concept_id corresponds to a free text value and record will be dropped --
                (3, 4, 0, date('2017-05-02'), 0, 333, 0, 0, 0, 0),
            -- qualifier_concept_id corresponds to a free text value and record will be dropped --
                (4, 5, 0, date('2017-05-02'), 0, 0, 111, 0, 0, 0),
            -- unit_concept_id corresponds to a free text value and record will be dropped --
                (5, 6, 0, date('2017-05-02'), 0, 0, 0, 222, 0, 0),
            -- observation_source_concept_id corresponds to a free text value and record will be dropped --
                (6, 7, 0, date('2017-05-02'), 0, 0, 0, 0, 333, 0),
            -- value_source_concept_id corresponds to a free text value and record will be dropped --
                (7, 8, 0, date('2017-05-02'), 0, 0, 0, 0, 0, 111),
            -- all valid *_concept_id, no records will dropped --
                (8, 9, 444, date('2017-05-02'), 444, 444, 444, 444, 444, 444),
                (9, 10, 555, date('2017-05-02'), 555, 555, 555, 555, 555, 555),
                (10, 11, 666, date('2017-05-02'), 666, 666, 666, 666, 666, 666),
                (11, 12, 777, date('2017-05-02'), 777, 777, 777, 777, 777, 777),
                (12, 13, 777, date('2017-05-02'), 777, 777, 777, 777, 777, 777)
        """)

        insert_concept_query = concept_table_tmpl.render(
            fq_dataset_name=self.fq_dataset_name)
        insert_observation_query = observation_table_tmpl.render(
            fq_dataset_name=self.fq_dataset_name)

        # Load test data
        self.load_test_data(
            [f'{insert_concept_query};', f'{insert_observation_query};'])

        # Expected results list
        tables_and_counts = [{
            'fq_table_name':
                f'{self.fq_dataset_name}.observation',
            'fq_sandbox_table_name':
                f'{self.fq_sandbox_name}.{self.rule_instance.sandbox_table_for(OBSERVATION)}',
            'loaded_ids': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
            'sandboxed_ids': [1, 2, 3, 4, 5, 6, 7, 10, 11, 12],
            'fields': [
                'observation_id', 'person_id', 'observation_concept_id',
                'observation_date', 'observation_type_concept_id',
                'value_as_concept_id', 'qualifier_concept_id',
                'unit_concept_id', 'observation_source_concept_id',
                'value_source_concept_id'
            ],
            'cleaned_values': [(8, 9, 444, self.observation_date, 444, 444, 444,
                                444, 444, 444),
                               (9, 10, 555, self.observation_date, 555, 555,
                                555, 555, 555, 555)]
        }]

        self.default_test(tables_and_counts)
