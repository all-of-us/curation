"""
Integration test for generalize_sex_gender_concepts.py module

Original Issues: DC-1224
"""

# Python Imports
import os
from datetime import date

# Third party imports
from dateutil import parser

# Project Imports
from app_identity import PROJECT_ID
from common import JINJA_ENV, OBSERVATION
from cdr_cleaner.cleaning_rules.generalize_sex_gender_concepts import GeneralizeSexGenderConcepts
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import BaseTest

GENERALIZED_CONCEPT_ID_TEST_QUERY_TEMPLATE = JINJA_ENV.from_string("""
INSERT INTO `{{project_id}}.{{dataset_id}}.observation`

(observation_id, person_id, observation_concept_id, observation_date, observation_datetime, 
observation_type_concept_id, value_as_number, value_as_string, value_as_concept_id, qualifier_concept_id, 
unit_concept_id, provider_id, visit_occurrence_id, visit_detail_id, observation_source_value,
observation_source_concept_id,unit_source_value,qualifier_source_value,value_source_concept_id,value_source_value,questionnaire_response_id)

VALUES
    (100, 1, 1585838, '2009-04-29', TIMESTAMP('2009-04-29'),
     8201211115, 1.0, NULL, 0, 0,
     0, 0, NULL, NULL, '99051',
     1585838, NULL, NULL, 1585840, NULL, NULL
    ),

    (100, 1, 1585845, '2009-04-29', TIMESTAMP('2009-04-29'),
     8201211115, 1.0, NULL, 0, 0,
     0, 0, NULL, NULL, '99051',
     1585845, NULL, NULL, 1585846, NULL, NULL
    ),

    (200, 2, 1585845, '2009-05-15', TIMESTAMP('2009-05-15'),
     8201211116, 1.0, NULL, 0, 0,
     0, 0, NULL, NULL, '99054',
     1585845, NULL, NULL, 1585846, NULL, NULL
    ),

    (300, 3, 1585838, '2009-05-15', TIMESTAMP('2009-05-15'),
     8201211116, 1.0, NULL, 0, 0,
     0, 0, NULL, NULL, '99054',
     1585838, NULL, NULL, 1585839, NULL, NULL
    ),

    (300, 3, 1585845, '2009-05-15', TIMESTAMP('2009-05-15'),
     8201211116, 1.0, NULL, 0, 0,
     0, 0, NULL, NULL, '99054',
     1585845, NULL, NULL, 1585847, NULL, NULL
    ),

    (400, 4, 1585845, '2009-05-15', TIMESTAMP('2009-05-15'),
     8201211116, 1.0, NULL, 0, 0,
     0, 0, NULL, NULL, '99054',
     1585845, NULL, NULL, 1585847, NULL, NULL
    ),

    (500, 5, 4271761, '2018-11-15', NULL,
     45905771, NULL, NULL, 5555555, NULL,
     NULL, NULL, NULL, NULL, NULL,
     4271761, NULL, NULL, 0, NULL, NULL
    ),

    (600, 6, 2617460, '2014-02-26', TIMESTAMP('2014-02-26'),
     8201211115, 1.0, NULL, 0, 0,
     0, 0, 224968761, NULL, '0390',
     2617460, NULL, NULL, NULL, NULL, NULL
    )
""")


class GeneralizeSexGenderConceptsTest(BaseTest.CleaningRulesTestBase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

        super().initialize_class_vars()

        # Set the test project identifier
        cls.project_id = os.environ.get(PROJECT_ID)

        # Set the expected test datasets
        cls.dataset_id = os.environ.get('COMBINED_DATASET_ID')
        cls.sandbox_id = f'{cls.dataset_id}_sandbox'

        # Instantiate class
        cls.rule_instance = GeneralizeSexGenderConcepts(
            cls.project_id,
            cls.dataset_id,
            cls.sandbox_id,
        )

        # Generate sandbox table names
        sandbox_table_names = cls.rule_instance.get_sandbox_tablenames()
        for table_name in sandbox_table_names:
            cls.fq_sandbox_table_names.append(
                f'{cls.project_id}.{cls.sandbox_id}.{table_name}')

        # Store observation table name
        observation_table_name = f'{cls.project_id}.{cls.dataset_id}.{OBSERVATION}'
        cls.fq_table_names = [observation_table_name]

        # call super to set up the client, create datasets, and create
        # empty test tables
        # NOTE:  does not create empty sandbox tables.
        super().setUpClass()

    def setUp(self):
        """
        Create test table for the rule to run on
        """
        super().setUp()

        # Query to insert test records into observation table
        generalized_concept_query = GENERALIZED_CONCEPT_ID_TEST_QUERY_TEMPLATE.render(
            project_id=self.project_id, dataset_id=self.dataset_id)

        #Load test data
        self.load_test_data([generalized_concept_query])

    def test_field_cleaning(self):
        """
        person_ids 1 and 2 with observation_concept_id = 1585838 are sandboxed
        """

        # Expected results list
        tables_and_counts = [{
            'fq_table_name':
                self.fq_table_names[0],
            'fq_sandbox_table_name':
                self.fq_sandbox_table_names[0],
            'loaded_ids': [100, 100, 200, 300, 300, 400, 500, 600],
            'sandboxed_ids': [100, 300],
            'fields': [
                'observation_id', 'person_id', 'observation_concept_id',
                'observation_date', 'observation_datetime',
                'observation_type_concept_id', 'value_as_number',
                'value_as_string', 'value_as_concept_id',
                'qualifier_concept_id', 'unit_concept_id', 'provider_id',
                'visit_occurrence_id', 'visit_detail_id',
                'observation_source_value', 'observation_source_concept_id',
                'unit_source_value', 'qualifier_source_value',
                'value_source_concept_id', 'value_source_value',
                'questionnaire_response_id'
            ],
            'cleaned_values': [
                (100, 1, 1585838, date.fromisoformat('2009-04-29'),
                 parser.parse('2009-04-29 00:00:00 UTC'), 8201211115, 1.0, None,
                 2000000002, 0, 0, 0, None, None, '99051', 1585838, None, None,
                 2000000002, None, None),
                (100, 1, 1585845, date.fromisoformat('2009-04-29'),
                 parser.parse('2009-04-29 00:00:00 UTC'), 8201211115, 1.0, None,
                 0, 0, 0, 0, None, None, '99051', 1585845, None, None, 1585846,
                 None, None),
                (200, 2, 1585845, date.fromisoformat('2009-05-15'),
                 parser.parse('2009-05-15 00:00:00 UTC'), 8201211116, 1.0, None,
                 0, 0, 0, 0, None, None, '99054', 1585845, None, None, 1585846,
                 None, None),
                (300, 3, 1585838, date.fromisoformat('2009-05-15'),
                 parser.parse('2009-05-15 00:00:00 UTC'), 8201211116, 1.0, None,
                 2000000002, 0, 0, 0, None, None, '99054', 1585838, None, None,
                 2000000002, None, None),
                (300, 3, 1585845, date.fromisoformat('2009-05-15'),
                 parser.parse('2009-05-15 00:00:00 UTC'), 8201211116, 1.0, None,
                 0, 0, 0, 0, None, None, '99054', 1585845, None, None, 1585847,
                 None, None),
                (400, 4, 1585845, date.fromisoformat('2009-05-15'),
                 parser.parse('2009-05-15 00:00:00 UTC'), 8201211116, 1.0, None,
                 0, 0, 0, 0, None, None, '99054', 1585845, None, None, 1585847,
                 None, None),
                (500, 5, 4271761, date.fromisoformat('2018-11-15'), None,
                 45905771, None, None, 5555555, None, None, None, None, None,
                 None, 4271761, None, None, 0, None, None),
                (600, 6, 2617460, date.fromisoformat('2014-02-26'),
                 parser.parse('2014-02-26 00:00:00 UTC'), 8201211115, 1.0, None,
                 0, 0, 0, 0, 224968761, None, '0390', 2617460, None, None, None,
                 None, None)
            ]
        }]

        self.default_test(tables_and_counts)
