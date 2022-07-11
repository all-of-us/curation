"""
Integration test for BackfillPmiSkipCodes.

For simplification, only the following 4 concepts are treated as potentially 
skipped questions in this test: [1586135, 1585386, 1586166, 1585784]
Out of the questions, only 1585784 is female(=8532) specific.

There are 4 persons in this test. Each person represents the following test cases.
PERSON ID:
    1 - Female. Has all of the 4 questions in Observation table. No backfill needed.
    2 - Female. Has 2 records that need backfill. One of them is female-specific (1585784).
    3 - Not female. Has 1 record that needs backfill. 
        The female-specific (1585784) record is not backfilled because #3 is not female.
        #3's newest observation_date(=2020-03-02) is set as the observation_date for the 
        backfilled record.
    4 - Not female. Has 2 records that needs backfill. 
        The female-specific (1585784) record is not backfilled because #4 is not female.
"""

# Python Imports
import os
import mock

# Third party imports
from dateutil import parser

# Project imports
from app_identity import PROJECT_ID
from cdr_cleaner.cleaning_rules.backfill_pmi_skip_codes import BackfillPmiSkipCodes
from common import JINJA_ENV, OBSERVATION, PERSON
from resources import fields_for
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import BaseTest

OBSERVATION_TEMPLATE = JINJA_ENV.from_string("""
    INSERT INTO `{{project}}.{{dataset}}.observation` ({{observation_fields}})
    VALUES
        (11, 1, 9999999, date('2020-01-01'), NULL, 99999999, NULL, 'value_as_string', 99, 99, 99, 99, 99, 99, 
         'observation_source_value', 9999999, 
         'unit_source_value', 'qualifier_source_value', 99, 'value_source_value', 99),
        (12, 1, 3005917, date('2020-01-01'), NULL, 45905771, NULL, 'value_as_string', 99, 99, 99, 99, 99, 99, 
         'TheBasics_Birthplace', 1586135, 
         'unit_source_value', 'qualifier_source_value', 99, 'value_source_value', 99),
        (13, 1, 40766240, date('2020-01-01'), NULL, 45905771, NULL, 'value_as_string', 99, 99, 99, 99, 99, 99, 
         'Insurance_HealthInsurance', 1585386, 
         'unit_source_value', 'qualifier_source_value', 99, 'value_source_value', 99),
        (14, 1, 1586166, date('2020-01-01'), NULL, 45905771, NULL, 'value_as_string', 99, 99, 99, 99, 99, 99, 
         'ElectronicSmoking_ElectricSmokeParticipant', 1586166, 
         'unit_source_value', 'qualifier_source_value', 99, 'value_source_value', 99),
        (15, 1, 40767407, date('2020-01-01'), NULL, 45905771, NULL, 'value_as_string', 99, 99, 99, 99, 99, 99, 
         'OverallHealth_MenstrualStopped', 1585784, 
         'unit_source_value', 'qualifier_source_value', 99, 'value_source_value', 99),
        (21, 2, 3005917, date('2020-02-01'), NULL, 45905771, NULL, 'value_as_string', 99, 99, 99, 99, 99, 99, 
         'TheBasics_Birthplace', 1586135, 
         'unit_source_value', 'qualifier_source_value', 99, 'value_source_value', 99),
        (31, 3, 3005917, date('2020-03-01'), NULL, 45905771, NULL, 'value_as_string', 99, 99, 99, 99, 99, 99, 
         'TheBasics_Birthplace', 1586135, 
         'unit_source_value', 'qualifier_source_value', 99, 'value_source_value', 99),
        (32, 3, 40766240, date('2020-03-02'), NULL, 45905771, NULL, 'value_as_string', 99, 99, 99, 99, 99, 99, 
         'Insurance_HealthInsurance', 1585386, 
         'unit_source_value', 'qualifier_source_value', 99, 'value_source_value', 99),
        (41, 4, 3005917, date('2020-01-01'), NULL, 45905771, NULL, 'value_as_string', 99, 99, 99, 99, 99, 99, 
         'TheBasics_Birthplace', 1586135, 
         'unit_source_value', 'qualifier_source_value', 99, 'value_source_value', 99)
""")

PERSON_TEMPLATE = JINJA_ENV.from_string("""
    INSERT INTO `{{project}}.{{dataset}}.person` ({{person_fields}})
    VALUES
        (1, 8532, 2001, 999, 99999),
        (2, 8532, 2001, 999, 99999),
        (3, 8532, 2001, 999, 99999),
        (4, 9999, 2001, 999, 99999)
""")

OBSERVATION_FIELDS = [field['name'] for field in fields_for(OBSERVATION)]
PERSON_FIELDS = [
    'person_id', 'gender_concept_id', 'year_of_birth', 'race_concept_id',
    'ethnicity_concept_id'
]


class BackfillPmiSkipCodesTest(BaseTest.CleaningRulesTestBase):

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
        dataset_id = os.environ.get('RDR_DATASET_ID')
        cls.dataset_id = dataset_id
        sandbox_id = dataset_id + '_sandbox'
        cls.sandbox_id = sandbox_id

        cls.rule_instance = BackfillPmiSkipCodes(project_id, dataset_id,
                                                 sandbox_id)

        cls.fq_sandbox_table_names = []

        cls.fq_table_names = [
            f'{project_id}.{dataset_id}.{OBSERVATION}',
            f'{project_id}.{dataset_id}.{PERSON}',
        ]

        super().setUpClass()

    def setUp(self):
        self.date = parser.parse('2020-01-01').date()

        super().setUp()

        insert_observation = OBSERVATION_TEMPLATE.render(
            project=self.project_id,
            dataset=self.dataset_id,
            observation_fields=', '.join(OBSERVATION_FIELDS))
        insert_person = PERSON_TEMPLATE.render(
            project=self.project_id,
            dataset=self.dataset_id,
            person_fields=', '.join(PERSON_FIELDS))

        queries = [insert_observation, insert_person]
        self.load_test_data(queries)

    @mock.patch(
        'cdr_cleaner.cleaning_rules.backfill_pmi_skip_codes.SKIPPED_QUESTIONS',
        [1586135, 1585386, 1586166, 1585784])
    def test_backfill_pmi_skip_codes(self):
        """
        Tests that the queries perform as designed.
        Validates pre conditions, tests execution, and post conditions based on the load
        statements and the tables_and_counts variable.
        """

        tables_and_counts = [
            {
                'fq_table_name':
                    f'{self.project_id}.{self.dataset_id}.observation',
                'fq_sandbox_table_name':
                    None,
                'loaded_ids': [11, 12, 13, 14, 15, 21, 31, 32, 41],
                'sandboxed_ids': [],
                'fields':
                    OBSERVATION_FIELDS,
                'cleaned_values': [
                    (11, 1, 9999999, parser.parse('2020-01-01').date(), None,
                     99999999, None, 'value_as_string', 99, 99, 99, 99, 99, 99,
                     'observation_source_value', 9999999, 'unit_source_value',
                     'qualifier_source_value', 99, 'value_source_value', 99),
                    (12, 1, 3005917, parser.parse('2020-01-01').date(), None,
                     45905771, None, 'value_as_string', 99, 99, 99, 99, 99, 99,
                     'TheBasics_Birthplace', 1586135, 'unit_source_value',
                     'qualifier_source_value', 99, 'value_source_value', 99),
                    (13, 1, 40766240, parser.parse('2020-01-01').date(), None,
                     45905771, None, 'value_as_string', 99, 99, 99, 99, 99, 99,
                     'Insurance_HealthInsurance', 1585386, 'unit_source_value',
                     'qualifier_source_value', 99, 'value_source_value', 99),
                    (14, 1, 1586166, parser.parse('2020-01-01').date(), None,
                     45905771, None, 'value_as_string', 99, 99, 99, 99, 99, 99,
                     'ElectronicSmoking_ElectricSmokeParticipant', 1586166,
                     'unit_source_value', 'qualifier_source_value', 99,
                     'value_source_value', 99),
                    (15, 1, 40767407, parser.parse('2020-01-01').date(), None,
                     45905771, None, 'value_as_string', 99, 99, 99, 99, 99, 99,
                     'OverallHealth_MenstrualStopped', 1585784,
                     'unit_source_value', 'qualifier_source_value', 99,
                     'value_source_value', 99),
                    (21, 2, 3005917, parser.parse('2020-02-01').date(), None,
                     45905771, None, 'value_as_string', 99, 99, 99, 99, 99, 99,
                     'TheBasics_Birthplace', 1586135, 'unit_source_value',
                     'qualifier_source_value', 99, 'value_source_value', 99),
                    (31, 3, 3005917, parser.parse('2020-03-01').date(), None,
                     45905771, None, 'value_as_string', 99, 99, 99, 99, 99, 99,
                     'TheBasics_Birthplace', 1586135, 'unit_source_value',
                     'qualifier_source_value', 99, 'value_source_value', 99),
                    (32, 3, 40766240, parser.parse('2020-03-02').date(), None,
                     45905771, None, 'value_as_string', 99, 99, 99, 99, 99, 99,
                     'Insurance_HealthInsurance', 1585386, 'unit_source_value',
                     'qualifier_source_value', 99, 'value_source_value', 99),
                    (41, 4, 3005917, parser.parse('2020-01-01').date(), None,
                     45905771, None, 'value_as_string', 99, 99, 99, 99, 99, 99,
                     'TheBasics_Birthplace', 1586135, 'unit_source_value',
                     'qualifier_source_value', 99, 'value_source_value', 99),
                    (1000000000005, 2, 40766240,
                     parser.parse('2020-02-01').date(), None, 45905771, None,
                     None, 903096, 0, 0, None, None, None,
                     'Insurance_HealthInsurance', 1585386, None, None, 903096,
                     'PMI_Skip', None),
                    (1000000000006, 2, 40767407,
                     parser.parse('2020-02-01').date(), None, 45905771, None,
                     None, 903096, 0, 0, None, None, None,
                     'OverallHealth_MenstrualStopped', 1585784, None, None,
                     903096, 'PMI_Skip', None),
                    (1000000000008, 2, 1586166,
                     parser.parse('2020-02-01').date(), None, 45905771, None,
                     None, 903096, 0, 0, None, None, None,
                     'ElectronicSmoking_ElectricSmokeParticipant', 1586166,
                     None, None, 903096, 'PMI_Skip', None),
                    (1000000000010, 3, 40767407,
                     parser.parse('2020-03-02').date(), None, 45905771, None,
                     None, 903096, 0, 0, None, None, None,
                     'OverallHealth_MenstrualStopped', 1585784, None, None,
                     903096, 'PMI_Skip', None),
                    (1000000000012, 3, 1586166,
                     parser.parse('2020-03-02').date(), None, 45905771, None,
                     None, 903096, 0, 0, None, None, None,
                     'ElectronicSmoking_ElectricSmokeParticipant', 1586166,
                     None, None, 903096, 'PMI_Skip', None),
                    (1000000000013, 4, 40766240,
                     parser.parse('2020-01-01').date(), None, 45905771, None,
                     None, 903096, 0, 0, None, None, None,
                     'Insurance_HealthInsurance', 1585386, None, None, 903096,
                     'PMI_Skip', None),
                    (1000000000015, 4, 1586166,
                     parser.parse('2020-01-01').date(), None, 45905771, None,
                     None, 903096, 0, 0, None, None, None,
                     'ElectronicSmoking_ElectricSmokeParticipant', 1586166,
                     None, None, 903096, 'PMI_Skip', None),
                ]
            },
            {
                'fq_table_name':
                    f'{self.project_id}.{self.dataset_id}.person',
                'fq_sandbox_table_name':
                    None,
                'loaded_ids': [1, 2, 3, 4],
                'sandboxed_ids': [],
                'fields':
                    PERSON_FIELDS,
                'cleaned_values': [
                    (1, 8532, 2001, 999, 99999),
                    (2, 8532, 2001, 999, 99999),
                    (3, 8532, 2001, 999, 99999),
                    (4, 9999, 2001, 999, 99999),
                ]
            },
        ]

        self.default_test(tables_and_counts)