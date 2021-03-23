"""
Integration test for aggregate_zip_codes module

To further obfuscate participant identity, some generalized zip codes will be aggregated together.
The PII zip code and state will be transformed to a neighboring zip code/state pair for those zip codes with low population density.
It is expected that this lookup table will be static and will remain unchanged. 
It is based on US population, and not on participant address metrics.


Original Issue: DC-1379, DC-1504
"""

# Python Imports
import os

# Third party imports
from dateutil import parser

# Project imports
from app_identity import PROJECT_ID
from cdr_cleaner.cleaning_rules.aggregate_zip_codes import AggregateZipCodes
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import BaseTest
from common import OBSERVATION


class AggregateZipCodesTest(BaseTest.CleaningRulesTestBase):

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

        cls.rule_instance = AggregateZipCodes(project_id, dataset_id,
                                              sandbox_id)

        cls.fq_table_names = [
            f'{project_id}.{dataset_id}.{OBSERVATION}',
        ]

        sb_table_name = cls.rule_instance.sandbox_table_for(OBSERVATION)

        cls.fq_sandbox_table_names.append(
            f'{cls.project_id}.{cls.sandbox_id}.{sb_table_name}')

        # call super to set up the client, create datasets, and create
        # empty test tables
        # NOTE:  does not create empty sandbox tables.
        super().setUpClass()

    def setUp(self):
        fq_dataset_name = self.fq_table_names[0].split('.')
        self.fq_dataset_name = '.'.join(fq_dataset_name[:-1])

        self.date = parser.parse('2020-05-05').date()

        super().setUp()

    def test_aggregate_zip_codes_cleaning(self):
        """
        Tests that the specifications for queries perform as designed.

        Validates pre conditions, tests execution, and post conditions based on the load
        statements and the tables_and_counts variable.        
        """

        # Append some queries
        observations_query_tmpl = self.jinja_env.from_string("""
            INSERT INTO `{{fq_dataset_name}}.observation`
                (observation_id, person_id, observation_date,
                value_as_string, value_as_concept_id, observation_source_concept_id, 
                value_source_concept_id, value_source_value, observation_source_value,
                observation_concept_id, observation_type_concept_id)
            VALUES
                (1, 1, date('2020-05-05'), "063**", 0, 1585250, 0, NULL, "StreetAddress_PIIZIP", 0, 0),
                (2, 2, date('2020-05-05'), "556**", 0, 1585250, 0, NULL, "StreetAddress_PIIZIP", 0, 0),
                (3, 3, date('2020-05-05'), "823**", 0, 1585250, 0, NULL, "StreetAddress_PIIZIP", 0, 0),
                (4, 4, date('2020-05-05'), "063**", 0, 1585250, 0, NULL, "StreetAddress_PIIZIP", 0, 0),
                (5, 5, date('2020-05-05'), "063**", 0, 1585250, 0, NULL, "StreetAddress_PIIZIP", 0, 0),
                (6, 1, date('2020-05-05'), "New York", 0, 1585249, 1585297, "PIIState_NY", "StreetAddress_PIIState", 0, 0),
                (7, 1, date('2020-05-05'), "Connecticut", 0, 1585249, 1585268, "PIIState_CT", "StreetAddress_PIIState", 0, 0),
                (8, 2, date('2020-05-05'), "Minnesota", 0, 1585249, 1585288, "PIIState_MN", "StreetAddress_PIIState", 0, 0),
                (9, 3, date('2020-05-05'), "Wyoming", 0, 1585249, 1585411, "PIIState_WY", "StreetAddress_PIIState", 0, 0),
                (10, 4, date('2020-05-05'), "Connecticut", 0, 1585249, 1585268, "PIIState_CT", "StreetAddress_PIIState", 0, 0),
                (11, 4, date('2020-05-05'), "Boston", 0, 1585248, 0, NULL, "StreetAddress_PIICity", 0, 0),
                (12, 3, date('2020-05-05'), "Kentucky", 0, 1585249, 1585281, "PIIState_KY", "StreetAddress_PIIState", 0, 0)
        """).render(fq_dataset_name=self.fq_dataset_name)

        queries = [observations_query_tmpl]
        self.load_test_data(queries)

        tables_and_counts = [{
            'fq_table_name':
                '.'.join([self.fq_dataset_name, 'observation']),
            'fq_sandbox_table_name':
                self.fq_sandbox_table_names[0],
            'loaded_ids': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
            'sandboxed_ids': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            'fields': [
                'observation_id', 'person_id', 'observation_date',
                'value_as_string', 'value_as_concept_id',
                'observation_source_concept_id', 'value_source_concept_id',
                'value_source_value', 'observation_source_value',
                'observation_concept_id', 'observation_type_concept_id'
            ],
            'cleaned_values': [
                (1, 1, self.date, "062**", 0, 1585250, 0, None,
                 "StreetAddress_PIIZIP", 0, 0),
                (2, 2, self.date, "557**", 0, 1585250, 0, None,
                 "StreetAddress_PIIZIP", 0, 0),
                (3, 3, self.date, "822**", 0, 1585250, 0, None,
                 "StreetAddress_PIIZIP", 0, 0),
                (4, 4, self.date, "062**", 0, 1585250, 0, None,
                 "StreetAddress_PIIZIP", 0, 0),
                (5, 5, self.date, "062**", 0, 1585250, 0, None,
                 "StreetAddress_PIIZIP", 0, 0),
                (6, 1, self.date, "New York", 1585268, 1585249, 1585268,
                 "PIIState_NY", "StreetAddress_PIIState", 0, 0),
                (7, 1, self.date, "Connecticut", 1585268, 1585249, 1585268,
                 "PIIState_CT", "StreetAddress_PIIState", 0, 0),
                (8, 2, self.date, "Minnesota", 1585288, 1585249, 1585288,
                 "PIIState_MN", "StreetAddress_PIIState", 0, 0),
                (9, 3, self.date, "Wyoming", 1585411, 1585249, 1585411,
                 "PIIState_WY", "StreetAddress_PIIState", 0, 0),
                (10, 4, self.date, "Connecticut", 1585268, 1585249, 1585268,
                 "PIIState_CT", "StreetAddress_PIIState", 0, 0),
                (11, 4, self.date, "Boston", 0, 1585248, 0, None,
                 "StreetAddress_PIICity", 0, 0),
                (12, 3, self.date, "Kentucky", 0, 1585249, 1585281,
                 "PIIState_KY", "StreetAddress_PIIState", 0, 0)
            ]
        }]

        self.default_test(tables_and_counts)
