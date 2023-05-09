"""
Integration test for fill_source_value_text_fields module

De-id for registered tier removes all free text fields. We are re-populating those fields with the concept_code
value for the concept_id where possible to improve the clarity/readability of the resource.


Original Issue: DC-399
"""

# Python Imports
import os

# Third party imports
from dateutil import parser

# Project imports
from app_identity import PROJECT_ID
from cdr_cleaner.cleaning_rules.fill_source_value_text_fields import FillSourceValueTextFields
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import BaseTest
from common import AOU_DEATH, OBSERVATION, VOCABULARY_TABLES


class FillSourceValueTextFieldsTest(BaseTest.CleaningRulesTestBase):

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
        cls.vocabulary_dataset = os.getenv('VOCABULARY_DATASET')

        cls.rule_instance = FillSourceValueTextFields(project_id, dataset_id,
                                                      sandbox_id)

        table_names = cls.rule_instance.affected_tables + VOCABULARY_TABLES

        cls.fq_table_names = [
            f'{project_id}.{dataset_id}.{table}' for table in table_names
        ]

        cls.fq_sandbox_table_names = [
            f'{cls.project_id}.{cls.sandbox_id}.{cls.rule_instance.sandbox_table_for(table)}'
            for table in table_names
        ]

        # call super to set up the client, create datasets, and create
        # empty test tables
        # NOTE:  does not create empty sandbox tables.
        super().setUpClass()

    def setUp(self):
        fq_dataset_name = self.fq_table_names[0].split('.')
        self.fq_dataset_name = '.'.join(fq_dataset_name[:-1])

        self.date = parser.parse('2020-05-05').date()

        super().setUp()

    def test_source_value_text_fields(self):
        """
        Test to validate source_value fields are re-populated with concept_code using concept_ids.
        """
        self.copy_vocab_tables(self.vocabulary_dataset)

        insert_aou_death = self.jinja_env.from_string("""
        INSERT INTO `{{fq_dataset_name}}.aou_death`
            (aou_death_id, person_id, death_date, death_type_concept_id, cause_concept_id, cause_source_value, cause_source_concept_id, src_id, primary_death_record)
        VALUES
            ('a1', 1, date('2020-05-05'), 0, 0, NULL, NULL, 'rdr', False),
            ('a2', 1, date('2021-05-05'), 0, 0, NULL, 0, 'hpo_a', True),
            ('a3', 1, date('2021-05-05'), 0, 0, NULL, 1569168, 'hpo_b', True)
        """).render(fq_dataset_name=self.fq_dataset_name)

        queries = [insert_aou_death]
        self.load_test_data(queries)

        tables_and_counts = [{
            'fq_table_name':
                '.'.join([self.fq_dataset_name, AOU_DEATH]),
            'fq_sandbox_table_name':
                '',
            'loaded_ids': ['a1', 'a2', 'a3'],
            'sandboxed_ids': [],
            'fields': [
                'aou_death_id', 'cause_source_value', 'cause_source_concept_id'
            ],
            'cleaned_values': [
                ('a1', None, None),
                ('a2', 'No matching concept', 0),
                ('a3', 'I46', 1569168),
            ]
        }]

        self.default_test(tables_and_counts)

    def test_aggregate_zip_codes_cleaning(self):
        """
        Test for observaton's additional condition.
        Generalized zip codes in observation must be filled differently than the rest.
        See DC-1510.
        """

        create_concepts_query_tmpl = self.jinja_env.from_string("""
            INSERT INTO `{{fq_dataset_name}}.concept`
                (concept_id, concept_name, domain_id, vocabulary_id, concept_class_id, concept_code, valid_start_date, valid_end_date)
            VALUES
                (1585250, "ZIP", "some text", "some text", "some text", "StreetAddress_PIIZIP", date('2020-05-05'), date('2020-05-05')),
                (1585249, "State", "some text", "some text", "some text", "StreetAddress_PIIState", date('2020-05-05'), date('2020-05-05')),
                (1585248, "City", "some text", "some text", "some text", "StreetAddress_PIICity", date('2020-05-05'), date('2020-05-05')),
                (1585297, "New York", "some text", "some text", "some text", "PIIState_NY", date('2020-05-05'), date('2020-05-05')),
                (1585288, "Minnesota", "some text", "some text", "some text", "PIIState_MN", date('2020-05-05'), date('2020-05-05'))
        """).render(fq_dataset_name=self.fq_dataset_name)

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
                (6, 1, date('2020-05-05'), NULL, 1585297, 1585249, 1585297, "PIIState_NY", "StreetAddress_PIIState", 0, 0),
                (7, 1, date('2020-05-05'), NULL, 0, 1585249, 1585268, "PIIState_CT", "StreetAddress_PIIState", 0, 0),
                (8, 2, date('2020-05-05'), NULL, 1585288, 1585249, 1585288, "PIIState_MN", "StreetAddress_PIIState", 0, 0),
                (9, 3, date('2020-05-05'), NULL, 0, 1585249, 1585411, "PIIState_WY", "StreetAddress_PIIState", 0, 0),
                (10, 4, date('2020-05-05'), NULL, 0, 1585249, 1585268, "PIIState_CT", "StreetAddress_PIIState", 0, 0),
                (11, 4, date('2020-05-05'), NULL, 0, 1585248, 0, NULL, "StreetAddress_PIICity", 0, 0),
                (12, 3, date('2020-05-05'), NULL, 0, 1585249, 1585281, "PIIState_KY", "StreetAddress_PIIState", 0, 0)
        """).render(fq_dataset_name=self.fq_dataset_name)

        queries = [create_concepts_query_tmpl, observations_query_tmpl]
        self.load_test_data(queries)

        tables_and_counts = [{
            'fq_table_name':
                '.'.join([self.fq_dataset_name, OBSERVATION]),
            'fq_sandbox_table_name':
                '',
            'loaded_ids': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
            'sandboxed_ids': [],
            'fields': [
                'observation_id', 'person_id', 'observation_date',
                'value_as_string', 'value_as_concept_id',
                'observation_source_concept_id', 'value_source_concept_id',
                'value_source_value', 'observation_source_value',
                'observation_concept_id', 'observation_type_concept_id'
            ],
            'cleaned_values': [
                (1, 1, self.date, "063**", 0, 1585250, 0, None,
                 "StreetAddress_PIIZIP", 0, 0),
                (2, 2, self.date, "556**", 0, 1585250, 0, None,
                 "StreetAddress_PIIZIP", 0, 0),
                (3, 3, self.date, "823**", 0, 1585250, 0, None,
                 "StreetAddress_PIIZIP", 0, 0),
                (4, 4, self.date, "063**", 0, 1585250, 0, None,
                 "StreetAddress_PIIZIP", 0, 0),
                (5, 5, self.date, "063**", 0, 1585250, 0, None,
                 "StreetAddress_PIIZIP", 0, 0),
                (6, 1, self.date, "PIIState_NY", 1585297, 1585249, 1585297,
                 "PIIState_NY", "StreetAddress_PIIState", 0, 0),
                (7, 1, self.date, None, 0, 1585249, 1585268, None,
                 "StreetAddress_PIIState", 0, 0),
                (8, 2, self.date, "PIIState_MN", 1585288, 1585249, 1585288,
                 "PIIState_MN", "StreetAddress_PIIState", 0, 0),
                (9, 3, self.date, None, 0, 1585249, 1585411, None,
                 "StreetAddress_PIIState", 0, 0),
                (10, 4, self.date, None, 0, 1585249, 1585268, None,
                 "StreetAddress_PIIState", 0, 0),
                (11, 4, self.date, None, 0, 1585248, 0, None,
                 "StreetAddress_PIICity", 0, 0),
                (12, 3, self.date, None, 0, 1585249, 1585281, None,
                 "StreetAddress_PIIState", 0, 0)
            ]
        }]

        self.default_test(tables_and_counts)
