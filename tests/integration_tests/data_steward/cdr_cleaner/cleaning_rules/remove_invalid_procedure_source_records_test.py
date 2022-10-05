"""
Integration test for remove_invalid_procedure_source_records module

Original Issues: DC-1210
"""

#Python Imports
import os
from datetime import date

# Third party imports
from dateutil import parser

# Project Imports
from app_identity import PROJECT_ID
from common import JINJA_ENV, PROCEDURE_OCCURRENCE, CONCEPT
from cdr_cleaner.cleaning_rules.remove_invalid_procedure_source_records import RemoveInvalidProcedureSourceRecords
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import BaseTest

PROCEDURE_SOURCE_CONCEPT_IDS_TEMPLATE = JINJA_ENV.from_string("""
INSERT INTO 
    `{{project_id}}.{{dataset_id}}.{{procedure_occurrence_table}}`
(procedure_occurrence_id, person_id, procedure_concept_id, procedure_date, procedure_datetime,
 procedure_type_concept_id, modifier_concept_id, quantity, provider_id, visit_occurrence_id,
 visit_detail_id, procedure_source_value, procedure_source_concept_id, modifier_source_value)

VALUES
    (51380225, 4, 4000000, '2009-04-27', TIMESTAMP('2009-04-27'), 38000270, 0, 1, 0, NULL,NULL,'92014', 4000000, NULL),
    (76392641, 5, 5000000, '2011-05-19', TIMESTAMP('2011-05-19'), 38000270, 0, 1, 0, NULL,NULL,'99243', 5000000, NULL),
    (22888767, 6, 5000000, '2007-12-23', TIMESTAMP('2007-12-28'), 38000270, 0, 1, 0, NULL,NULL,'99243', 5000000, NULL),

    (20074825, 1, 1000000, '2012-09-23', TIMESTAMP('2012-09-23'), 44786631, 0, 1, 0, NULL,NULL,'99458', 2514636, NULL),
    (12557074, 2, 2000000, '2010-06-21', TIMESTAMP('2010-06-21'), 47876139, 0, 1, 0, NULL,NULL,'45734', 4578984, NULL),
    (56145455, 3, 3000000, '2011-02-14', TIMESTAMP('2011-02-14'), 45454515, 0, 1, 0, NULL,NULL,'48445', 8545787, NULL)

""")

CONCEPT_IDS_TEMPLATE = JINJA_ENV.from_string("""
INSERT INTO
    `{{project_id}}.{{dataset_id}}.concept`
(concept_id, concept_name, domain_id, vocabulary_id, concept_class_id, standard_concept, concept_code, valid_start_date, valid_end_date, invalid_reason)

VALUES
    (1000000, 'Respiratory-1', 'Procedure', 'ABC',  'CPT4',           'S', '1010882', '2011-02-14', '2011-02-15', NULL ),
    (2000000, 'Respiratory-2', 'Procedure', 'ABC',  'CPT4 Hierarchy', 'S', '1010882', '2011-02-14', '2011-02-15', NULL),
    (3000000, 'Respiratory-3', 'Procedure', 'ABC',  'Procedure',      'S', '1010882', '2011-02-14', '2011-02-15', NULL),
    
    (4000000, 'Respiratory-4', 'Drug',        'ABC', 'CPT4 Modifier', NULL, '1010882', '2011-02-14', '2011-02-15', NULL),
    (5000000, 'Respiratory-5', 'Observation', 'ABC', 'CPT4 Modifier', NULL, '1010882', '2011-02-14', '2011-02-15', NULL),
    (5000000, 'Respiratory-6', 'Procedure',   'ABC', 'CPT4 Modifier', NULL, '1010882', '2011-02-14', '2011-02-15', NULL)

""")


class RemoveInvalidProcedureSourceRecordsTest(BaseTest.CleaningRulesTestBase):

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
        cls.rule_instance = RemoveInvalidProcedureSourceRecords(
            cls.project_id, cls.dataset_id, cls.sandbox_id)

        # Generate sandbox table names
        sandbox_table_names = cls.rule_instance.get_sandbox_tablenames()
        for table_name in sandbox_table_names:
            cls.fq_sandbox_table_names.append(
                f'{cls.project_id}.{cls.sandbox_id}.{table_name}')

        # Store procedure_occurrence and concept table names
        procedure_occurance_table_name = f'{cls.project_id}.{cls.dataset_id}.{PROCEDURE_OCCURRENCE}'
        concept_table_name = f'{cls.project_id}.{cls.dataset_id}.{CONCEPT}'
        cls.fq_table_names = [
            procedure_occurance_table_name, concept_table_name
        ]

        # call super to set up the client, create datasets, and create
        # empty test tables
        # NOTE:  does not create empty sandbox tables.
        super().setUpClass()

    def setUp(self):
        """
        Create empty tables for the rule to run on
        """
        super().setUp()

        # Query to insert test records into procedure_occurrence table
        procedure_source_concept_ids_query = PROCEDURE_SOURCE_CONCEPT_IDS_TEMPLATE.render(
            project_id=self.project_id,
            dataset_id=self.dataset_id,
            procedure_occurrence_table=PROCEDURE_OCCURRENCE)

        # Query to insert test records into concept table
        concept_ids_template = CONCEPT_IDS_TEMPLATE.render(
            project_id=self.project_id, dataset_id=self.dataset_id)

        self.load_test_data(
            [procedure_source_concept_ids_query, concept_ids_template])

    def test_field_cleaning(self):
        """
        person_ids 4, 5, and 6 should be sandboxed
        """

        # Expected results list
        tables_and_counts = [{
            'fq_table_name':
                self.fq_table_names[0],
            'fq_sandbox_table_name':
                self.fq_sandbox_table_names[0],
            'loaded_ids': [
                20074825, 12557074, 56145455, 51380225, 76392641, 22888767
            ],
            'sandboxed_ids': [51380225, 76392641, 22888767],
            'fields': [
                'procedure_occurrence_id', 'person_id', 'procedure_concept_id',
                'procedure_date', 'procedure_datetime',
                'procedure_type_concept_id', 'modifier_concept_id', 'quantity',
                'provider_id', 'visit_occurrence_id', 'visit_detail_id',
                'procedure_source_value', 'procedure_source_concept_id',
                'modifier_source_value'
            ],
            'cleaned_values': [
                (20074825, 1, 1000000, date.fromisoformat('2012-09-23'),
                 parser.parse('2012-09-23 00:00:00 UTC'), 44786631, 0, 1, 0,
                 None, None, '99458', 2514636, None),
                (12557074, 2, 2000000, date.fromisoformat('2010-06-21'),
                 parser.parse('2010-06-21 00:00:00 UTC'), 47876139, 0, 1, 0,
                 None, None, '45734', 4578984, None),
                (56145455, 3, 3000000, date.fromisoformat('2011-02-14'),
                 parser.parse('2011-02-14 00:00:00 UTC'), 45454515, 0, 1, 0,
                 None, None, '48445', 8545787, None)
            ]
        }]

        self.default_test(tables_and_counts)
