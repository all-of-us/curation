"""
Integration test for remove_invalid_procedure_source_records module

Original Issues: DC-1210
"""

#Python Imports
import os

# Project Imports
from app_identity import PROJECT_ID
from common import JINJA_ENV, PROCEDURE_OCCURRENCE
from cdr_cleaner.cleaning_rules.remove_invalid_procedure_source_records import RemoveInvalidProcedureSourceRecords
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import BaseTest

PROCEDURE_SOURCE_CONCEPT_IDS_TEMPLATE = JINJA_ENV.from_string("""
INSERT INTO 
    `{{project_id}}.{{dataset_id}}.{{procedure_occurrence_table}}`
(procedure_occurrence_id, person_id, procedure_concept_id,procedure_date,procedure_datetime,
procedure_type_concept_id, modifier_concept_id, quantity,provider_id,visit_occurrence_id,
procedure_source_value, procedure_source_concept_id, qualifier_source_value)

VALUES
    (51380225, 4, 4000000, '2009-04-27', '2009-04-27T00:00:00', 38000270, 0, 1, 0, NULL, '92014', 4000000, NULL),
    (76392641, 5, 5000000, '2011-05-19', '2011-05-19T00:00:00', 38000270, 0, 1, 0, NULL, '99243', 5000000, NULL),
    (22888767, 6, 5000000, '2007-12-23', '2007-12-28T00:00:00', 38000270, 0, 1, 0, NULL, '99243', 5000000, NULL),

    (20074825, 1, 1000000, 2012-09-23, '2012-09-23T00:00:00', 44786631, 0, 1, 0, NULL, '99458', 2514636, NULL),
    (12557074, 2, 2000000, 2010-06-21, '2010-06-21T00:00:00', 47876139, 0, 1, 0, NULL, '45734', 4578984, NULL),
    (56145455, 3, 3000000, 2011-02-14, '2011-02-14T00:00:00', 45454515, 0, 1, 0, NULL, '48445', 8545787, NULL),

""")

CONCEPT_IDS_TEMPLATE = JINJA_ENV.from_string("""
INSERT INTO
    `{{project_id}}.{{dataset_id}}.concept`
(concept_id, concept_name, domain_id, vocabulary_id, concept_class_id, standard_concept, concept_code, valid_start_date, valid_end_date, invalid_reason)

VALUES
    (1000000, NULL, 'Procedure', NULL, 'CPT4', 'S', NULL, NULL, NULL, NULL )
    (2000000, NULL, 'Procedure', NULL, 'CPT4 Hierarchy', 'S', NULL, NULL, NULL, NULL)
    (3000000, NULL, 'Procedure', NULL, 'Procedure', 'S', NULL, NULL, NULL, NULL)
    
    (4000000, NULL, 'Drug', NULL, 'CPT4 Modifier', NULL, NULL, NULL, NULL, NULL)
    (5000000, NULL, 'Observation', NULL, 'CPT4 Modifier', NULL, NULL, NULL, NULL, NULL)
    (5000000, NULL, 'Procedure', NULL, 'CPT4 Modifier', NULL, NULL, NULL, NULL, NULL)

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

        # Store procedure_occurrence table name
        procedure_occurance_table_name = f'{cls.project_id}.{cls.dataset_id}.{PROCEDURE_OCCURRENCE}'
        cls.fq_table_names = [procedure_occurance_table_name]

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
