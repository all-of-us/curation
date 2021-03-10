"""
Integration test for motor_vehicle_accident_suppression module

Original Issues: DC-1367

The intent is to suppress the vehicle accident related concepts across all *concept_id fields in 
all domain tables """

# Python Imports
import os
from dateutil.parser import parse

from google.cloud.bigquery import Table

# Project Imports
from common import VOCABULARY_TABLES
from cdr_cleaner.cleaning_rules.domain_mapping import DOMAIN_TABLE_NAMES
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule
from tools.combine_ehr_rdr import mapping_table_for
from utils import bq
from app_identity import PROJECT_ID
from cdr_cleaner.cleaning_rules.domain_alignment import domain_alignment, \
    DOMAIN_ALIGNMENT_TABLE_NAME
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import \
    BaseTest


class DomainAlignment(BaseCleaningRule):
    """
    Imitate a base cleaning rule implementation so that we can use CleaningRulesTestBase
    """

    def __init__(self, project_id, dataset_id, sandbox_dataset_id):
        super().__init__(
            project_id=project_id,
            dataset_id=dataset_id,
            sandbox_dataset_id=sandbox_dataset_id,
            description=
            'fake CR class for the domain alignment integration test',
            issue_numbers=[],
            affected_datasets=[])

    def get_query_specs(self):
        return domain_alignment(self._project_id, self._dataset_id,
                                self._sandbox_dataset_id)

    def setup_function(self, client):
        pass

    def get_sandbox_tablenames(self):
        pass

    def setup_rule(self, client, *args, **keyword_args):
        pass

    def setup_validation(self, client, *args, **keyword_args):
        pass

    def validate_rule(self, client, *args, **keyword_args):
        pass


class DomainAlignmentTest(BaseTest.CleaningRulesTestBase):

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
        cls.sandbox_id = cls.dataset_id + '_sandbox'
        cls.vocabulary_id = os.environ.get('VOCABULARY_DATASET')

        cls.rule_instance = DomainAlignment(cls.project_id, cls.dataset_id,
                                            cls.sandbox_id)

        # Generates list of fully qualified table names
        for table_name in DOMAIN_TABLE_NAMES + VOCABULARY_TABLES + [
                DOMAIN_ALIGNMENT_TABLE_NAME
        ]:
            cls.fq_table_names.append(
                f'{cls.project_id}.{cls.dataset_id}.{table_name}')

        # Generates list of fully qualified mapping table names
        for table_name in DOMAIN_TABLE_NAMES:
            cls.fq_table_names.append(
                f'{cls.project_id}.{cls.dataset_id}.{mapping_table_for(table_name)}'
            )

        # call super to set up the client, create datasets
        cls.up_class = super().setUpClass()

        # Copy vocab tables over to the test dataset
        cls.copy_vocab_tables(cls.vocabulary_id)

    @classmethod
    def copy_vocab_tables(cls, vocabulary_id):
        """
        A function for copying the vocab tables to the test dataset_id
        :param vocabulary_id: 
        :return: 
        """
        # Copy vocab tables over to the test dataset
        vocabulary_dataset = bq.get_dataset(cls.project_id, vocabulary_id)
        for src_table in bq.list_tables(cls.client, vocabulary_dataset):
            schema = bq.get_table_schema(src_table.table_id)
            destination = f'{cls.project_id}.{cls.dataset_id}.{src_table.table_id}'
            dst_table = cls.client.create_table(Table(destination,
                                                      schema=schema),
                                                exists_ok=True)
            cls.client.copy_table(src_table, dst_table)

    def setUp(self):
        """
        Create empty tables for the rule to run on
        """
        # Create domain tables required for the test
        super().setUp()

        # Load the test data
        condition_occurrence_data_template = self.jinja_env.from_string("""
                    INSERT INTO `{{project_id}}.{{dataset_id}}.condition_occurrence`
                    (condition_occurrence_id, person_id, condition_concept_id, 
                    condition_start_date, condition_start_datetime, condition_type_concept_id,
                    visit_occurrence_id)
                    VALUES
                        (100, 1, 201826, '2015-07-15', TIMESTAMP '2015-07-15T00:00:00', 42894222, 1),
                        (101, 2, 36676219, '2015-07-15', TIMESTAMP '2015-07-15T00:00:00', 42865906, 2),
                        (102, 3, 201826, '2015-07-15', TIMESTAMP '2015-07-15T00:00:00', 42894222, 3),
                        (103, 4, 201826, '2015-07-15', TIMESTAMP '2015-07-15T00:00:00', 42894222, 4)
                    """)

        mapping_condition_occurrence_data_template = self.jinja_env.from_string(
            """
                    INSERT INTO `{{project_id}}.{{dataset_id}}._mapping_condition_occurrence`
                    (condition_occurrence_id, src_dataset_id, src_condition_occurrence_id, 
                     src_hpo_id, src_table_id)
                    VALUES
                        (100, '{{dataset_id}}', 1, 'hpo_1', 'condition_occurrence'),
                        (101, '{{dataset_id}}', 2, 'hpo_2', 'condition_occurrence'),
                        (102, '{{dataset_id}}', 3, 'hpo_3', 'condition_occurrence'),
                        (103, '{{dataset_id}}', 4, 'hpo_4', 'condition_occurrence')
                    """)

        procedure_occurrence_tmpl = self.jinja_env.from_string("""
                    INSERT INTO `{{project_id}}.{{dataset_id}}.procedure_occurrence`
                    (procedure_occurrence_id, person_id, procedure_concept_id, procedure_date, 
                     procedure_datetime, procedure_type_concept_id, visit_occurrence_id)
                    VALUES
                        (200, 5, 36676219, '2015-07-15', TIMESTAMP '2015-07-15T00:00:00', 42865906, 5),
                        (201, 6, 320128, '2015-08-15', TIMESTAMP '2015-08-15T00:00:00', 42894222, 6)
                        """)

        mapping_procedure_occurrence_data_template = self.jinja_env.from_string(
            """
                    INSERT INTO `{{project_id}}.{{dataset_id}}._mapping_procedure_occurrence`
                    (procedure_occurrence_id, src_dataset_id, src_procedure_occurrence_id, 
                     src_hpo_id, src_table_id)
                    VALUES
                        (200, '{{dataset_id}}', 10, 'hpo_1', 'procedure_occurrence'),
                        (201, '{{dataset_id}}', 20, 'hpo_2', 'procedure_occurrence')
                    """)

        insert_condition_query = condition_occurrence_data_template.render(
            project_id=self.project_id, dataset_id=self.dataset_id)

        insert_condition_mapping_query = mapping_condition_occurrence_data_template.render(
            project_id=self.project_id, dataset_id=self.dataset_id)

        insert_procedure_query = procedure_occurrence_tmpl.render(
            project_id=self.project_id, dataset_id=self.dataset_id)

        insert_procedure_mapping_query = mapping_procedure_occurrence_data_template.render(
            project_id=self.project_id, dataset_id=self.dataset_id)

        # Load test data
        self.load_test_data([
            f'''{insert_condition_query};
                {insert_condition_mapping_query};
                {insert_procedure_query};
                {insert_procedure_mapping_query}; '''
        ])

    def test_domain_alignment(self):
        from datetime import date
        # Expected results list
        tables_and_counts = [{
            'fq_table_name':
                f'{self.project_id}.{self.dataset_id}.condition_occurrence',
            'loaded_ids': [100, 102, 103, 104],
            'fields': [
                'condition_occurrence_id', 'person_id', 'condition_concept_id',
                'condition_start_date', 'condition_start_datetime',
                'condition_type_concept_id', 'visit_occurrence_id'
            ],
            'cleaned_values': [(100, 1, 201826, date(2015, 7, 15),
                                parse('2015-07-15 00:00:00 UTC'), 42894222, 1),
                               (102, 3, 201826, date(2015, 7, 15),
                                parse('2015-07-15 00:00:00 UTC'), 42894222, 3),
                               (103, 4, 201826, date(2015, 7, 15),
                                parse('2015-07-15 00:00:00 UTC'), 42894222, 4),
                               (104, 6, 320128, date(2015, 8, 15),
                                parse('2015-08-15 00:00:00 UTC'), None, 6)]
        }, {
            'fq_table_name':
                f'{self.project_id}.{self.dataset_id}.procedure_occurrence',
            'loaded_ids': [200, 202],
            'fields': [
                'procedure_occurrence_id', 'person_id', 'procedure_concept_id',
                'procedure_date', 'procedure_datetime',
                'procedure_type_concept_id', 'visit_occurrence_id'
            ],
            'cleaned_values': [(200, 5, 36676219, date(2015, 7, 15),
                                parse('2015-07-15 00:00:00 UTC'), 42865906, 5),
                               (202, 2, 36676219, date(2015, 7, 15),
                                parse('2015-07-15 00:00:00 UTC'), None, 2)]
        }]
        # Apply the cleaning rule to the dataset
        self.default_test(tables_and_counts)
