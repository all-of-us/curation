"""
Integration test for the organ_transplant_concept_suppression.py module

Original Issue: DC-1529

The intent of this cleaning rule is to suppress any rows in the observation table where the response is related to
 organ transplant

OrganTransplantDescription_OtherOrgan - 1585807 -> https://athena.ohdsi.org/search-terms/terms/1585807
OrganTransplantDescription_OtherTissue - 1585808 -> https://athena.ohdsi.org/search-terms/terms/1585808
"""

# Python Imports
import os

# Project Imports
from common import OBSERVATION
from app_identity import PROJECT_ID
from cdr_cleaner.cleaning_rules.deid.organ_transplant_concept_suppression import OrganTransplantConceptSuppression
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import BaseTest


class OrganTransplantConceptSuppressionTest(BaseTest.CleaningRulesTestBase):

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

        cls.rule_instance = OrganTransplantConceptSuppression(
            cls.project_id, cls.dataset_id, cls.sandbox_id)

        # Generates list of fully qualified table names
        for table_name in [OBSERVATION]:
            cls.fq_table_names.append(
                f'{cls.project_id}.{cls.dataset_id}.{table_name}')
            sandbox_table_name = cls.rule_instance.sandbox_table_for(table_name)
            cls.fq_sandbox_table_names.append(
                f'{cls.project_id}.{cls.sandbox_id}.{sandbox_table_name}')

        # call super to set up the client, create datasets
        cls.up_class = super().setUpClass()

    def setUp(self):
        """
        Create empty tables on which the rule will run
        """

        # Create domain tables required for the test
        super().setUp()

        # Load the test data
        observation_tmpl = self.jinja_env.from_string("""
        INSERT INTO `{{project_id}}.{{dataset_id}}.observation`
        (observation_id, person_id, observation_concept_id,
         observation_date, observation_type_concept_id, value_as_concept_id,
         qualifier_concept_id, unit_concept_id, observation_source_concept_id,
         value_source_concept_id)
         VALUES
      -- Concepts to suppress --
      -- 903079: PMI Prefer Not To Answer --
         (1, 1, 0, '2017-05-02', 0, 0, 0, 0, 0, 1585808),
         (2, 1, 0, '2017-05-02', 0, 0, 0, 0, 0, 1585807),
         (3, 1, 0, '2017-05-02', 0, 0, 0, 0, 0, 1585806),
         (4, 1, 0, '2017-05-02', 0, 0, 0, 0, 0, 1585834),
         (5, 1, 0, '2017-05-02', 0, 0, 0, 0, 0, 1585825),
         (6, 1, 0, '2017-05-02', 0, 0, 0, 0, 903079, 0)
            """)

        insert_observation_query = observation_tmpl.render(
            project_id=self.project_id, dataset_id=self.dataset_id)

        # Load the test data
        self.load_test_data([f'''{insert_observation_query};'''])

    def test_organ_transplant_concept_suppression(self):
        # Expected results list
        tables_and_counts = [{
            'fq_table_name':
                f'{self.project_id}.{self.dataset_id}.observation',
            'fq_sandbox_table_name':
                f'{self.project_id}.{self.sandbox_id}.'
                f'{self.rule_instance.sandbox_table_for("observation")}',
            'loaded_ids': [1, 2, 3, 4, 5, 6],
            'sandboxed_ids': [1, 2],
            'fields': [
                'observation_id', 'person_id', 'observation_concept_id',
                'observation_type_concept_id', 'value_as_concept_id',
                'qualifier_concept_id', 'unit_concept_id',
                'observation_source_concept_id', 'value_source_concept_id'
            ],
            'cleaned_values': [(3, 1, 0, 0, 0, 0, 0, 0, 1585806),
                               (4, 1, 0, 0, 0, 0, 0, 0, 1585834),
                               (5, 1, 0, 0, 0, 0, 0, 0, 1585825),
                               (6, 1, 0, 0, 0, 0, 0, 903079, 0)]
        }]

        self.default_test(tables_and_counts)
