"""
Integration test for unit_normalization cleaning rule.

Original Issue: DC-414
"""
# Python imports
import os

# Third party imports
from google.cloud import bigquery

# Project Imports
from app_identity import PROJECT_ID
from cdr_cleaner.cleaning_rules.update_unmapped_question_answer_concepts_test import (
    SetConceptIdsForSurveyQuestionsAnswers, OLD_MAP_SHORT_CODES_TABLE)
from common import JINJA_ENV, OBSERVATION
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import (
    BaseTest.CleaningRulesTestBase)

test_query = JINJA_ENV.from_string("""select * from `{{intermediary_table}}`""")

INSERT_UNITS_RAW_DATA = JINJA_ENV.from_string("""
  INSERT INTO `{{project_id}}.{{dataset_id}}.observation` (
      observation_id,
      person_id,
      observation_concept_id,
      observation_source_concept_id,
      observation_source_value,
      value_as_concept_id,
      value_source_concept_id,
      value_source_value,
      observation_date,
      observation_type_concept_id
      )
    VALUES
      -- checking good and bad question concepts --
      (1,1,0,0,'DiagnosedHealthCondition_GrandparentMentalCondition',43528359,43528359,'GrandparentMentalCondition_Addiction', '2020-01-01' ,0),
      (2,1,43529812,43529812,'DiagnosedHealthCondition_GrandparentSkelMusc',43528709,43528709,'GrandparentSkeletalMuscularCondition_Fibromyalgia','2020-01-01',0),
      -- checking good and bad answer concepts --
      (3,1,43529634,43529634,'DiagnosedHealthCondition_GrandparentOtherHealth',0,0,'GrandparentOtherHealthCondition_ReactionsToAnesthesia','2020-01-01',0),
      (4,1,43529634,43529634,'DiagnosedHealthCondition_GrandparentOtherHealth',43529827,43529827,'GrandparentOtherHealthCondition_SkinCondition','2020-01-01',,0)
""")


class SetConceptIdsForSurveyQuestionsAnswersTest(CleaningRulesTestBase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

        super().initialize_class_vars()

        # Set the test project identifier
        cls.project_id = os.environ.get(PROJECT_ID)

        # Set the expected test datasets
        cls.dataset_id = os.environ.get('RDR_DATASET_ID')
        cls.sandbox_id = cls.dataset_id + '_sandbox'
        cls.rule_instance = SetConceptIdsForSurveyQuestionsAnswers(cls.project_id, cls.dataset_id,
                                              cls.sandbox_id)
        # Generates list of fully qualified table names and their corresponding sandbox table names
        cls.fq_table_names.append(
            f'{cls.project_id}.{cls.dataset_id}.{OBSERVATION}')

        sandbox_table_names = cls.rule_instance.get_sandbox_tablenames()
        for table in sandbox_table_names:
            cls.fq_sandbox_table_names.append(
                f'{cls.project_id}.{cls.sandbox_id}.{table}')

        # Appending OLD_MAP_SHORT_CODES_TABLE table to fq_sandbox_table_names to delete after the test
        cls.fq_sandbox_table_names.append(
            f'{cls.project_id}.{cls.sandbox_id}.{OLD_MAP_SHORT_CODES_TABLE}')

        # call super to set up the client, create datasets
        cls.up_class = super().setUpClass()

    def setUp(self):
        # Set the test project identifier
        self.client.delete_table(
            f'{self.project_id}.{self.sandbox_id}.{OLD_MAP_SHORT_CODES_TABLE}',
            not_found_ok=True)
        super().setUp()
        raw_units_load_query = INSERT_UNITS_RAW_DATA.render(
            project_id=self.project_id, dataset_id=self.dataset_id)

        # Load test data
        self.load_test_data([f'{raw_units_load_query}'])

    def test_setup_rule(self):

        # test if intermediary table exists before running the cleaning rule
        intermediary_table = f'{self.project_id}.{self.sandbox_id}.{UNIT_MAPPING_TABLE}'

        # run setup_rule and see if the table is created
        self.rule_instance.setup_rule(self.client)

        actual_table = self.client.get_table(intermediary_table)
        self.assertIsNotNone(actual_table.created)

        # test if exception is raised if table already exists
        with self.assertRaises(RuntimeError) as c:
            self.rule_instance.setup_rule(self.client)

        self.assertEqual(str(c.exception),
                         f"Unable to create tables: ['{intermediary_table}']")

        query = test_query.render(intermediary_table=intermediary_table)
        query_job_config = bigquery.job.QueryJobConfig(use_query_cache=False)
        result = self.client.query(query,
                                   job_config=query_job_config).to_dataframe()
        self.assertEqual(result.empty, False)

    def test_unit_normalization(self):
        """
        Tests unit_normalization for the loaded test data
        """
        # Expected results list
        tables_and_counts = [{
            'fq_table_name':
                f'{self.project_id}.{self.dataset_id}.{MEASUREMENT}',
            'loaded_ids': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            'sandboxed_ids': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            'fields': [
                'measurement_id', 'person_id', 'measurement_concept_id',
                'value_as_number', 'unit_concept_id', 'range_low', 'range_high'
            ],
            'cleaned_values': [(1, 1, 3020509, 0.4, 8523, 1.0, 2.4),
                               (2, 1, 3020891, 36.5, 8653, -17.77777777777778,
                                65.55555555555556),
                               (3, 1, 3016293, 25.0, 8753, 21.0, 31.0),
                               (4, 1, 3027970, 23.0, 8636, 15.0, 45.0),
                               (5, 1, 3027970, 26.0, 8636, 19.0, 37.0),
                               (6, 1, 3000963, 158.0, 8636, 132.0, 171.0),
                               (7, 1, 3000905, 0.0101, 8848,
                                0.0045000000000000005, 0.011),
                               (8, 1, 3020630, 0.006200000000000001, 8713,
                                0.0064, 0.008199999999999999),
                               (9, 1, 3020416, 5060.0, 8815, 4200.0, 5800.0),
                               (10, 1, 3000963, 0.34, 8636, 0.05, 0.5)]
        }]

        self.default_test(tables_and_counts)
