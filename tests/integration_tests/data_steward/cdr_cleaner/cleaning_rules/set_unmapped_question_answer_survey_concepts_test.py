"""
Integration test for setting PPI concept_ids for unmapped concept_codes.

Original Issue: DC-499
"""
# Python imports
import os

# Third party imports
from google.cloud import bigquery

# Project Imports
from app_identity import PROJECT_ID
from cdr_cleaner.cleaning_rules.set_unmapped_question_answer_survey_concepts import (
    SetConceptIdsForSurveyQuestionsAnswers, OLD_MAP_SHORT_CODES_TABLE)
from common import JINJA_ENV, OBSERVATION, VOCABULARY_TABLES
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import (
    BaseTest)

test_query = JINJA_ENV.from_string("""select * from `{{intermediary_table}}`""")

INSERT_RAW_DATA = JINJA_ENV.from_string("""
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
      (1,1,0,0,'DiagnosedHealthCondition_GrandparentMentalCondition',43528359,43528359,'GrandparentMentalCondition_Addiction','2020-01-01',0),
      (2,1,43529812,43529812,'DiagnosedHealthCondition_GrandparentSkelMusc',43528709,43528709,'GrandparentSkeletalMuscularCondition_Fibromyalgia','2020-01-01',0),
      -- checking good and bad answer concepts --
      (3,1,43529634,43529634,'DiagnosedHealthCondition_GrandparentOtherHealth',0,0,'GrandparentOtherHealthCondition_ReactionsToAnesthesia','2020-01-01',0),
      (4,1,43529634,43529634,'DiagnosedHealthCondition_GrandparentOtherHealth',43529827,43529827,'GrandparentOtherHealthCondition_SkinCondition','2020-01-01',0)
""")


class SetConceptIdsForSurveyQuestionsAnswersTest(BaseTest.CleaningRulesTestBase
                                                ):

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
        cls.vocabulary_id = os.environ.get('VOCABULARY_DATASET')

        cls.rule_instance = SetConceptIdsForSurveyQuestionsAnswers(
            cls.project_id, cls.dataset_id, cls.sandbox_id)
        # Generates list of fully qualified table names and their corresponding sandbox table names
        cls.fq_table_names.append(
            f'{cls.project_id}.{cls.dataset_id}.{OBSERVATION}')
        for table in VOCABULARY_TABLES:
            cls.fq_table_names.append(
                f'{cls.project_id}.{cls.dataset_id}.{table}')

        sandbox_table_names = cls.rule_instance.get_sandbox_tablenames()
        for table in sandbox_table_names:
            cls.fq_sandbox_table_names.append(
                f'{cls.project_id}.{cls.sandbox_id}.{table}')

        # Appending OLD_MAP_SHORT_CODES_TABLE table to fq_sandbox_table_names to delete after the test
        for table in [OLD_MAP_SHORT_CODES_TABLE
                     ] + cls.rule_instance.get_sandbox_tablenames():
            cls.fq_sandbox_table_names.append(
                f'{cls.project_id}.{cls.sandbox_id}.{table}')

        # call super to set up the client, create datasets
        cls.up_class = super().setUpClass()

    def setUp(self):
        # Set the test project identifier
        super().setUp()
        self.copy_vocab_tables(self.vocabulary_id)
        raw_data_load_query = INSERT_RAW_DATA.render(project_id=self.project_id,
                                                     dataset_id=self.dataset_id)

        # Load test data
        self.load_test_data([f'{raw_data_load_query}'])

    def test_setup_rule(self):

        # test if intermediary table exists before running the cleaning rule
        intermediary_table = f'{self.project_id}.{self.sandbox_id}.{OLD_MAP_SHORT_CODES_TABLE}'

        # run setup_rule and see if the table is created
        self.rule_instance.setup_rule(self.client)

        actual_table = self.client.get_table(intermediary_table)
        self.assertIsNotNone(actual_table.created)

        query = test_query.render(intermediary_table=intermediary_table)
        query_job_config = bigquery.job.QueryJobConfig(use_query_cache=False)
        result = self.client.query(query,
                                   job_config=query_job_config).to_dataframe()
        self.assertEqual(result.empty, False)

    def test_setting_concept_identifiers(self):
        """
        Tests concept_identifiers are updated or unchanged for the loaded test data
        """
        # Expected results list
        tables_and_counts = [{
            'fq_table_name':
                f'{self.project_id}.{self.dataset_id}.{OBSERVATION}',
            'loaded_ids': [1, 2, 3, 4],
            'sandboxed_ids': [1, 3],
            'fields': [
                'observation_id', 'person_id', 'observation_concept_id',
                'observation_source_concept_id', 'observation_source_value',
                'value_as_concept_id', 'value_source_concept_id',
                'value_source_value'
            ],
            'cleaned_values': [
                (1, 1, 43529214, 43529214,
                 'DiagnosedHealthCondition_GrandparentMentalCondition',
                 43528359, 43528359, 'GrandparentMentalCondition_Addiction'),
                (2, 1, 43529812, 43529812,
                 'DiagnosedHealthCondition_GrandparentSkelMusc', 43528709,
                 43528709, 'GrandparentSkeletalMuscularCondition_Fibromyalgia'),
                (3, 1, 43529634, 43529634,
                 'DiagnosedHealthCondition_GrandparentOtherHealth', 4171869,
                 43529757,
                 'GrandparentOtherHealthCondition_ReactionsToAnesthesia'),
                (4, 1, 43529634, 43529634,
                 'DiagnosedHealthCondition_GrandparentOtherHealth', 43529827,
                 43529827, 'GrandparentOtherHealthCondition_SkinCondition')
            ]
        }]

        self.default_test(tables_and_counts)
