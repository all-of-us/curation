"""
Integration test for DropOrphanedSurveyConductIds module
Original Issue: DC-2735
"""
# Python Imports
import os

# Project imports
from app_identity import PROJECT_ID
from cdr_cleaner.cleaning_rules.drop_orphaned_survey_conduct_ids import DropOrphanedSurveyConductIds
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import BaseTest
from common import OBSERVATION, SURVEY_CONDUCT

# Third party imports


class DropOrphanedSurveyConductIdsTest(BaseTest.CleaningRulesTestBase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

        super().initialize_class_vars()

        cls.project_id = os.environ.get(PROJECT_ID)
        cls.dataset_id = os.environ.get('COMBINED_DATASET_ID')
        cls.sandbox_id = f'{cls.dataset_id}_sandbox'

        cls.rule_instance = DropOrphanedSurveyConductIds(
            cls.project_id, cls.dataset_id, cls.sandbox_id)

        cls.fq_table_names = [
            f'{cls.project_id}.{cls.dataset_id}.{SURVEY_CONDUCT}',
            f'{cls.project_id}.{cls.dataset_id}.{OBSERVATION}'
        ]

        cls.fq_sandbox_table_names = []
        for table in cls.rule_instance.get_sandbox_tablenames():
            cls.fq_sandbox_table_names.append(
                f'{cls.project_id}.{cls.sandbox_id}.{table}')

        super().setUpClass()

    def test_drop_orphaned_survey_conduct_ids(self):
        """
        Tests that the specifications perform as designed.
        """

        INSERT_OBSERVATIONS_QUERY = self.jinja_env.from_string("""
            INSERT INTO `{{project_id}}.{{dataset_id}}.observation`
                (observation_id, person_id, observation_concept_id, observation_source_concept_id,
                 observation_date, observation_type_concept_id, questionnaire_response_id)
            VALUES
                (11, 101, 0, 0, date('2022-09-01'), 0, 1),
                (12, 102, 0, 0, date('2022-09-01'), 0, 2),
                (13, 102, 0, 0, date('2022-09-01'), 0, 2),
                (14, 104, 0, 0, date('2022-09-01'), 0, NULL)
        """).render(project_id=self.project_id, dataset_id=self.dataset_id)

        INSERT_SURVEY_CONDUCT_QUERY = self.jinja_env.from_string("""
            INSERT INTO `{{project_id}}.{{dataset_id}}.survey_conduct`
                (survey_conduct_id, person_id, survey_concept_id, survey_end_datetime,
                 assisted_concept_id, respondent_type_concept_id, timing_concept_id,
                 collection_method_concept_id, survey_source_concept_id,
                 validated_survey_concept_id)
            VALUES
                (1, 101, 0, timestamp('2022-09-01 00:00:00'), 0, 0, 0, 0, 0, 0),
                (2, 102, 0, timestamp('2022-09-01 00:00:00'), 0, 0, 0, 0, 0, 0),
                (3, 103, 0, timestamp('2022-09-01 00:00:00'), 0, 0, 0, 0, 0, 0)
        """).render(project_id=self.project_id, dataset_id=self.dataset_id)

        queries = [INSERT_OBSERVATIONS_QUERY, INSERT_SURVEY_CONDUCT_QUERY]

        self.load_test_data(queries)

        tables_and_counts = [{
            'fq_table_name':
                f'{self.project_id}.{self.dataset_id}.{SURVEY_CONDUCT}',
            'fq_sandbox_table_name':
                self.fq_sandbox_table_names[0],
            'loaded_ids': [1, 2, 3],
            'sandboxed_ids': [3],
            'fields': ['survey_conduct_id'],
            'cleaned_values': [(1,), (2,)]
        }, {
            'fq_table_name':
                f'{self.project_id}.{self.dataset_id}.{OBSERVATION}',
            'fq_sandbox_table_name':
                '',
            'loaded_ids': [11, 12, 13, 14],
            'sandboxed_ids': [],
            'fields': ['observation_id'],
            'cleaned_values': [(11,), (12,), (13,), (14,)]
        }]

        self.default_test(tables_and_counts)
