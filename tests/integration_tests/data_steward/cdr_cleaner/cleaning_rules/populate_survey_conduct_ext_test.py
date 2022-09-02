"""
Integration test for PopulateSurveyConductExt.
"""

# Python Imports
import os

# Third party imports

# Project imports
from app_identity import PROJECT_ID
from cdr_cleaner.cleaning_rules.populate_survey_conduct_ext import PopulateSurveyConductExt
from common import EXT_SUFFIX, JINJA_ENV, QUESTIONNAIRE_RESPONSE_ADDITIONAL_INFO, SURVEY_CONDUCT
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import BaseTest

INSERT_SURVEY_CONDUCT_EXT = JINJA_ENV.from_string("""
    INSERT INTO `{{project}}.{{dataset}}.survey_conduct_ext` 
        (survey_conduct_id, src_id, type, value)
    VALUES
        (11, 'pi/pm', NULL, NULL),
        (12, 'site foo', NULL, NULL),
        (13, 'site foo', NULL, NULL),
        (14, 'site bar', NULL, NULL)
""")

INSERT_QUESTIONNAIRE_RESPONSE_ADDITIONAL_INFO = JINJA_ENV.from_string("""
    INSERT INTO `{{project}}.{{dataset}}.questionnaire_response_additional_info` 
        (questionnaire_response_id, type, value)
    VALUES
        (11, 'LANGUAGE', 'en'),
        (12, 'LANGUAGE', 'es'),
        (13, 'NON_PARTICIPANT_AUTHOR_INDICATOR', 'CATI'),
        (14, 'CODE', 'TheBasics')
""")


class PopulateSurveyConductExtTest(BaseTest.CleaningRulesTestBase):

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

        cls.rule_instance = PopulateSurveyConductExt(project_id, dataset_id,
                                                     sandbox_id)

        sb_table_name = cls.rule_instance.sandbox_table_for(
            f"{SURVEY_CONDUCT}{EXT_SUFFIX}")

        cls.fq_sandbox_table_names = [
            f'{cls.project_id}.{cls.sandbox_id}.{sb_table_name}'
        ]

        cls.fq_table_names = [
            f'{project_id}.{dataset_id}.{SURVEY_CONDUCT}{EXT_SUFFIX}',
            f'{project_id}.{dataset_id}.{QUESTIONNAIRE_RESPONSE_ADDITIONAL_INFO}',
        ]

        super().setUpClass()

    def setUp(self):
        super().setUp()

        insert_survey_conduct_ext = INSERT_SURVEY_CONDUCT_EXT.render(
            project=self.project_id, dataset=self.dataset_id)
        insert_questionnaire_response_additional_info = INSERT_QUESTIONNAIRE_RESPONSE_ADDITIONAL_INFO.render(
            project=self.project_id, dataset=self.dataset_id)

        queries = [
            insert_survey_conduct_ext,
            insert_questionnaire_response_additional_info
        ]
        self.load_test_data(queries)

    def test_populate_survey_conduct_ext(self):
        """
        Tests that the queries perform as designed.
        """

        tables_and_counts = [{
            'fq_table_name':
                f'{self.project_id}.{self.dataset_id}.{SURVEY_CONDUCT}{EXT_SUFFIX}',
            'fq_sandbox_table_name':
                self.fq_sandbox_table_names[0],
            'fields': ['survey_conduct_id', 'src_id', 'type', 'value'],
            'loaded_ids': [11, 12, 13, 14],
            'sandboxed_ids': [11, 12, 13, 14],
            'cleaned_values': [
                (11, 'pi/pm', 'LANGUAGE', 'en'),
                (12, 'site foo', 'LANGUAGE', 'es'),
                (13, 'site foo', 'NON_PARTICIPANT_AUTHOR_INDICATOR', 'CATI'),
                (14, 'site bar', 'CODE', 'TheBasics'),
            ]
        }]

        self.default_test(tables_and_counts)