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
        (survey_conduct_id, src_id, language)
    VALUES
        (11, 'pi/pm', NULL),
        (12, 'site foo', NULL),
        (13, 'site foo', NULL),
        (14, 'site bar', NULL)
""")

INSERT_QUESTIONNAIRE_RESPONSE_ADDITIONAL_INFO = JINJA_ENV.from_string("""
    INSERT INTO `{{project}}.{{clean_survey_dataset_id}}.questionnaire_response_additional_info`
        (questionnaire_response_id, type, value)
    VALUES
        (11, 'LANGUAGE', 'en'),
        (11, 'NON_PARTICIPANT_AUTHOR_INDICATOR', 'CATI'),
        (11, 'CODE', 'TheBasics'),
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

        cls.project_id = os.environ.get(PROJECT_ID)
        cls.dataset_id = os.environ.get('RDR_DATASET_ID')
        cls.clean_survey_dataset_id = os.environ.get('RDR_DATASET_ID')
        cls.kwargs.update(
            {'clean_survey_dataset_id': cls.clean_survey_dataset_id})
        sandbox_id = f"{cls.dataset_id}_sandbox"
        cls.sandbox_id = sandbox_id

        cls.rule_instance = PopulateSurveyConductExt(
            cls.project_id,
            cls.dataset_id,
            sandbox_id,
            clean_survey_dataset_id=cls.clean_survey_dataset_id)

        cls.fq_sandbox_table_names = []

        cls.fq_table_names = [
            f'{cls.project_id}.{cls.dataset_id}.{SURVEY_CONDUCT}{EXT_SUFFIX}',
            f'{cls.project_id}.{cls.clean_survey_dataset_id}.{QUESTIONNAIRE_RESPONSE_ADDITIONAL_INFO}',
        ]

        super().setUpClass()

    def setUp(self):
        super().setUp()

        insert_survey_conduct_ext = INSERT_SURVEY_CONDUCT_EXT.render(
            project=self.project_id, dataset=self.dataset_id)
        insert_questionnaire_response_additional_info = INSERT_QUESTIONNAIRE_RESPONSE_ADDITIONAL_INFO.render(
            project=self.project_id,
            clean_survey_dataset_id=self.clean_survey_dataset_id)

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
                '',
            'fields': ['survey_conduct_id', 'src_id', 'language'],
            'loaded_ids': [11, 12, 13, 14],
            'sandboxed_ids': [],
            'cleaned_values': [
                (11, 'pi/pm', 'en'),
                (12, 'site foo', 'es'),
                (13, 'site foo', None),
                (14, 'site bar', None),
            ]
        }]

        self.default_test(tables_and_counts)
