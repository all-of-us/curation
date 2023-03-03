"""
Integration test for clean_survey_conduct_custom_ids.py
"""

# Python imports
import os

# Third party imports
from dateutil.parser import parse
import pytz

# Project imports
from common import JINJA_ENV, COPE_SURVEY_MAP
from app_identity import PROJECT_ID
from cdr_cleaner.cleaning_rules.clean_survey_conduct_recurring_surveys import CleanSurveyConductRecurringSurveys, DOMAIN_TABLES, REFERENCE_TABLES
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import BaseTest\


class CleanSurveyConductRecurringSurveysTest(BaseTest.CleaningRulesTestBase):

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
        # intended to be run on the rdr dataset
        dataset_id = os.environ.get('RDR_DATASET_ID')
        cls.dataset_id = dataset_id
        sandbox_id = dataset_id + '_sandbox'
        cls.sandbox_id = sandbox_id

        cls.rule_instance = CleanSurveyConductRecurringSurveys(
            project_id, dataset_id, sandbox_id)

        sb_table_names = cls.rule_instance.get_sandbox_tablenames()
        for table_name in sb_table_names:
            cls.fq_sandbox_table_names.append(
                f'{cls.project_id}.{cls.sandbox_id}.{table_name}')

        for table_name in DOMAIN_TABLES + REFERENCE_TABLES:
            cls.fq_table_names.append(
                f'{cls.project_id}.{cls.dataset_id}.{table_name}')

        # call super to set up the client, create datasets, and create
        # empty test tables
        # NOTE:  does not create empty sandbox tables.
        super().setUpClass()

    def test_clean_survey_conduct_recurring_surveys(self):
        """
        Tests unit_normalization for the loaded test data
        """

        COPE_SURVEY_MAP_TEMPLATE = self.jinja_env.from_string("""
            INSERT INTO `{{project_id}}.{{dataset_id}}.{{cope_survey_map}}`
            (participant_id, questionnaire_response_id, semantic_version, cope_month)
            VALUES
                (1, 1, 'semantic_version', 'nov'),
                (2, 2, 'semantic_version', 'nov'),
                (3, 3, 'semantic_version', 'vaccine1'),
                (4, 4, 'semantic_version', 'nov'),
                (5, 5, 'semantic_version', 'nov'),
                (6, 6, 'semantic_version', 'vaccine1'),                
                (7, 7, 'semantic_version', 'july'),
                (8, 8, 'semantic_version', 'july')
            """).render(project_id=self.project_id,
                        dataset_id=self.dataset_id,
                        cope_survey_map=COPE_SURVEY_MAP)

        SURVEY_CONDUCT_TEMPLATE = JINJA_ENV.from_string("""
        INSERT INTO `{{project_id}}.{{dataset_id}}.survey_conduct`
        (survey_conduct_id, person_id,survey_concept_id,survey_end_datetime,assisted_concept_id,
        respondent_type_concept_id, timing_concept_id, collection_method_concept_id, survey_source_value,
        survey_source_concept_id, validated_survey_concept_id)
        VALUES
        -- SC data is populated correctly at this stage. Not sandboxed, not changed. --
              (1, 1, 2100000005, '2020-01-01 00:00:00 UTC', 111, 1111, 11111, 111111,'AoUDRC_SurveyVersion_CopeNovember2020', 2100000005, 111111111),
              (2, 2, 2100000005, '2020-01-01 00:00:00 UTC', 111, 1111, 11111, 111111,'AoUDRC_SurveyVersion_CopeNovember2020', 222, 111111111),
              (3, 3, 905047, '2020-01-01 00:00:00 UTC', 111, 1111, 11111, 111111,'cope_vaccine1', 1111, 111111111),
        -- SC data does not define cope version in expected fields. Sandbox and update. --
              (4, 4, 1333342, '2020-01-01 00:00:00 UTC', 111, 1111, 11111, 111111,'COPE', 1111, 111111111),
              (5, 5, 0, '2020-01-01 00:00:00 UTC', 111, 1111, 11111, 111111,'cope_nov', 1111, 111111111),
              (6, 6, 0, '2020-01-01 00:00:00 UTC', 111, 1111, 11111, 111111,'summer', 1111, 111111111),
              (7, 7, 2100000004, '2020-01-01 00:00:00 UTC', 111, 1111, 11111, 111111,'july', 1111, 111111111),
              (8, 8, 0, '2020-01-01 00:00:00 UTC', 111, 1111, 11111, 111111,'AoUDRC_SurveyVersion_CopeJuly2020', 1111, 111111111),
        -- SC data does not have a row in the semantic map. Not COPE/Minute, not sandboxed, not changed. --
              (9, 9, 1585855, '2020-01-01 00:00:00 UTC', 111, 1111, 11111, 111111,'Lifestyle', 1111, 111111111),
        -- SC data does not have a row in the semantic map. COPE/Minute, not sandboxed, not changed. Represents a bug. --
              (10, 10, 1333342, '2020-01-01 00:00:00 UTC', 111, 1111, 11111, 111111,'COPE', 1111, 111111111)
        """).render(project_id=self.project_id, dataset_id=self.dataset_id)

        self.load_test_data([COPE_SURVEY_MAP_TEMPLATE, SURVEY_CONDUCT_TEMPLATE])

        tables_and_counts = [{
            'fq_table_name':
                self.fq_table_names[0],
            'fq_sandbox_table_name':
                self.fq_sandbox_table_names[0],
            'fields': [
                'survey_conduct_id', 'person_id', 'survey_concept_id',
                'survey_end_datetime', 'assisted_concept_id',
                'respondent_type_concept_id', 'timing_concept_id',
                'collection_method_concept_id', 'survey_source_value',
                'survey_source_concept_id', 'validated_survey_concept_id'
            ],
            'loaded_ids': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            'sandboxed_ids': [4, 5, 6, 7, 8],
            'cleaned_values': [
                (1, 1, 2100000005,
                 parse('2020-01-01 00:00:00 UTC').astimezone(pytz.utc), 111,
                 1111, 11111, 111111, 'AoUDRC_SurveyVersion_CopeNovember2020',
                 2100000005, 111111111),
                (2, 2, 2100000005, parse('2020-01-01 00:00:00 UTC').astimezone(
                    pytz.utc), 111, 1111, 11111, 111111,
                 'AoUDRC_SurveyVersion_CopeNovember2020', 222, 111111111),
                (3, 3, 905047,
                 parse('2020-01-01 00:00:00 UTC').astimezone(pytz.utc), 111,
                 1111, 11111, 111111, 'cope_vaccine1', 1111, 111111111),
                (4, 4, 2100000005, parse('2020-01-01 00:00:00 UTC').astimezone(
                    pytz.utc), 111, 1111, 11111, 111111,
                 'AoUDRC_SurveyVersion_CopeNovember2020', 1111, 111111111),
                (5, 5, 2100000005, parse('2020-01-01 00:00:00 UTC').astimezone(
                    pytz.utc), 111, 1111, 11111, 111111,
                 'AoUDRC_SurveyVersion_CopeNovember2020', 1111, 111111111),
                (6, 6, 905047,
                 parse('2020-01-01 00:00:00 UTC').astimezone(pytz.utc), 111,
                 1111, 11111, 111111, 'cope_vaccine1', 1111, 111111111),
                (7, 7, 2100000004, parse('2020-01-01 00:00:00 UTC').astimezone(
                    pytz.utc), 111, 1111, 11111, 111111,
                 'AoUDRC_SurveyVersion_CopeJuly2020', 1111, 111111111),
                (8, 8, 2100000004, parse('2020-01-01 00:00:00 UTC').astimezone(
                    pytz.utc), 111, 1111, 11111, 111111,
                 'AoUDRC_SurveyVersion_CopeJuly2020', 1111, 111111111),
                (9, 9, 1585855,
                 parse('2020-01-01 00:00:00 UTC').astimezone(pytz.utc), 111,
                 1111, 11111, 111111, 'Lifestyle', 1111, 111111111),
                (10, 10, 1333342,
                 parse('2020-01-01 00:00:00 UTC').astimezone(pytz.utc), 111,
                 1111, 11111, 111111, 'COPE', 1111, 111111111)
            ]
        }]

        self.default_test(tables_and_counts)
