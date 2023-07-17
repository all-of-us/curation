"""
Integration test for drop_survey_data_via_survey_conduct.py

"""

# Python imports
import os

# Third party imports
from datetime import date
from dateutil.parser import parse
import pytz

# Project imports
from common import JINJA_ENV
from app_identity import PROJECT_ID
from cdr_cleaner.cleaning_rules.drop_survey_data_via_survey_conduct import DropViaSurveyConduct, DOMAIN_TABLES
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import BaseTest\


class DropViaSurveyConductTest(BaseTest.CleaningRulesTestBase):

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

        cls.rule_instance = DropViaSurveyConduct(project_id, dataset_id,
                                                 sandbox_id)

        sb_table_names = cls.rule_instance.get_sandbox_tablenames()
        for table_name in sb_table_names:
            cls.fq_sandbox_table_names.append(
                f'{cls.project_id}.{cls.sandbox_id}.{table_name}')

        for table_name in DOMAIN_TABLES:
            cls.fq_table_names.append(
                f'{cls.project_id}.{cls.dataset_id}.{table_name}')

        # call super to set up the client, create datasets, and create
        # empty test tables
        # NOTE:  does not create empty sandbox tables.
        super().setUpClass()

    def test_drop_via_survey_conduct(self):
        """
        Tests unit_normalization for the loaded test data
        """

        OBSERVATION_TEMPLATE = JINJA_ENV.from_string("""
        INSERT INTO `{{project_id}}.{{dataset_id}}.observation`
        (observation_id, person_id, observation_date, observation_concept_id,
         observation_type_concept_id, questionnaire_response_id)
        VALUES
            -- 0 survey_source_concept_id. Cleaned --
              (1, 1, '2020-01-01', 1, 1, 1),
            -- Valid survey_conduct. Not cleaned --
              (2, 1, '2020-01-01', 1, 1, 2),
            -- No associated survey_conduct_id. Represents a problem with the export. Not cleaned --
              (15, 1, '2020-01-01', 1, 1, 15),
            -- 0 survey_concept_id. Cleaned --
              (3, 1, '2020-01-01', 1, 1, 3),
            -- Associated with WEAR consent survey. Cleaned --
              (4, 1, '2020-01-01', 1, 1, 4),
              (5, 1, '2020-01-01', 1, 1, 5)
        """).render(project_id=self.project_id, dataset_id=self.dataset_id)

        SURVEY_CONDUCT_TEMPLATE = JINJA_ENV.from_string("""
        INSERT INTO `{{project_id}}.{{dataset_id}}.survey_conduct`
        (survey_conduct_id, person_id,survey_concept_id,survey_end_datetime,assisted_concept_id,
        respondent_type_concept_id, timing_concept_id, collection_method_concept_id,
        survey_source_concept_id, validated_survey_concept_id)
        VALUES
            -- 0 survey_source_concept_id. Cleaned --
              (1, 1, 33333333, '2020-01-01 00:00:00 UTC', 111, 111, 111, 111, 0, 111),
            -- Valid survey_conduct. Not cleaned --
              (2, 1, 33333333, '2020-01-01 00:00:00 UTC', 111, 111, 111, 111, 33333333, 111),
            -- 0 survey_concept_id. Cleaned --  
              (3, 1, 0, '2020-01-01 00:00:00 UTC', 111, 111, 111, 111, 33333333, 111),
            -- If either concept_id are associated with WEAR modules. Cleaned --
              (4, 1, 2100000011, '2020-01-01 00:00:00 UTC', 111, 111, 111, 111, 33333333, 111),
              (5, 1, 33333333, '2020-01-01 00:00:00 UTC', 111, 111, 111, 111, 2100000012, 111)
        """).render(project_id=self.project_id, dataset_id=self.dataset_id)

        self.load_test_data([OBSERVATION_TEMPLATE, SURVEY_CONDUCT_TEMPLATE])

        tables_and_counts = [{
            'fq_table_name':
                self.fq_table_names[0],
            'fq_sandbox_table_name':
                self.fq_sandbox_table_names[0],
            'fields': [
                'observation_id', 'person_id', 'observation_date',
                'observation_concept_id', 'observation_type_concept_id',
                'questionnaire_response_id'
            ],
            'loaded_ids': [1, 2, 15, 3, 4, 5],
            'sandboxed_ids': [1, 3, 4, 5],
            'cleaned_values': [(2, 1, date(2020, 1, 1), 1, 1, 2),
                               (15, 1, date(2020, 1, 1), 1, 1, 15)]
        }, {
            'fq_table_name':
                self.fq_table_names[1],
            'fq_sandbox_table_name':
                self.fq_sandbox_table_names[1],
            'fields': [
                'survey_conduct_id', 'person_id', 'survey_concept_id',
                'survey_end_datetime', 'assisted_concept_id',
                'respondent_type_concept_id', 'timing_concept_id',
                'collection_method_concept_id', 'survey_source_concept_id',
                'validated_survey_concept_id'
            ],
            'loaded_ids': [1, 2, 3, 4, 5],
            'sandboxed_ids': [1, 3, 4, 5],
            'cleaned_values': [
                (2, 1, 33333333,
                 parse('2020-01-01 00:00:00 UTC').astimezone(pytz.utc), 111,
                 111, 111, 111, 33333333, 111)
            ]
        }]

        self.default_test(tables_and_counts)
