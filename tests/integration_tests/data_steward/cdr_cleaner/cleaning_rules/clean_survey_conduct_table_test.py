"""
Integration test for clean_survey_conduct_table.py

"""

# Python imports
import os

# Third party imports
from dateutil.parser import parse
import pytz

# Project imports
from common import JINJA_ENV, VOCABULARY_TABLES
from app_identity import PROJECT_ID
from cdr_cleaner.cleaning_rules.clean_survey_conduct_table import CleanSurveyConduct, DOMAIN_TABLES, AOU_CUSTOM_VOCAB
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import BaseTest\


class CleanSurveyConductTest(BaseTest.CleaningRulesTestBase):

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
        cls.vocabulary_id = os.environ.get('VOCABULARY_DATASET')

        cls.rule_instance = CleanSurveyConduct(project_id, dataset_id,
                                               sandbox_id)

        sb_table_names = cls.rule_instance.get_sandbox_tablenames()
        for table_name in sb_table_names:
            cls.fq_sandbox_table_names.append(
                f'{cls.project_id}.{cls.sandbox_id}.{table_name}')

        for table_name in DOMAIN_TABLES + VOCABULARY_TABLES:
            cls.fq_table_names.append(
                f'{cls.project_id}.{cls.dataset_id}.{table_name}')
        cls.fq_table_names.append(
            f'{cls.project_id}.{cls.sandbox_id}.{AOU_CUSTOM_VOCAB}')

        # call super to set up the client, create datasets, and create
        # empty test tables
        # NOTE:  does not create empty sandbox tables.
        super().setUpClass()

        # Copy vocab tables over to the test dataset
        for src_table in cls.client.list_tables(cls.vocabulary_id):
            destination = f'{cls.project_id}.{cls.dataset_id}.{src_table.table_id}'
            cls.client.copy_table(src_table, destination)

    def test_clean_survey_conduct_data(self):
        """
        Tests unit_normalization for the loaded test data
        """

        SURVEY_CONDUCT_TEMPLATE = JINJA_ENV.from_string("""
          INSERT INTO `{{project_id}}.{{dataset_id}}.survey_conduct`
        (survey_conduct_id, person_id,survey_concept_id,survey_end_datetime,assisted_concept_id,
        respondent_type_concept_id, timing_concept_id, collection_method_concept_id, survey_source_value,
        survey_source_concept_id, validated_survey_concept_id)
        VALUES
        -- survey_concept_id is valid, survey_source_concept_id is not valid. Should be sandboxed and cleaned. --
              (1, 1, 1585855, '2020-01-01 00:00:00 UTC', 111, 1111, 11111, 111111,'Lifestyle', 0, 111111111),
              (2, 2, 1585855, '2020-01-01 00:00:00 UTC', 111, 1111, 11111, 111111,'Lifestyle', 0, 111111111),
        -- survey_source_concept_id is valid, survey_concept_id is not valid. Should be sandboxed and cleaned. --
              (3, 3, 0, '2020-01-01 00:00:00 UTC', 111, 1111, 11111, 111111,'Lifestyle', 1585855, 111111111),
              (4, 4, 0, '2020-01-01 00:00:00 UTC', 111, 1111, 11111, 111111,'Lifestyle', 1585855, 111111111),          
        -- Both concept_id fields are valid and the same. Should not be affected. --
              (5, 5, 1585855, '2020-01-01 00:00:00 UTC', 111, 1111, 11111, 111111,'Lifestyle', 1585855, 111111111),
              (6, 6, 2100000004, '2020-01-01 00:00:00 UTC', 111, 1111, 11111, 111111,'AoUDRC_SurveyVersion_CopeJuly2020', 2100000004, 111111111),
        -- At least one concept_id field is invalid. Should be sandboxed and concept_id fields set to 0. --
              (7, 7, 0, '2020-01-01 00:00:00 UTC', 111, 1111, 11111, 111111,'Lifestyle', 0, 111111111),
              (8, 8, 0, '2020-01-01 00:00:00 UTC', 111, 1111, 11111, 111111,'Lifestyle', 0, 111111111),
              (9, 9, 1111, '2020-01-01 00:00:00 UTC', 111, 1111, 11111, 111111,'Lifestyle', 1111, 111111111),
              (10, 10, 1585855, '2020-01-01 00:00:00 UTC', 111, 1111, 11111, 111111,'Lifestyle', 1111, 111111111),
              (11, 11, 1111, '2020-01-01 00:00:00 UTC', 111, 1111, 11111, 111111,'Lifestyle', 1585855, 111111111)
        """).render(project_id=self.project_id, dataset_id=self.dataset_id)

        self.load_test_data([SURVEY_CONDUCT_TEMPLATE])

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
            'loaded_ids': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11],
            'sandboxed_ids': [1, 2, 3, 4, 7, 8, 9, 10, 11],
            'cleaned_values': [
                (1, 1, 1585855,
                 parse('2020-01-01 00:00:00 UTC').astimezone(pytz.utc), 111,
                 1111, 11111, 111111, 'Lifestyle', 1585855, 111111111),
                (2, 2, 1585855,
                 parse('2020-01-01 00:00:00 UTC').astimezone(pytz.utc), 111,
                 1111, 11111, 111111, 'Lifestyle', 1585855, 111111111),
                (3, 3, 1585855,
                 parse('2020-01-01 00:00:00 UTC').astimezone(pytz.utc), 111,
                 1111, 11111, 111111, 'Lifestyle', 1585855, 111111111),
                (4, 4, 1585855,
                 parse('2020-01-01 00:00:00 UTC').astimezone(pytz.utc), 111,
                 1111, 11111, 111111, 'Lifestyle', 1585855, 111111111),
                (5, 5, 1585855,
                 parse('2020-01-01 00:00:00 UTC').astimezone(pytz.utc), 111,
                 1111, 11111, 111111, 'Lifestyle', 1585855, 111111111),
                (6, 6, 2100000004, parse('2020-01-01 00:00:00 UTC').astimezone(
                    pytz.utc), 111, 1111, 11111, 111111,
                 'AoUDRC_SurveyVersion_CopeJuly2020', 2100000004, 111111111),
                (7, 7, 0, parse('2020-01-01 00:00:00 UTC').astimezone(pytz.utc),
                 111, 1111, 11111, 111111, 'Lifestyle', 0, 111111111),
                (8, 8, 0, parse('2020-01-01 00:00:00 UTC').astimezone(pytz.utc),
                 111, 1111, 11111, 111111, 'Lifestyle', 0, 111111111),
                (9, 9, 0, parse('2020-01-01 00:00:00 UTC').astimezone(pytz.utc),
                 111, 1111, 11111, 111111, 'Lifestyle', 0, 111111111),
                (10, 10, 0,
                 parse('2020-01-01 00:00:00 UTC').astimezone(pytz.utc), 111,
                 1111, 11111, 111111, 'Lifestyle', 0, 111111111),
                (11, 11, 0,
                 parse('2020-01-01 00:00:00 UTC').astimezone(pytz.utc), 111,
                 1111, 11111, 111111, 'Lifestyle', 0, 111111111)
            ]
        }]

        self.default_test(tables_and_counts)
