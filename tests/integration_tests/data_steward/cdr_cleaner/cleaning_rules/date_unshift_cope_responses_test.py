# coding=utf-8
"""
Integration Test for the date_unshift_cope_responses module.

Date shift any fields that are of type DATE, DATETIME, or TIMESTAMP.
"""
# Python imports
import os
from datetime import date, datetime
from unittest.mock import patch

# Project imports
from app_identity import PROJECT_ID
from cdr_cleaner.cleaning_rules.date_unshift_cope_responses import DateUnShiftCopeResponses
from common import SURVEY_CONDUCT, DEID_MAP
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import BaseTest


class DateUnShiftCopeResponsesTest(BaseTest.CleaningRulesTestBase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

        super().initialize_class_vars()

        # set the test project identifier
        cls.project_id = os.environ.get(PROJECT_ID)

        # set the expected test datasets
        dataset_id = os.environ.get('UNIONED_DATASET_ID')
        cls.dataset_id = dataset_id
        sandbox_id = f'{dataset_id}_sandbox'
        cls.sandbox_id = sandbox_id

        # include the mapping dataset
        mapping_dataset_id = os.environ.get('COMBINED_DATASET_ID')
        cls.mapping_dataset_id = mapping_dataset_id
        cls.kwargs.update({'mapping_dataset_id': mapping_dataset_id})
        cls.fq_deid_map_table_name = f'{cls.project_id}.{mapping_dataset_id}.{DEID_MAP}'

        cls.combined_dataset_id = os.environ.get('COMBINED_DATASET_ID')

        cls.rule_instance = DateUnShiftCopeResponses(cls.project_id, dataset_id,
                                                     sandbox_id,
                                                     mapping_dataset_id)

        cls.rule_instance.affected_tables = [SURVEY_CONDUCT]

        # can test the full functionality with one table
        cls.fq_table_names = [
            f"{cls.project_id}.{cls.combined_dataset_id}.{cls.rule_instance.affected_tables[0]}",
            f"{cls.project_id}.{dataset_id}.{cls.rule_instance.affected_tables[0]}",
            cls.fq_deid_map_table_name
        ]

        cls.fq_sandbox_table_names = [
            f"{cls.project_id}.{sandbox_id}.{cls.rule_instance.sandbox_table_for(cls.fq_table_names[1].split('.')[-1])}"
        ]

        # call super to set up the client, create datasets, and create
        # empty test tables
        super().setUpClass()

    def setUp(self):
        """
        Add data to the tables for the rule to run on.
        """

        deid_map_query = self.jinja_env.from_string("""
             INSERT INTO `{{fq_table_name}}` (person_id, research_id, shift)
             VALUES
             (81, 801, 7),
             (82, 802, 4)
             """)

        self.client.create_tables(self.fq_table_names)

        # load statement for the test data to unshift
        query_combined = self.jinja_env.from_string("""
        INSERT INTO `{{fq_table_name}}`
        (survey_conduct_id, person_id, survey_concept_id, survey_start_date,
        survey_start_datetime, survey_end_date, survey_end_datetime,
        assisted_concept_id, respondent_type_concept_id, timing_concept_id,
        collection_method_concept_id, survey_source_concept_id, validated_survey_concept_id)
        VALUES
        (8, 801, 200, date(2016, 05, 11), timestamp(datetime(2016, 05, 11, 12, 45, 00)), date(2017, 05, 11), timestamp(datetime(2017, 05, 11, 12, 45, 00)), 1, 2, 3, 4, 2100000005, 6),
        (9, 802, 201, date(2016, 05, 10), timestamp(datetime(2016, 05, 10, 12, 45, 00)), date(2017, 05, 10), timestamp(datetime(2017, 05, 10, 12, 45, 00)), 1, 2, 3, 4, 2100000005, 6)
        """)

        query_deid = self.jinja_env.from_string("""
        INSERT INTO `{{fq_table_name}}`
        (survey_conduct_id, person_id, survey_concept_id, survey_start_date,
        survey_start_datetime, survey_end_date, survey_end_datetime,
        assisted_concept_id, respondent_type_concept_id, timing_concept_id,
        collection_method_concept_id, survey_source_concept_id, validated_survey_concept_id)
        VALUES
        -- cope concept in survey_source_concept_id. Should be unshifted. --
        (10, 801, 200, date(2016, 05, 4),timestamp(datetime(2016, 05, 4, 12, 45, 00)), date(2017, 05, 4), timestamp(datetime(2017, 05, 4, 12, 45, 00)), 1, 2, 3, 4, 2100000005, 6),
        -- cope concept in survey_concept_id. Should be unshifted. --
        (11, 802, 2100000005, date(2016, 05, 6),timestamp(datetime(2016, 05, 6, 12, 45, 00)), date(2017, 05, 6), timestamp(datetime(2017, 05, 6, 12, 45, 00)), 1, 2, 3, 4, 2100000005, 6),
        -- Multiple cope records for the same participant. All cope data should be unshifted. --
        (12, 802, 201, date(2016, 05, 7),timestamp(datetime(2016, 05, 7, 12, 45, 00)), date(2017, 05, 7), timestamp(datetime(2017, 05, 7, 12, 45, 00)), 1, 2, 3, 4, 2100000005, 6),
        -- Multiple records for the same participant. Non-cope data should remain shifted. --
        (13, 802, 201, date(2016, 05, 7),timestamp(datetime(2016, 05, 7, 12, 45, 00)), date(2017, 05, 7), timestamp(datetime(2017, 05, 7, 12, 45, 00)), 1, 2, 3, 4, 201, 6),
        -- Handling nulls in nullable fields --
        (14, 802, 201, NULL, NULL, NULL, timestamp(datetime(2017, 05, 7, 12, 45, 00)), 1, 2, 3, 4, 2100000005, 6)
        """)

        load_statements = [
            query_combined.render(fq_table_name=self.fq_table_names[0]),
            query_deid.render(fq_table_name=self.fq_table_names[1]),
            deid_map_query.render(fq_table_name=self.fq_table_names[2])
        ]
        self.load_test_data(load_statements)

    @patch.object(DateUnShiftCopeResponses,
                  'get_combined_dataset_from_deid_dataset')
    def test_date_unshifting(self, mock_combined):
        """
        Use the default drop rows test function.
        """

        # Using the 1 position because there is only one sandbox table and
        # one affected OMOP table
        mock_combined.return_value = self.combined_dataset_id
        tables_and_counts = [{
            'name':
                self.fq_table_names[1].split('.')[-1],
            'fq_table_name':
                self.fq_table_names[1],
            'fq_sandbox_table_name':
                self.fq_sandbox_table_names[0],
            'fields': [
                'survey_conduct_id', 'person_id', 'survey_concept_id',
                'survey_start_date', 'survey_start_datetime', 'survey_end_date',
                'survey_end_datetime'
            ],
            'loaded_ids': [10, 11, 12, 13, 14],
            'sandboxed_ids': [10, 11, 12, 14],
            'cleaned_values': [
                (10, 801, 200, date.fromisoformat('2016-05-11'),
                 datetime.fromisoformat('2016-05-11 12:45:00+00:00'),
                 date.fromisoformat('2017-05-11'),
                 datetime.fromisoformat('2017-05-11 12:45:00+00:00')),
                (11, 802, 2100000005, date.fromisoformat('2016-05-10'),
                 datetime.fromisoformat('2016-05-10 12:45:00+00:00'),
                 date.fromisoformat('2017-05-10'),
                 datetime.fromisoformat('2017-05-10 12:45:00+00:00')),
                (12, 802, 201, date.fromisoformat('2016-05-11'),
                 datetime.fromisoformat('2016-05-11 12:45:00+00:00'),
                 date.fromisoformat('2017-05-11'),
                 datetime.fromisoformat('2017-05-11 12:45:00+00:00')),
                (13, 802, 201, date.fromisoformat('2016-05-07'),
                 datetime.fromisoformat('2016-05-07 12:45:00+00:00'),
                 date.fromisoformat('2017-05-07'),
                 datetime.fromisoformat('2017-05-07 12:45:00+00:00')),
                (14, 802, 201, None, None, None,
                 datetime.fromisoformat('2017-05-11 12:45:00+00:00')),
            ]
        }]

        self.default_test(tables_and_counts)
