"""
Integration test for RemoveRecordsWithWrongDate.

Original Issues: DC-489, DC-828
"""
# Python imports
import os
from unittest import mock

# Third party imports
from dateutil import parser
import pytz

# Project imports
from common import JINJA_ENV, OBSERVATION, OBSERVATION_PERIOD, VISIT_DETAIL
from app_identity import PROJECT_ID
from cdr_cleaner.cleaning_rules.remove_records_with_wrong_date import RemoveRecordsWithWrongDate
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import BaseTest


class RemoveRecordsWithWrongDateTest(BaseTest.CleaningRulesTestBase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

        super().initialize_class_vars()

        cls.project_id = os.environ.get(PROJECT_ID)
        cls.dataset_id = os.environ.get('COMBINED_DATASET_ID')
        cls.sandbox_id = f"{cls.dataset_id}_sandbox"

        cls.kwargs = {'cutoff_date': '2022-01-01'}
        cls.rule_instance = RemoveRecordsWithWrongDate(cls.project_id,
                                                       cls.dataset_id,
                                                       cls.sandbox_id,
                                                       **cls.kwargs)

        cls.fq_table_names = [
            f"{cls.project_id}.{cls.dataset_id}.{OBSERVATION}",
            f"{cls.project_id}.{cls.dataset_id}.{VISIT_DETAIL}",
            f"{cls.project_id}.{cls.dataset_id}.{OBSERVATION_PERIOD}",
        ]

        cls.fq_sandbox_table_names = [
            f'{cls.project_id}.{cls.sandbox_id}.{cls.rule_instance.sandbox_table_for(OBSERVATION)}',
            f'{cls.project_id}.{cls.sandbox_id}.{cls.rule_instance.sandbox_table_for(VISIT_DETAIL)}',
            f'{cls.project_id}.{cls.sandbox_id}.{cls.rule_instance.sandbox_table_for(OBSERVATION_PERIOD)}',
        ]

        super().setUpClass()

    @mock.patch(
        'cdr_cleaner.cleaning_rules.remove_records_with_wrong_date.RemoveRecordsWithWrongDate.affected_tables',
        [OBSERVATION, VISIT_DETAIL, OBSERVATION_PERIOD])
    def test_remove_records_with_wrong_date(self):
        """
        Tests that the specifications perform as designed.

        OBSERVATION: threshold = 1900
            11... Not cleaned: date/datetime columns are between threshold year and cutoff date.
            12... Deleted: observation_date(a required column) before or equal to threshold year
            13... Deleted: observation_date(a required column) after cutoff date
            14... Updated: observation_datetime(a nullable column) before threshold year
            15... Updated: observation_datetime(a nullable column) after cutoff date

        VISIT_DETAIL: threshold = 1980
            21... Not cleaned: date/datetime columns are between threshold year and cutoff date.
            22... Deleted: visit_detail_start_date(a required column) before or equal to threshold year
            23... Deleted: visit_detail_end_date(a required column) after cutoff date
            24... Updated: visit_detail_start_datetime(a nullable column) before threshold year
            25... Updated: visit_detail_end_datetime(a nullable column) after cutoff date

        OBSERVATION_PERIOD: threshold = 1980. This table does not have nullable date/datetime columns.
            31... Not cleaned: date columns are between threshold year and cutoff date.
            32... Deleted: observation_period_start_date(a required column) before or equal to threshold year
            33... Deleted: observation_period_end_date(a required column) after cutoff date
            No test case for "updated" for this table because it does not have nullable date/datetime columns.

        """

        INSERT_OBSERVATION_QUERY = JINJA_ENV.from_string("""
            INSERT INTO `{{project_id}}.{{dataset_id}}.observation`
            (observation_id, person_id, observation_concept_id, 
             observation_date, observation_datetime, observation_type_concept_id)
            VALUES
            (11, 1, 0, '2000-01-01', '2000-01-01 01:00:00 UTC', 0),
            (12, 2, 0, '1900-01-01', '1900-01-01 01:00:00 UTC', 0),
            (13, 3, 0, '2022-12-31', '2022-12-31 01:00:00 UTC', 0),
            (14, 4, 0, '1901-01-01', '1900-12-31 23:59:59 UTC', 0),
            (15, 5, 0, '2022-01-01', '2022-01-02 00:00:01 UTC', 0)
        """).render(project_id=self.project_id, dataset_id=self.dataset_id)

        INSERT_VISIT_DETAIL_QUERY = JINJA_ENV.from_string("""
            INSERT INTO `{{project_id}}.{{dataset_id}}.visit_detail`
            (visit_detail_id, person_id, visit_detail_concept_id, 
             visit_detail_start_date, visit_detail_start_datetime, 
             visit_detail_end_date, visit_detail_end_datetime,
             visit_detail_type_concept_id, visit_occurrence_id)
            VALUES
            (21, 1, 0, '2000-01-01', '2000-01-01 01:00:00 UTC', '2000-01-02', '2000-01-02 01:00:00 UTC', 0, 0),
            (22, 2, 0, '1980-12-31', '1980-12-31 23:59:59 UTC', '1981-01-01', '1981-01-01 00:00:00 UTC', 0, 0),
            (23, 3, 0, '2022-01-01', '2022-01-01 23:59:59 UTC', '2022-01-02', '2022-01-02 00:00:00 UTC', 0, 0),
            (24, 4, 0, '1981-01-01', '1980-12-31 23:59:59 UTC', '1981-01-01', '1981-01-01 00:00:00 UTC', 0, 0),
            (25, 5, 0, '2022-01-01', '2022-01-01 23:59:59 UTC', '2022-01-01', '2022-01-02 00:00:00 UTC', 0, 0)
        """).render(project_id=self.project_id, dataset_id=self.dataset_id)

        INSERT_OBSERVATION_PERIOD_QUERY = JINJA_ENV.from_string("""
            INSERT INTO `{{project_id}}.{{dataset_id}}.observation_period`
            (observation_period_id, person_id, 
             observation_period_start_date, observation_period_end_date, period_type_concept_id)
            VALUES
            (31, 1, '2000-01-01', '2000-01-01', 0),
            (32, 2, '1980-12-31', '1981-01-01', 0),
            (33, 3, '2022-01-01', '2022-01-02', 0)
        """).render(project_id=self.project_id, dataset_id=self.dataset_id)

        queries = [
            INSERT_OBSERVATION_QUERY,
            INSERT_VISIT_DETAIL_QUERY,
            INSERT_OBSERVATION_PERIOD_QUERY,
        ]

        self.load_test_data(queries)

        tables_and_counts = [
            {
                'name':
                    OBSERVATION,
                'fq_table_name':
                    f'{self.project_id}.{self.dataset_id}.{OBSERVATION}',
                'fq_sandbox_table_name':
                    f'{self.project_id}.{self.sandbox_id}.{self.rule_instance.sandbox_table_for(OBSERVATION)}',
                'fields': [
                    'observation_id', 'observation_date', 'observation_datetime'
                ],
                'loaded_ids': [11, 12, 13, 14, 15],
                'sandboxed_ids': [12, 13, 14, 15],
                'cleaned_values': [
                    (11, parser.parse('2000-01-01').date(),
                     pytz.utc.localize(parser.parse('2000-01-01 01:00:00'))),
                    (14, parser.parse('1901-01-01').date(), None),
                    (15, parser.parse('2022-01-01').date(), None),
                ]
            },
            {
                'name':
                    VISIT_DETAIL,
                'fq_table_name':
                    f'{self.project_id}.{self.dataset_id}.{VISIT_DETAIL}',
                'fq_sandbox_table_name':
                    f'{self.project_id}.{self.sandbox_id}.{self.rule_instance.sandbox_table_for(VISIT_DETAIL)}',
                'fields': [
                    'visit_detail_id', 'visit_detail_start_date',
                    'visit_detail_start_datetime', 'visit_detail_end_date',
                    'visit_detail_end_datetime'
                ],
                'loaded_ids': [21, 22, 23, 24, 25],
                'sandboxed_ids': [22, 23, 24, 25],
                'cleaned_values': [
                    (21, parser.parse('2000-01-01').date(),
                     pytz.utc.localize(parser.parse('2000-01-01 01:00:00')),
                     parser.parse('2000-01-02').date(),
                     pytz.utc.localize(parser.parse('2000-01-02 01:00:00'))),
                    (24, parser.parse('1981-01-01').date(), None,
                     parser.parse('1981-01-01').date(),
                     pytz.utc.localize(parser.parse('1981-01-01 00:00:00'))),
                    (25, parser.parse('2022-01-01').date(),
                     pytz.utc.localize(parser.parse('2022-01-01 23:59:59')),
                     parser.parse('2022-01-01').date(), None),
                ]
            },
            {
                'name':
                    OBSERVATION_PERIOD,
                'fq_table_name':
                    f'{self.project_id}.{self.dataset_id}.{OBSERVATION_PERIOD}',
                'fq_sandbox_table_name':
                    f'{self.project_id}.{self.sandbox_id}.{self.rule_instance.sandbox_table_for(OBSERVATION_PERIOD)}',
                'fields': [
                    'observation_period_id', 'observation_period_start_date',
                    'observation_period_end_date'
                ],
                'loaded_ids': [31, 32, 33],
                'sandboxed_ids': [32, 33],
                'cleaned_values': [(31, parser.parse('2000-01-01').date(),
                                    parser.parse('2000-01-01').date()),]
            },
        ]

        self.default_test(tables_and_counts)
