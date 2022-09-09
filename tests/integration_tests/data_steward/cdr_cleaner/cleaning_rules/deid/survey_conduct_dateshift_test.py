"""
Integration Test for the survey_conduct_dateshift module.

Date shift any fields that are of type DATE, DATETIME, or TIMESTAMP.
"""
# Python imports
import os
from datetime import timedelta

# Third party imports
import pytz
from dateutil import parser

# Project imports
from app_identity import PROJECT_ID
from cdr_cleaner.cleaning_rules.deid.survey_conduct_dateshift import SurveyConductDateShiftRule
from common import DEID_MAP
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import BaseTest


class SurveyConductDateShiftTest(BaseTest.DeidRulesTestBase):

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

        mapping_dataset_id = os.environ.get('COMBINED_DATASET_ID')
        mapping_table_id = DEID_MAP
        cls.kwargs.update({
            'mapping_dataset_id': mapping_dataset_id,
            'mapping_table_id': mapping_table_id
        })

        cls.rule_instance = SurveyConductDateShiftRule(project_id, dataset_id,
                                                       sandbox_id,
                                                       mapping_dataset_id,
                                                       mapping_table_id)

        # can test the full functionality with one table
        cls.fq_table_names = [
            f"{project_id}.{dataset_id}.{cls.rule_instance.tables[0]}"
        ]

        # provide mapping table info
        cls.fq_mapping_tablename = f"{project_id}.{mapping_dataset_id}.{mapping_table_id}"

        # call super to set up the client, create datasets, and create
        # empty test tables
        super().setUpClass()

    def setUp(self):
        """
        Add data to the tables for the rule to run on.
        """

        self.client.create_tables(self.fq_table_names)

        # create a mapping table
        self.create_mapping_table()

        # load statement for the test data to shift
        query = self.jinja_env.from_string("""
        INSERT INTO `{{fq_table_name}}`
        (survey_conduct_id, person_id, survey_concept_id, survey_start_date,
        survey_start_datetime, survey_end_date, survey_end_datetime,
        assisted_concept_id, respondent_type_concept_id, timing_concept_id,
        collection_method_concept_id, survey_source_concept_id, validated_survey_concept_id)
        VALUES
          -- setting day to the 11th to make it easier to calculate the shifted date --
        (10, 801, 200, date(2016, 05, 11), timestamp(datetime(2016, 05, 11, 12, 45, 00)), date(2017, 05, 11), timestamp(datetime(2017, 05, 11, 12, 45, 00)), 1, 2, 3, 4, 5, 6),
        (11, 802, 201, date(2016, 05, 10), timestamp(datetime(2016, 05, 10, 12, 45, 00)), date(2017, 05, 11), timestamp(datetime(2017, 05, 11, 12, 45, 00)), 1, 2, 3, 4, 5, 6)
        """)

        # load statement for mapping table under test
        map_query = self.jinja_env.from_string("""
        INSERT INTO `{{map_table_name}}`
        (person_id, research_id, shift)
        -- setting the date shift to 10 days in the past for participant with research_id 801 --
        -- the research_id maps to the real person_id here --
        -- this assumes the pid/rid mapping has already occurred. --
        VALUES 
        -- a date shift of 10 days --
        (700, 801, 10),
        -- a date shift of 5 days --
        (500, 802, 5)
        """)
        load_statements = [
            query.render(fq_table_name=self.fq_table_names[0]),
            map_query.render(map_table_name=self.fq_mapping_tablename)
        ]
        self.load_test_data(load_statements)

    def test_dateshifting(self):
        """
        Use the default drop rows test function.
        """
        shifted_start_date = parser.parse('2016-05-01').date()
        shifted_start_datetime = pytz.utc.localize(
            parser.parse('2016-05-01 12:45:00'))
        four_days = timedelta(days=4)
        five_days = four_days + timedelta(days=1)
        one_year = timedelta(days=365)

        # Using the 0 position because there is only one sandbox table and
        # one affected OMOP table
        tables_and_counts = [{
            'name':
                self.fq_table_names[0].split('.')[-1],
            'fq_table_name':
                self.fq_table_names[0],
            'fq_sandbox_table_name':
                '',
            'fields': [
                'survey_conduct_id', 'person_id', 'survey_concept_id',
                'survey_start_date', 'survey_start_datetime', 'survey_end_date',
                'survey_end_datetime'
            ],
            'loaded_ids': [10, 11],
            'sandboxed_ids': [],
            'cleaned_values': [
                (10, 801, 200, shifted_start_date, shifted_start_datetime,
                 shifted_start_date + one_year,
                 shifted_start_datetime + one_year),
                (11, 802, 201, shifted_start_date + four_days,
                 shifted_start_datetime + four_days,
                 shifted_start_date + one_year + five_days,
                 shifted_start_datetime + one_year + five_days)
            ]
        }]

        self.default_test(tables_and_counts)
