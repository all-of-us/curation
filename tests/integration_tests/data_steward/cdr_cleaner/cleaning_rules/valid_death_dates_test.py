"""
Integration test for valid_death_dates.py module

This cleaning rule removes data containing death_dates which fall outside of the AoU program dates or
    after the current date.

Original Issue: DC-1376, DC-1206

Ensures that any records that have death_dates falling outside of the AoU program start date or after the current date
    are sandboxed and dropped.
"""

# Python Imports
import os

# Third party imports
from dateutil.parser import parse

# Project imports
from common import DEATH
from app_identity import PROJECT_ID
from cdr_cleaner.cleaning_rules.valid_death_dates import ValidDeathDates, program_start_date, current_date
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import BaseTest


class ValidDeathDatesTest(BaseTest.CleaningRulesTestBase):

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
        dataset_id = os.environ.get('COMBINED_DATASET_ID')
        cls.dataset_id = dataset_id
        sandbox_id = dataset_id + '_sandbox'
        cls.sandbox_id = sandbox_id

        cls.rule_instance = ValidDeathDates(project_id, dataset_id, sandbox_id)

        sb_table_names = cls.rule_instance.sandbox_table_for(DEATH)

        # Generates list of fully qualified sandbox table names
        cls.fq_sandbox_table_names.append(
            f'{cls.project_id}.{cls.sandbox_id}.{sb_table_names}')

        cls.fq_table_names = [f'{project_id}.{cls.dataset_id}.{DEATH}']

        # call super to set up the client, create datasets, and create
        # empty test tables
        # NOTE:  does not create empty sandbox tables.
        super().setUpClass()

    def setUp(self):
        """
        Create common information for tests.

        Creates common expected parameter types from cleaned tables and a common
        fully qualified (fq) dataset name string to load the data.
        """
        fq_dataset_name = self.fq_table_names[0].split('.')
        self.fq_dataset_name = '.'.join(fq_dataset_name[:-1])

        super().setUp()

    def test_valid_death_dates(self):
        """
        Tests that the specifications for the KEEP_VALID_DEATH_DATE_ROWS and SANDBOX_INVALID_DEATH_DATE_ROWS
        perform as designed.

        Validates pre conditions, tests execution, and post conditions based on the load
        statements and the tables_and_counts variable.
        """

        death = self.jinja_env.from_string("""
        INSERT INTO `{{fq_dataset_name}}.death` (person_id, death_date, death_type_concept_id)
        VALUES 
        -- records will be dropped because death_date is before AoU start date (Jan 1, 2017) --
        (101, DATE('2015-01-01'), 1), 
        (102, DATE('2016-01-01'), 2),
        -- records will be dropped because death_date is in the future -- 
        (103, DATE_ADD(CURRENT_DATE(), INTERVAL 1 DAY), 3), -- death_date will be one day in the future -- 
        (104, DATE_ADD(CURRENT_DATE(), INTERVAL 5 DAY), 4), -- death_date will be five days in the future --
        -- records won't be dropped because death_date is between AoU program start date and current date --
        (105, DATE('2017-01-01'), 5),
        (106, DATE('2020-01-01'), 6)""").render(fq_dataset_name=self.fq_dataset_name)

        self.load_test_data([death])

        tables_and_counts = [{
            'fq_table_name':
                '.'.join([self.fq_dataset_name, DEATH]),
            'fq_sandbox_table_name':
                self.fq_sandbox_table_names[0],
            'loaded_ids': [
                101, 102, 103, 104, 105, 106
            ],
            'sandboxed_ids': [101, 102, 103, 104],
            'fields': [
                'person_id', 'death_date', 'death_type_concept_id'
            ],
            'cleaned_values': [(105, parse('2017-01-01').date(), 5),
                               (106, parse('2020-01-01').date(), 6)
                               ]
        }]

        self.default_test(tables_and_counts)


