"""
Unit test for valid_death_dates.py

Removes data containing death_dates which fall outside of the AoU program dates or after the current date

Original Issues: DC-431, DC-822

The intent is to ensure there are no death dates that occur before the start of the AoU program or after the current
date. A death date is considered "valid" if it is after the program start date and before the current date. Allowing for more flexibility,
we chose Jan 1, 2017 as the program start date.
"""

# Python imports
import unittest

# Project imports
from constants.bq_utils import WRITE_TRUNCATE
from constants.cdr_cleaner import clean_cdr as cdr_consts
from cdr_cleaner.cleaning_rules.valid_death_dates import (
    ValidDeathDates, KEEP_VALID_DEATH_DATE_ROWS,
    SANDBOX_INVALID_DEATH_DATE_ROWS, program_start_date, current_date)


class ValidDeathDatesTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.project_id = 'foo_project'
        self.dataset_id = 'foo_dataset'
        self.sandbox_dataset_id = 'foo_sandbox'
        self.client = None

        self.rule_instance = ValidDeathDates(self.project_id, self.dataset_id,
                                             self.sandbox_dataset_id)

        self.assertEqual(self.rule_instance.project_id, self.project_id)
        self.assertEqual(self.rule_instance.dataset_id, self.dataset_id)
        self.assertEqual(self.rule_instance.sandbox_dataset_id,
                         self.sandbox_dataset_id)

    def test_setup_rule(self):
        # Test
        self.rule_instance.setup_rule(self.client)

    def test_get_query_spec(self):
        # Pre conditions
        self.assertEqual(self.rule_instance.affected_datasets,
                         [cdr_consts.COMBINED])

        # Test
        result_list = self.rule_instance.get_query_specs()

        # Post conditions
        expected_list = [{
            cdr_consts.QUERY:
                SANDBOX_INVALID_DEATH_DATE_ROWS.render(
                    project_id=self.project_id,
                    sandbox_id=self.sandbox_dataset_id,
                    sandbox_table=self.rule_instance.sandbox_table_for(table),
                    dataset_id=self.dataset_id,
                    table=table,
                    program_start_date=program_start_date,
                    current_date=current_date)
        } for table in self.rule_instance.affected_tables] + [{
            cdr_consts.QUERY:
                KEEP_VALID_DEATH_DATE_ROWS.render(
                    project_id=self.project_id,
                    dataset_id=self.dataset_id,
                    table=table,
                    sandbox_id=self.sandbox_dataset_id,
                    sandbox_table=self.rule_instance.sandbox_table_for(table)),
            cdr_consts.DESTINATION_TABLE:
                table,
            cdr_consts.DESTINATION_DATASET:
                self.dataset_id,
            cdr_consts.DISPOSITION:
                WRITE_TRUNCATE
        } for table in self.rule_instance.affected_tables]

        self.assertEqual(result_list, expected_list)
