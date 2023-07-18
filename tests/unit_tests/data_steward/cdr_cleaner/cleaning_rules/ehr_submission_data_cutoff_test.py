"""
Unit test for the cleaning_rules.ehr_submission_data_cutoff.py module

Original Issue: DC-1445

Intent of this unit test is to ensure that the data cutoff for the PPI data in all CDM tables is enforced by sandboxing
 and removing any records that persist after the data cutoff date.
"""

# Python imports
import unittest
from mock import patch
from datetime import datetime

# Project imports
import common
from constants import bq_utils as bq_consts
from constants.cdr_cleaner import clean_cdr as cdr_consts
import cdr_cleaner.cleaning_rules.ehr_submission_data_cutoff as data_cutoff


class EhrSubmissionDataCutoffTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.project_id = 'foo_project'
        self.dataset_id = 'bar_dataset'
        self.sandbox_id = 'baz_sandbox'
        self.client = None

        self.date_fields = ['visit_start_date', 'visit_end_date']
        self.updated_date_fields = [
            f'COALESCE({field}, DATE("1900-01-01"))'
            for field in self.date_fields
        ]
        self.datetime_fields = ['visit_start_datetime', 'visit_end_datetime']
        self.updated_datetime_fields = [
            f'COALESCE({field}, TIMESTAMP("1900-01-01"))'
            for field in self.datetime_fields
        ]
        self.cutoff_date = str(datetime.now().date())

        get_affected_tables_patch = patch('cdr_cleaner.cleaning_rules.ehr_submission_data_cutoff.get_affected_tables')
        mock_get_affected_tables = get_affected_tables_patch.start()
        mock_get_affected_tables.return_value = [common.VISIT_OCCURRENCE]
        self.addCleanup(mock_get_affected_tables.stop)

        self.rule_instance = data_cutoff.EhrSubmissionDataCutoff(
            self.project_id, self.dataset_id, self.sandbox_id)

        self.assertEqual(self.rule_instance.project_id, self.project_id)
        self.assertEqual(self.rule_instance.dataset_id, self.dataset_id)
        self.assertEqual(self.rule_instance.sandbox_dataset_id, self.sandbox_id)

    def test_setup_rule(self):
        # Test
        self.rule_instance.setup_rule(self.client)

        # No errors are raised, nothing will happen

    def test_get_query_specs(self):
        table = common.VISIT_OCCURRENCE

        sandbox_query = {
            cdr_consts.QUERY:
                data_cutoff.SANDBOX_QUERY.render(
                    project_id=self.project_id,
                    sandbox_id=self.sandbox_id,
                    intermediary_table=self.rule_instance.sandbox_table_for(
                        table),
                    dataset_id=self.dataset_id,
                    cdm_table=table,
                    date_fields=(", ".join(self.updated_date_fields)),
                    datetime_fields=(", ".join(self.updated_datetime_fields)),
                    cutoff_date=self.cutoff_date),
        }

        date_cutoff_query = {
            cdr_consts.QUERY:
                data_cutoff.DATE_CUTOFF_QUERY.render(
                    project_id=self.project_id,
                    dataset_id=self.dataset_id,
                    cdm_table=table,
                    sandbox_id=self.sandbox_id,
                    intermediary_table=self.rule_instance.sandbox_table_for(
                        table)),
            cdr_consts.DESTINATION_TABLE:
                table,
            cdr_consts.DESTINATION_DATASET:
                self.dataset_id,
            cdr_consts.DISPOSITION:
                bq_consts.WRITE_TRUNCATE
        }

        expected_list = [sandbox_query] + [date_cutoff_query]

        # Test
        results_list = self.rule_instance.get_query_specs()

        # Post conditions
        self.assertEqual(results_list, expected_list)
