"""
Unit test for the truncate_fitbit_data module

Original Issue: DC-1046

Ensures there is no data after the cutoff date for participants in
the Fitbit tables by testing the Device table by sandboxing the
applicable records and then dropping them.
"""

# Python imports
import unittest
from mock import patch

# Project imports
import constants.cdr_cleaner.clean_cdr as cdr_consts
import cdr_cleaner.cleaning_rules.truncate_fitbit_data as truncate_fitbit
from constants.bq_utils import WRITE_TRUNCATE
from common import DEVICE


class TruncateFitbitDataTest(unittest.TestCase):

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
        self.table_namer = None
        self.truncation_date = '2019-11-26'

        self.date_fields = ['device_date', 'last_sync_time']

        self.updated_date_fields = []
        for field in self.date_fields:
            if field == 'device_date':
                self.updated_date_fields.append(
                    f'COALESCE({field}, DATE("1900-01-01"))')
            else:
                self.updated_date_fields.append(
                    f'COALESCE(DATE({field}), DATE("1900-01-01"))')

        self.rule_instance = truncate_fitbit.TruncateFitbitData(
            self.project_id, self.dataset_id, self.sandbox_id, self.table_namer,
            self.truncation_date)

        self.assertEqual(self.rule_instance.project_id, self.project_id)
        self.assertEqual(self.rule_instance.dataset_id, self.dataset_id)
        self.assertEqual(self.rule_instance.sandbox_dataset_id, self.sandbox_id)

    @patch.object(truncate_fitbit.TruncateFitbitData, 'get_affected_tables')
    def test_get_query_specs(self, mock_affected_tables):
        # Pre conditions
        mock_affected_tables.return_value = [DEVICE]

        table = DEVICE

        sandbox_query = {
            cdr_consts.QUERY:
                truncate_fitbit.SANDBOX_QUERY.render(
                    project_id=self.project_id,
                    sandbox_id=self.sandbox_id,
                    intermediary_table=self.rule_instance.sandbox_table_for(
                        table),
                    dataset_id=self.dataset_id,
                    fitbit_table=table,
                    date_fields=(", ".join(self.updated_date_fields)),
                    truncation_date=self.truncation_date),
        }

        truncate_query = {
            cdr_consts.QUERY:
                truncate_fitbit.TRUNCATE_FITBIT_DATA_QUERY.render(
                    project_id=self.project_id,
                    dataset_id=self.dataset_id,
                    fitbit_table=table,
                    sandbox_id=self.sandbox_id,
                    intermediary_table=self.rule_instance.sandbox_table_for(
                        table)),
            cdr_consts.DESTINATION_TABLE:
                table,
            cdr_consts.DESTINATION_DATASET:
                self.dataset_id,
            cdr_consts.DISPOSITION:
                WRITE_TRUNCATE
        }

        expected_list = [sandbox_query] + [truncate_query]

        # Test
        results_list = self.rule_instance.get_query_specs()

        # Post conditions
        self.assertEqual(results_list, expected_list)
