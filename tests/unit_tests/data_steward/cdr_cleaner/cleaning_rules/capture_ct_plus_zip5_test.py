"""
Unit test for capture_ct_plus_zip5 module

Original Issues: DL2437
"""

# Python imports
import unittest
from types import SimpleNamespace
from unittest import mock

# Project imports
from common import AIAN_LIST, CT_PLUS_ZIP5, UNDER18_PARTICIPANTS_LOOKUP_TABLE
from constants.cdr_cleaner import clean_cdr as cdr_consts
from cdr_cleaner.cleaning_rules.capture_ct_plus_zip5 import (
    CaptureCtPlusZip5, ConvertCtPlusZip5Ids, PruneCtPlusZip5)


class CaptureCtPlusZip5Test(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.project_id = 'foo_project'
        self.dataset_id = 'foo_dataset'
        self.sandbox_id = 'foo_sandbox'
        self.rdr_sandbox_id = 'foo_rdr_sandbox'
        self.under18_lookup_dataset_id = 'foo_under18_sandbox'
        self.ct_plus_ids_view = 'foo_ids_view'

        self.capture = CaptureCtPlusZip5(self.project_id, self.dataset_id,
                                         self.sandbox_id, self.rdr_sandbox_id,
                                         self.under18_lookup_dataset_id)
        self.prune = PruneCtPlusZip5(self.project_id, self.dataset_id,
                                     self.sandbox_id)
        self.convert = ConvertCtPlusZip5Ids(self.project_id, self.dataset_id,
                                            self.sandbox_id,
                                            self.ct_plus_ids_view)

    def test_rules_target_only_ct_plus_deid(self):
        for rule in [self.capture, self.prune, self.convert]:
            self.assertEqual(rule.affected_datasets,
                             [cdr_consts.CONTROLLED_TIER_PLUS_DEID])
            self.assertEqual(rule.get_sandbox_tablenames(), [CT_PLUS_ZIP5])

    def test_capture_excludes_aian_and_pediatric_participants(self):
        query = self.capture.get_query_specs()[0][cdr_consts.QUERY]

        self.assertIn(f'`{self.project_id}.{self.sandbox_id}.{CT_PLUS_ZIP5}`',
                      query)
        self.assertIn(f'`{self.project_id}.{self.rdr_sandbox_id}.{AIAN_LIST}`',
                      query)
        self.assertIn(
            f'`{self.project_id}.{self.under18_lookup_dataset_id}.'
            f'{UNDER18_PARTICIPANTS_LOOKUP_TABLE}`', query)
        self.assertEqual(query.count('NOT EXISTS'), 2)

    def test_prune_compares_against_person(self):
        query = self.prune.get_query_specs()[0][cdr_consts.QUERY]

        self.assertIn(f'`{self.project_id}.{self.dataset_id}.person`', query)
        self.assertIn('NOT EXISTS', query)

    def _client_returning(self, total_rows, unresolved_rows):
        client = mock.MagicMock()
        client.query.return_value.result.return_value = [
            SimpleNamespace(total_rows=total_rows,
                            unresolved_rows=unresolved_rows)
        ]
        return client

    def test_convert_stops_on_unresolved_rows(self):
        client = self._client_returning(total_rows=5, unresolved_rows=2)

        with self.assertRaises(RuntimeError) as raised:
            self.convert.setup_rule(client)
        self.assertIn('2 of 5 rows', str(raised.exception))

    def test_convert_logs_row_count_when_all_resolve(self):
        client = self._client_returning(total_rows=5, unresolved_rows=0)

        with self.assertLogs(
                'cdr_cleaner.cleaning_rules.ct_plus_side_tables') as logs:
            self.convert.setup_rule(client)
        self.assertIn('Converting 5 rows', logs.output[0])

    def test_convert_reads_a_distinct_view(self):
        query = self.convert.get_query_specs()[0][cdr_consts.QUERY]

        self.assertIn('SELECT DISTINCT', query)
        self.assertIn(
            f'`{self.project_id}.pipeline_tables.'
            f'{self.ct_plus_ids_view}`', query)
        self.assertIn('SET z.person_id = v.controlled_tier_plus_id', query)
