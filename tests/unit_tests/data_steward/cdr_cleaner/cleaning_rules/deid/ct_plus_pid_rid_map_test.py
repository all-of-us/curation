"""
Unit test for ct_plus_pid_rid_map.py

Covers the SQL the rule renders and the order it runs in, which the integration test
exercises only through their effects.

Original Issue: DL-2477
"""

# Python imports
import unittest
from unittest import mock

# Project imports
from common import PIPELINE_TABLES, RDR_PARTICIPANT_RESEARCH_IDS_VIEW
from constants.cdr_cleaner import clean_cdr as cdr_consts
from cdr_cleaner.cleaning_rules.deid.ct_plus_pid_rid_map import CtPlusPIDtoRID
from cdr_cleaner.cleaning_rules.deid.rt_ct_pid_rid_map import RtCtPIDtoRID


class CtPlusPIDtoRIDTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.project_id = 'foo_project'
        self.dataset_id = 'foo_dataset'
        self.sandbox_id = 'foo_sandbox'
        self.rule_instance = CtPlusPIDtoRID(self.project_id, self.dataset_id,
                                            self.sandbox_id)

    @staticmethod
    def _client(conflicting_count=0, examples=()):
        """A client whose first query is the conflict check."""
        client = mock.MagicMock()
        client.query.return_value.result.return_value = [{
            'conflicting_count': conflicting_count,
            'examples': list(examples)
        }]
        return client

    @staticmethod
    def _queries(client):
        return [call.args[0] for call in client.query.call_args_list]

    def test_the_map_is_built_from_the_ids_view_into_the_sandbox(self):
        client = self._client()

        self.rule_instance.build_deid_map(client)

        build = self._queries(client)[-1]
        self.assertIn(
            f'CREATE OR REPLACE TABLE `{self.project_id}.{self.sandbox_id}._deid_map`',
            build)
        self.assertIn(
            f'FROM `{self.project_id}.{PIPELINE_TABLES}.'
            f'{RDR_PARTICIPANT_RESEARCH_IDS_VIEW}`', build)
        self.assertIn('participant_id AS person_id', build)
        self.assertIn('controlled_tier_id AS research_id', build)

    def test_a_null_id_is_excluded_and_a_null_shift_is_not(self):
        """The pediatric cohort arrives with a NULL shift; filtering it deletes them."""
        client = self._client()

        self.rule_instance.build_deid_map(client)

        for query in self._queries(client):
            with self.subTest(query=query[:40]):
                self.assertIn('participant_id IS NOT NULL', query)
                self.assertIn('controlled_tier_id IS NOT NULL', query)
                self.assertNotIn('registered_tier_date_shift IS NOT NULL',
                                 query)

    def test_the_map_holds_one_row_per_participant(self):
        """A NULL shift beside a valued one must not put the participant in twice."""
        client = self._client()

        self.rule_instance.build_deid_map(client)

        build = self._queries(client)[-1]
        self.assertIn('GROUP BY participant_id, controlled_tier_id', build)
        self.assertIn('MAX(registered_tier_date_shift) AS shift', build)

    def test_a_null_shift_is_not_counted_as_a_disagreement(self):
        """COUNT(DISTINCT ...) ignores NULLs, which is the point of counting per column."""
        client = self._client()

        self.rule_instance.build_deid_map(client)

        check = self._queries(client)[0]
        self.assertIn('COUNT(DISTINCT controlled_tier_id) > 1', check)
        self.assertIn('COUNT(DISTINCT registered_tier_date_shift) > 1', check)

    def test_a_disagreement_stops_before_the_map_is_written(self):
        client = self._client(conflicting_count=2, examples=[22, 11])

        with self.assertRaises(RuntimeError) as ctx:
            self.rule_instance.build_deid_map(client)

        self.assertEqual(len(self._queries(client)), 1)
        self.assertIn('[11, 22]', str(ctx.exception))

    def test_the_map_is_built_before_the_inherited_setup_runs(self):
        """The inherited setup copies primary_pid_rid_mapping only when no map exists."""
        order = mock.Mock()

        with mock.patch.object(CtPlusPIDtoRID, 'build_deid_map',
                               order.build), \
                mock.patch.object(RtCtPIDtoRID, 'setup_rule', order.inherited):
            self.rule_instance.setup_rule(mock.MagicMock())

        self.assertEqual([call[0] for call in order.mock_calls],
                         ['build', 'inherited'])

    def test_queries_run_sandbox_then_update_then_delete(self):
        """Deleting before updating would remove every participant the map covers."""
        specs = self.rule_instance.get_query_specs()
        queries = [spec[cdr_consts.QUERY] for spec in specs]
        table_count = len(self.rule_instance.pid_tables)

        self.assertEqual(len(queries), 3 * table_count)
        for i, query in enumerate(queries):
            with self.subTest(position=i):
                if i < table_count:
                    self.assertIn('CREATE OR REPLACE TABLE', query)
                elif i < 2 * table_count:
                    self.assertIn('UPDATE', query)
                else:
                    self.assertIn('DELETE', query)
                if i < 2 * table_count:
                    self.assertIn(f'{self.sandbox_id}._deid_map', query)


if __name__ == '__main__':
    unittest.main()
