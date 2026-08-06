"""Unit tests for the ETM CT+ ID regeneration query builders."""
import unittest
from unittest import mock

from common import PERSON
from tools import regenerate_ct_plus_ids as ct
from tools import regenerate_etm_ct_plus_ids as etm


class RegenerateEtmCtPlusIds(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.project_id = 'fake_project'
        self.input_dataset_id = 'fake_controlled_tier'
        self.output_dataset_id = 'fake_controlled_tier_plus'
        self.mapping_dataset_id = 'fake_mapping'
        self.pipeline_dataset_id = 'fake_pipeline'
        self.ids_view = 'fake_ids_view'

    def _mapping_query(self, table_name, append=False):
        return etm.mapping_query(table_name, self.input_dataset_id,
                                 self.project_id, ct.DEFAULT_MAPPING_NAMESPACE,
                                 self.mapping_dataset_id, append)

    @staticmethod
    def _schema(*names):
        # name cannot be passed to the MagicMock constructor: it sets the mock's own
        # name rather than an attribute, and field.name would return a repr instead.
        fields = []
        for field_name in names:
            field = mock.MagicMock()
            field.name = field_name
            fields.append(field)
        return fields

    def _table_query(self, table_name, schema=None):
        client = mock.MagicMock()
        client.get_table.return_value.schema = schema or self._schema(
            'sitting_id', 'person_id', 'src_id')
        return etm.table_query(client, table_name, self.input_dataset_id,
                               self.project_id, self.pipeline_dataset_id,
                               self.ids_view, ct.DEFAULT_MAPPING_NAMESPACE,
                               self.mapping_dataset_id)

    def test_the_four_etm_tables_are_covered(self):
        self.assertEqual(sorted(etm.ETM_TABLES),
                         ['delaydiscounting', 'emorecog', 'flanker', 'gradcpt'])

    def test_sitting_ids_are_shuffled_not_order_preserving(self):
        """An order preserving draw is undone by sorting, which is the whole exposure."""
        for table in etm.ETM_TABLES:
            with self.subTest(table=table):
                q = self._mapping_query(table)
                self.assertIn('ORDER BY shuffle_key', q)
                self.assertNotIn('ORDER BY sitting_id', q)

    def test_rand_is_drawn_outside_the_distinct(self):
        """Inside it, every duplicate sitting_id becomes a distinct row and survives."""
        q = self._mapping_query('gradcpt')
        rand_at = q.index('RAND() AS shuffle_key')
        distinct_at = q.index('SELECT DISTINCT sitting_id')
        self.assertLess(rand_at, distinct_at)

    def test_a_first_run_draws_from_zero(self):
        q = self._mapping_query('flanker', append=False)
        self.assertIn('0 + ROW_NUMBER()', q)
        self.assertNotIn('NOT EXISTS', q)

    def test_a_later_run_preserves_existing_ids(self):
        """Persistence: previously published rows keep their CT+ sitting_id."""
        q = self._mapping_query('flanker', append=True)
        self.assertIn('NOT EXISTS', q)
        self.assertIn('COALESCE(MAX(sitting_id), 0)', q)
        self.assertIn('mt.src_sitting_id = t.src_sitting_id', q)

    def test_the_load_takes_sitting_id_from_the_mapping(self):
        q = self._table_query('emorecog')
        self.assertIn('m.sitting_id', q)
        self.assertIn('t.sitting_id = m.src_sitting_id', q)

    def test_the_load_takes_person_id_from_the_ct_plus_mapping(self):
        """Matching an RT stamp here would hand CT+ the RT person_ids with no error."""
        q = self._table_query('emorecog')
        expected = ct.person_mapping_src_table_id(self.pipeline_dataset_id,
                                                  self.ids_view)
        self.assertIn('mp.person_id', q)
        self.assertIn(f"mp.src_table_id = '{expected}'", q)

    def test_other_columns_come_from_the_source_row(self):
        q = self._table_query('gradcpt',
                              schema=self._schema('sitting_id', 'person_id',
                                                  'src_id', 'score'))
        self.assertIn('t.src_id', q)
        self.assertIn('t.score', q)

    def test_a_mistyped_output_dataset_stops_the_run(self):
        """This script deletes the ETM tables wherever --output_dataset_id points."""
        client = mock.MagicMock()
        client.list_tables.return_value = [
            mock.MagicMock(**{'table_id': t}) for t in ('person', 'observation')
        ]

        with self.assertRaises(RuntimeError) as ctx:
            etm.assert_output_is_a_ct_plus_run(client, self.output_dataset_id)

        self.assertIn('does not hold a finished CT+ run', str(ctx.exception))

    def test_a_finished_ct_plus_output_is_accepted(self):
        client = mock.MagicMock()
        client.list_tables.return_value = [
            mock.MagicMock(**{'table_id': t}) for t in [PERSON] + etm.ETM_TABLES
        ]

        etm.assert_output_is_a_ct_plus_run(client, self.output_dataset_id)

    def test_a_missing_output_dataset_stops_the_run(self):
        client = mock.MagicMock()
        client.list_tables.side_effect = Exception('no such dataset')

        with self.assertRaises(RuntimeError) as ctx:
            etm.assert_output_is_a_ct_plus_run(client, self.output_dataset_id)

        self.assertIn('does not exist', str(ctx.exception))

    @staticmethod
    def _counts_client(rows_in, rows_out):
        client = mock.MagicMock()
        client.query.return_value.result.return_value = [{
            'rows_in': rows_in,
            'rows_out': rows_out
        }]
        return client

    def test_matching_row_counts_are_accepted(self):
        etm.assert_no_rows_dropped(self._counts_client(74, 74), self.project_id,
                                   'gradcpt', self.input_dataset_id,
                                   self.output_dataset_id)

    def test_rows_lost_to_an_unmatched_join_stop_the_run(self):
        """Both joins are inner, so an unmapped row leaves the released data silently."""
        client = self._counts_client(74, 70)

        with self.assertRaises(RuntimeError) as ctx:
            etm.assert_no_rows_dropped(client, self.project_id, 'gradcpt',
                                       self.input_dataset_id,
                                       self.output_dataset_id)

        self.assertIn('lost 4 of 74 rows', str(ctx.exception))

    @mock.patch('tools.regenerate_etm_ct_plus_ids.run_query_to_table')
    @mock.patch('tools.regenerate_etm_ct_plus_ids.BigQueryClient')
    def test_an_existing_mapping_is_appended_to(self, mock_client, mock_write):
        mock_client.return_value.table_exists.return_value = True

        etm.mapping('flanker', self.input_dataset_id, self.project_id,
                    ct.DEFAULT_MAPPING_NAMESPACE, self.mapping_dataset_id)

        self.assertEqual(mock_write.call_args.kwargs['write_disposition'],
                         'WRITE_APPEND')

    @mock.patch('tools.regenerate_etm_ct_plus_ids.run_query_to_table')
    @mock.patch('tools.regenerate_etm_ct_plus_ids.BigQueryClient')
    def test_a_first_mapping_is_created(self, mock_client, mock_write):
        mock_client.return_value.table_exists.return_value = False

        etm.mapping('flanker', self.input_dataset_id, self.project_id,
                    ct.DEFAULT_MAPPING_NAMESPACE, self.mapping_dataset_id)

        self.assertEqual(mock_write.call_args.kwargs['write_disposition'],
                         'WRITE_TRUNCATE')

    def test_writes_go_through_the_checked_helper(self):
        """bq_utils.query ignores --project_id and does not inspect the job result."""
        import inspect
        # Import lines only: the module docstring names bq_utils when explaining why
        # this script does not use it, so searching the whole source matches prose.
        imports = [
            line for line in inspect.getsource(etm).splitlines()
            if line.startswith(('import ', 'from '))
        ]
        self.assertFalse([line for line in imports if 'bq_utils' in line])
        # The binding, not the import line: run_query_to_table arrives on a
        # continuation line of a multi-line import and would not be matched above.
        self.assertIs(etm.run_query_to_table, ct.run_query_to_table)
