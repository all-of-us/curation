"""
Unit test for build_ct_plus_dob_dataset.

Original Issues: DL-2535
"""
# Python imports
import unittest
from unittest import mock

# Third party imports
from google.api_core.exceptions import NotFound

# Project imports
from common import CT_PLUS_BIRTHDATE_TABLE, UNDER18_PARTICIPANTS_LOOKUP_TABLE
from tools import build_ct_plus_dob_dataset as dob

CLEAN_CHECKS = {
    'total_rows': 3,
    'duplicate_rows': 0,
    'unresolved_rows': 0,
    'non_ct_plus_rows': 0,
    'aian_rows': 0,
    'pediatric_rows': 0,
    'missing_birth_datetime_rows': 0
}


class BuildCtPlusDobDatasetTest(unittest.TestCase):

    def setUp(self):
        self.project_id = 'fake_project'
        self.input_dataset_id = 'CP2025q4r7_deid_clean_pre_split'
        self.capture_dataset_id = 'CP2025q4r7_deid_sandbox'
        self.output_dataset_id = 'CP2025q4r7_dob'
        self.rdr_sandbox_id = 'R2025q4r7_rdr_sandbox'
        self.under18_lookup_dataset_id = 'R2025q4r7_under18_sandbox'
        self.ids_dataset_id = 'pipeline_tables'
        self.ids_view_id = 'rdr_participant_research_ids_view'

    def test_checks_read_the_capture_input_and_ids_view(self):
        query = dob.CAPTURE_CHECKS.render(
            project=self.project_id,
            input_dataset=self.input_dataset_id,
            capture_dataset=self.capture_dataset_id,
            capture_table=CT_PLUS_BIRTHDATE_TABLE,
            rdr_sandbox=self.rdr_sandbox_id,
            aian_list='aian_list',
            under18_dataset=self.under18_lookup_dataset_id,
            under18_table=UNDER18_PARTICIPANTS_LOOKUP_TABLE,
            ids_dataset=self.ids_dataset_id,
            ids_view=self.ids_view_id,
            person='person')

        self.assertIn(f'{self.capture_dataset_id}.{CT_PLUS_BIRTHDATE_TABLE}`',
                      query)
        self.assertIn(f'{self.input_dataset_id}.person`', query)
        self.assertIn(f'{self.rdr_sandbox_id}.aian_list`', query)
        self.assertIn('v.participant_id = al.person_id', query)
        self.assertIn(
            f'{self.under18_lookup_dataset_id}.'
            f'{UNDER18_PARTICIPANTS_LOOKUP_TABLE}`', query)
        self.assertIn('v.participant_id = un.person_id', query)
        self.assertIn('b.birth_datetime IS NULL', query)

    def test_delivered_table_lists_its_columns(self):
        query = dob.WRITE_DELIVERED_TABLE.render(
            project=self.project_id,
            output_dataset=self.output_dataset_id,
            delivered_table=dob.DELIVERED_TABLE,
            columns=dob.DELIVERED_COLUMNS,
            capture_dataset=self.capture_dataset_id,
            capture_table=CT_PLUS_BIRTHDATE_TABLE)

        self.assertIn('CREATE TABLE `fake_project.CP2025q4r7_dob.dob`', query)
        self.assertNotIn('CREATE OR REPLACE', query)
        self.assertNotIn('*', query)
        self.assertNotIn('year_of_birth', query)
        self.assertIn(', '.join(dob.DELIVERED_COLUMNS), query)

    def _client(self, aian_count=5, under18_count=5, **overrides):
        checks = dict(CLEAN_CHECKS, **overrides)
        client = mock.MagicMock()
        client.query.return_value.result.side_effect = [[{
            'lookup_count': aian_count
        }], [{
            'lookup_count': under18_count
        }], [checks]]
        return client

    def _assert_capture(self, client):
        return dob.assert_capture_is_publishable(
            client, self.project_id, self.input_dataset_id,
            self.capture_dataset_id, self.rdr_sandbox_id,
            self.under18_lookup_dataset_id, self.ids_dataset_id,
            self.ids_view_id)

    def test_clean_capture_is_accepted(self):
        self.assertEqual(self._assert_capture(self._client()), 3)

    def test_empty_lookups_are_refused_before_the_checks(self):
        for kwargs, flag, calls in [
            ({
                'aian_count': 0
            }, '--rdr_sandbox_id', 1),
            ({
                'under18_count': 0
            }, '--under18_lookup_dataset_id', 2),
        ]:
            with self.subTest(flag=flag):
                client = self._client(**kwargs)

                with self.assertRaises(RuntimeError) as ctx:
                    self._assert_capture(client)

                self.assertIn('is empty', str(ctx.exception))
                self.assertIn(flag, str(ctx.exception))
                self.assertEqual(client.query.call_count, calls)

    def test_empty_capture_is_refused(self):
        with self.assertRaises(RuntimeError) as ctx:
            self._assert_capture(self._client(total_rows=0))

        self.assertIn('is empty', str(ctx.exception))

    def test_each_failed_property_is_refused(self):
        for column, phrase in [
            ('duplicate_rows', 'repeat a participant'),
            ('unresolved_rows', 'no person row'),
            ('non_ct_plus_rows', 'not a CT+ research ID'),
            ('aian_rows', 'AIAN'),
            ('pediatric_rows', 'pediatric'),
            ('missing_birth_datetime_rows', 'no birth_datetime'),
        ]:
            with self.subTest(column=column):
                with self.assertRaises(RuntimeError) as ctx:
                    self._assert_capture(self._client(**{column: 1}))

                self.assertIn(phrase, str(ctx.exception))

    def test_every_failed_property_is_reported_together(self):
        with self.assertRaises(RuntimeError) as ctx:
            self._assert_capture(
                self._client(pediatric_rows=1, non_ct_plus_rows=2))

        self.assertIn('pediatric', str(ctx.exception))
        self.assertIn('not a CT+ research ID', str(ctx.exception))

    def _output_client(self, tables, labels=None):
        client = mock.MagicMock()
        client.get_dataset.return_value.labels = (
            labels if labels is not None else {
                'ct_plus_component': 'dob_indicators'
            })
        client.list_tables.return_value = [
            mock.MagicMock(table_id=table) for table in tables
        ]
        return client

    def test_split_output_dataset_is_accepted(self):
        dob.assert_output_dataset_is_ready(
            self._output_client(['observation', 'observation_ext']),
            self.project_id, self.output_dataset_id)

    def test_unready_output_dataset_is_refused(self):
        missing = mock.MagicMock()
        missing.get_dataset.side_effect = NotFound('absent')

        for name, client, phrase in [
            ('missing', missing, 'does not exist'),
            ('wrong label',
             self._output_client(['observation'],
                                 {'ct_plus_component': 'zip5'}), 'labelled'),
            ('empty', self._output_client([]), 'holds no tables'),
            ('already written', self._output_client(['observation',
                                                     'dob']), 'already exists'),
        ]:
            with self.subTest(name=name):
                with self.assertRaises(RuntimeError) as ctx:
                    dob.assert_output_dataset_is_ready(client, self.project_id,
                                                       self.output_dataset_id)

                self.assertIn(phrase, str(ctx.exception))

    @mock.patch.object(dob, 'write_delivered_table', return_value=3)
    @mock.patch.object(dob, 'log_coverage')
    @mock.patch.object(dob, 'assert_capture_is_publishable')
    @mock.patch.object(dob, 'assert_output_dataset_is_ready')
    @mock.patch.object(dob, 'BigQueryClient')
    def test_main_checks_before_it_writes_and_creates_no_dataset(
            self, mock_client_class, mock_ready, mock_capture, _, mock_write):
        client = mock_client_class.return_value
        order = mock.MagicMock()
        order.attach_mock(mock_ready, 'ready')
        order.attach_mock(mock_capture, 'capture')
        order.attach_mock(mock_write, 'write')

        dob.main(self.project_id, '2025q4r7', self.input_dataset_id,
                 self.capture_dataset_id, self.rdr_sandbox_id,
                 self.under18_lookup_dataset_id, self.ids_view_id)

        self.assertEqual([call[0] for call in order.mock_calls],
                         ['ready', 'capture', 'write'])
        self.assertEqual(mock_ready.call_args.args[1:],
                         (self.project_id, self.output_dataset_id))
        self.assertEqual(mock_capture.call_args.args[4:],
                         (self.rdr_sandbox_id, self.under18_lookup_dataset_id,
                          'pipeline_tables', self.ids_view_id))
        client.create_dataset.assert_not_called()

    def test_other_inputs_are_refused(self):
        with self.assertRaises(ValueError):
            dob.main(self.project_id, '2025q4r7', 'CP2025q4r7_deid_clean',
                     self.capture_dataset_id, self.rdr_sandbox_id,
                     self.under18_lookup_dataset_id, self.ids_view_id)


if __name__ == '__main__':
    unittest.main()
