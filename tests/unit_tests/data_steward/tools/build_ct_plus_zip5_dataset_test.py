"""
Unit test for build_ct_plus_zip5_dataset.
"""
# Python imports
import unittest
from unittest import mock

# Third party imports
from google.api_core.exceptions import NotFound

# Project imports
from common import CT_PLUS_ZIP5_TABLE, UNDER18_PARTICIPANTS_LOOKUP_TABLE
from tools import build_ct_plus_zip5_dataset as zip5

CLEAN_CHECKS = {
    'total_rows': 3,
    'duplicate_rows': 0,
    'unresolved_rows': 0,
    'non_ct_plus_rows': 0,
    'aian_rows': 0,
    'pediatric_rows': 0,
    'not_five_digit_rows': 0
}


class BuildCtPlusZip5DatasetTest(unittest.TestCase):

    def setUp(self):
        self.project_id = 'fake_project'
        self.input_dataset_id = 'CP2025q4r7_deid_clean_pre_split'
        self.capture_dataset_id = 'CP2025q4r7_deid_sandbox'
        self.rdr_sandbox_id = 'R2025q4r7_rdr_sandbox'
        self.under18_lookup_dataset_id = 'R2025q4r7_under18_sandbox'
        self.ids_dataset_id = 'pipeline_tables'
        self.ids_view_id = 'rdr_participant_research_ids_view'

    def test_pre_split_input_of_the_release_is_accepted(self):
        zip5.validate_dataset_names('2025q4r7', self.input_dataset_id)

    def test_other_inputs_are_refused(self):
        for input_dataset_id in [
                'CP2025q4r7_deid_clean', 'CP2025q4r6_deid_clean_pre_split',
                'CP2025q4r7_deid_clean_pre_rekey',
                'C2025q4r7_deid_clean_pre_split'
        ]:
            with self.subTest(input_dataset_id=input_dataset_id):
                with self.assertRaises(ValueError):
                    zip5.validate_dataset_names('2025q4r7', input_dataset_id)

    def test_checks_read_the_capture_input_and_ids_view(self):
        query = zip5.CAPTURE_CHECKS.render(
            project=self.project_id,
            input_dataset=self.input_dataset_id,
            capture_dataset=self.capture_dataset_id,
            capture_table=CT_PLUS_ZIP5_TABLE,
            rdr_sandbox=self.rdr_sandbox_id,
            aian_list='aian_list',
            under18_dataset=self.under18_lookup_dataset_id,
            under18_table=UNDER18_PARTICIPANTS_LOOKUP_TABLE,
            ids_dataset=self.ids_dataset_id,
            ids_view=self.ids_view_id,
            person='person')

        self.assertIn(f'{self.capture_dataset_id}.{CT_PLUS_ZIP5_TABLE}`', query)
        self.assertIn(f'{self.input_dataset_id}.person`', query)
        self.assertIn(f'{self.rdr_sandbox_id}.aian_list`', query)
        self.assertIn('v.participant_id = al.person_id', query)
        self.assertIn(
            f'{self.under18_lookup_dataset_id}.'
            f'{UNDER18_PARTICIPANTS_LOOKUP_TABLE}`', query)
        self.assertIn('v.participant_id = un.person_id', query)
        self.assertIn(r"r'^[0-9]{5}$'", query)

    def test_delivered_table_lists_its_columns(self):
        query = zip5.WRITE_DELIVERED_TABLE.render(
            project=self.project_id,
            output_dataset='CP2025q4r7_zip5',
            delivered_table=zip5.DELIVERED_TABLE,
            columns=zip5.DELIVERED_COLUMNS,
            capture_dataset=self.capture_dataset_id,
            capture_table=CT_PLUS_ZIP5_TABLE)

        self.assertIn('CREATE TABLE `fake_project.CP2025q4r7_zip5.zip5`', query)
        self.assertNotIn('CREATE OR REPLACE', query)
        self.assertNotIn('*', query)
        self.assertIn(', '.join(zip5.DELIVERED_COLUMNS), query)

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
        return zip5.assert_capture_is_publishable(
            client, self.project_id, self.input_dataset_id,
            self.capture_dataset_id, self.rdr_sandbox_id,
            self.under18_lookup_dataset_id, self.ids_dataset_id,
            self.ids_view_id)

    def test_clean_capture_is_accepted(self):
        self.assertEqual(self._assert_capture(self._client()), 3)

    def test_empty_aian_list_is_refused_before_the_checks(self):
        client = self._client(aian_count=0)

        with self.assertRaises(RuntimeError) as ctx:
            self._assert_capture(client)

        self.assertIn('aian_list is empty', str(ctx.exception))
        self.assertIn('--rdr_sandbox_id', str(ctx.exception))
        self.assertEqual(client.query.call_count, 1)

    def test_empty_under18_lookup_is_refused_before_the_checks(self):
        client = self._client(under18_count=0)

        with self.assertRaises(RuntimeError) as ctx:
            self._assert_capture(client)

        self.assertIn(f'{UNDER18_PARTICIPANTS_LOOKUP_TABLE} is empty',
                      str(ctx.exception))
        self.assertIn('--under18_lookup_dataset_id', str(ctx.exception))
        self.assertEqual(client.query.call_count, 2)

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
            ('not_five_digit_rows', 'five digits'),
        ]:
            with self.subTest(column=column):
                with self.assertRaises(RuntimeError) as ctx:
                    self._assert_capture(self._client(**{column: 1}))

                self.assertIn(phrase, str(ctx.exception))

    def test_every_failed_property_is_reported_together(self):
        with self.assertRaises(RuntimeError) as ctx:
            self._assert_capture(
                self._client(duplicate_rows=1, non_ct_plus_rows=2))

        self.assertIn('repeat a participant', str(ctx.exception))
        self.assertIn('not a CT+ research ID', str(ctx.exception))

    def test_existing_output_dataset_is_refused(self):
        client = mock.MagicMock()

        with self.assertRaises(RuntimeError):
            zip5.assert_output_dataset_is_absent(client, self.project_id,
                                                 'CP2025q4r7_zip5')

    def test_absent_output_dataset_is_accepted(self):
        client = mock.MagicMock()
        client.get_dataset.side_effect = NotFound('absent')

        zip5.assert_output_dataset_is_absent(client, self.project_id,
                                             'CP2025q4r7_zip5')

    @mock.patch.object(zip5, 'write_delivered_table', return_value=3)
    @mock.patch.object(zip5, 'log_coverage')
    @mock.patch.object(zip5, 'assert_capture_is_publishable')
    @mock.patch.object(zip5, 'assert_output_dataset_is_absent')
    @mock.patch.object(zip5, 'BigQueryClient')
    def test_main_checks_before_it_creates_anything(self, mock_client_class,
                                                    mock_absent, mock_capture,
                                                    _, mock_write):
        """The dataset is created only after both refusals have passed, and is
        named from the shared helper."""
        client = mock_client_class.return_value
        order = mock.MagicMock()
        order.attach_mock(mock_absent, 'absent')
        order.attach_mock(mock_capture, 'capture')
        order.attach_mock(client.create_dataset, 'create_dataset')
        order.attach_mock(mock_write, 'write')

        zip5.main(self.project_id, '2025q4r7', self.input_dataset_id,
                  self.capture_dataset_id, self.rdr_sandbox_id,
                  self.under18_lookup_dataset_id, self.ids_view_id)

        self.assertEqual([call[0] for call in order.mock_calls],
                         ['absent', 'capture', 'create_dataset', 'write'])
        self.assertEqual(mock_capture.call_args.args[4:],
                         (self.rdr_sandbox_id, self.under18_lookup_dataset_id,
                          'pipeline_tables', self.ids_view_id))
        client.define_dataset.assert_called_once()
        self.assertEqual(client.define_dataset.call_args.args[0],
                         'CP2025q4r7_zip5')
        self.assertEqual(client.define_dataset.call_args.args[2],
                         {'ct_plus_component': 'zip5'})
        client.create_dataset.assert_called_once_with(
            client.define_dataset.return_value, exists_ok=False)


if __name__ == '__main__':
    unittest.main()
