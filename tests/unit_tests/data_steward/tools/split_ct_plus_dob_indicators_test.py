"""
Unit test for split_ct_plus_dob_indicators.

Original Issues: DL-2495
"""
# Python imports
import unittest
from unittest import mock

# Project imports
from common import AIAN_LIST, UNDER18_PARTICIPANTS_LOOKUP_TABLE
from tools import split_ct_plus_dob_indicators as dob

CLEAN_CHECKS = {
    'dob_rows': 2,
    'non_ct_plus_rows': 0,
    'aian_rows': 0,
    'pediatric_rows': 0
}


def _column(table, column, data_type='INT64'):
    return {'table_name': table, 'column_name': column, 'data_type': data_type}


class SplitCtPlusDobIndicatorsTest(unittest.TestCase):

    def setUp(self):
        self.project_id = 'fake_project'
        self.input_dataset_id = 'CP2025q4r7_deid_clean_pre_split'
        self.rdr_sandbox_id = 'R2025q4r7_rdr_sandbox'
        self.under18_lookup_dataset_id = 'R2025q4r7_under18_sandbox'
        self.ids_view_id = 'rdr_participant_research_ids_view'
        self.carried = {'observation': ['observation_concept_id']}
        self.uncarriable = {'death': ['cause_concept_id']}

    def test_tables_are_sorted_by_whether_they_can_be_carried(self):
        client = mock.MagicMock()
        client.query.return_value.result.return_value = [
            _column('observation', 'observation_id'),
            _column('observation', 'person_id'),
            _column('observation', 'observation_concept_id'),
            _column('observation', 'value_as_string', 'STRING'),
            _column('observation_ext', 'observation_id'),
            _column('observation_ext', 'src_id', 'STRING'),
            _column('person', 'person_id'),
            _column('person', 'gender_concept_id'),
            _column('death', 'person_id'),
            _column('death', 'cause_concept_id'),
            _column('visit_occurrence', 'visit_occurrence_id'),
            _column('visit_occurrence', 'person_id'),
            _column('visit_occurrence', 'visit_concept_id'),
            _column('note_nlp', 'note_nlp_id'),
            _column('note_nlp', 'note_nlp_concept_id'),
            _column('wear_study', 'person_id'),
        ]

        carried, uncarriable = dob.classify_tables(client, self.project_id,
                                                   self.input_dataset_id)

        self.assertEqual({'observation': ['observation_concept_id']}, carried)
        self.assertEqual(
            {
                'person': ['gender_concept_id'],
                'death': ['cause_concept_id'],
                'visit_occurrence': ['visit_concept_id']
            }, uncarriable)

    def test_row_predicate_checks_every_concept_column(self):
        predicate = dob.dob_row([1, 2], ['a_concept_id', 'b_concept_id'])

        self.assertIn('UNNEST([1, 2])', predicate)
        self.assertIn('c IN (t.a_concept_id, t.b_concept_id)', predicate)

    def test_checks_map_rows_back_to_participants(self):
        query = dob.CHECKS.render(
            project=self.project_id,
            input_dataset=self.input_dataset_id,
            table='observation',
            ids_dataset='pipeline_tables',
            ids_view=self.ids_view_id,
            rdr_sandbox=self.rdr_sandbox_id,
            aian_list=AIAN_LIST,
            under18_dataset=self.under18_lookup_dataset_id,
            under18_table=UNDER18_PARTICIPANTS_LOOKUP_TABLE,
            dob_row='TRUE')

        self.assertIn('v.controlled_tier_plus_id = t.person_id', query)
        self.assertIn('a.person_id = v.participant_id', query)
        self.assertIn('u.person_id = v.participant_id', query)
        self.assertIn(f'{self.rdr_sandbox_id}.{AIAN_LIST}`', query)
        self.assertIn(
            f'{self.under18_lookup_dataset_id}.'
            f'{UNDER18_PARTICIPANTS_LOOKUP_TABLE}`', query)

    def _client(self,
                aian_count=5,
                under18_count=5,
                carried_checks=None,
                uncarriable_checks=None):
        client = mock.MagicMock()
        # Tables are checked in name order: death, then observation
        client.query.return_value.result.side_effect = [
            [{
                'lookup_count': aian_count
            }],
            [{
                'lookup_count': under18_count
            }],
            [
                dict(CLEAN_CHECKS,
                     **dict({'dob_rows': 0}, **(uncarriable_checks or {})))
            ],
            [dict(CLEAN_CHECKS, **(carried_checks or {}))],
        ]
        return client

    def _assert(self, client):
        dob.assert_split_is_publishable(client, self.project_id,
                                        self.input_dataset_id,
                                        self.rdr_sandbox_id,
                                        self.under18_lookup_dataset_id,
                                        'pipeline_tables', self.ids_view_id,
                                        self.carried, self.uncarriable, [1, 2])

    def test_clean_input_is_accepted(self):
        client = self._client()

        self._assert(client)

        self.assertEqual(client.query.call_count, 4)

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
                    self._assert(client)

                self.assertIn('is empty', str(ctx.exception))
                self.assertIn(flag, str(ctx.exception))
                self.assertEqual(client.query.call_count, calls)

    def test_each_failed_property_is_refused(self):
        for column, phrase in [
            ('non_ct_plus_rows', 'not a CT+ research ID'),
            ('aian_rows', 'AIAN'),
            ('pediatric_rows', 'pediatric'),
        ]:
            with self.subTest(column=column):
                with self.assertRaises(RuntimeError) as ctx:
                    self._assert(self._client(carried_checks={column: 1}))

                self.assertIn(phrase, str(ctx.exception))

    def test_birth_row_in_an_uncarriable_table_is_refused(self):
        with self.assertRaises(RuntimeError) as ctx:
            self._assert(self._client(uncarriable_checks={'dob_rows': 3}))

        self.assertIn('3 birth rows sit in death', str(ctx.exception))

    def test_every_failure_is_reported_together(self):
        with self.assertRaises(RuntimeError) as ctx:
            self._assert(
                self._client(carried_checks={'aian_rows': 1},
                             uncarriable_checks={'dob_rows': 1}))

        self.assertIn('AIAN', str(ctx.exception))
        self.assertIn('sit in death', str(ctx.exception))

    @mock.patch.object(dob, 'write_table', return_value=2)
    def test_split_writes_carried_tables_then_their_ext_rows(self, mock_write):
        client = mock.MagicMock()
        client.list_tables.return_value = [
            mock.MagicMock(table_id=table)
            for table in ['observation', 'observation_ext', 'person']
        ]

        counts = dob.split(client, self.project_id, self.input_dataset_id,
                           'CP2025q4r7_dob', self.carried, [1, 2])

        written = [call.args[4] for call in mock_write.call_args_list]
        self.assertEqual(['observation', 'observation_ext'], written)
        self.assertEqual({'observation': 2, 'observation_ext': 2}, counts)
        self.assertIn('observation_id IN', mock_write.call_args_list[1].args[5])

    @mock.patch.object(dob, 'split', return_value={})
    @mock.patch.object(dob, 'assert_output_dataset_is_empty')
    @mock.patch.object(dob, 'assert_split_is_publishable')
    @mock.patch.object(dob, 'classify_tables', return_value=({}, {}))
    @mock.patch.object(dob, 'BigQueryClient')
    def test_main_checks_before_it_creates_anything(self, mock_client_class, _,
                                                    mock_publishable,
                                                    mock_empty, mock_split):
        client = mock_client_class.return_value
        order = mock.MagicMock()
        order.attach_mock(mock_publishable, 'publishable')
        order.attach_mock(mock_empty, 'empty')
        order.attach_mock(client.create_dataset, 'create_dataset')
        order.attach_mock(mock_split, 'split')

        dob.main(self.project_id, '2025q4r7', self.input_dataset_id,
                 self.rdr_sandbox_id, self.under18_lookup_dataset_id,
                 self.ids_view_id)

        self.assertEqual([call[0] for call in order.mock_calls],
                         ['publishable', 'empty', 'create_dataset', 'split'])
        self.assertEqual(mock_publishable.call_args.args[4:7],
                         (self.under18_lookup_dataset_id, 'pipeline_tables',
                          self.ids_view_id))
        self.assertEqual(client.define_dataset.call_args.args[0],
                         'CP2025q4r7_dob')
        self.assertEqual(client.define_dataset.call_args.args[2],
                         {'ct_plus_component': 'dob_indicators'})

    def test_other_inputs_are_refused(self):
        with self.assertRaises(ValueError):
            dob.main(self.project_id, '2025q4r7', 'CP2025q4r7_deid_clean',
                     self.rdr_sandbox_id, self.under18_lookup_dataset_id,
                     self.ids_view_id)


if __name__ == '__main__':
    unittest.main()
