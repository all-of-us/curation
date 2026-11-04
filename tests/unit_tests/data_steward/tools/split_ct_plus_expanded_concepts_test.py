"""
Unit test for split_ct_plus_expanded_concepts and the concept set it reads.

Original Issues: DL-2538
"""
# Python imports
import os
import tempfile
import unittest
from unittest import mock

# Third party imports
import pandas as pd

# Project imports
from cdr_cleaner.cleaning_rules.deid import ct_plus_expanded_concepts as concepts
from tools import split_ct_plus_expanded_concepts as split

LABEL_MAP = {
    'vehicle.csv': 'motor_vehicle_accident',
    'suicide.csv': 'suicide',
    'assault.csv': 'assault_homicide',
}

CLEAN_CHECKS = {'non_ct_plus_rows': 0, 'aian_rows': 0}


def _labels(rows):
    return pd.DataFrame(rows, columns=['concept_id', 'privacy_rule'])


class ExpandedConceptsTest(unittest.TestCase):

    def test_only_rows_flagged_expanded_are_read(self):
        with tempfile.TemporaryDirectory() as directory:
            path = os.path.join(directory, 'privacy.csv')
            with open(path, 'w') as csv_file:
                csv_file.write('concept_id,privacy_rule,ct_plus_suppressed\n'
                               '1,suicide.csv,False\n'
                               '2,abortion,True\n'
                               '3,vehicle.csv,\n')

            labels = concepts.get_expanded_concept_labels([path])

        self.assertEqual([(1, 'suicide.csv')],
                         list(labels.itertuples(index=False, name=None)))

    def test_a_concept_resolves_to_every_element_it_carries(self):
        resolved = concepts.resolve_concept_elements(
            _labels([(1, 'suicide.csv'), (1, 'assault.csv'),
                     (2, 'vehicle.csv; suicide.csv')]), LABEL_MAP)

        self.assertEqual([(1, 'assault_homicide'), (1, 'suicide'),
                          (2, 'motor_vehicle_accident'), (2, 'suicide')],
                         list(resolved.itertuples(index=False, name=None)))

    def test_labels_sharing_an_element_resolve_once(self):
        resolved = concepts.resolve_concept_elements(
            _labels([(1, 'suicide.csv'), (1, 'suicide')]),
            dict(LABEL_MAP, suicide='suicide'))

        self.assertEqual([(1, 'suicide')],
                         list(resolved.itertuples(index=False, name=None)))

    def test_unmapped_labels_are_refused_by_name(self):
        with self.assertRaises(ValueError) as ctx:
            concepts.resolve_concept_elements(
                _labels([(1, 'suicide.csv'), (2, 'new_label'), (3, '')]),
                LABEL_MAP)

        self.assertIn('new_label', str(ctx.exception))
        self.assertIn("''", str(ctx.exception))

    def test_every_shipped_expanded_label_is_on_the_shipped_map(self):
        resolved = concepts.resolve_concept_elements(
            concepts.get_expanded_concept_labels(), concepts.get_label_map())

        self.assertFalse(resolved.empty)

    def test_vehicle_sources_reach_only_their_rules_tables(self):
        self.assertEqual(['csv', 'motor_vehicle', 'vehicular'],
                         concepts.get_lookup_sources('condition_occurrence'))
        self.assertEqual(['csv', 'motor_vehicle'],
                         concepts.get_lookup_sources('aou_death'))
        self.assertEqual(['csv'], concepts.get_lookup_sources('note_nlp'))

    def test_expanded_tables_follow_the_ct_rules(self):
        tables = concepts.get_expanded_tables()

        self.assertNotIn('person', tables)
        self.assertNotIn('fact_relationship', tables)
        self.assertIn('note_nlp', tables)
        self.assertIn('aou_death', tables)
        self.assertIn('observation_source_concept_id', tables['observation'])

    def test_predicate_tests_each_column_against_the_lookup(self):
        predicate = concepts.expanded_row('p', 's', 'measurement',
                                          ['a_concept_id', 'b_concept_id'], 't',
                                          'suicide')

        self.assertIn('t.a_concept_id IN (', predicate)
        self.assertIn(' OR t.b_concept_id IN (', predicate)
        self.assertIn("source IN ('csv', 'motor_vehicle')", predicate)
        self.assertIn("data_element = 'suicide'", predicate)

    def test_note_nlp_is_attributed_through_its_note(self):
        join, person_id = concepts.get_row_owner('p', 'd', 'note_nlp', 't')

        self.assertIn('`p.d.note` AS owner', join)
        self.assertEqual('owner.person_id', person_id)
        self.assertEqual(('', 't.person_id'),
                         concepts.get_row_owner('p', 'd', 'observation', 't'))

    @mock.patch.object(concepts, 'get_label_map', return_value=LABEL_MAP)
    @mock.patch.object(concepts, 'get_expanded_concept_labels')
    def test_lookup_loads_both_vehicle_rule_lists(self, mock_labels, _):
        mock_labels.return_value = _labels([(1, 'suicide.csv')])
        client = mock.MagicMock()

        elements = concepts.build_expanded_lookup(client, 'p', 'vocab', 's')

        self.assertEqual(['motor_vehicle_accident', 'suicide'], elements)
        queries = [call.args[0] for call in client.query.call_args_list]
        self.assertEqual(4, len(queries))
        self.assertIn('s.ct_plus_motor_vehicle_concepts', queries[0])
        self.assertIn('p.vocab.concept', queries[0])
        self.assertIn('s.ct_plus_vehicular_concepts', queries[1])
        self.assertIn("'motor_vehicle_accident', 'motor_vehicle'", queries[2])
        self.assertIn("'motor_vehicle_accident', 'vehicular'", queries[3])

    @mock.patch.object(concepts, 'get_label_map', return_value=LABEL_MAP)
    @mock.patch.object(concepts,
                       'get_expanded_concept_labels',
                       return_value=_labels([]))
    def test_no_expanded_concept_is_refused(self, *_):
        client = mock.MagicMock()

        with self.assertRaises(RuntimeError) as ctx:
            concepts.build_expanded_lookup(client, 'p', 'vocab', 's')

        self.assertIn('flips are missing', str(ctx.exception))
        client.load_table_from_dataframe.assert_not_called()


class SplitCtPlusExpandedConceptsTest(unittest.TestCase):

    def setUp(self):
        self.project_id = 'fake_project'
        self.input_dataset_id = 'CP2025q4r7_deid_clean_pre_split'
        self.sandbox_id = 'CP2025q4r7_deid_sandbox'
        self.rdr_sandbox_id = 'R2025q4r7_rdr_sandbox'
        self.ids_view_id = 'rdr_participant_research_ids_view'
        self.carried = {'observation': ['observation_concept_id']}
        self.uncarriable = {'visit_occurrence': ['visit_concept_id']}

    def test_tables_are_sorted_by_whether_they_can_be_carried(self):
        input_columns = {
            'observation': [
                'observation_id', 'person_id', 'observation_concept_id'
            ],
            'note_nlp': ['note_nlp_id', 'note_id', 'note_nlp_concept_id'],
            'visit_occurrence': [
                'visit_occurrence_id', 'person_id', 'visit_concept_id'
            ],
            'provider': ['provider_id', 'specialty_concept_id'],
            'measurement': ['measurement_id', 'person_id'],
        }

        carried, uncarriable = split.classify_tables(input_columns, set())

        self.assertEqual(
            {
                'observation': ['observation_concept_id'],
                'note_nlp': ['note_nlp_concept_id']
            }, carried)
        self.assertEqual(
            {
                'visit_occurrence': ['visit_concept_id'],
                'provider': ['specialty_concept_id']
            }, uncarriable)

    def test_shape_holds_only_tables_the_assembly_can_match(self):
        input_columns = {
            'person': ['person_id'],
            'person_ext': ['person_id'],
            'observation': ['observation_id', 'person_id'],
            'observation_ext': ['observation_id', 'src_id'],
            'fact_relationship': ['fact_id_1'],
            'cdm_source': ['cdm_source_name'],
            'concept': ['concept_id'],
            'wear_study': ['person_id'],
            'flanker_task_view': ['person_id'],
        }

        tables = split.get_shape_tables(input_columns, {'flanker_task_view'})

        self.assertEqual([
            'fact_relationship', 'observation', 'observation_ext', 'person',
            'person_ext'
        ], tables)

    def _client(self, aian_count=5, carried_checks=None, uncarriable_rows=0):
        client = mock.MagicMock()
        # The lookup, then uncarriable tables, then carried ones
        client.query.return_value.result.side_effect = [
            [{
                'lookup_count': aian_count
            }],
            [{
                'expanded_rows': uncarriable_rows
            }],
            [dict(CLEAN_CHECKS, **(carried_checks or {}))],
        ]
        return client

    def _assert(self, client):
        split.assert_split_is_publishable(client, self.project_id,
                                          self.input_dataset_id,
                                          self.sandbox_id, self.rdr_sandbox_id,
                                          'pipeline_tables', self.ids_view_id,
                                          self.carried, self.uncarriable)

    def test_clean_input_is_accepted(self):
        client = self._client()

        self._assert(client)

        self.assertEqual(client.query.call_count, 3)

    def test_empty_aian_list_is_refused_before_the_checks(self):
        client = self._client(aian_count=0)

        with self.assertRaises(RuntimeError) as ctx:
            self._assert(client)

        self.assertIn('is empty', str(ctx.exception))
        self.assertEqual(client.query.call_count, 1)

    def test_each_failed_property_is_refused(self):
        for column, phrase in [('non_ct_plus_rows', 'not a CT+ research ID'),
                               ('aian_rows', 'AIAN')]:
            with self.subTest(column=column):
                with self.assertRaises(RuntimeError) as ctx:
                    self._assert(self._client(carried_checks={column: 1}))

                self.assertIn(phrase, str(ctx.exception))

    def test_every_failure_is_reported_together(self):
        with self.assertRaises(RuntimeError) as ctx:
            self._assert(
                self._client(carried_checks={'aian_rows': 1},
                             uncarriable_rows=3))

        self.assertIn('AIAN', str(ctx.exception))
        self.assertIn('3 expanded rows sit in visit_occurrence',
                      str(ctx.exception))

    def test_pediatric_rows_in_uncarriable_tables_are_left_to_their_add_on(
            self):
        client = self._client()

        self._assert(client)

        query = client.query.call_args_list[1].args[0]
        self.assertIn('ct_plus_pediatric_cohort', query)
        self.assertIn('c.person_id = t.person_id', query)

    def test_a_table_with_no_person_id_has_no_cohort_to_exclude(self):
        self.uncarriable = {'provider': ['specialty_concept_id']}
        client = self._client()

        self._assert(client)

        self.assertNotIn('ct_plus_pediatric_cohort',
                         client.query.call_args_list[1].args[0])

    @mock.patch.object(split, 'write_table', return_value=2)
    def test_element_writes_rows_then_ext_then_the_empty_tables(
            self, mock_write):
        counts = split.write_element(
            mock.MagicMock(), self.project_id, self.input_dataset_id,
            self.sandbox_id, 'CP2025q4r7_suicide', 'suicide', self.carried,
            ['observation', 'observation_ext', 'person'])

        written = [call.args[4] for call in mock_write.call_args_list]
        queries = [call.args[5] for call in mock_write.call_args_list]
        self.assertEqual(['observation', 'observation_ext', 'person'], written)
        self.assertEqual(['observation', 'observation_ext', 'person'],
                         sorted(counts))
        self.assertIn("data_element = 'suicide'", queries[0])
        self.assertIn('ct_plus_pediatric_cohort', queries[0])
        self.assertIn('observation_id IN', queries[1])
        self.assertIn('WHERE FALSE', queries[2])

    def test_a_row_left_behind_is_refused(self):
        client = mock.MagicMock()
        client.query.return_value.result.return_value = [{
            'input_rows': 3,
            'claimed_rows': 2,
            'repeated_rows': 0
        }]

        with self.assertRaises(RuntimeError) as ctx:
            split.assert_nothing_left_behind(client, self.project_id,
                                             self.input_dataset_id,
                                             self.sandbox_id,
                                             ['CP2025q4r7_suicide'],
                                             self.carried)

        self.assertIn('observation: 3 expanded rows', str(ctx.exception))
        self.assertIn('CP2025q4r7_suicide', str(ctx.exception))

    def test_a_row_repeated_inside_one_dataset_is_refused(self):
        client = mock.MagicMock()
        client.query.return_value.result.return_value = [{
            'input_rows': 2,
            'claimed_rows': 2,
            'repeated_rows': 1
        }]

        with self.assertRaises(RuntimeError) as ctx:
            split.assert_nothing_left_behind(client, self.project_id,
                                             self.input_dataset_id,
                                             self.sandbox_id,
                                             ['CP2025q4r7_suicide'],
                                             self.carried)

        self.assertIn('observation: 1 rows written more than once',
                      str(ctx.exception))

    def test_union_counts_overlapping_rows_once(self):
        client = mock.MagicMock()
        client.query.return_value.result.return_value = [{
            'input_rows': 2,
            'claimed_rows': 2,
            'repeated_rows': 0
        }]

        split.assert_nothing_left_behind(client, self.project_id,
                                         self.input_dataset_id, self.sandbox_id,
                                         ['CP2025q4r7_a', 'CP2025q4r7_b'],
                                         self.carried)

        query = client.query.call_args.args[0]
        self.assertIn('COUNT(DISTINCT observation_id)', query)
        self.assertIn('UNION ALL', query)

    @mock.patch.object(split, 'assert_nothing_left_behind')
    @mock.patch.object(split, 'write_element', return_value={})
    @mock.patch.object(split, 'assert_output_dataset_is_empty')
    @mock.patch.object(split, 'assert_split_is_publishable')
    @mock.patch.object(split, 'get_views', return_value=set())
    @mock.patch.object(split, 'get_table_columns', return_value={})
    @mock.patch.object(split,
                       'build_expanded_lookup',
                       return_value=['suicide', 'terrorism'])
    @mock.patch.object(split, 'assert_cohort_is_usable')
    @mock.patch.object(split, 'BigQueryClient')
    def test_main_checks_before_it_creates_anything(self, mock_client_class,
                                                    mock_cohort, mock_lookup, _,
                                                    __, mock_publishable,
                                                    mock_empty, mock_write,
                                                    mock_left_behind):
        client = mock_client_class.return_value
        order = mock.MagicMock()
        order.attach_mock(mock_cohort, 'cohort')
        order.attach_mock(mock_lookup, 'lookup')
        order.attach_mock(mock_publishable, 'publishable')
        order.attach_mock(mock_empty, 'empty')
        order.attach_mock(client.create_dataset, 'create_dataset')
        order.attach_mock(mock_write, 'write')
        order.attach_mock(mock_left_behind, 'left_behind')

        split.main(self.project_id, '2025q4r7', self.input_dataset_id,
                   self.sandbox_id, self.rdr_sandbox_id, self.ids_view_id)

        self.assertEqual([call[0] for call in order.mock_calls], [
            'cohort', 'lookup', 'publishable', 'empty', 'empty',
            'create_dataset', 'write', 'create_dataset', 'write', 'left_behind'
        ])
        self.assertEqual(client.define_dataset.call_args_list[0].args[0],
                         'CP2025q4r7_suicide')
        self.assertEqual(client.define_dataset.call_args_list[0].args[2], {
            'ct_plus_component': 'element_delta',
            'data_element': 'suicide'
        })

    def test_other_inputs_are_refused(self):
        with self.assertRaises(ValueError):
            split.main(self.project_id, '2025q4r7', 'CP2025q4r7_deid_clean',
                       self.sandbox_id, self.rdr_sandbox_id, self.ids_view_id)


if __name__ == '__main__':
    unittest.main()
