"""
Unit test for AianExpandedConceptSuppressionCtPlus.

Original Issues: DL-2538
"""
# Python imports
import unittest
from unittest import mock

# Project imports
from cdr_cleaner.cleaning_rules.deid import aian_expanded_concept_suppression as rule
from constants.cdr_cleaner import clean_cdr as cdr_consts
from resources import has_person_id


class AianExpandedConceptSuppressionCtPlusTest(unittest.TestCase):

    def setUp(self):
        self.rule = rule.AianExpandedConceptSuppressionCtPlus(
            'fake_project', 'deid', 'deid_sandbox', 'rdr_sandbox')

    def test_runs_only_at_the_ct_plus_deid_stage(self):
        self.assertEqual([cdr_consts.CONTROLLED_TIER_PLUS_DEID],
                         self.rule.affected_datasets)

    def test_person_rows_are_never_removed(self):
        self.assertNotIn('person', self.rule.affected_tables)

    def test_every_affected_table_can_reach_a_participant(self):
        self.assertNotIn('provider', self.rule.affected_tables)
        self.assertNotIn('care_site', self.rule.affected_tables)

        for table in self.rule.affected_tables:
            with self.subTest(table=table):
                self.assertTrue(has_person_id(table) or table == 'note_nlp')

    def test_sandbox_restricts_expanded_rows_to_aian_participants(self):
        query = self.rule.get_sandbox_query('observation')[cdr_consts.QUERY]

        self.assertIn('d.observation_concept_id IN (', query)
        self.assertIn('d.person_id IN (', query)
        self.assertIn('`fake_project.rdr_sandbox.aian_list`', query)
        self.assertIn('`fake_project.deid_sandbox._deid_map`', query)

    def test_note_nlp_is_attributed_through_its_note(self):
        query = self.rule.get_sandbox_query('note_nlp')[cdr_consts.QUERY]

        self.assertIn('`fake_project.deid.note` AS owner', query)
        self.assertIn('owner.person_id IN (', query)

    @mock.patch.object(rule, 'build_expanded_lookup')
    @mock.patch(
        'cdr_cleaner.cleaning_rules.deid.concept_suppression.get_tables_in_dataset'
    )
    def test_setup_builds_the_lookup_from_the_stage_vocabulary(
            self, mock_tables, mock_lookup):
        mock_tables.return_value = ['observation']
        client = mock.MagicMock()

        self.rule.setup_rule(client)

        mock_lookup.assert_called_once_with(client, 'fake_project', 'deid',
                                            'deid_sandbox')


if __name__ == '__main__':
    unittest.main()
