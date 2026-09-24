"""
Unit test for the capture_ct_plus_pediatric_cohort module.

Original Issues: DL2537
"""
# Python imports
import unittest
from unittest import mock

# Project imports
import common
from constants.cdr_cleaner import clean_cdr as cdr_consts
from cdr_cleaner.cleaning_rules.capture_ct_plus_pediatric_cohort import (
    CaptureCtPlusPediatricCohort, ConvertCtPlusPediatricCohortIds)
from cdr_cleaner.cleaning_rules.deid.remove_flagged_under18_participants import \
    CT_PLUS_AGE_BAND_TO_RETAIN


class CaptureCtPlusPediatricCohortTest(unittest.TestCase):

    def setUp(self):
        self.project_id = 'fake_project'
        self.dataset_id = 'fake_dataset'
        self.sandbox_id = 'fake_sandbox'
        self.lookup_dataset_id = 'fake_rdr_sandbox'

    def test_capture_selects_the_retained_band_from_the_lookup(self):
        rule = CaptureCtPlusPediatricCohort(self.project_id, self.dataset_id,
                                            self.sandbox_id,
                                            self.lookup_dataset_id)

        query = rule.get_query_specs()[0][cdr_consts.QUERY]

        self.assertIn(f"u.age_band = '{CT_PLUS_AGE_BAND_TO_RETAIN}'", query)
        self.assertIn(
            f'{self.lookup_dataset_id}.'
            f'{common.UNDER18_PARTICIPANTS_LOOKUP_TABLE}', query)
        self.assertIn(f'{self.dataset_id}.person', query)
        self.assertIn(f'{self.sandbox_id}.{common.CT_PLUS_PEDIATRIC_COHORT}',
                      query)

    def test_capture_fails_when_the_lookup_is_missing(self):
        rule = CaptureCtPlusPediatricCohort(self.project_id, self.dataset_id,
                                            self.sandbox_id,
                                            self.lookup_dataset_id)

        with mock.patch(
                'cdr_cleaner.cleaning_rules.capture_ct_plus_pediatric_cohort.'
                'get_tables_in_dataset',
                return_value=[]):
            with self.assertRaises(RuntimeError):
                rule.setup_rule(mock.MagicMock())

    def test_conversion_rebuilds_from_the_captured_participant(self):
        """A rerun must convert from participant_id again rather than from a
        person_id it already converted."""
        rule = ConvertCtPlusPediatricCohortIds(self.project_id, self.dataset_id,
                                               self.sandbox_id)

        query = rule.get_query_specs()[0][cdr_consts.QUERY]

        self.assertIn('c.participant_id = d.person_id', query)
        self.assertIn('controlled_tier_plus_id AS person_id', query)
        self.assertIn(f'{self.sandbox_id}.{common.DEID_MAP}', query)
        self.assertIn(
            f'{common.PIPELINE_TABLES}.'
            f'{common.RDR_PARTICIPANT_RESEARCH_IDS_VIEW}', query)

    @staticmethod
    def _resolution_client(cohort_count, unresolved_count, ambiguous_count):
        client = mock.MagicMock()
        client.query.return_value.result.return_value = [{
            'cohort_count': cohort_count,
            'unresolved_count': unresolved_count,
            'ambiguous_count': ambiguous_count,
            'unresolved_examples': [101] if unresolved_count else []
        }]
        return client

    def test_conversion_accepts_a_fully_resolved_cohort(self):
        rule = ConvertCtPlusPediatricCohortIds(self.project_id, self.dataset_id,
                                               self.sandbox_id)

        rule.setup_rule(self._resolution_client(3, 0, 0))

    def test_conversion_fails_on_an_unresolved_participant(self):
        rule = ConvertCtPlusPediatricCohortIds(self.project_id, self.dataset_id,
                                               self.sandbox_id)

        with self.assertRaises(RuntimeError) as ctx:
            rule.setup_rule(self._resolution_client(3, 1, 0))

        self.assertIn('1 resolve to no CT+ research ID', str(ctx.exception))

    def test_conversion_fails_on_an_ambiguous_participant(self):
        rule = ConvertCtPlusPediatricCohortIds(self.project_id, self.dataset_id,
                                               self.sandbox_id)

        with self.assertRaises(RuntimeError):
            rule.setup_rule(self._resolution_client(3, 0, 1))


if __name__ == '__main__':
    unittest.main()
