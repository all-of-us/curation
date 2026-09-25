"""
Unit test for capture_ct_plus_birthdate module

Original Issues: DL2436

The prune and conversion share their code with the Zip5 rules, which
capture_ct_plus_zip5_test.py covers; these tests check the date-of-birth
wiring.
"""

# Python imports
import unittest

# Project imports
from common import (AIAN_LIST, CT_PLUS_BIRTHDATE,
                    UNDER18_PARTICIPANTS_LOOKUP_TABLE)
from constants.cdr_cleaner import clean_cdr as cdr_consts
from cdr_cleaner.cleaning_rules.capture_ct_plus_birthdate import (
    CaptureCtPlusBirthdate, ConvertCtPlusBirthdateIds, PruneCtPlusBirthdate)


class CaptureCtPlusBirthdateTest(unittest.TestCase):

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

        self.capture = CaptureCtPlusBirthdate(self.project_id, self.dataset_id,
                                              self.sandbox_id,
                                              self.rdr_sandbox_id,
                                              self.under18_lookup_dataset_id)
        self.prune = PruneCtPlusBirthdate(self.project_id, self.dataset_id,
                                          self.sandbox_id)
        self.convert = ConvertCtPlusBirthdateIds(self.project_id,
                                                 self.dataset_id,
                                                 self.sandbox_id,
                                                 'foo_ids_view')

    def test_rules_target_only_ct_plus_deid_and_their_own_table(self):
        for rule in [self.capture, self.prune, self.convert]:
            self.assertEqual(rule.affected_datasets,
                             [cdr_consts.CONTROLLED_TIER_PLUS_DEID])
            self.assertEqual(rule.get_sandbox_tablenames(), [CT_PLUS_BIRTHDATE])
            self.assertIn(f'.{CT_PLUS_BIRTHDATE}`',
                          rule.get_query_specs()[0][cdr_consts.QUERY])

    def test_capture_reads_person_and_excludes_aian_and_pediatric(self):
        query = self.capture.get_query_specs()[0][cdr_consts.QUERY]

        self.assertIn(f'`{self.project_id}.{self.dataset_id}.person` p', query)
        self.assertIn(f'`{self.project_id}.{self.rdr_sandbox_id}.{AIAN_LIST}`',
                      query)
        self.assertIn(
            f'`{self.project_id}.{self.under18_lookup_dataset_id}.'
            f'{UNDER18_PARTICIPANTS_LOOKUP_TABLE}`', query)
        self.assertEqual(query.count('m.research_id = p.person_id'), 2)
