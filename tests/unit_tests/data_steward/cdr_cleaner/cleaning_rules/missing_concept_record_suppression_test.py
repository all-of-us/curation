"""
Unit test for missing_concept_record_suppression module

None

Original Issue: DC1601
"""

# Python imports
import unittest

# Project imports
from cdr_cleaner.cleaning_rules.missing_concept_record_suppression import MissingConceptRecordSuppression
from constants.cdr_cleaner import clean_cdr as clean_consts
from common import OBSERVATION


class MissingConceptRecordSuppressionTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.project_id = 'foo_project'
        self.dataset_id = 'bar_dataset'
        self.sandbox_id = 'baz_sandbox'

        self.rule_instance = MissingConceptRecordSuppression(self.project_id,
                                                 self.dataset_id,
                                                 self.sandbox_id)

        self.assertEqual(self.rule_instance.project_id, self.project_id)
        self.assertEqual(self.rule_instance.dataset_id, self.dataset_id)
        self.assertEqual(self.rule_instance.sandbox_dataset_id, self.sandbox_id)

    def test_get_query_specs(self):
        # Pre conditions
        self.assertEqual(self.rule_instance.affected_datasets,
                         [clean_consts.COMBINED])

        #Test
        results_list = self.rule_instance.get_query_specs()

        # Post conditions
        expected_list = [
            # Expected queries go here
        ]

        self.assertEqual(results_list, expected_list)