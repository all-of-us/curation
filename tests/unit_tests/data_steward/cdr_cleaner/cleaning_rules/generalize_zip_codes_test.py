"""
Unit test for generalize_zip_codes module

This cleaning rule generalizes all PPI concepts containing zip codes (observation_source_concept_id = 1585250)
to their primary three digit representation, (e.g. 35400 → 354**).

This cleaning rule is specific to the controlled tier.


Original Issue: DC1376
"""

# Python imports
import unittest

# Project imports
from cdr_cleaner.cleaning_rules.generalize_zip_codes import (
    GeneralizeZipCodes, GENERALIZE_ZIP_CODES_QUERY)
from constants.bq_utils import WRITE_TRUNCATE
from constants.cdr_cleaner import clean_cdr as clean_consts
from common import OBSERVATION


class GeneralizeZipCodesTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.project_id = 'foo_project'
        self.dataset_id = 'bar_dataset'
        self.sandbox_id = 'baz_sandbox'

        self.rule_instance = GeneralizeZipCodes(self.project_id,
                                                self.dataset_id,
                                                self.sandbox_id)

        self.assertEqual(self.rule_instance.project_id, self.project_id)
        self.assertEqual(self.rule_instance.dataset_id, self.dataset_id)
        self.assertEqual(self.rule_instance.sandbox_dataset_id, self.sandbox_id)

    def test_get_query_specs(self):
        # Pre conditions
        self.assertEqual(self.rule_instance.affected_datasets,
                         [clean_consts.CONTROLLED_TIER_DEID])

        #Test
        results_list = self.rule_instance.get_query_specs()

        # Post conditions
        expected_list = [{
            clean_consts.QUERY:
                GENERALIZE_ZIP_CODES_QUERY.render(project_id=self.project_id,
                                                  dataset_id=self.dataset_id,
                                                  obs_table=OBSERVATION),
            clean_consts.DESTINATION_TABLE:
                OBSERVATION,
            clean_consts.DESTINATION_DATASET:
                self.dataset_id,
            clean_consts.DISPOSITION:
                WRITE_TRUNCATE
        }]

        self.assertEqual(results_list, expected_list)