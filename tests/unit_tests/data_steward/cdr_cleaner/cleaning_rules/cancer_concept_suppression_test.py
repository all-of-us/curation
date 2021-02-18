"""
Unit test for cancer_concept_suppression module

This rule sandboxes and suppresses reccords whose concept_codes end in 
'History_WhichConditions', 'Condition_OtherCancer', ‘History_AdditionalDiagnosis’,
and 'OutsideTravel6MonthsWhere'.

Runs on the controlled tier.


Original Issue: DC-1381
"""

# Python imports
import unittest

# Project imports
from cdr_cleaner.cleaning_rules.cancer_concept_suppression import (
    CancerConceptSuppression, SANDBOXING_QUERY, CONCEPT_SUPPRESSION_QUERY)
from constants.bq_utils import WRITE_TRUNCATE
from constants.cdr_cleaner import clean_cdr as clean_consts
from common import OBSERVATION


class CancerConceptSuppressionTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.project_id = 'foo_project'
        self.dataset_id = 'bar_dataset'
        self.sandbox_id = 'baz_sandbox'

        self.rule_instance = CancerConceptSuppression(self.project_id,
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
                SANDBOXING_QUERY.render(
                    project_id=self.project_id,
                    sandbox_id=self.sandbox_id,
                    sandbox_table=self.rule_instance.sandbox_table_for(
                        OBSERVATION),
                    dataset_id=self.dataset_id),
        }, {
            clean_consts.QUERY:
                CONCEPT_SUPPRESSION_QUERY.render(project_id=self.project_id,
                                                 dataset_id=self.dataset_id),
            clean_consts.DESTINATION_TABLE:
                OBSERVATION,
            clean_consts.DESTINATION_DATASET:
                self.dataset_id,
            clean_consts.DISPOSITION:
                WRITE_TRUNCATE
        }]

        self.assertEqual(results_list, expected_list)