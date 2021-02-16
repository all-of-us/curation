"""
Unit test for questionnaire_response_id_map.py

Maps questionnaire_response_ids from the observation table to the research_response_id in the
_deid_questionnaire_response_map lookup table.

Original Issue: DC-1347

The purpose of this cleaning rule is to create (if it does not already exist) the questionnaire mapping lookup table
and use that lookup table to remap the questionnaire_response_id in the observation table to the randomly
generated research_response_id in the _deid_questionnaire_response_map table.
"""

# Python imports
import unittest

# Project imports
import common
from constants.bq_utils import WRITE_TRUNCATE
from constants.cdr_cleaner import clean_cdr as cdr_consts
from cdr_cleaner.cleaning_rules.deid.questionnaire_response_id_map import QRIDtoRID, QRID_RID_MAPPING_QUERY, \
    LOOKUP_TABLE_CREATION_QUERY


class QRIDtoRIDTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.project_id = 'foo_project'
        self.dataset_id = 'foo_dataset'
        self.sandbox_dataset_id = 'foo_sandbox'
        self.client = None

        self.rule_instance = QRIDtoRID(self.project_id, self.dataset_id,
                                       self.sandbox_dataset_id)

        self.assertEqual(self.rule_instance.project_id, self.project_id)
        self.assertEqual(self.rule_instance.dataset_id, self.dataset_id)
        self.assertEqual(self.rule_instance.sandbox_dataset_id,
                         self.sandbox_dataset_id)

    def test_setup_rule(self):
        # Test
        self.rule_instance.setup_rule(self.client)

    def test_get_query_spec(self):
        # Pre conditions
        self.assertEqual(
            self.rule_instance.affected_datasets,
            [cdr_consts.CONTROLLED_TIER_DEID, cdr_consts.REGISTERED_TIER_DEID])

        # Test
        result_list = self.rule_instance.get_query_specs()

        # Post conditions
        expected_list = [{
            cdr_consts.QUERY:
                LOOKUP_TABLE_CREATION_QUERY.render(
                    project_id=self.project_id,
                    shared_sandbox_id=self.sandbox_dataset_id,
                    dataset_id=self.dataset_id)
        }, {
            cdr_consts.QUERY:
                QRID_RID_MAPPING_QUERY.render(
                    project_id=self.project_id,
                    dataset_id=self.dataset_id,
                    shared_sandbox_id=self.sandbox_dataset_id),
            cdr_consts.DESTINATION_TABLE:
                common.OBSERVATION,
            cdr_consts.DESTINATION_DATASET:
                self.dataset_id,
            cdr_consts.DISPOSITION:
                WRITE_TRUNCATE
        }]

        self.assertEqual(result_list, expected_list)
