"""
Unit test to ensure the queries in the race_ethnicity_record_suppression.py module work properly.

Removes any records that have a observation_source_concept_id as either of these values: 1586151, 1586150, 1586152,
1586153, 1586154, 1586155, 1586156, 1586149)

Original Issue: DC-1365

The intent is to ensure that no records exists that have any of the observation_source_concept_id above by sandboxing
any rows that have those observation_source_concept_id and removing them from the observation table.
"""

# Python imports
import unittest

# Project imports
import common
from constants.bq_utils import WRITE_TRUNCATE
from constants.cdr_cleaner import clean_cdr as cdr_consts
from cdr_cleaner.cleaning_rules.race_ethnicity_record_suppression import RaceEthnicityRecordSuppression, \
    SANDBOX_RECORDS_QUERY, DROP_RECORDS_QUERY


class RaceEthnicityRecordSuppressionTest(unittest.TestCase):

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

        self.rule_instance = RaceEthnicityRecordSuppression(
            self.project_id, self.dataset_id, self.sandbox_dataset_id)

        self.assertEqual(self.rule_instance.project_id, self.project_id)
        self.assertEqual(self.rule_instance.dataset_id, self.dataset_id)
        self.assertEqual(self.rule_instance.sandbox_dataset_id,
                         self.sandbox_dataset_id)

    def test_setup_rule(self):
        # Test
        self.rule_instance.setup_rule(self.client)

    def test_get_query_spec(self):
        # Pre conditions
        self.assertEqual(self.rule_instance.affected_datasets,
                         [cdr_consts.CONTROLLED_TIER_DEID])

        # Test
        results_list = self.rule_instance.get_query_specs()

        # Post conditions
        expected_list = [{
            cdr_consts.QUERY:
                SANDBOX_RECORDS_QUERY.render(
                    project_id=self.project_id,
                    sandbox_id=self.sandbox_dataset_id,
                    sandbox_table=self.rule_instance.sandbox_table_for(
                        common.OBSERVATION),
                    dataset_id=self.dataset_id)
        }, {
            cdr_consts.QUERY:
                DROP_RECORDS_QUERY.render(
                    project_id=self.project_id,
                    dataset_id=self.dataset_id,
                    sandbox_id=self.sandbox_dataset_id,
                    sandbox_table=self.rule_instance.sandbox_table_for(
                        common.OBSERVATION)),
            cdr_consts.DESTINATION_TABLE:
                common.OBSERVATION,
            cdr_consts.DESTINATION_DATASET:
                self.dataset_id,
            cdr_consts.DISPOSITION:
                WRITE_TRUNCATE
        }]

        self.assertEqual(results_list, expected_list)
