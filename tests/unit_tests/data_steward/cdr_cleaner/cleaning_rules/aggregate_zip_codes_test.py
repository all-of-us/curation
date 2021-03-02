"""
Unit test for aggregate_zip_codes module

To further obfuscate participant identity, some generalized zip codes will be aggregated together.
The PII zip code and state will be transformed to a neighboring zip code/state pair for those zip codes with low population density.
It is expected that this lookup table will be static and will remain unchanged. 
It is based on US population, and not on participant address metrics.


Original Issue: DC-1379
"""

# Python imports
import unittest

# Project imports
from cdr_cleaner.cleaning_rules.aggregate_zip_codes import (
    AggregateZipCodes, PII_STATE_VOCAB, ZIP_CODE_AGGREGATION_MAP,
    MODIFY_ZIP_CODES_AND_STATES_QUERY, SANDBOX_QUERY)
from constants.bq_utils import WRITE_TRUNCATE
from constants.cdr_cleaner import clean_cdr as clean_consts
from common import OBSERVATION, PIPELINE_TABLES


class AggregateZipCodesTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.project_id = 'foo_project'
        self.dataset_id = 'bar_dataset'
        self.sandbox_id = 'baz_sandbox'

        self.rule_instance = AggregateZipCodes(self.project_id, self.dataset_id,
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
                SANDBOX_QUERY.render(
                    project_id=self.project_id,
                    dataset_id=self.dataset_id,
                    obs_table=OBSERVATION,
                    pii_state_vocab=PII_STATE_VOCAB,
                    zip_code_aggregation_map=ZIP_CODE_AGGREGATION_MAP,
                    pipeline_tables_dataset=PIPELINE_TABLES),
            clean_consts.DESTINATION_TABLE:
                self.rule_instance.sandbox_table_for(OBSERVATION),
            clean_consts.DESTINATION_DATASET:
                self.sandbox_id,
            clean_consts.DISPOSITION:
                WRITE_TRUNCATE
        }, {
            clean_consts.QUERY:
                MODIFY_ZIP_CODES_AND_STATES_QUERY.render(
                    project_id=self.project_id,
                    dataset_id=self.dataset_id,
                    obs_table=OBSERVATION,
                    sandbox_id=self.sandbox_id,
                    zip_code_aggregation_map=ZIP_CODE_AGGREGATION_MAP,
                    pipeline_tables_dataset=PIPELINE_TABLES,
                    pii_state_vocab=PII_STATE_VOCAB),
            clean_consts.DESTINATION_TABLE:
                OBSERVATION,
            clean_consts.DESTINATION_DATASET:
                self.dataset_id,
            clean_consts.DISPOSITION:
                WRITE_TRUNCATE
        }]

        self.assertEqual(results_list, expected_list)