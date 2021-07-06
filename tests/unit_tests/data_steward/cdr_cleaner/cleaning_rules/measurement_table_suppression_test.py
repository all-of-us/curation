"""
Unit Test for the measurement_table_suppression module.

The intent is to clean measurement data from sites that submit only
junk, clean records that do not provide any quality data, and
remove duplicates. 
"""
# Python imports
import unittest

# Third party imports

# Project imports
from common import MEASUREMENT
from constants.bq_utils import WRITE_TRUNCATE
from constants.cdr_cleaner import clean_cdr as clean_consts
import cdr_cleaner.cleaning_rules.measurement_table_suppression as mts


class MeasurementTableSuppressionTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.project_id = 'foo'
        self.dataset_id = 'bar'
        self.sandbox_id = 'baz'
        self.client = None

        self.rule_instance = mts.MeasurementRecordsSuppression(
            self.project_id, self.dataset_id, self.sandbox_id)

        self.assertEqual(self.rule_instance.project_id, self.project_id)
        self.assertEqual(self.rule_instance.dataset_id, self.dataset_id)
        self.assertEqual(self.rule_instance.sandbox_dataset_id, self.sandbox_id)

    def test_setup_rule(self):
        # test
        self.rule_instance.setup_rule(self.client)

    def test_get_sandbox_tablenames(self):
        # no pre-conditions

        # test
        sandbox_tables = self.rule_instance.get_sandbox_tablenames()

        # post conditions
        expected = [
            mts.SAVE_BAD_SITE_DATA, mts.SAVE_NULL_VALUE_RECORDS,
            mts.INVALID_VALUES_RECORDS, mts.SITES_WITH_ONLY_BAD_DATA,
            mts.SAVE_DUPLICATE_RECORDS
        ]

        # assert both lists contain same elements regardless of order
        self.assertCountEqual(sandbox_tables, expected)

    def test_get_query_specs(self):
        # pre-conditions
        self.assertEqual(
            self.rule_instance.affected_datasets,
            [clean_consts.DEID_BASE, clean_consts.CONTROLLED_TIER_DEID_CLEAN])

        # test
        result_list = self.rule_instance.get_query_specs()

        # post conditions
        expected_list = [{
            clean_consts.QUERY:
                mts.NULL_VALUES_SAVE_QUERY.render(
                    project=self.project_id,
                    dataset=self.dataset_id,
                    sandbox=self.sandbox_id,
                    save_table=mts.INVALID_VALUES_RECORDS)
        }, {
            clean_consts.QUERY:
                mts.NULL_VALUES_UPDATE_QUERY.render(project=self.project_id,
                                                    dataset=self.dataset_id),
            clean_consts.DESTINATION_TABLE:
                MEASUREMENT,
            clean_consts.DESTINATION_DATASET:
                self.dataset_id,
            clean_consts.DISPOSITION:
                WRITE_TRUNCATE
        }, {
            clean_consts.QUERY:
                mts.SITES_TO_REMOVE_DATA_FOR.render(
                    project=self.project_id,
                    dataset=self.dataset_id,
                    sandbox=self.sandbox_id,
                    save_table=mts.SITES_WITH_ONLY_BAD_DATA)
        }, {
            clean_consts.QUERY:
                mts.NULL_AND_ZERO_VALUES_SAVE_QUERY.render(
                    project=self.project_id,
                    dataset=self.dataset_id,
                    sandbox=self.sandbox_id,
                    save_table=mts.SAVE_BAD_SITE_DATA,
                    id_table=mts.SITES_WITH_ONLY_BAD_DATA),
        }, {
            clean_consts.QUERY:
                mts.SET_NULL_WHEN_ONLY_ZEROS_SUBMITTED.render(
                    project=self.project_id,
                    dataset=self.dataset_id,
                    sandbox=self.sandbox_id,
                    id_table=mts.SITES_WITH_ONLY_BAD_DATA),
        }, {
            clean_consts.QUERY:
                mts.SAVE_NULL_DROP_RECORDS.render(
                    project=self.project_id,
                    dataset=self.dataset_id,
                    sandbox=self.sandbox_id,
                    save_table=mts.SAVE_NULL_VALUE_RECORDS),
        }, {
            clean_consts.QUERY:
                mts.SELECT_RECORDS_WITH_VALID_DATA.render(
                    project=self.project_id, dataset=self.dataset_id),
            clean_consts.DESTINATION_TABLE:
                MEASUREMENT,
            clean_consts.DESTINATION_DATASET:
                self.dataset_id,
            clean_consts.DISPOSITION:
                WRITE_TRUNCATE
        }, {
            clean_consts.QUERY:
                mts.SANDBOX_DUPLICATES.render(
                    project=self.project_id,
                    dataset=self.dataset_id,
                    sandbox=self.sandbox_id,
                    save_table=mts.SAVE_DUPLICATE_RECORDS),
        }, {
            clean_consts.QUERY:
                mts.REMOVE_DUPLICATES.render(
                    project=self.project_id,
                    dataset=self.dataset_id,
                    sandbox=self.sandbox_id,
                    id_table=mts.SAVE_DUPLICATE_RECORDS),
            clean_consts.DESTINATION_TABLE:
                MEASUREMENT,
            clean_consts.DESTINATION_DATASET:
                self.dataset_id,
            clean_consts.DISPOSITION:
                WRITE_TRUNCATE
        }]

        self.assertEqual(result_list, expected_list)
