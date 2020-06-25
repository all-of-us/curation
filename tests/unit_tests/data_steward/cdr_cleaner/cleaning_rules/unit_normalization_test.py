# Python imports
import unittest

from cdr_cleaner.cleaning_rules.unit_normalization import UnitNormalization, \
    UNIT_NORMALIZATION_QUERY, SANDBOX_UNITS_QUERY, UNIT_MAPPING_TABLE, MEASUREMENT
# Project imports
from constants.bq_utils import WRITE_TRUNCATE
from constants.cdr_cleaner import clean_cdr as clean_consts

QUERY = 'query'


class UnitNormalizationTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.project_id = 'foo_project'
        self.dataset_id = 'foo_dataset'
        self.sandbox_id = 'foo_sandbox'
        self.client = None

        self.query_class = UnitNormalization(self.project_id, self.dataset_id,
                                             self.sandbox_id)

        self.assertEqual(self.query_class.get_project_id(), self.project_id)
        self.assertEqual(self.query_class.get_dataset_id(), self.dataset_id)
        self.assertEqual(self.query_class.get_sandbox_dataset_id(),
                         self.sandbox_id)

    def test_setup_rule(self):
        # Test
        with self.assertRaises(RuntimeError) as c:
            self.query_class.setup_rule(None)

        self.assertEqual(
            str(c.exception),
            "Unable to create tables: ['foo_project.foo_dataset._unit_mapping']"
        )

        # No errors are raised, nothing will happen

    def test_get_query_specs(self):
        # Pre conditions
        self.assertEqual(self.query_class.get_affected_datasets(),
                         [clean_consts.DEID_CLEAN])

        # Test
        results_list = self.query_class.get_query_specs()
        # Post conditions
        sandbox_query = dict()
        sandbox_query[clean_consts.QUERY] = SANDBOX_UNITS_QUERY.render(
            project_id=self.project_id,
            sandbox_dataset=self.sandbox_id,
            intermediary_table=self.query_class.get_sandbox_tablenames()[0],
            dataset_id=self.dataset_id,
            unit_table_name=UNIT_MAPPING_TABLE,
            measurement_table=MEASUREMENT)

        update_query = dict()
        update_query[clean_consts.QUERY] = UNIT_NORMALIZATION_QUERY.render(
            project_id=self.project_id,
            dataset_id=self.dataset_id,
            unit_table_name=UNIT_MAPPING_TABLE,
            measurement_table=MEASUREMENT)
        update_query[clean_consts.DESTINATION_TABLE] = MEASUREMENT
        update_query[clean_consts.DESTINATION_DATASET] = self.dataset_id
        update_query[clean_consts.DISPOSITION] = WRITE_TRUNCATE

        expected_list = [sandbox_query, update_query]

        for ex_dict, rs_dict in zip(expected_list, results_list):
            self.assertDictEqual(ex_dict, rs_dict)

    def test_log_queries(self):
        # Pre conditions
        self.assertEqual(self.query_class.get_affected_datasets(),
                         [clean_consts.DEID_CLEAN])

        store_rows_to_be_changed = SANDBOX_UNITS_QUERY.render(
            project_id=self.project_id,
            sandbox_dataset=self.sandbox_id,
            intermediary_table=self.query_class.get_sandbox_tablenames()[0],
            dataset_id=self.dataset_id,
            unit_table_name=UNIT_MAPPING_TABLE,
            measurement_table=MEASUREMENT)

        select_rows_to_be_changed = UNIT_NORMALIZATION_QUERY.render(
            project_id=self.project_id,
            dataset_id=self.dataset_id,
            unit_table_name=UNIT_MAPPING_TABLE,
            measurement_table=MEASUREMENT)

        # Test
        with self.assertLogs(level='INFO') as cm:
            self.query_class.log_queries()

            expected = [
                'INFO:cdr_cleaner.cleaning_rules.base_cleaning_rule:Generated SQL Query:\n'
                + store_rows_to_be_changed,
                'INFO:cdr_cleaner.cleaning_rules.base_cleaning_rule:Generated SQL Query:\n'
                + select_rows_to_be_changed
            ]

            # Post condition
            self.assertEqual(cm.output, expected)
