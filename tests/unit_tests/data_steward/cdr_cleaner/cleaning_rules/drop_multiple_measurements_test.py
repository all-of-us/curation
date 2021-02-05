import unittest

import cdr_cleaner.cleaning_rules.drop_multiple_measurements as drop_mult_meas
import constants.cdr_cleaner.clean_cdr as cdr_consts


class RemoveMultipleMeasurementsTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.project_id = 'project_id'
        self.dataset_id = 'dataset_id'
        self.sandbox_dataset = 'sandbox_dataset'

    def test_query_generation(self):
        result = drop_mult_meas.get_drop_multiple_measurement_queries(
            self.project_id, self.dataset_id, self.sandbox_dataset)
        expected = list()
        expected.append({
            cdr_consts.QUERY:
                drop_mult_meas.INVALID_MULT_MEASUREMENTS.format(
                    project=self.project_id,
                    dataset=self.dataset_id,
                    sandbox_dataset=self.sandbox_dataset,
                    intermediary_table=drop_mult_meas.INTERMEDIARY_TABLE)
        })
        expected.append({
            cdr_consts.QUERY:
                drop_mult_meas.VALID_MEASUREMENTS.format(
                    project=self.project_id,
                    dataset=self.dataset_id,
                    sandbox_dataset=self.sandbox_dataset,
                    intermediary_table=drop_mult_meas.INTERMEDIARY_TABLE)
        })
        self.assertEquals(result, expected)
