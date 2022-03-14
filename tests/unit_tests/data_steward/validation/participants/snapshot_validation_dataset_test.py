# Python imports
import unittest

# Third party imports
from pandas import DataFrame

# Project imports
from validation.participants.snapshot_validation_dataset import get_partition_date_df


class SnapshotValidationDatasetTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.dataset_id = 'test_dataset'
        self.hpo_id = 'test_hpo_1'

    def test_get_partition_date(self):
        test_df = DataFrame.from_dict({
            'table_name': [
                'identity_match_test_hpo_1', 'identity_match_test_hpo_2',
                'identity_match_test_hpo_3'
            ],
            'partition_id': ['2022010112', '2022020112', '2022030112']
        })
        expected_date = '2022010112'
        partition_df = get_partition_date_df(test_df, self.hpo_id)
        self.assertEqual(partition_df['partition_id'].count(), 1)
        partition_date = partition_df['partition_id'].iloc[0]
        self.assertEqual(partition_date, expected_date)
