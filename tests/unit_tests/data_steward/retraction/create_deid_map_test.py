# Python imports
import unittest
import re

# Third party imports
import mock
import pandas as pd

# Project imports
import retraction.create_deid_map as create_deid_map
import common
from constants.retraction import create_deid_map as consts


class CreateDeidMapTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.project_id = 'test_project_id'
        self.dataset_id = 'test_dataset_id'
        self.all_datasets = [
            'R2021q1r1_combined', 'R2021q1r1_deid', 'R2021q1r2_combined',
            '2021q1r3_combined', 'R2021q1r3_deid'
        ]
        self.deid_datasets = ['R2021q1r1_deid', 'R2021q1r3_deid']
        self.combined_datasets = [
            'R2021q1r1_combined', 'R2021q1r2_combined', '2021q1r3_combined'
        ]

    @mock.patch('retraction.create_deid_map.utils.bq.list_datasets')
    def test_get_combined_datasets_for_deid_map(self, mock_list_datasets):
        result_all_datasets = self.all_datasets
        result_deid_datasets = []
        result_release_datasets = [
            d for d in result_all_datasets
            if consts.CURRENT_RELEASE_REGEX.match(d)
        ]
        for dataset in result_release_datasets:
            if bool(re.match(create_deid_map.DEID_REGEX, dataset)) is True:
                result_deid_datasets.append(dataset)
        result_deid_and_combined_df = create_deid_map.get_corresponding_combined_dataset(
            result_all_datasets, result_deid_datasets)

        expected_all_datasets = mock_list_datasets.return_value = self.all_datasets
        expected_deid_datasets = self.deid_datasets
        expected_combined_datasets_df = create_deid_map.get_corresponding_combined_dataset(
            expected_all_datasets, expected_deid_datasets)

        pd.testing.assert_frame_equal(expected_combined_datasets_df,
                                      result_deid_and_combined_df)

    def test_get_corresponding_combined_dataset(self):
        result = create_deid_map.get_corresponding_combined_dataset(
            self.all_datasets, self.deid_datasets)
        d = {
            'deid_dataset': ['R2021q1r3_deid'],
            'combined_dataset': ['2021q1r3_combined']
        }
        expected = pd.DataFrame(data=d)
        pd.testing.assert_frame_equal(expected, result)

    @mock.patch('utils.bq.get_table_info_for_dataset')
    def test_check_if_deid_map_exists(self, mock_get_table_info):

        expected_create_df = mock_get_table_info.return_value = pd.DataFrame(
            data={
                'table_name':
                    ['_ehr_consent', common.PERSON, common.OBSERVATION]
            })
        expected_create_column_list = expected_create_df['table_name'].tolist()
        if '_deid_map' in expected_create_column_list:
            expected_create_return = consts.SKIP
        elif 'deid_map' in expected_create_column_list:
            expected_create_return = consts.RENAME
        else:
            expected_create_return = consts.CREATE

        expected_skip_df = mock_get_table_info.return_value = pd.DataFrame(
            data={
                'table_name': [
                    '_ehr_consent', '_deid_map', common.PERSON,
                    common.OBSERVATION
                ]
            })
        expected_skip_column_list = expected_skip_df['table_name'].tolist()
        if '_deid_map' in expected_skip_column_list:
            expected_skip_return = consts.SKIP
        elif 'deid_map' in expected_skip_column_list:
            expected_skip_return = consts.RENAME
        else:
            expected_skip_return = consts.CREATE

        expected_rename_df = mock_get_table_info.return_value = pd.DataFrame(
            data={
                'table_name': [
                    '_ehr_consent', 'deid_map', common.PERSON,
                    common.OBSERVATION
                ]
            })
        expected_rename_column_list = expected_rename_df['table_name'].tolist()
        if '_deid_map' in expected_rename_column_list:
            expected_rename_return = consts.SKIP
        elif 'deid_map' in expected_rename_column_list:
            expected_rename_return = consts.RENAME
        else:
            expected_rename_return = consts.CREATE

        self.assertEquals(expected_create_return, 'create')
        self.assertEquals(expected_skip_return, 'skip')
        self.assertEquals(expected_rename_return, 'rename')

    def test_rename_deid_map_table_query(self):
        result = create_deid_map.rename_deid_map_table_query(
            self.project_id, self.dataset_id)
        expected = consts.RENAME_DEID_MAP_TABLE_QUERY.format(
            project=self.project_id, dataset=self.dataset_id)
        self.assertEquals(result, expected)

    def test_create_deid_map_table_query(self):
        result = create_deid_map.create_deid_map_table_query(
            self.project_id, self.dataset_id)
        expected = consts.CREATE_DEID_MAP_TABLE_QUERY.format(
            project=self.project_id, dataset=self.dataset_id)
        self.assertEquals(result, expected)

    @mock.patch('retraction.create_deid_map.utils.bq.list_datasets')
    @mock.patch('retraction.create_deid_map.utils.bq.get_table_info_for_dataset'
               )
    def test_create_deid_map_table_queries(self, mock_table_info,
                                           mock_list_datasets):

        result = [
            consts.CREATE_DEID_MAP_TABLE_QUERY.format(
                project=self.project_id, dataset='R2021q1r1_combined'),
            consts.RENAME_DEID_MAP_TABLE_QUERY.format(
                project=self.project_id, dataset='2021q1r3_combined')
        ]

        mock_list_datasets.return_value = self.all_datasets
        dataframe_1 = pd.DataFrame(data={
            'table_name': ['_ehr_consent', common.PERSON, common.OBSERVATION]
        })
        dataframe_2 = pd.DataFrame(data={
            'table_name': ['_deid_map', common.PERSON, common.OBSERVATION]
        })
        dataframe_3 = pd.DataFrame(data={
            'table_name': ['deid_map', common.PERSON, common.OBSERVATION]
        })
        mock_table_info.side_effect = [dataframe_1, dataframe_2, dataframe_3]

        combined_datasets = self.combined_datasets
        expected_queries = list()

        for dataset in combined_datasets:
            check = create_deid_map.check_if_deid_map_exists(
                self.project_id, dataset)
            if check == 'skip':
                continue
            if check == 'rename':
                expected_queries.append(
                    create_deid_map.rename_deid_map_table_query(
                        self.project_id, dataset))
            if check == 'create':
                expected_queries.append(
                    create_deid_map.create_deid_map_table_query(
                        self.project_id, dataset))

        self.assertEquals(expected_queries, result)
