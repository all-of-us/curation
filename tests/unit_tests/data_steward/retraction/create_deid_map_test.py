# Python imports
import unittest

# Third party imports
import mock
import pandas as pd

# Project imports
import retraction.create_deid_map as create_deid_map
import utils.bq


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
            'R2021Q1R1_combined', 'R2021Q1R1_deid', 'R2021Q1R2_combined',
            '2021q1r3_combined', 'R2021Q1R3_deid'
        ]
        self.deid_datasets = ['R2021Q1R1_deid', 'R2021Q1R3_deid']
        self.combined_datasets = [
            'R2021Q1R1_combined', 'R2021Q1R2_combined', '2021q1r3_combined'
        ]

    @mock.patch('retraction.create_deid_map.utils.bq.list_datasets')
    def test_get_combined_datasets_for_deid_map(self, mock_list_datasets):
        result = create_deid_map.get_combined_datasets_for_deid_map(
            self.project_id)

        expected_all_datasets = mock_list_datasets.return_value = utils.bq.list_datasets(
            self.project_id)
        expected_deid_datasets = self.deid_datasets
        expected_combined_datasets = create_deid_map.get_corresponding_combined_dataset(
            expected_all_datasets, expected_deid_datasets)

        self.assertListEqual(expected_combined_datasets, result)

    def test_get_corresponding_combined_dataset(self):
        result = create_deid_map.get_corresponding_combined_dataset(
            self.all_datasets, self.deid_datasets)
        expected = list()

        for d in self.deid_datasets:
            release = d.split('_')[0]
            older_release = release[1:]
            combined, older_combined = release + '_combined', older_release + '_combined'

            if combined in self.all_datasets:
                expected.append(combined)
            if older_combined in self.all_datasets:
                expected.append(older_combined)
            # Remove duplicates
        expected = list(set(expected))

        self.assertEquals(expected, result)

    @mock.patch('utils.bq.get_table_info_for_dataset')
    def test_check_if_deid_map_exists(self, mock_get_table_info):
        result = create_deid_map.check_if_deid_map_exists(
            self.project_id, self.dataset_id)

        expected_table_info_df = mock_get_table_info.return_value = pd.DataFrame(
            columns=[
                'table_catalog', 'table_schema', 'table_name', 'column_name',
                'ordinal_position', 'is_nullable', 'data_type', 'is_generated',
                'generation_expression', 'is_stored', 'is_hidden',
                'is_updatable', 'is_system_defined', 'is_partitioning_column',
                'clustering_ordinal_position'
            ])
        expected_column_list = expected_table_info_df['table_name'].tolist()

        if 'deid_map' in expected_column_list:
            self.assertEquals('rename required', result)
        if '_deid_map' in expected_column_list:
            self.assertEquals(True, result)
        if ['deid_map', '_deid_map'] not in expected_column_list:
            self.assertEquals(False, result)

    def test_rename_deid_map_table_query(self):
        result = create_deid_map.rename_deid_map_table_query(
            self.project_id, self.dataset_id)
        expected = create_deid_map.RENAME_DEID_MAP_TABLE_QUERY.format(
            project=self.project_id, dataset=self.dataset_id)
        self.assertEquals(result, expected)

    def test_create_deid_map_table_query(self):
        result = create_deid_map.create_deid_map_table_query(
            self.project_id, self.dataset_id)
        expected = create_deid_map.CREATE_DEID_MAP_TABLE_QUERY.format(
            project=self.project_id, dataset=self.dataset_id)
        self.assertEquals(result, expected)

    @mock.patch('retraction.create_deid_map.utils.bq.list_datasets')
    def test_create_deid_map_table_queries(self, mock_list_datasets):
        result = create_deid_map.create_deid_map_table_queries(self.project_id)
        mock_list_datasets.return_value = utils.bq.list_datasets(
            self.project_id)

        combined_datasets = create_deid_map.get_combined_datasets_for_deid_map(
            self.project_id)
        expected_queries = list()

        for dataset in combined_datasets:
            check = create_deid_map.check_if_deid_map_exists(
                self.project_id, dataset)
            if check is True:
                continue
            if check is False:
                expected_queries.append(
                    create_deid_map.create_deid_map_table_query(
                        self.project_id, dataset))
            if check == 'rename required':
                expected_queries.extend(
                    create_deid_map.rename_deid_map_table_query(
                        self.project_id, dataset))

        self.assertEquals(expected_queries, result)
