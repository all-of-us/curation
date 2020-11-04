import unittest
from unittest import mock

from google.cloud.bigquery.table import TableListItem
from google.cloud.bigquery import DatasetReference

import bq_utils
from resources import CDM_TABLES
from tests.bq_test_helpers import list_item_from_table_id
from tools.exclude_site_submission import exclude_site_submission, _filter_hpo_tables


class ExcludeSiteSubmissionTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print(
            '\n**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.project_id = 'fake_project'
        self.dataset_id = 'fake_dataset'
        self.dataset = DatasetReference(self.project_id, self.dataset_id)
        self.hpo_ids = ['fake_hpo1', 'fake_hpo2']
        self.hpo_cdm_tables = [
            self._table_list_item(hpo_id, cdm_table)
            for hpo_id in self.hpo_ids
            for cdm_table in CDM_TABLES
        ]
        self.hpo_cdm_and_achilles_tables = [
            self._table_list_item(hpo_id, cdm_table)
            for hpo_id in self.hpo_ids
            for cdm_table in CDM_TABLES + ['achilles', 'achilles_results']
        ]

    def _full_table_id(self, table_id):
        return f'{self.project_id}.{self.dataset_id}.{table_id}'

    def _table_id_to_list_item(self, table_id):
        full_table_id = self._full_table_id(table_id)
        return list_item_from_table_id(full_table_id)

    def _table_list_item(self, hpo_id, cdm_table) -> TableListItem:
        table_id = bq_utils.get_table_id(hpo_id, cdm_table)
        return self._table_id_to_list_item(table_id)

    def test_exclude_site_data(self):
        # tables we intend to empty
        tables_to_empty = ['hpo1_person', 'hpo1_condition_occurrence']
        unaffected = ['hpo1_achilles', 'hpo2_person']
        all_tables = tables_to_empty + unaffected
        with mock.patch('utils.bq.list_tables') as mock_list_tables:
            # client.query is called with the expected script
            expected_script = ''
            for table in tables_to_empty:
                full_table_id = self._full_table_id(table)
                expected_script += f'DELETE FROM `{full_table_id}` WHERE 1=1;'
            mock_client = mock.MagicMock()
            mock_list_tables.return_value = [
                self._table_id_to_list_item(table) for table in all_tables
            ]
            exclude_site_submission(mock_client, self.dataset, hpo_ids=['hpo1'])
            mock_client.query.assert_called_once_with(expected_script)

            # if no tables found for input hpo_id
            # error is raised and client.query is NOT called
            mock_client = mock.MagicMock()
            with self.assertRaises(RuntimeError) as c:
                exclude_site_submission(mock_client, self.dataset,
                                        ['hpo1', 'hpo_missing'])
            mock_client.query.assert_not_called()
