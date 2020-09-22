import unittest
from unittest import mock

from google.cloud.bigquery.table import TableListItem, TableReference
from google.cloud.bigquery import Client, DatasetReference

import bq_utils
from resources import CDM_TABLES
from tools.purge_hpo_data import purge_hpo_data, _filter_hpo_tables


class PurgeHpoDataTest(unittest.TestCase):

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
        resource = {
            "tableReference":
                TableReference.from_string(full_table_id).to_api_repr()
        }
        return TableListItem(resource)

    def _table_list_item(self, hpo_id, cdm_table) -> TableListItem:
        table_id = bq_utils.get_table_id(hpo_id, cdm_table)
        return self._table_id_to_list_item(table_id)

    def test_filter_hpo_tables(self):
        # clinical tables for specified hpo are returned
        results = _filter_hpo_tables(self.hpo_cdm_tables, 'fake_hpo1')
        expected_ids = [
            bq_utils.get_table_id('fake_hpo1', cdm_table)
            for cdm_table in CDM_TABLES
        ]
        actual_ids = [table.table_id for table in results]
        self.assertListEqual(expected_ids, actual_ids)

        # non-clinical tables are NOT returned
        results = _filter_hpo_tables(self.hpo_cdm_and_achilles_tables,
                                     'fake_hpo1')
        actual_ids = [table.table_id for table in results]
        self.assertListEqual(expected_ids, actual_ids)

        # empty list if no tables associated with hpo_id found
        results = _filter_hpo_tables(self.hpo_cdm_tables, 'fake_hpo3')
        self.assertListEqual([], results)

    def _mock_client_with(self, tables):
        """
        Get a mock BQ client object
        :param tables: names of tables the client should list
        :return: the mock client object
        """
        mock_client = mock.MagicMock(wraps=Client(project=self.project_id))
        mock_client.list_tables.return_value = [
            self._table_id_to_list_item(table) for table in tables
        ]
        mock_client.query.return_value = mock.MagicMock()
        return mock_client

    def test_purge_hpo_data(self):
        # hpo_id whose data will be purged
        purge_hpo_ids = ['hpo1']
        # tables we intend to empty
        purge_tables = ['hpo1_person', 'hpo1_condition_occurrence']
        unaffected = ['hpo1_achilles', 'hpo2_person']
        mock_client = self._mock_client_with(purge_tables + unaffected)
        # construct script we expect to be passed to client query
        expected_script = ''
        for table in purge_tables:
            full_table_id = self._full_table_id(table)
            expected_script += f'DELETE FROM `{full_table_id}` WHERE 1=1;'
        purge_hpo_data(mock_client, self.dataset, purge_hpo_ids)
        mock_client.query.assert_called_once_with(expected_script)
