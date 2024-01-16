"""
Unit Test for the bq module

Ensures the define_dataset and update_labels_and_tag functions have the proper parameters passed
    to them. Ensures update_labels_and_tags function returns a dictionary of either the existing
    labels and or tags or the labels and or tags that need to be updated.

Original Issues: DC-757, DC-758

The intent is to check the proper parameters are passed to define_dataset and update_labels_and_tags
    function as well as to check to make sure the right labels and tags are returned in the
    update_labels_and_tags function.
"""

# Python imports
import datetime
import os
import typing
from unittest import TestCase

# Third-party imports
from google.cloud import bigquery
from google.cloud.bigquery import DatasetReference
from mock import MagicMock, patch

# Project imports
import resources
from tests.bq_test_helpers import mock_query_result, list_item_from_table_id
from utils import bq
from resources import CDM_TABLES


def _get_all_field_types() -> typing.FrozenSet[str]:
    """
    Helper to get all field types referenced in fields (json) files

    :return: names of all types in fields files
    """
    all_field_types = set()
    for _, dir_paths, files in os.walk(resources.fields_path):
        for dir_path in dir_paths:
            for fields_file in files:
                table, _ = os.path.splitext(fields_file)
                try:
                    fields = resources.fields_for(table, sub_path=dir_path)
                except RuntimeError:
                    pass
                else:
                    for field in fields:
                        all_field_types.add(field.get('type'))
    return frozenset(all_field_types)


class BqTest(TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        # Input parameters expected by the class
        self.project_id = 'bar_project'
        self.dataset_id = 'foo_dataset'
        self.description = 'fake_description'
        self.existing_labels_or_tags = {'label': 'value', 'tag': ''}
        self.new_labels_or_tags = {'label': 'new_value', 'new_tag': ''}
        self.updated = {'tag': '', 'label': 'new_value', 'new_tag': ''}
        self.dataset_ref = DatasetReference(self.project_id, self.dataset_id)

    def test_get_table_schema(self):
        actual_fields = bq.get_table_schema('digital_health_sharing_status')

        for field in actual_fields:
            if field.field_type.upper() == "RECORD":
                self.assertEqual(len(field.fields), 2)

    def test_define_dataset(self):
        # Tests if project_id is given
        self.assertRaises(RuntimeError, bq.define_dataset, None,
                          self.dataset_id, self.description,
                          self.existing_labels_or_tags)

        # Tests if dataset_id is given
        self.assertRaises(RuntimeError, bq.define_dataset, self.project_id,
                          None, self.description, self.existing_labels_or_tags)

        # Tests if description is given
        self.assertRaises(RuntimeError, bq.define_dataset, self.project_id,
                          self.dataset_id, (None or ''),
                          self.existing_labels_or_tags)

        # Tests if no label or tag is given
        self.assertRaises(RuntimeError, bq.define_dataset, self.project_id,
                          self.dataset_id, self.description, None)

        # Pre-conditions
        results = bq.define_dataset(self.project_id, self.dataset_id,
                                    self.description,
                                    self.existing_labels_or_tags)

        # Post conditions
        self.assertIsInstance(results, bigquery.Dataset)
        self.assertEqual(results.labels, self.existing_labels_or_tags)

    def test_update_labels_and_tags(self):
        # Tests if dataset_id param is provided
        self.assertRaises(RuntimeError, bq.update_labels_and_tags, None,
                          self.existing_labels_or_tags, self.new_labels_or_tags)

        # Tests if new_labels_or_tags param is provided
        self.assertRaises(RuntimeError, bq.update_labels_and_tags,
                          self.dataset_id, self.existing_labels_or_tags, None)

        # Pre-conditions
        results = bq.update_labels_and_tags(self.dataset_id,
                                            self.existing_labels_or_tags,
                                            self.new_labels_or_tags, True)

        # Post conditions
        self.assertEqual(results, self.updated)
        with self.assertRaises(RuntimeError):
            bq.update_labels_and_tags(
                self.dataset_id,
                existing_labels_or_tags={'label': 'apples'},
                new_labels_or_tags={'label': 'oranges'},
                overwrite_ok=False)

    def test_get_table_ddl(self):
        # Schema is determined by table name
        ddl = bq.get_create_or_replace_table_ddl(self.project_id,
                                                 self.dataset_id,
                                                 'observation').strip()
        self.assertTrue(
            ddl.startswith(
                f'CREATE OR REPLACE TABLE `{self.project_id}.{self.dataset_id}.observation`'
            ))
        self.assertTrue(ddl.endswith(')'))

        # Explicitly provided table name and schema are rendered
        observation_schema = bq.get_table_schema('observation')
        ddl = bq.get_create_or_replace_table_ddl(
            self.project_id,
            self.dataset_id,
            table_id='custom_observation',
            schema=observation_schema).strip()
        self.assertTrue(
            ddl.startswith(
                f'CREATE OR REPLACE TABLE `{self.project_id}.{self.dataset_id}.custom_observation`'
            ))
        # Sanity check that observation schema is rendered
        self.assertTrue(
            all(field.description in ddl for field in observation_schema))
        self.assertTrue(ddl.endswith(')'))

        # Parameter as_query is rendered
        fake_as_query = "SELECT 1 FROM fake"
        ddl = bq.get_create_or_replace_table_ddl(
            self.project_id,
            self.dataset_id,
            'observation',
            as_query=fake_as_query).strip()
        self.assertTrue(
            ddl.startswith(
                f'CREATE OR REPLACE TABLE `{self.project_id}.{self.dataset_id}.observation`'
            ))
        self.assertTrue(ddl.endswith(fake_as_query))

    def test_to_scalar(self):
        scalar_int = dict(num=100)
        mock_iter = mock_query_result([scalar_int])
        result = bq.to_scalar(mock_iter)
        self.assertEqual(100, result)

        today = datetime.date.today()
        scalar_date = dict(today=today)
        mock_iter = mock_query_result([scalar_date])
        result = bq.to_scalar(mock_iter)
        self.assertEqual(today, result)

        now = datetime.datetime.now()
        scalar_datetime = dict(now=now)
        mock_iter = mock_query_result([scalar_datetime])
        result = bq.to_scalar(mock_iter)
        self.assertEqual(now, result)

        scalar_struct = dict(num_1=100, num_2=200)
        scalar_struct_iter = mock_query_result([scalar_struct],
                                               ['num_2', 'num_1'])
        result = bq.to_scalar(scalar_struct_iter)
        self.assertDictEqual(scalar_struct, result)

        scalar_int_1 = dict(num=1)
        scalar_int_2 = dict(num=2)
        mock_iter = mock_query_result([scalar_int_1, scalar_int_2])
        with self.assertRaises(ValueError) as c:
            bq.to_scalar(mock_iter)

    def _mock_client_with(self, table_ids):
        """
        Get a mock client
        :param table_ids:
        :return:
        """
        full_table_ids = [
            f'{self.project_id}.{self.dataset_id}.{table_id}'
            for table_id in table_ids
        ]
        list_tables_results = [
            list_item_from_table_id(table_id) for table_id in full_table_ids
        ]
        table_count_query_results = [dict(table_count=len(list_tables_results))]
        mock_client = MagicMock()
        mock_client.list_tables.return_value = list_tables_results
        mock_client.query.return_value = mock_query_result(
            table_count_query_results)
        return mock_client

    def test_list_tables(self):
        table_ids = ['table_1', 'table_2']
        table_count = len(table_ids)
        expected_max_results = table_count + bq._MAX_RESULTS_PADDING
        # mock client calls
        client = self._mock_client_with(table_ids)
        bq.list_tables(client, self.dataset_ref)
        client.list_tables.assert_called_with(dataset=self.dataset_ref,
                                              max_results=expected_max_results)

    @patch('utils.bq.bigquery.Client')
    def test_bq_client(self, mock_bq_client):
        credentials = MagicMock()
        bq.get_client(self.project_id, credentials=credentials)
        mock_bq_client.assert_called_once_with(project=self.project_id,
                                               credentials=credentials)
        bq.get_client(self.project_id, credentials=None)
        mock_bq_client.assert_called_with(project=self.project_id,
                                          credentials=None)
        self.assertRaises(TypeError, bq.get_client, project=None)

    @patch('utils.bq.bigquery.Client.copy_table')
    @patch('utils.bq.bigquery.Client.list_tables')
    @patch('utils.bq.bigquery.Client')
    def test_copy_datasets(self, mock_client, mock_list_tables,
                           mock_copy_table):
        full_table_ids = [
            f'{self.project_id}.{self.dataset_id}.{table_id}'
            for table_id in CDM_TABLES
        ]
        list_tables_results = [
            list_item_from_table_id(table_id) for table_id in full_table_ids
        ]
        mock_list_tables.return_value = list_tables_results

        bq.copy_datasets(mock_client, self.dataset_id,
                         f'{self.dataset_id}_snapshot')
        mock_list_tables.assert_called_once_with(self.dataset_id)
        self.assertEqual(mock_copy_table.call_count, len(list_tables_results))
