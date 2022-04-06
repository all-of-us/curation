# Python imports
from unittest import TestCase
import os
import typing
from unittest.mock import MagicMock

# Third party imports
from google.cloud import bigquery
from mock import patch
from google.cloud.bigquery import TableReference
from google.cloud.bigquery.table import TableListItem
from google.cloud.bigquery import DatasetReference

# Project imports
from gcloud.bq import BigQueryClient
import resources


class DummyClient(BigQueryClient):
    """
    A class which inherits all of BigQueryClient but doesn't authenticate
    """

    # pylint: disable=super-init-not-called
    def __init__(self):
        self.project: str = 'bar_project'

    def _get_all_field_types(self,) -> typing.FrozenSet[str]:
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

    def list_item_from_table_id(self, table_id: str) -> TableListItem:
        """
        Get a table list item as returned by :meth:`bigquery.Client.list_tables` 
        from a table ID
        
        :param table_id: A table ID including project ID, dataset ID, and table ID, 
        each separated by ``.``. 
        :return: a table list item
        """
        resource = {
            "tableReference":
                TableReference.from_string(table_id).to_api_repr()
        }
        return TableListItem(resource)


class BQCTest(TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.client = DummyClient()
        self.dataset_id: str = 'fake_dataset'
        self.description: str = 'fake_description'
        self.existing_labels_or_tags: dict = {'label': 'value', 'tag': ''}
        self.dataset_ref = DatasetReference(self.client.project,
                                            self.dataset_id)

    def test_get_table_ddl(self):
        # Schema is determined by table name
        ddl = self.client.get_create_or_replace_table_ddl(
            self.dataset_id, 'observation').strip()
        self.assertTrue(
            ddl.startswith(
                f'CREATE OR REPLACE TABLE `{self.client.project}.{self.dataset_id}.observation`'
            ))
        self.assertTrue(ddl.endswith(')'))

        # Explicitly provided table name and schema are rendered
        observation_schema = self.client.get_table_schema('observation')
        ddl = self.client.get_create_or_replace_table_ddl(
            self.dataset_id,
            table_id='custom_observation',
            schema=observation_schema).strip()
        self.assertTrue(
            ddl.startswith(
                f'CREATE OR REPLACE TABLE `{self.client.project}.{self.dataset_id}.custom_observation`'
            ))
        # Sanity check that observation schema is rendered
        self.assertTrue(
            all(field.description in ddl for field in observation_schema))
        self.assertTrue(ddl.endswith(')'))

        # Parameter as_query is rendered
        fake_as_query = "SELECT 1 FROM fake"
        ddl = self.client.get_create_or_replace_table_ddl(
            self.dataset_id, 'observation', as_query=fake_as_query).strip()
        self.assertTrue(
            ddl.startswith(
                f'CREATE OR REPLACE TABLE `{self.client.project}.{self.dataset_id}.observation`'
            ))
        self.assertTrue(ddl.endswith(fake_as_query))

    def test_define_dataset(self):
        # Tests if dataset_id is given
        self.assertRaises(RuntimeError, self.client.define_dataset, None,
                          self.description, self.existing_labels_or_tags)

        # Tests if description is given
        self.assertRaises(RuntimeError, self.client.define_dataset,
                          self.dataset_id, (None or ''),
                          self.existing_labels_or_tags)

        # Tests if no label or tag is given
        self.assertRaises(RuntimeError, self.client.define_dataset,
                          self.dataset_id, self.description, None)

        # Pre-conditions
        results = self.client.define_dataset(self.dataset_id, self.description,
                                             self.existing_labels_or_tags)

        # Post conditions
        self.assertIsInstance(results, bigquery.Dataset)
        self.assertEqual(results.labels, self.existing_labels_or_tags)

    def test_get_table_schema(self):
        actual_fields = self.client.get_table_schema(
            'digital_health_sharing_status')

        for field in actual_fields:
            if field.field_type.upper() == "RECORD":
                self.assertEqual(len(field.fields), 2)

    def test_to_standard_sql_type(self):
        # All types used in schema files should successfully map to standard sql types
        all_field_types = self.client._get_all_field_types()
        for field_type in all_field_types:
            result = self.client._to_standard_sql_type(field_type)
            self.assertTrue(result)

        # Unknown types should raise ValueError
        with self.assertRaises(ValueError) as c:
            self.client._to_standard_sql_type('unknown_type')
            self.assertEqual(str(c.exception),
                             f'unknown_type is not a valid field type')

    @patch.object(BigQueryClient, 'copy_table')
    @patch('gcloud.bq.Client.list_tables')
    def test_copy_datasets(self, mock_list_tables, mock_copy_table):
        full_table_ids = [
            f'{self.client.project}.{self.dataset_id}.{table_id}'
            for table_id in resources.CDM_TABLES
        ]
        list_tables_results = [
            self.client.list_item_from_table_id(table_id)
            for table_id in full_table_ids
        ]
        mock_list_tables.return_value = list_tables_results

        self.client.copy_datasets(self.dataset_id,
                                  f'{self.dataset_id}_snapshot')
        mock_list_tables.assert_called_once_with(self.dataset_id)
        self.assertEqual(mock_copy_table.call_count, len(list_tables_results))

    @patch('gcloud.bq.Client.list_tables')
    def test_list_tables(self, mock_list_tables):
        #pre conditions
        table_ids = ['table_1', 'table_2']
        table_count = len(table_ids)
        _MAX_RESULTS_PADDING = 100
        expected_max_results = table_count + _MAX_RESULTS_PADDING
        mock_list_results = MagicMock()
        mock_list_tables.side_effect = [mock_list_results, mock_list_tables]
        mock_list_results.num_results = table_count
        self.client.list_tables(self.dataset_ref)
        #post condition
        mock_list_tables.assert_called_with(dataset=self.dataset_ref,
                                            max_results=expected_max_results)
