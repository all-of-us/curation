# Python imports
import datetime
from unittest import TestCase
import os
from typing import FrozenSet, List, Union, Dict, Iterable, Any
from collections import OrderedDict

# Third party imports
from unittest.mock import ANY


from google.cloud import bigquery
from google.cloud.bigquery import TableReference, DatasetReference
from google.cloud.bigquery.table import TableListItem
from google.cloud.exceptions import NotFound
from mock import patch, MagicMock, Mock, call

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

    def _get_all_field_types(self,) -> FrozenSet[str]:
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

    def list_item_from_table_id(self, fq_table_id: str) -> TableListItem:
        """
        Get a table list item as returned by :meth:`bigquery.Client.list_tables`
        from a fully qualified table ID

        :param table_id: A fully qualified table ID that includes project ID, dataset ID, and table ID,
        each separated by ``.``.
        :return: a table list item
        """
        resource = {
            "tableReference":
                TableReference.from_string(fq_table_id).to_api_repr()
        }
        return TableListItem(resource)

    def mock_query_result(self,
                          rows: List[Union[Dict, OrderedDict]],
                          key_order: Iterable[Any] = None):
        """
        Create a mock RowIterator as returned by :meth:`bigquery.QueryJob.result`
        from rows represented as a list of dictionaries

        :param rows: A list of dictionaries representing result rows
        :param key_order: If `rows` refers to a list of dict rather than OrderedDict,
            specifies how fields are ordered in the result schema. This parameter is
            ignored if `rows` refers to a list of OrderedDict.
        :return: a mock RowIterator
        """
        mock_row_iter = MagicMock(spec=bigquery.table.RowIterator)
        mock_row_iter.total_rows = len(rows)
        row0 = rows[0]
        if isinstance(row0, OrderedDict):
            _rows = rows
        else:
            _rows = []
            for row in rows:
                if len(row) == 1:
                    _rows.append(OrderedDict(row))
                    continue
                else:
                    if key_order is None:
                        raise ValueError(
                            'Parameter key_order is required in order to convert'
                            ' a dict with multiple items to OrderedDict')
                _rows.append(OrderedDict((key, row[key]) for key in key_order))
            row0 = _rows[0]

        mock_row_iter.schema = list(self.fields_from_dict(row0))
        field_to_index = {key: i for i, key in enumerate(row0.keys())}
        mock_row_iter.__iter__ = Mock(return_value=iter([
            bigquery.table.Row(list(row.values()), field_to_index)
            for row in _rows
        ]))
        return mock_row_iter

    def fields_from_dict(self, row):
        """
        Get schema fields from a row represented as a dictionary

        :param row: the dictionary to infer schema for
        :return: list of schema field objects

        Example:
            >>> from tests import bq_test_helpers
            >>> d = {'item': 'book', 'qty': 2, 'price': 1.99}
            >>> bq_test_helpers.fields_from_dict(d)
            [SchemaField('item', 'STRING', 'NULLABLE', None, (), None), SchemaField('qty', 'INT64', 'NULLABLE', None, (), None), SchemaField('price', 'FLOAT64', 'NULLABLE', None, (), None)]
        """
        return [
            self._field_from_key_value(key, value)
            for key, value in row.items()
        ]

    def _field_from_key_value(self, key: str,
                              value: Any) -> bigquery.SchemaField:
        """
        Get a schema field object from a key and value

        :param key: name of the field
        :param value: value of the field
        :return: an appropriate schema field object
        """
        _TYPE_TO_FIELD_TYPE = {
            str(int): 'INT64',
            str(str): 'STRING',
            str(float): 'FLOAT64',
            str(datetime.date): 'DATE',
            str(datetime.datetime): 'TIMESTAMP'
        }
        tpe = str(type(value))
        field_type = _TYPE_TO_FIELD_TYPE.get(tpe)
        if not field_type:
            raise NotImplementedError(f'The type for {value} is not supported')
        return bigquery.SchemaField(name=key, field_type=field_type)


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
    @patch('gcloud.bq.Client.list_jobs')
    def test_copy_dataset(self, mock_list_jobs, mock_list_tables,
                          mock_copy_table):
        jobs = []
        fake_job_ids = []
        for i in resources.CDM_TABLES:
            fake_job = MagicMock()
            fake_job_id = f'fake_job_{i.lower()}'
            fake_job_ids.append(fake_job_id)
            fake_job.job_id = fake_job_id
            jobs.append(fake_job)
        mock_copy_table.side_effect = jobs
        mock_list_jobs.return_value = jobs
        mock_job_config = MagicMock()
        mock_job_config.labels = {'foo_key': 'bar_value'}

        full_table_ids = [
            f'{self.client.project}.{self.dataset_id}.{table_id}'
            for table_id in resources.CDM_TABLES
        ]
        list_tables_results = [
            self.client.list_item_from_table_id(table_id)
            for table_id in full_table_ids
        ]
        mock_list_tables.return_value = list_tables_results

        self.client.copy_dataset(
            f'{self.client.project}.{self.dataset_id}',
            f'{self.client.project}.{self.dataset_id}_snapshot',
            job_config=mock_job_config)
        mock_list_tables.assert_called_once_with(
            f'{self.client.project}.{self.dataset_id}')
        self.assertEqual(mock_copy_table.call_count, len(list_tables_results))
        expected_calls = [
            call(
                table_object,
                f'{self.client.project}.{self.dataset_id}_snapshot.{table_object.table_id}',
                job_config=mock_job_config)
            for table_object in list_tables_results
        ]
        mock_copy_table.assert_has_calls(expected_calls)

    @patch('gcloud.bq.Client.list_jobs')
    def test_wait_on_jobs(self, mock_list_jobs):
        jobs = []
        fake_job_ids = []
        for i in range(1, 4):
            fake_job = MagicMock()
            fake_job_id = f'fake_job_{i}'
            fake_job_ids.append(fake_job_id)
            fake_job.job_id = fake_job_id
            jobs.append(fake_job)
        mock_list_jobs.return_value = jobs

        self.client.wait_on_jobs(fake_job_ids)
        mock_list_jobs.assert_called_once_with(max_results=9,
                                               state_filter='DONE',
                                               retry=ANY)
        self.assertEqual(mock_list_jobs.call_count, 1)

    @patch.object(BigQueryClient, 'get_dataset')
    @patch.object(BigQueryClient, 'get_table_count')
    @patch('gcloud.bq.Client.list_tables')
    def test_list_tables(self, mock_list_tables, mock_get_table_count,
                         mock_get_dataset):
        #pre conditions
        table_ids = ['table_1', 'table_2']
        table_count = len(table_ids)
        mock_get_table_count.return_value = table_count
        mock_get_dataset.return_value = self.dataset_ref
        _MAX_RESULTS_PADDING = 100
        expected_max_results = table_count + _MAX_RESULTS_PADDING

        self.client.list_tables(self.dataset_ref)
        #post conditions
        mock_list_tables.assert_called_with(dataset=self.dataset_ref,
                                            max_results=expected_max_results)

    def test_to_scalar(self):
        scalar_int = dict(num=100)
        mock_iter = self.client.mock_query_result([scalar_int])
        result = self.client.to_scalar(mock_iter)
        self.assertEqual(100, result)

        today = datetime.date.today()
        scalar_date = dict(today=today)
        mock_iter = self.client.mock_query_result([scalar_date])
        result = self.client.to_scalar(mock_iter)
        self.assertEqual(today, result)

        now = datetime.datetime.now()
        scalar_datetime = dict(now=now)
        mock_iter = self.client.mock_query_result([scalar_datetime])
        result = self.client.to_scalar(mock_iter)
        self.assertEqual(now, result)

        scalar_struct = dict(num_1=100, num_2=200)
        scalar_struct_iter = self.client.mock_query_result([scalar_struct],
                                                           ['num_2', 'num_1'])
        result = self.client.to_scalar(scalar_struct_iter)
        self.assertDictEqual(scalar_struct, result)

        scalar_int_1 = dict(num=1)
        scalar_int_2 = dict(num=2)
        mock_iter = self.client.mock_query_result([scalar_int_1, scalar_int_2])
        with self.assertRaises(ValueError) as c:
            self.client.to_scalar(mock_iter)

    def test_update_labels_and_tags(self):
        new_labels_or_tags = {'label': 'new_value', 'new_tag': ''}
        updated_labels_or_tags = {
            'tag': '',
            'label': 'new_value',
            'new_tag': ''
        }

        # Tests if dataset_id param is provided
        self.assertRaises(RuntimeError, self.client.update_labels_and_tags,
                          None, self.existing_labels_or_tags,
                          new_labels_or_tags)

        # Tests if existing_labels_or_tags param is provided
        self.assertRaises(RuntimeError, self.client.update_labels_and_tags,
                          self.dataset_id, None, new_labels_or_tags)

        # Tests if new_labels_or_tags param is provided
        self.assertRaises(RuntimeError, self.client.update_labels_and_tags,
                          self.dataset_id, self.existing_labels_or_tags, None)

        # Pre-conditions
        results = self.client.update_labels_and_tags(
            self.dataset_id, self.existing_labels_or_tags, new_labels_or_tags,
            True)

        # Post conditions
        self.assertEqual(results, updated_labels_or_tags)
        with self.assertRaises(RuntimeError):
            self.client.update_labels_and_tags(
                self.dataset_id,
                existing_labels_or_tags={'label': 'existing_label'},
                new_labels_or_tags={'label': 'new_label'},
                overwrite_ok=False)

    @patch('os.environ.get')
    @patch('gcloud.bq.Client.get_table')
    def test_table_exists(self, mock_get_table, mock_environ_get):
        table_id = 'fake_table'
        table_name = f'{self.client.project}.{self.dataset_id}.{table_id}'
        mock_environ_get.return_value = self.dataset_id

        # Test case 1 ... dataset_id not provided
        result = self.client.table_exists(table_id)
        self.assertEqual(result, True)
        mock_environ_get.assert_called_once()
        mock_get_table.assert_called_with(table_name)

        # Test case 2 ... whitespaces for dataset_id
        mock_environ_get.call_count = 0
        result = self.client.table_exists(table_id, ' ')
        self.assertEqual(result, True)
        mock_environ_get.assert_called_once()
        mock_get_table.assert_called_with(table_name)

        # Test case 3 ... dataset_id provided
        mock_environ_get.call_count = 0
        result = self.client.table_exists(table_id, self.dataset_id)
        self.assertEqual(result, True)
        self.assertEqual(mock_environ_get.call_count, 0)
        mock_get_table.assert_called_with(table_name)

        #Test case 4 ... table_id not provided
        self.assertRaises(RuntimeError, self.client.table_exists, None)

        #Test case 5 ... whitespaces for table_id
        self.assertRaises(RuntimeError, self.client.table_exists, ' ')

        # Test case 6 ... NotFound exception, dataset_id provided
        mock_get_table.side_effect = NotFound('')
        result = self.client.table_exists(table_id, self.dataset_id)
        self.assertEqual(result, False)
        self.assertEqual(mock_environ_get.call_count, 0)
        mock_get_table.assert_called_with(table_name)

        #Test case 7 ... NotFound exception, dataset_id not provided
        result = self.client.table_exists(table_id)
        self.assertEqual(result, False)
        mock_environ_get.assert_called_once()
        mock_get_table.assert_called_with(table_name)

        #Test case 8 ... NotFound exception, whitespaces for dataset_id
        mock_environ_get.call_count = 0
        result = self.client.table_exists(table_id, ' ')
        self.assertEqual(result, False)
        mock_environ_get.assert_called_once()
        mock_get_table.assert_called_with(table_name)
