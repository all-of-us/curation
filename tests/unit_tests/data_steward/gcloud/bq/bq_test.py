# Python imports
from unittest import TestCase
import os
import typing

# Third party imports
from google.cloud import bigquery

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


class BQCTest(TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.client = DummyClient()
        self.dataset_id: str = 'fake_dataset'
        self.description = 'fake_description'
        self.existing_labels_or_tags = {'label': 'value', 'tag': ''}

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