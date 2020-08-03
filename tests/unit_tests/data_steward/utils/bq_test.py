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
import os
import typing
import unittest

# Third-party imports
from google.cloud import bigquery

# Project imports
import resources
from utils.bq import (define_dataset, update_labels_and_tags,
                      get_create_or_replace_table_ddl, _to_standard_sql_type,
                      get_table_schema)


def _get_all_field_types() -> typing.FrozenSet[str]:
    """
    Helper to get all field types referenced in fields (json) files

    :return: names of all types in fields files
    """
    all_field_types = set()
    fields_files = os.listdir(resources.fields_path)
    for fields_file in fields_files:
        table, _ = os.path.splitext(os.path.basename(fields_file))
        fields = resources.fields_for(table)
        for field in fields:
            all_field_types.add(field['type'])
    return frozenset(all_field_types)


class BqTest(unittest.TestCase):

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

    def test_define_dataset(self):
        # Tests if project_id is given
        self.assertRaises(RuntimeError, define_dataset, None, self.dataset_id,
                          self.description, self.existing_labels_or_tags)

        # Tests if dataset_id is given
        self.assertRaises(RuntimeError, define_dataset, self.project_id, None,
                          self.description, self.existing_labels_or_tags)

        # Tests if description is given
        self.assertRaises(RuntimeError, define_dataset, self.project_id,
                          self.dataset_id, (None or ''),
                          self.existing_labels_or_tags)

        # Tests if no label or tag is given
        self.assertRaises(RuntimeError, define_dataset, self.project_id,
                          self.dataset_id, self.description, None)

        # Pre-conditions
        results = define_dataset(self.project_id, self.dataset_id,
                                 self.description, self.existing_labels_or_tags)

        # Post conditions
        self.assertIsInstance(results, bigquery.Dataset)
        self.assertEqual(results.labels, self.existing_labels_or_tags)

    def test_update_labels_and_tags(self):
        # Tests if dataset_id param is provided
        self.assertRaises(RuntimeError, update_labels_and_tags, None,
                          self.existing_labels_or_tags, self.new_labels_or_tags)

        # Tests if new_labels_or_tags param is provided
        self.assertRaises(RuntimeError, update_labels_and_tags, self.dataset_id,
                          self.existing_labels_or_tags, None)

        # Pre-conditions
        results = update_labels_and_tags(self.dataset_id,
                                         self.existing_labels_or_tags,
                                         self.new_labels_or_tags, True)

        # Post conditions
        self.assertEqual(results, self.updated)
        with self.assertRaises(RuntimeError):
            update_labels_and_tags(self.dataset_id,
                                   existing_labels_or_tags={'label': 'apples'},
                                   new_labels_or_tags={'label': 'oranges'},
                                   overwrite_ok=False)

    def test_to_standard_sql_type(self):
        # All types used in schema files should successfully map to standard sql types
        all_field_types = _get_all_field_types()
        for field_type in all_field_types:
            result = _to_standard_sql_type(field_type)
            self.assertTrue(result)

        # Unknown types should raise ValueError
        with self.assertRaises(ValueError) as c:
            _to_standard_sql_type('unknown_type')
            self.assertEqual(str(c.exception),
                             f'unknown_type is not a valid field type')

    def test_get_table_ddl(self):
        # Schema is determined by table name
        ddl = get_create_or_replace_table_ddl(self.project_id, self.dataset_id,
                                              'observation').strip()
        self.assertTrue(
            ddl.startswith(
                f'CREATE OR REPLACE TABLE `{self.project_id}.{self.dataset_id}.observation`'
            ))
        self.assertTrue(ddl.endswith(')'))

        # Explicitly provided table name and schema are rendered
        observation_schema = get_table_schema('observation')
        ddl = get_create_or_replace_table_ddl(
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
        ddl = get_create_or_replace_table_ddl(self.project_id,
                                              self.dataset_id,
                                              'observation',
                                              as_query=fake_as_query).strip()
        self.assertTrue(
            ddl.startswith(
                f'CREATE OR REPLACE TABLE `{self.project_id}.{self.dataset_id}.observation`'
            ))
        self.assertTrue(ddl.endswith(fake_as_query))
