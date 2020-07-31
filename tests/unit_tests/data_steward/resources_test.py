import json
import mock
import os
import unittest

import common
import resources


class ResourcesTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def test_cdm_csv(self):
        cdm_data_rows = resources.cdm_csv()
        expected_keys = {
            'table_name', 'column_name', 'is_nullable', 'data_type',
            'description'
        }
        expected_table_names = {
            'person', 'visit_occurrence', 'condition_occurrence',
            'procedure_occurrence', 'drug_exposure', 'measurement'
        }
        actual_table_names = set()
        for row in cdm_data_rows:
            keys = set(row.keys())
            self.assertSetEqual(expected_keys, keys)
            actual_table_names.add(row['table_name'])

        for expected_table_name in expected_table_names:
            self.assertIn(expected_table_name, actual_table_names)

    def test_cdm_schemas(self):
        schemas = resources.cdm_schemas()
        table_names = schemas.keys()

        result_internal_tables = [
            table_name for table_name in table_names
            if resources.is_internal_table(table_name)
        ]
        self.assertCountEqual(
            [],
            result_internal_tables,
            msg='Internal tables %s should not be in result of cdm_schemas()' %
            result_internal_tables)

        achilles_tables = common.ACHILLES_TABLES + common.ACHILLES_HEEL_TABLES
        result_achilles_tables = [
            table_name for table_name in table_names
            if table_name in achilles_tables
        ]
        self.assertCountEqual(
            [],
            result_achilles_tables,
            msg='Achilles tables %s should not be in result of cdm_schemas()' %
            result_achilles_tables)

        result_vocab_tables = [
            table_name for table_name in table_names
            if table_name in resources.VOCABULARY_TABLES
        ]
        self.assertCountEqual(
            [],
            result_vocab_tables,
            msg='Vocabulary tables %s should not be in result of cdm_schemas()'
            % result_vocab_tables)

    def test_fields_for(self):
        """
        Testing that fields for works as expected with sub-directory structures.
        """
        # preconditions

        # test
        actual_fields = resources.fields_for('person')

        # post conditions
        person_path = os.path.join('data_steward', 'resource_files', 'fields',
                                   'person.json')
        with open(person_path, 'r') as fp:
            expected_fields = json.load(fp)

        self.assertEqual(actual_fields, expected_fields)

    @mock.patch('resources.os.walk')
    def test_fields_for_duplicate_files(self, mock_walk):
        """
        Testing that fields for works as expected with sub-directory structures.
        """
        # preconditions
        walk_results = [[
            os.path.join('foo', 'bar'), '', ['duplicate.json', 'unique1.json']
        ],
                        [
                            os.path.join('foo', 'bar', 'baz'), '',
                            ['duplicate.json', 'unique2.json']
                        ]]

        mock_walk.return_value = walk_results

        # test
        self.assertRaises(RuntimeError, resources.fields_for, 'duplicate')

        # test
        data = '[{"id": "fake id desc", "type": "fake type"}]'
        json_data = json.loads(data)
        with mock.patch('resources.open',
                        mock.mock_open(read_data=data)) as mock_file:
            with mock.patch('resources.json.load') as mock_json:
                mock_json.return_value = json_data
                fake_path = 'baz'
                actual_fields = resources.fields_for('duplicate', fake_path)
                self.assertEqual(actual_fields, json_data)

    @mock.patch('resources.os.walk')
    def test_fields_for_duplicate_files_with_defined_path(self, mock_walk):
        """
        Testing that fields for works as expected with sub-directory structures.
        """
        # preconditions
        walk_results = [[
            os.path.join('foo', 'bar', 'baz'), '',
            ['duplicate.json', 'unique2.json']
        ]]

        mock_walk.return_value = walk_results

        data = '[{"id": "fake id desc", "type": "fake type"}]'
        json_data = json.loads(data)

        # test
        with mock.patch('resources.open',
                        mock.mock_open(read_data=data)) as mock_file:
            with mock.patch('resources.json.load') as mock_json:
                mock_json.return_value = json_data
                fake_path = os.path.join('foo', 'bar', 'baz')
                actual_fields = resources.fields_for('duplicate', fake_path)

        # post conditions
        self.assertEqual(actual_fields, json_data)
