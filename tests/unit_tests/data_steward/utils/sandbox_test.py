import unittest
from unittest.mock import patch

from utils import sandbox


class SandboxTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.table_namer = 'controlled'

    def test_get_sandbox_table_name(self):
        base_name = 'base_name'
        expected = f'{self.table_namer}_{base_name}'
        actual = sandbox.get_sandbox_table_name(self.table_namer, base_name)
        self.assertEqual(expected, actual)
        self.assertEqual(base_name,
                         sandbox.get_sandbox_table_name(None, base_name))
        self.assertEqual(base_name,
                         sandbox.get_sandbox_table_name('', base_name))
        self.assertEqual(base_name,
                         sandbox.get_sandbox_table_name('   ', base_name))
        self.assertEqual(base_name,
                         sandbox.get_sandbox_table_name('\t', base_name))
        self.assertEqual(base_name,
                         sandbox.get_sandbox_table_name('\n', base_name))

    def test_get_sandbox_labels_string(self):
        dataset_name = "my_dataset_name"
        class_name = "my_class_name"
        table_tag = "my_table_tag"
        shared_lookup = True

        #Test that standard arguments run without error
        actual = sandbox.get_sandbox_labels_string(dataset_name,
                                                   class_name,
                                                   table_tag,
                                                   shared_lookup=shared_lookup)
        self.assertIn(f'("src_dataset", "{dataset_name}")', actual)
        self.assertIn(f'("class_name", "{class_name}")', actual)
        self.assertIn(f'("table_tag", "{table_tag}")', actual)
        self.assertIn(f'("shared_lookup", "true")', actual)

        #Test that an empty string argument will throw an error
        empty_class_name = ""
        with self.assertRaises(ValueError):
            sandbox.get_sandbox_labels_string(dataset_name,
                                              empty_class_name,
                                              table_tag,
                                              shared_lookup=shared_lookup)

        #Test that a non-boolean shared_lookup argument will throw an error
        non_bool_shared_lookup = "Yes"
        with self.assertRaises(ValueError):
            sandbox.get_sandbox_labels_string(
                dataset_name,
                empty_class_name,
                table_tag,
                shared_lookup=non_bool_shared_lookup)

    def test_get_sandbox_table_description_string(self):
        description = "This is a helpful description."

        actual = sandbox.get_sandbox_table_description_string(description)
        self.assertIn(f'description="{description}"', actual)

        #Test description empty
        description = "    "
        with self.assertRaises(ValueError):
            sandbox.get_sandbox_table_description_string(description)

    @patch('utils.sandbox.get_sandbox_labels_string')
    @patch('utils.sandbox.get_sandbox_table_description_string')
    def test_get_sandbox_options(self,
                                 mock_get_sandbox_table_description_string,
                                 mock_get_sandbox_labels_string):

        self.maxDiff = None

        dataset_name = "my_dataset_name"
        class_name = "my_class_name"
        table_tag = "my_table_tag"
        shared_lookup = True
        description = "This is a helpful description."

        labels_string = f"""
            labels=[
                ("src_dataset", "{dataset_name}"),
                ("class_name", "{class_name}"),
                ("table_tag", "{table_tag}"),
                ("shared_lookup", "true")
            ]
        """

        description_string = f"""
            description="{description}"
        """

        mock_get_sandbox_labels_string.return_value = labels_string

        mock_get_sandbox_table_description_string.return_value = description_string

        #Test that contents are formatted as expected
        actual_options = sandbox.get_sandbox_options(dataset_name, class_name,
                                                     table_tag, description,
                                                     shared_lookup)

        expected_options = sandbox.TABLE_OPTIONS_CLAUSE.render(
            contents=',\n'.join([description_string, labels_string]))

        self.assertEqual(actual_options, expected_options)
