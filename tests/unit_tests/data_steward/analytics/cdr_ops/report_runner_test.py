import unittest
import mock
import analytics.cdr_ops.report_runner as runner
from collections import OrderedDict
import copy
from typing import Any, Dict

from papermill.exceptions import PapermillExecutionError


class TestNotebookRunner(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.notebook_py_path = 'my_notebook_path.py'
        self.notebook_ipynb_path = 'my_notebook_path.ipynb'
        self.notebook_html_path = 'my_notebook_path.html'

    @mock.patch('jupytext.write')
    @mock.patch('jupytext.read')
    @mock.patch('analytics.cdr_ops.report_runner.PurePath')
    def test_create_ipynb_from_py(self, mock_pure_path, mock_read, mock_write):
        # Define the return object for PurePath constructor
        pure_path_returned_value = mock.MagicMock(
            name='returned_value_pure_path', return_value=self.notebook_py_path)
        mock_pure_path.return_value = pure_path_returned_value

        # Set up with_suffix
        mock_with_suffix = mock_pure_path.return_value.with_suffix
        with_suffix_returned_value = mock.MagicMock(
            name='with_suffix', return_value=self.notebook_ipynb_path)
        # This makes sure str(MagicMock) returns the desired value
        with_suffix_returned_value.__str__.return_value = self.notebook_ipynb_path
        mock_with_suffix.return_value = with_suffix_returned_value

        # Set up jupytext.read value
        jupytext_returned_value = mock.MagicMock(name='mock_read_return_value')
        mock_read.return_value = jupytext_returned_value

        # Assertions
        actual_value = runner.create_ipynb_from_py(self.notebook_py_path)
        self.assertEqual(self.notebook_ipynb_path, actual_value)

        mock_pure_path.assert_called_once_with(self.notebook_py_path)
        mock_read.assert_called_once_with(pure_path_returned_value)
        mock_with_suffix.assert_called_once_with(runner.IPYNB_SUFFIX)
        mock_write.assert_called_once_with(jupytext_returned_value,
                                           with_suffix_returned_value)

    @mock.patch('nbformat.reads')
    @mock.patch('builtins.open',
                new_callable=mock.mock_open,
                read_data='fake_data')
    @mock.patch('analytics.cdr_ops.report_runner.HTMLExporter')
    @mock.patch('analytics.cdr_ops.report_runner.PurePath')
    def test_create_html_from_ipynb(self, mock_pure_path, mock_html_exporter,
                                    mock_open, mock_nbformat_reads):
        # Define the return object for PurePath constructor
        pure_path_returned_value = mock.MagicMock(
            name='returned_value_pure_path',
            return_value=self.notebook_ipynb_path)
        mock_pure_path.return_value = pure_path_returned_value

        # Set up with_suffix
        mock_with_suffix = mock_pure_path.return_value.with_suffix
        with_suffix_returned_value = mock.MagicMock(
            name='with_suffix', return_value=self.notebook_html_path)
        # This makes sure str(MagicMock) returns the desired value
        with_suffix_returned_value.__str__.return_value = self.notebook_html_path
        mock_with_suffix.return_value = with_suffix_returned_value

        # Set up html_exporter
        mock_html_exporter.return_value.from_notebook_node.return_value = (
            'return fake_data', '')

        runner.create_html_from_ipynb(self.notebook_ipynb_path)

        # Assertions in reading the notebook
        mock_open.assert_any_call(self.notebook_ipynb_path,
                                  'r',
                                  encoding='utf-8')
        mock_nbformat_reads.assert_any_call('fake_data', as_version=4)
        mock_html_exporter.return_value.from_notebook_node.assert_any_call(
            mock_nbformat_reads.return_value)

        # Assertions in writing the notebook to a html page
        mock_open.assert_any_call(with_suffix_returned_value,
                                  'w',
                                  encoding='utf-8')
        mock_open.return_value.write.assert_any_call('return fake_data')

    def test_infer_required(self):

        def create_base_dict() -> Dict[Any, Any]:
            return OrderedDict({'name': 'dataset_id', 'default': '""'})

        base_dict = create_base_dict()

        # Case 1 default = '""'
        actual = runner.infer_required(base_dict)
        expected = copy.deepcopy(base_dict)
        expected['required'] = True
        self.assertEqual(actual, expected)

        # Case 2 default = '\'\''
        base_dict['default'] = '\'\''
        actual = runner.infer_required(base_dict)
        expected = copy.deepcopy(base_dict)
        expected['required'] = True
        self.assertEqual(actual, expected)

        # Case 3 default = 'None'
        base_dict['default'] = 'None'
        actual = runner.infer_required(base_dict)
        expected = copy.deepcopy(base_dict)
        expected['required'] = True
        self.assertEqual(actual, expected)

        # Case 4 default = None
        base_dict['default'] = None
        actual = runner.infer_required(base_dict)
        expected = copy.deepcopy(base_dict)
        expected['required'] = True
        self.assertEqual(actual, expected)

        # Case 4 default = 'dataset_id'
        base_dict['default'] = 'dataset_id'
        actual = runner.infer_required(base_dict)
        expected = copy.deepcopy(base_dict)
        expected['required'] = False
        self.assertEqual(actual, expected)

    @mock.patch('analytics.cdr_ops.report_runner.is_parameter_required')
    @mock.patch('analytics.cdr_ops.report_runner.infer_notebook_params')
    def test_validate_notebook_params(self, mock_infer_notebook_params,
                                      mock_is_parameter_required):

        mock_infer_notebook_params.return_value = [
            ('dataset_id', OrderedDict({
                'name': 'dataset_id',
                'type': 'string'
            })),
            ('old_rdr', OrderedDict({
                'name': 'old_rdr',
                'type': 'string'
            })),
        ]

        notebook_path = 'my_notebook_path.ipynb'

        #Test normal case
        mock_is_parameter_required.side_effect = [True, False]
        provided_params = {'dataset_id': '23486219', 'old_rdr': '20200114'}
        result = runner.validate_notebook_params(notebook_path, provided_params)
        self.assertTrue(result)

        #Test expected call counts
        self.assertEqual(mock_is_parameter_required.call_count, 2)
        mock_infer_notebook_params.assert_any_call(notebook_path)

        #Test missing value
        mock_is_parameter_required.side_effect = [True, False]
        provided_params = {'dataset_id': None, 'old_rdr': '20200114'}
        result = runner.validate_notebook_params(notebook_path, provided_params)
        self.assertFalse(result)

        #Test missing parameter
        mock_is_parameter_required.side_effect = [True, False]
        provided_params = {'old_rdr': '20200114'}
        result = runner.validate_notebook_params(notebook_path, provided_params)
        self.assertFalse(result)

        #Test unknown parameter
        mock_is_parameter_required.side_effect = [True, False]
        provided_params = {
            'dataset_id': '23486219',
            'old_rdr': '20200114',
            'new_rdr': '20210104'
        }
        result = runner.validate_notebook_params(notebook_path, provided_params)
        self.assertFalse(result)

    @mock.patch('analytics.cdr_ops.report_runner.infer_notebook_params')
    def test_display_notebook_help(self, mock_infer_notebook_params):
        #Doesn't do much, but useful for testing if function runs
        mock_infer_notebook_params.return_value = [
            ('dataset_id',
             OrderedDict({
                 'name': 'dataset_id',
                 'inferred_type_name': 'str',
                 'default': '',
                 'required': True,
                 'help': 'help 1'
             })),
            ('old_rdr',
             OrderedDict({
                 'name': 'old_rdr',
                 'inferred_type_name': 'str',
                 'default': 'str',
                 'required': True,
                 'help': 'help 2'
             })),
        ]

        notebook_path = 'my_notebook_path.ipynb'

        runner.display_notebook_help(notebook_path)

    def test_is_parameter_required(self):
        # value of required=True should return True
        properties = OrderedDict({
            'name': 'dataset_id',
            'type': 'string',
            'required': True
        })

        result = runner.is_parameter_required(properties)
        self.assertTrue(result)

        properties = OrderedDict({'name': 'dataset_id', 'type': 'string'})

        result = runner.is_parameter_required(properties)
        self.assertTrue(result)

        # value of required=True should return False
        properties = OrderedDict({
            'name': 'dataset_id',
            'type': 'string',
            'required': False
        })

        result = runner.is_parameter_required(properties)
        self.assertFalse(result)

    @mock.patch('analytics.cdr_ops.report_runner.create_html_from_ipynb')
    @mock.patch('analytics.cdr_ops.report_runner.execute_notebook')
    @mock.patch('analytics.cdr_ops.report_runner.display_notebook_help')
    @mock.patch('analytics.cdr_ops.report_runner.validate_notebook_params')
    @mock.patch('analytics.cdr_ops.report_runner.create_ipynb_from_py')
    def test_main(self, mock_create_ipynb_from_py,
                  mock_validate_notebook_params, mock_display_notebook_help,
                  mock_execute_notebook, mock_create_html_from_ipynb):
        ipynb_path = self.notebook_ipynb_path
        mock_create_ipynb_from_py.return_value = ipynb_path

        #Case where help_notebook == True
        mock_validate_notebook_params.return_value = True
        notebook_jupytext_path = self.notebook_py_path
        params = {'dataset_id': '3142352351', 'old_rdr': '20201003'}
        output_path = 'my_notebook.html'
        help_notebook = True

        with self.assertRaises(SystemExit):
            runner.main(notebook_jupytext_path, params, output_path,
                        help_notebook)

        mock_display_notebook_help.assert_called_once_with(ipynb_path)

        #Case where help_notebook == False and notebook params invalid
        mock_display_notebook_help.reset_mock()

        mock_validate_notebook_params.return_value = False
        notebook_jupytext_path = self.notebook_py_path
        params = {'dataset_id': '3142352351', 'old_rdr': '20201003'}
        output_path = 'my_notebook.html'
        help_notebook = False

        with self.assertRaises(SystemExit):
            runner.main(notebook_jupytext_path, params, output_path,
                        help_notebook)

        mock_display_notebook_help.assert_called_once_with(ipynb_path)

        # Case where help_notebook == False and notebook params valid
        mock_display_notebook_help.reset_mock()

        mock_validate_notebook_params.return_value = True
        notebook_jupytext_path = 'my_notebook.py'
        params = {'dataset_id': '3142352351', 'old_rdr': '20201003'}
        output_path = 'my_notebook.html'
        help_notebook = False

        runner.main(notebook_jupytext_path, params, output_path, help_notebook)
        mock_execute_notebook.assert_called_once()

        #Test that html is created even after Papermill execution error
        mock_execute_notebook.side_effect = PapermillExecutionError(
            0, 1, 'test', 'test', 'test', '')

        mock_execute_notebook.reset_mock()
        mock_create_html_from_ipynb.reset_mock()

        runner.main(notebook_jupytext_path, params, output_path, help_notebook)
        mock_execute_notebook.assert_called_once()
        mock_create_html_from_ipynb.assert_called_once()


if __name__ == '__main__':
    unittest.main()
