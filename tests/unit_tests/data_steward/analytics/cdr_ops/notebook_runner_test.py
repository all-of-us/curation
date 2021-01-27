import unittest
import mock
import analytics.cdr_ops.report_runner as runner
import pathlib
from collections import OrderedDict
from papermill.execute import execute_notebook

from papermill.exceptions import PapermillExecutionError


class TestNotebookRunner(unittest.TestCase):

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
        ipynb_path = 'my_notebook.ipynb'
        mock_create_ipynb_from_py.return_value = ipynb_path
        mock_validate_notebook_params.return_value = True

        notebook_jupytext_path = 'my_notebook.py'
        params = {'dataset_id': '3142352351', 'old_rdr': '20201003'}
        output_path = 'my_notebook.html'
        help_notebook = False

        runner.main(notebook_jupytext_path, params, output_path, help_notebook)

        #Test that html is created even after Papermill execution error
        mock_execute_notebook.side_effect = PapermillExecutionError(
            0, 1, 'test', 'test', 'test', '')
        mock_create_ipynb_from_py.return_value = ipynb_path
        mock_validate_notebook_params.return_value = True

        runner.main(notebook_jupytext_path, params, output_path, help_notebook)
        self.assertEqual(mock_create_html_from_ipynb.call_count, 2)


if __name__ == '__main__':
    unittest.main()