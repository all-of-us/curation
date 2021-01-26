import unittest
import mock
import analytics.cdr_ops.report_runner as runner
import pathlib
from collections import OrderedDict


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

        mock_is_parameter_required.side_effect = [True, False]
        provided_params = {'dataset_id': '23486219', 'old_rdr': '20200114'}
        result = runner.validate_notebook_params(notebook_path, provided_params)
        self.assertTrue(result)

        self.assertEqual(mock_is_parameter_required.call_count, 2)
        mock_infer_notebook_params.assert_any_call(notebook_path)

        mock_is_parameter_required.side_effect = [True, False]
        provided_params = {'dataset_id': None, 'old_rdr': '20200114'}
        result = runner.validate_notebook_params(notebook_path, provided_params)
        self.assertFalse(result)

        mock_is_parameter_required.side_effect = [True, False]
        provided_params = {'old_rdr': '20200114'}
        result = runner.validate_notebook_params(notebook_path, provided_params)
        self.assertFalse(result)

        mock_is_parameter_required.side_effect = [True, False]
        provided_params = {
            'dataset_id': '23486219',
            'old_rdr': '20200114',
            'new_rdr': '20210104'
        }
        result = runner.validate_notebook_params(notebook_path, provided_params)
        self.assertFalse(result)

        mock_is_parameter_required.side_effect = [True, False]


if __name__ == '__main__':
    unittest.main()