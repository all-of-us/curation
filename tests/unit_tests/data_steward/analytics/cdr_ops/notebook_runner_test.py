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

        mock_is_parameter_required.side_effect = [True, False]

        notebook_path = 'my_notebook_path.ipynb'
        provided_params = {'dataset_id': False, 'old_rdr': '20200114'}

        success = runner.validate_notebook_params(notebook_path,
                                                  provided_params)

        self.assertTrue(success)


if __name__ == '__main__':
    unittest.main()