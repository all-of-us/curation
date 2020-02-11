import mock
import unittest
from cdr_cleaner import clean_cdr
from constants.cdr_cleaner import clean_cdr as cdr_consts


class CleanCDRTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.project_id = 'test project'
        self.dataset_id = 'test dataset'
        self.sandbox_dataset_id = 'test sandbox'
        self.dest_table = 'dest_table'

        self.function_name = 'anonymous function'
        self.model_name = 'test module'
        self.line_no = 1

        self.query_dict = {
            cdr_consts.QUERY: 'query',
            cdr_consts.DESTINATION_DATASET: self.dataset_id,
            cdr_consts.DESTINATION_TABLE: self.dest_table
        }

        self.module_info_dict = {
            cdr_consts.MODULE_NAME: self.model_name,
            cdr_consts.FUNCTION_NAME: self.function_name,
            cdr_consts.LINE_NO: self.line_no
        }

    @mock.patch('inspect.getsourcelines')
    @mock.patch('inspect.getmodule')
    def test_add_module_info_decorator(self, mock_get_module,
                                       mock_getsourcelines):
        mock_function = mock.Mock(__name__=self.function_name)
        mock_function.return_value = [self.query_dict]
        mock_get_module.return_value = mock.Mock(__name__=self.model_name)
        mock_getsourcelines.return_value = (dict(), self.line_no)

        actual_query_dict = clean_cdr.add_module_info_decorator(
            mock_function, self.project_id, self.dataset_id,
            self.sandbox_dataset_id)

        expected_query_dict = [dict(**self.query_dict, **self.module_info_dict)]

        self.assertListEqual(actual_query_dict, expected_query_dict)

        mock_get_module.assert_called_once_with(mock_function)
        mock_getsourcelines.assert_called_once_with(mock_function)
