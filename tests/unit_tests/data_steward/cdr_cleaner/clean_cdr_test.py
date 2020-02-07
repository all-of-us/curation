import mock
import unittest
import inspect
from cdr_cleaner import clean_cdr


class CleanCDRTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.project_id = 'test project'
        self.dataset_id = 'test dataset'

    @mock.patch('inspect.getmodule')
    def test_add_module_info_decorator(self, mock_get_module):
        mock_function = mock.Mock(__name__='anonymous_function')
        pass
