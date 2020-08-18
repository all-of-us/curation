# Python imports
from unittest import mock, TestCase

# Project imports
import cdr_cleaner.clean_cdr_engine as ce
from constants.cdr_cleaner import clean_cdr as clean_consts


class CleanCDREngineTest(TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.project_id = 'foo_project'
        self.dataset_id = 'bar_dataset'
        self.sandbox_id = 'baz_sandbox'
        self.client = None
