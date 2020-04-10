# Python imports
import unittest


# Project imports
from deid.parser import odataset_name_verification, parse_args
from tools import run_deid

class ParserTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.input_dataset = "foo_input"
        self.output_dataset = "food_output_deid"
        self.tablename = "bar_table"
        self.log_path = 'deid-fake.log'
        self.parameter_list = ['--rules',
                               ]


    def test_odataset_name_verification(self, output_dataset):
        parse_args()



    # def test_parse_args(self, parameter_list):