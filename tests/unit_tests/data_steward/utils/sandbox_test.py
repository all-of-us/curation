import unittest

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
