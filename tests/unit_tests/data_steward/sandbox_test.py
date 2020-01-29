import unittest
import sandbox


class SandboxTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.dataset_id = 'test_dataset'

    def test_get_sandbox_table_name(self):
        rule_name = 'abc_123'
        expected = '{dataset_id}_{rule_name}'.format(dataset_id=self.dataset_id,
                                                     rule_name=rule_name)
        actual = sandbox.get_sandbox_table_name(self.dataset_id, 'abc_123')
        self.assertEqual(expected, actual)
        actual = sandbox.get_sandbox_table_name(self.dataset_id, 'abc 123')
        self.assertEqual(expected, actual)
        actual = sandbox.get_sandbox_table_name(self.dataset_id, 'abc\t123')
        self.assertEqual(expected, actual)
        actual = sandbox.get_sandbox_table_name(self.dataset_id, 'abc~123')
        self.assertEqual(expected, actual)
