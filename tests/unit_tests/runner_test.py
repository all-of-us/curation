from unittest import TestCase

from tests.runner import validate_test_path


class RunnerTest(TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self) -> None:
        self.start_dir = 'unit_tests'

    def test_validate_test_path(self) -> None:
        # Verify absolute paths not allowed
        test_path = '/Users/local/curation/tests/unit_tests/data_steward/tools/add_hpo_test.py'
        with self.assertRaises(RuntimeError) as e:
            validate_test_path(test_path)
        self.assertIn('absolute', str(e.exception))
        test_path = "C:\\Users\\local\\curation\\tests\\unit_tests\\data_steward\\tools\\add_hpo_test.py"
        with self.assertRaises(RuntimeError) as e:
            validate_test_path(test_path)
        self.assertIn('absolute', str(e.exception))
