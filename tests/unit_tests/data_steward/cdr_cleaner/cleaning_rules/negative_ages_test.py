"""
Unit test for negative_ages.py

Age should not be negative for the person at any dates/start dates.
Using rule 20, 21 in Achilles Heel for reference.
Also ensure ages are not beyond 150.

Original Issues: DC-393, DC-811

"""

# Python imports
import unittest

# Project imports
from constants.cdr_cleaner import clean_cdr as cdr_consts
from cdr_cleaner.cleaning_rules.negative_ages import NegativeAges


class NegativeAgesTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.project_id = 'foo_project'
        self.dataset_id = 'foo_dataset'
        self.sandbox_dataset_id = 'foo_sandbox'
        self.client = None

        self.rule_instance = NegativeAges(self.project_id, self.dataset_id,
                                          self.sandbox_dataset_id)

        self.assertEqual(self.rule_instance.project_id, self.project_id)
        self.assertEqual(self.rule_instance.dataset_id, self.dataset_id)
        self.assertEqual(self.rule_instance.sandbox_dataset_id,
                         self.sandbox_dataset_id)

    def test_setup_rule(self):
        # Test
        self.rule_instance.setup_rule(self.client)

    def test_get_query_spec(self):
        # Pre conditions
        self.assertEqual(self.rule_instance.affected_datasets,
                         [cdr_consts.COMBINED])

        # Test
        result_list = self.rule_instance.get_query_specs()

        # Post conditions
        self.assertIsInstance(result_list, list,
                              'get_query_specs should return a list')
        for query_spec in result_list:
            self.assertTrue(
                type(query_spec) is dict,
                'Each item in get_query_specs should be a query spec dict')
            self.assertTrue(cdr_consts.QUERY in query_spec.keys(),
                            'Each query spec should specify a query')
