import os
import unittest

from jinja2 import Environment

import tests.test_util as test_util
from app_identity import PROJECT_ID
from cdr_cleaner.cleaning_rules.unit_normalization import UnitNormalization, UNIT_MAPPING_TABLE
# Project Imports
from utils import bq

jinja_env = Environment(
    # help protect against cross-site scripting vulnerabilities
    autoescape=True,
    # block tags on their own lines
    # will not cause extra white space
    trim_blocks=True,
    lstrip_blocks=True,
    # syntax highlighting should be better
    # with these comment delimiters
    comment_start_string='--',
    comment_end_string=' --')

test_query = jinja_env.from_string("""select * from `{{intermediary_table}}`""")


class UnitNormalizationTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        # Set the test project identifier
        project_id = os.environ.get(PROJECT_ID)
        self.project_id = project_id

        # Set the expected test datasets
        self.dataset_id = os.environ.get('COMBINED_DATASET_ID')
        self.sandbox_id = self.dataset_id + '_sandbox'

        self.query_class = UnitNormalization(self.project_id, self.dataset_id,
                                             self.sandbox_id)

        self.assertEqual(self.query_class.get_project_id(), self.project_id)
        self.assertEqual(self.query_class.get_dataset_id(), self.dataset_id)
        self.assertEqual(self.query_class.get_sandbox_dataset_id(),
                         self.sandbox_id)

    def test_setup_rule(self):

        # test if intermediary table exists before running the cleaning rule
        intermediary_table = f'{self.project_id}.{self.dataset_id}.{UNIT_MAPPING_TABLE}'

        client = bq.get_client(self.project_id)
        # run setup_rule and see if the table is created
        self.query_class.setup_rule(client)

        actual_table = client.get_table(intermediary_table)
        self.assertIsNotNone(actual_table.created)

        query = test_query.render(intermediary_table=intermediary_table)
        result = bq.query(query, self.project_id)
        self.assertEqual(result.empty, False)

    def tearDown(self):
        test_util.delete_all_tables(self.dataset_id)
