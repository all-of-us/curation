# Python imports
import os
import unittest

# Project Imports
from utils import bq
import tests.test_util as test_util
from app_identity import PROJECT_ID
from cdr_cleaner.cleaning_rules.unit_normalization import UnitNormalization, UNIT_MAPPING_TABLE
from common import JINJA_ENV

test_query = JINJA_ENV.from_string("""select * from `{{intermediary_table}}`""")


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

        self.rule_instance = UnitNormalization(self.project_id, self.dataset_id,
                                               self.sandbox_id)

    def test_setup_rule(self):

        # test if intermediary table exists before running the cleaning rule
        intermediary_table = f'{self.project_id}.{self.dataset_id}.{UNIT_MAPPING_TABLE}'

        client = bq.get_client(self.project_id)
        # run setup_rule and see if the table is created
        self.rule_instance.setup_rule(client)

        actual_table = client.get_table(intermediary_table)
        self.assertIsNotNone(actual_table.created)

        # test if exception is raised if table already exists
        with self.assertRaises(RuntimeError) as c:
            self.rule_instance.setup_rule(client)

        self.assertEqual(str(c.exception),
                         f"Unable to create tables: ['{intermediary_table}']")

        query = test_query.render(intermediary_table=intermediary_table)
        query_job_config = bq.bigquery.job.QueryJobConfig(use_query_cache=False)
        result = client.query(query, job_config=query_job_config).to_dataframe()
        self.assertEqual(result.empty, False)

    def tearDown(self):
        test_util.delete_all_tables(self.dataset_id)
