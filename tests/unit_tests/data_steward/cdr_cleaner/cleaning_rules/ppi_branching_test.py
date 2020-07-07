import unittest

from google.cloud.bigquery import DatasetReference, TableReference

from data_steward.cdr_cleaner.cleaning_rules import ppi_branching
from utils.bq import get_table_schema


class PpiBranchingTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self) -> None:
        dataset_ref = DatasetReference('fake_project', 'fake_dataset')
        sandbox_dataset_ref = DatasetReference('fake_project', 'fake_sandbox')
        self.src_table = TableReference(dataset_ref, 'src_table')
        self.backup_table = TableReference(sandbox_dataset_ref, 'backup_table')
        self.lookup_table = TableReference(sandbox_dataset_ref, 'lookup_table')
        self.observation_schema = get_table_schema('observation')

    def test_get_backup_rows_query(self):
        result = ppi_branching.get_backup_rows_query(self.src_table,
                                                     self.backup_table,
                                                     self.lookup_table).strip()
        self.assertTrue(result.startswith('CREATE OR REPLACE TABLE fake_sandbox.backup_table'))
        self.assertTrue(all(field.description in result for field in self.observation_schema))

    def test_get_observation_replace_query(self):
        result = ppi_branching.get_observation_replace_query(self.src_table,
                                                             self.backup_table).strip()
        self.assertTrue(result.startswith('CREATE OR REPLACE TABLE fake_dataset.src_table'))
        self.assertTrue(all(field.description in result for field in self.observation_schema))
