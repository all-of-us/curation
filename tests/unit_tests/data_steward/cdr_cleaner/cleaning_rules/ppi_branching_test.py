import unittest

from data_steward.cdr_cleaner.cleaning_rules.ppi_branching import OBSERVATION_BACKUP_TABLE_ID
from data_steward.cdr_cleaner.cleaning_rules.ppi_branching import PpiBranching, OBSERVATION
from data_steward.cdr_cleaner.cleaning_rules.ppi_branching import PPI_BRANCHING_RULE_PATHS
from utils.bq import get_table_schema

from google.cloud import bigquery
from pandas import DataFrame
import mock


def _get_csv_row_count() -> int:
    """
    Get total number of rows in rule csv files

    :return: number of rows
    """
    csv_row_count = 0
    for rule_path in PPI_BRANCHING_RULE_PATHS:
        with open(rule_path, ) as rule_fp:
            header, *lines = rule_fp.readlines()
            csv_row_count += len(lines)
    return csv_row_count


class PpiBranchingTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self) -> None:
        self.project_id = 'fake_project'
        self.dataset_id = 'fake_dataset'
        self.sandbox_dataset_id = 'fake_sandbox'
        self.observation_schema = get_table_schema('observation')
        self.cleaning_rule = PpiBranching(self.project_id, self.dataset_id, self.sandbox_dataset_id)

    def test_load_rules_lookup(self):
        # dataframe has same number of rows as all input csv files (minus headers)
        expected_row_count = _get_csv_row_count()

        client = bigquery.Client(self.project_id)
        client.load_table_from_dataframe = mock.MagicMock()
        self.cleaning_rule.load_rules_lookup(client)
        # unpacking first arg; call_args is collection of (args: Tuple, kwargs: dict)
        dataframe_arg, *_ = client.load_table_from_dataframe.call_args[0]
        self.assertIsInstance(dataframe_arg, DataFrame)
        self.assertEqual(expected_row_count, len(dataframe_arg.index))

    def test_get_backup_rows_query(self):
        # check that DDL table location is correct and contains all field descriptions
        result = self.cleaning_rule.get_backup_rows_ddl().strip()
        expected_sql = (f'CREATE OR REPLACE TABLE {self.sandbox_dataset_id}.'
                        f'{OBSERVATION_BACKUP_TABLE_ID}')
        self.assertTrue(result.startswith(expected_sql))
        self.assertTrue(all(field.description in result for field in self.observation_schema))

    def test_get_observation_replace_query(self):
        # check that DDL table location is correct and contains all field descriptions
        result = self.cleaning_rule.get_drop_rows_ddl().strip()
        expected_sql = f'CREATE OR REPLACE TABLE {self.dataset_id}.{OBSERVATION}'
        self.assertTrue(result.startswith(expected_sql))
        self.assertTrue(all(field.description in result for field in self.observation_schema))
