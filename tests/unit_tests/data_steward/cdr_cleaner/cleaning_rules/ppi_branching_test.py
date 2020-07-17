import unittest

import mock
from google.cloud import bigquery
from pandas import DataFrame

from cdr_cleaner.cleaning_rules.ppi_branching import OBSERVATION_BACKUP_TABLE_ID
from cdr_cleaner.cleaning_rules.ppi_branching import PPI_BRANCHING_RULE_PATHS
from cdr_cleaner.cleaning_rules.ppi_branching import PpiBranching, OBSERVATION
from utils.bq import get_table_schema


def _get_csv_row_count() -> int:
    """
    Get total number of rows in rule csv files

    :return: number of rows
    """
    csv_row_count = 0
    for rule_path in PPI_BRANCHING_RULE_PATHS:
        with open(rule_path) as rule_fp:
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
        self.cleaning_rule = PpiBranching(self.project_id, self.dataset_id,
                                          self.sandbox_dataset_id)

    def test_load_rules_lookup(self):

        def check_load_table_from_dataframe(dataframe, destination, job_config):
            """
            Mocks bigquery.Client.load_table_from_dataframe to
            ensure that it is called by the rule as expected
            """
            expected_row_count = _get_csv_row_count()
            self.assertIsInstance(dataframe, DataFrame)
            self.assertEqual(expected_row_count, len(dataframe))
            self.assertEqual(destination, self.cleaning_rule.lookup_table)
            self.assertEqual(job_config.write_disposition,
                             bigquery.WriteDisposition.WRITE_TRUNCATE)
            # return a mock for the job result
            return mock.MagicMock()

        # dataframe has same number of rows as all input csv files (minus headers)
        with mock.patch('google.cloud.bigquery.Client') as m:
            instance = m.return_value
            instance.load_table_from_dataframe = check_load_table_from_dataframe
            self.cleaning_rule.load_rules_lookup(instance)

    def test_get_backup_rows_query(self):
        # check that DDL table location is correct and contains all field descriptions
        result = self.cleaning_rule.backup_rows_to_drop_ddl().strip()
        expected_sql = (f'CREATE OR REPLACE TABLE {self.sandbox_dataset_id}.'
                        f'{OBSERVATION_BACKUP_TABLE_ID}')
        self.assertTrue(result.startswith(expected_sql))
        self.assertTrue(
            all(field.description in result
                for field in self.observation_schema))

    def test_get_observation_replace_query(self):
        # check that DDL table location is correct and contains all field descriptions
        result = self.cleaning_rule.stage_to_target_ddl().strip()
        expected_sql = f'CREATE OR REPLACE TABLE {self.dataset_id}.{OBSERVATION}'
        self.assertTrue(result.startswith(expected_sql))
        self.assertTrue(
            all(field.description in result
                for field in self.observation_schema))
