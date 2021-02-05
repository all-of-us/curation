# Python imports
import unittest

# Project imports
from cdr_cleaner.cleaning_rules.truncate_rdr_using_date import (
    TruncateRdrData, TABLES_DATES_FIELDS, CUTOFF_DATE, SANDBOX_QUERY,
    TRUNCATE_ROWS)
from constants.cdr_cleaner import clean_cdr as clean_consts


class TruncateRdrDataTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.project_id = 'foo_project'
        self.dataset_id = 'bar_dataset'
        self.sandbox_id = 'baz_sandbox'

        self.rule_instance = TruncateRdrData(self.project_id, self.dataset_id,
                                             self.sandbox_id)

        self.assertEqual(self.rule_instance.project_id, self.project_id)
        self.assertEqual(self.rule_instance.dataset_id, self.dataset_id)
        self.assertEqual(self.rule_instance.sandbox_dataset_id, self.sandbox_id)

    def test_get_query_specs(self):
        # Pre conditions
        self.assertEqual(self.rule_instance.affected_datasets,
                         [clean_consts.RDR])

        # Test
        results_list = self.rule_instance.get_query_specs()

        # Post conditions
        counter = 0
        sandbox_queries = []
        truncate_queries = []
        for table in self.rule_instance.affected_tables:
            save_changed_rows = {
                clean_consts.QUERY:
                    SANDBOX_QUERY.render(project=self.project_id,
                                         dataset=self.dataset_id,
                                         sandbox_dataset=self.sandbox_id,
                                         intermediary_table=self.rule_instance.
                                         get_sandbox_tablenames()[counter],
                                         table_name=table,
                                         field_name=TABLES_DATES_FIELDS[table],
                                         cutoff_date=CUTOFF_DATE)
            }

            sandbox_queries.append(save_changed_rows)

            truncate_query = {
                clean_consts.QUERY:
                    TRUNCATE_ROWS.render(project=self.project_id,
                                         dataset=self.dataset_id,
                                         table_name=table,
                                         field_name=TABLES_DATES_FIELDS[table],
                                         cutoff_date=CUTOFF_DATE),
            }

            truncate_queries.append(truncate_query)
            counter += 1

        expected_list = sandbox_queries + truncate_queries

        self.assertEqual(results_list, expected_list)

    def test_log_queries(self):
        # Pre conditions
        self.assertEqual(self.rule_instance.affected_datasets,
                         [clean_consts.RDR])

        counter = 0
        sandbox_queries = []
        truncate_queries = []
        for table in self.rule_instance.affected_tables:
            save_changed_rows = {
                clean_consts.QUERY:
                    SANDBOX_QUERY.render(project=self.project_id,
                                         dataset=self.dataset_id,
                                         sandbox_dataset=self.sandbox_id,
                                         intermediary_table=self.rule_instance.
                                         get_sandbox_tablenames()[counter],
                                         table_name=table,
                                         field_name=TABLES_DATES_FIELDS[table],
                                         cutoff_date=CUTOFF_DATE)
            }

            sandbox_queries.append(
                f'INFO:cdr_cleaner.cleaning_rules.base_cleaning_rule:Generated SQL Query:\n'
                f'{save_changed_rows[clean_consts.QUERY]}')

            truncate_query = {
                clean_consts.QUERY:
                    TRUNCATE_ROWS.render(project=self.project_id,
                                         dataset=self.dataset_id,
                                         table_name=table,
                                         field_name=TABLES_DATES_FIELDS[table],
                                         cutoff_date=CUTOFF_DATE),
            }

            truncate_queries.append(
                f'INFO:cdr_cleaner.cleaning_rules.base_cleaning_rule:Generated SQL Query:\n'
                f'{truncate_query[clean_consts.QUERY]}')
            counter += 1

        # Test
        with self.assertLogs(level='INFO') as cm:
            self.rule_instance.log_queries()

            expected = sandbox_queries + truncate_queries

            # Post condition
            self.assertEqual(cm.output, expected)
