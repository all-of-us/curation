"""
Unit test for temporal_consistency.py

Returns queries to updated end dates, end dates should not be prior to any start date

Original Issues: DC-400, DC-813

Bad end dates:
End dates should not be prior to start dates in any table
* If end date is nullable, it will be nulled
* If end date is required,
    * If visit type is inpatient(id 9201)
        * If other tables have dates for that visit, end date = max(all dates from other tables for that visit)
        * Else, end date = start date.
    * Else, If visit type is ER(id 9203)/Outpatient(id 9202), end date = start date
"""

# Python imports
import unittest

# Project imports
import resources
from constants.cdr_cleaner import clean_cdr as cdr_consts
from cdr_cleaner.cleaning_rules.temporal_consistency import TemporalConsistency, table_dates, visit_occurrence, \
    placeholder_date, NULL_BAD_END_DATES, POPULATE_VISIT_END_DATES, SANDBOX_BAD_END_DATES, visit_occurrence_dates


class TemporalConsistencyTest(unittest.TestCase):

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

        self.rule_instance = TemporalConsistency(self.project_id,
                                                 self.dataset_id,
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
        expected_query_list = []
        expected_sandbox_query_list = []
        for table in table_dates:
            sandbox_query = dict()
            sandbox_query[cdr_consts.QUERY] = SANDBOX_BAD_END_DATES.render(
                project_id=self.project_id,
                sandbox_id=self.sandbox_dataset_id,
                intermediary_table=self.rule_instance.sandbox_table_for(table),
                dataset_id=self.dataset_id,
                table=table,
                table_end_date=table_dates[table][1],
                table_start_date=table_dates[table][0])
            expected_sandbox_query_list.append(sandbox_query)

            fields = resources.fields_for(table)
            col_exprs = [
                'r.' + field['name'] if field['name'] == table_dates[table][1]
                else 'l.' + field['name'] for field in fields
            ]
            cols = ', '.join(col_exprs)
            query = dict()
            query[cdr_consts.QUERY] = NULL_BAD_END_DATES.render(
                project_id=self.project_id,
                dataset_id=self.dataset_id,
                cols=cols,
                table=table,
                table_start_date=table_dates[table][0],
                table_end_date=table_dates[table][1])
            expected_query_list.append(query)
        sandbox_query = dict()
        sandbox_query[cdr_consts.QUERY] = SANDBOX_BAD_END_DATES.render(
            project_id=self.project_id,
            sandbox_id=self.sandbox_dataset_id,
            intermediary_table=self.rule_instance.sandbox_table_for(
                visit_occurrence),
            dataset_id=self.dataset_id,
            table=visit_occurrence,
            table_end_date=visit_occurrence_dates[visit_occurrence][1],
            table_start_date=visit_occurrence_dates[visit_occurrence][0])
        expected_sandbox_query_list.append(sandbox_query)
        query = dict()
        query[cdr_consts.QUERY] = POPULATE_VISIT_END_DATES.render(
            project_id=self.project_id,
            dataset_id=self.dataset_id,
            placeholder_date=placeholder_date)
        expected_query_list.append(query)

        expected_list = expected_sandbox_query_list + expected_query_list

        self.assertEqual(result_list, expected_list)
