# Python imports
import unittest
from mock import patch

# Project imports
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule
import cdr_cleaner.cleaning_rules.no_data_30_days_after_death as death
from cdr_cleaner.cleaning_rules.no_data_30_days_after_death import (
    TEMPORAL_TABLES_WITH_START_DATE,
    TEMPORAL_TABLES_WITH_END_DATE,
    TEMPORAL_TABLES_WITH_DATE,
    SANDBOX_DEATH_DATE_WITH_END_DATES_QUERY,
    SANDBOX_DEATH_DATE_QUERY,
    REMOVE_DEATH_DATE_QUERY,
    NoDataAfterDeath,
)
import constants.cdr_cleaner.clean_cdr as cdr_consts
from common import PERSON, VISIT_OCCURRENCE


class NoDataAfterDeathTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.project_id = 'test_project'
        self.dataset_id = 'test_dataset'
        self.sandbox_id = 'test_sandbox'
        self.client = None

        self.rule_instance = NoDataAfterDeath(self.project_id, self.dataset_id,
                                              self.sandbox_id)

        self.assertEqual(self.rule_instance.project_id, self.project_id)
        self.assertEqual(self.rule_instance.dataset_id, self.dataset_id)
        self.assertEqual(self.rule_instance.sandbox_dataset_id, self.sandbox_id)

    def test_get_affected_tables(self):
        expected_affected_tables = list(
            set(
                list(TEMPORAL_TABLES_WITH_END_DATE.keys()) +
                list(TEMPORAL_TABLES_WITH_START_DATE.keys()) +
                list(TEMPORAL_TABLES_WITH_DATE.keys())))
        actual_affected_tables = death.get_affected_tables()
        self.assertListEqual(sorted(expected_affected_tables),
                             sorted(actual_affected_tables))

    def test_get_date(self):
        self.assertEqual(death.get_date(PERSON),
                         TEMPORAL_TABLES_WITH_DATE[PERSON])
        with self.assertRaises(death.TableDateColumnException):
            death.get_date(VISIT_OCCURRENCE)

    def test_get_start_date(self):
        self.assertEqual(death.get_start_date(VISIT_OCCURRENCE),
                         TEMPORAL_TABLES_WITH_START_DATE[VISIT_OCCURRENCE])
        with self.assertRaises(death.TableDateColumnException):
            death.get_start_date(PERSON)

    def test_get_end_date(self):
        self.assertEqual(death.get_end_date(VISIT_OCCURRENCE),
                         TEMPORAL_TABLES_WITH_END_DATE[VISIT_OCCURRENCE])
        with self.assertRaises(death.TableDateColumnException):
            death.get_end_date(PERSON)

    def test_get_sandbox_query_for(self):
        actual_query = self.rule_instance.get_sandbox_query_for(PERSON)
        expected_query = SANDBOX_DEATH_DATE_QUERY.render(
            project=self.project_id,
            dataset=self.dataset_id,
            sandbox_dataset=self.sandbox_id,
            sandbox_table=self.rule_instance.sandbox_table_for(PERSON),
            table_name=PERSON,
            date_column=TEMPORAL_TABLES_WITH_DATE[PERSON])
        self.assertEqual(expected_query, actual_query)

        actual_query = self.rule_instance.get_sandbox_query_for(
            VISIT_OCCURRENCE)
        expected_query = SANDBOX_DEATH_DATE_WITH_END_DATES_QUERY.render(
            project=self.project_id,
            dataset=self.dataset_id,
            table_name=VISIT_OCCURRENCE,
            sandbox_dataset=self.sandbox_id,
            sandbox_table=self.rule_instance.sandbox_table_for(
                VISIT_OCCURRENCE),
            start_date=TEMPORAL_TABLES_WITH_START_DATE[VISIT_OCCURRENCE],
            end_date=TEMPORAL_TABLES_WITH_END_DATE[VISIT_OCCURRENCE])
        self.assertEqual(expected_query, actual_query)

    @patch.object(BaseCleaningRule, 'sandbox_table_for')
    def test_get_query_for(self, mock_sandbox_table_for):
        sandbox_table = 'sandbox_table'
        mock_sandbox_table_for.return_value = sandbox_table
        actual_query = self.rule_instance.get_query_for(PERSON)
        expected_query = REMOVE_DEATH_DATE_QUERY.render(
            project=self.project_id,
            dataset=self.dataset_id,
            table_name=PERSON,
            sandbox_dataset=self.sandbox_id,
            sandbox_table_name=sandbox_table)
        self.assertEqual(expected_query, actual_query)

    @patch.object(NoDataAfterDeath, 'get_query_for')
    @patch.object(BaseCleaningRule, 'sandbox_table_for')
    @patch.object(NoDataAfterDeath, 'get_sandbox_query_for')
    @patch(
        'cdr_cleaner.cleaning_rules.no_data_30_days_after_death.get_affected_tables'
    )
    def test_get_query_specs(self, mock_get_affected_tables,
                             mock_get_sandbox_query_for, mock_sandbox_table_for,
                             mock_get_query_for):
        mock_get_affected_tables.return_value = [PERSON, VISIT_OCCURRENCE]
        sandbox_query_1 = 'sandbox query 1'
        sandbox_query_2 = 'sandbox query 2'
        mock_get_sandbox_query_for.side_effect = [
            sandbox_query_1, sandbox_query_2
        ]
        sandbox_table_1 = 'sandbox_table_1'
        sandbox_table_2 = 'sandbox_table_2'
        mock_sandbox_table_for.side_effect = [sandbox_table_1, sandbox_table_2]
        query_1 = 'query 1'
        query_2 = 'query 2'
        mock_get_query_for.side_effect = [query_1, query_2]

        expected_query_dicts = [{
            cdr_consts.QUERY: sandbox_query_1
        }, {
            cdr_consts.QUERY: query_1
        }, {
            cdr_consts.QUERY: sandbox_query_2
        }, {
            cdr_consts.QUERY: query_2
        }]

        actual_query_dicts = self.rule_instance.get_query_specs()

        self.assertListEqual(expected_query_dicts, actual_query_dicts)
