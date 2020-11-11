# Python imports
import unittest

# Project imports
import cdr_cleaner.cleaning_rules.no_data_30_days_after_death as death
from cdr_cleaner.cleaning_rules.no_data_30_days_after_death import (
    TEMPORAL_TABLES_WITH_START_DATE, TEMPORAL_TABLES_WITH_END_DATE,
    TEMPORAL_TABLES_WITH_DATE, NoDataAfterDeath,
    SANDBOX_DEATH_DATE_WITH_END_DATES_QUERY, SANDBOX_DEATH_DATE_QUERY_QUERY)
from constants.bq_utils import WRITE_TRUNCATE
from constants.cdr_cleaner import clean_cdr as clean_consts
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
        expected_query = SANDBOX_DEATH_DATE_QUERY_QUERY.render(
            project=self.project_id,
            dataset=self.dataset_id,
            table_name=PERSON,
            date_column=TEMPORAL_TABLES_WITH_DATE[PERSON])
        self.assertEqual(expected_query, actual_query)

        actual_query = self.rule_instance.get_sandbox_query_for(
            VISIT_OCCURRENCE)
        expected_query = SANDBOX_DEATH_DATE_WITH_END_DATES_QUERY.render(
            project=self.project_id,
            dataset=self.dataset_id,
            table_name=VISIT_OCCURRENCE,
            start_date=TEMPORAL_TABLES_WITH_START_DATE[VISIT_OCCURRENCE],
            end_date=TEMPORAL_TABLES_WITH_END_DATE[VISIT_OCCURRENCE])
        self.assertEqual(expected_query, actual_query)

    def test_get_query_specs(self):
        pass
        # # Pre conditions
        # self.assertEqual(self.rule_instance.affected_datasets,
        #                  [clean_consts.RDR])
        #
        # # Test
        # results_list = self.rule_instance.get_query_specs()
        #
        # sandbox_query_dict = {
        #     cdr_consts.QUERY:
        #         SANDBOX_FIX_UNMAPPED_SURVEY_ANSWERS_QUERY.render(
        #             project=self.project_id,
        #             sandbox_dataset=self.sandbox_id,
        #             sandbox_table=self.sandbox_table_name,
        #             dataset=self.dataset_id)
        # }
        #
        # update_query_dict = {
        #     cdr_consts.QUERY:
        #         UPDATE_FIX_UNMAPPED_SURVEY_ANSWERS_QUERY.render(
        #             project=self.rule_instance.project_id,
        #             sandbox_dataset=self.sandbox_id,
        #             sandbox_table=self.sandbox_table_name,
        #             dataset=self.dataset_id),
        #     cdr_consts.DESTINATION_TABLE:
        #         OBSERVATION,
        #     cdr_consts.DESTINATION_DATASET:
        #         self.dataset_id,
        #     cdr_consts.DISPOSITION:
        #         WRITE_TRUNCATE
        # }
        #
        # self.assertEqual(results_list, [sandbox_query_dict, update_query_dict])
