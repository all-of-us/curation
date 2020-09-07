# Python imports
import unittest

from cdr_cleaner.cleaning_rules.date_shift_cope_responses import (
    DateShiftCopeResponses, SANDBOX_COPE_SURVEY_QUERY, DATE_SHIFT_QUERY,
    PIPELINE_DATASET, OBSERVATION, COPE_CONCEPTS_TABLE)
# Project imports
from constants.bq_utils import WRITE_TRUNCATE
from constants.cdr_cleaner import clean_cdr as clean_consts


class DateShiftCopeResponsesTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.project_id = 'foo_project'
        self.dataset_id = 'bar_dataset'
        self.sandbox_id = 'baz_sandbox'

        self.rule_instance = DateShiftCopeResponses(self.project_id,
                                                    self.dataset_id,
                                                    self.sandbox_id)

        self.assertEqual(self.rule_instance.project_id, self.project_id)
        self.assertEqual(self.rule_instance.dataset_id, self.dataset_id)
        self.assertEqual(self.rule_instance.sandbox_dataset_id, self.sandbox_id)

    def test_get_query_specs(self):
        # Pre conditions
        self.assertEqual(self.rule_instance.affected_datasets,
                         [clean_consts.DEID_BASE])

        # Test
        results_list = self.rule_instance.get_query_specs()

        # Post conditions
        expected_list = [{
            clean_consts.QUERY:
                SANDBOX_COPE_SURVEY_QUERY.render(
                    project_id=self.project_id,
                    dataset_id=self.dataset_id,
                    sandbox_dataset=self.sandbox_id,
                    intermediary_table=self.rule_instance.
                    get_sandbox_tablenames()[0],
                    pipeline_tables_dataset=PIPELINE_DATASET,
                    cope_concepts_table=COPE_CONCEPTS_TABLE,
                    observation_table=OBSERVATION)
        }, {
            clean_consts.QUERY:
                DATE_SHIFT_QUERY.render(
                    project_id=self.project_id,
                    pre_deid_dataset=self.rule_instance.
                    get_combined_dataset_from_deid_dataset(self.dataset_id),
                    dataset_id=self.dataset_id,
                    pipeline_tables_dataset=PIPELINE_DATASET,
                    cope_concepts_table=COPE_CONCEPTS_TABLE,
                    observation_table=OBSERVATION),
            clean_consts.DESTINATION_TABLE:
                OBSERVATION,
            clean_consts.DESTINATION_DATASET:
                self.dataset_id,
            clean_consts.DISPOSITION:
                WRITE_TRUNCATE
        }]

        self.assertEqual(results_list, expected_list)

    def test_log_queries(self):
        # Pre conditions
        self.assertEqual(self.rule_instance.affected_datasets,
                         [clean_consts.DEID_BASE])

        store_rows_to_be_changed = SANDBOX_COPE_SURVEY_QUERY.render(
            project_id=self.project_id,
            dataset_id=self.dataset_id,
            sandbox_dataset=self.sandbox_id,
            intermediary_table=self.rule_instance.get_sandbox_tablenames()[0],
            pipeline_tables_dataset=PIPELINE_DATASET,
            cope_concepts_table=COPE_CONCEPTS_TABLE,
            observation_table=OBSERVATION)

        select_rows_to_be_changed = DATE_SHIFT_QUERY.render(
            project_id=self.project_id,
            pre_deid_dataset=self.rule_instance.
            get_combined_dataset_from_deid_dataset(self.dataset_id),
            dataset_id=self.dataset_id,
            pipeline_tables_dataset=PIPELINE_DATASET,
            cope_concepts_table=COPE_CONCEPTS_TABLE,
            observation_table=OBSERVATION)

        # Test
        with self.assertLogs(level='INFO') as cm:
            self.rule_instance.log_queries()

            expected = [
                'INFO:cdr_cleaner.cleaning_rules.base_cleaning_rule:Generated SQL Query:\n'
                + store_rows_to_be_changed,
                'INFO:cdr_cleaner.cleaning_rules.base_cleaning_rule:Generated SQL Query:\n'
                + select_rows_to_be_changed
            ]

            # Post condition
            self.assertEqual(cm.output, expected)
