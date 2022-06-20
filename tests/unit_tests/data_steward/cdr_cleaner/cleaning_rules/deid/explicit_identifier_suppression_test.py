"""
Unit test for explicit_identifier_suppression.py

Original Issue: DC-1347
"""

# Python imports
import unittest

# Project imports
from constants.cdr_cleaner import clean_cdr as cdr_consts
from cdr_cleaner.cleaning_rules.deid.explicit_identifier_suppression import (
    ExplicitIdentifierSuppression, LOOKUP_TABLE_CREATION_QUERY,
    SANDBOX_EXPLICIT_IDENTIFIER_RECORDS, SUPPRESS_EXPLICIT_IDENTIFIER_RECORDS,
    EXPLICIT_IDENTIFIER_CONCEPTS, ISSUE_NUMBERS, get_concept_id_fields,
    OBSERVATION)


class ExplicitIdentifierSuppressionTest(unittest.TestCase):

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
        self.intermediary_table = f'{ISSUE_NUMBERS[0].lower()}_{OBSERVATION}'

        self.rule_instance = ExplicitIdentifierSuppression(
            self.project_id, self.dataset_id, self.sandbox_dataset_id)

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
                         [cdr_consts.CONTROLLED_TIER_DEID, cdr_consts.COMBINED])

        # Test
        result_list = self.rule_instance.get_query_specs()

        # Post conditions
        expected_list = [{
            cdr_consts.QUERY:
                LOOKUP_TABLE_CREATION_QUERY.render(
                    project_id=self.project_id,
                    sandbox_dataset=self.sandbox_dataset_id,
                    dataset_id=self.dataset_id,
                    lookup_table=EXPLICIT_IDENTIFIER_CONCEPTS)
        }, {
            cdr_consts.QUERY:
                SANDBOX_EXPLICIT_IDENTIFIER_RECORDS.render(
                    project_id=self.project_id,
                    dataset_id=self.dataset_id,
                    sandbox_dataset_id=self.sandbox_dataset_id,
                    observation_table=OBSERVATION,
                    intermediary_table=self.intermediary_table,
                    lookup_table=EXPLICIT_IDENTIFIER_CONCEPTS,
                    concept_fields=get_concept_id_fields(OBSERVATION))
        }, {
            cdr_consts.QUERY:
                SUPPRESS_EXPLICIT_IDENTIFIER_RECORDS.render(
                    project_id=self.project_id,
                    dataset_id=self.dataset_id,
                    sandbox_dataset_id=self.sandbox_dataset_id,
                    intermediary_table=self.intermediary_table,
                    observation_table=OBSERVATION)
        }]

        self.assertEqual(result_list, expected_list)

    def test_log_queries(self):
        # Pre conditions
        self.assertEqual(self.rule_instance.affected_datasets,
                         [cdr_consts.CONTROLLED_TIER_DEID, cdr_consts.COMBINED])
        look_up_table = LOOKUP_TABLE_CREATION_QUERY.render(
            project_id=self.project_id,
            sandbox_dataset=self.sandbox_dataset_id,
            dataset_id=self.dataset_id,
            lookup_table=EXPLICIT_IDENTIFIER_CONCEPTS)

        sandbox_query = SANDBOX_EXPLICIT_IDENTIFIER_RECORDS.render(
            project_id=self.project_id,
            dataset_id=self.dataset_id,
            sandbox_dataset_id=self.sandbox_dataset_id,
            observation_table=OBSERVATION,
            intermediary_table=self.intermediary_table,
            lookup_table=EXPLICIT_IDENTIFIER_CONCEPTS,
            concept_fields=get_concept_id_fields(OBSERVATION))

        drop_query = SUPPRESS_EXPLICIT_IDENTIFIER_RECORDS.render(
            project_id=self.project_id,
            dataset_id=self.dataset_id,
            sandbox_dataset_id=self.sandbox_dataset_id,
            intermediary_table=self.intermediary_table,
            observation_table=OBSERVATION)

        # Test
        with self.assertLogs(level='INFO') as cm:
            self.rule_instance.log_queries()

        expected = [
            'INFO:cdr_cleaner.cleaning_rules.base_cleaning_rule:Generated SQL Query:\n'
            + look_up_table,
            'INFO:cdr_cleaner.cleaning_rules.base_cleaning_rule:Generated SQL Query:\n'
            + sandbox_query,
            'INFO:cdr_cleaner.cleaning_rules.base_cleaning_rule:Generated SQL Query:\n'
            + drop_query
        ]
        # Post condition
        self.assertEqual(cm.output, expected)
