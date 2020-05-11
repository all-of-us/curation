"""
Unit Test for the rdr_observation_source_concept_id_suppression module.

Remove three irrelevant observation_source_concept_ids from the RDR dataset.

Original Issue:  DC-529

The intent is to remove PPI records from the observation table in the RDR
export where observation_source_concept_id in (43530490, 43528818, 43530333).
The records for removal should be archived in the dataset sandbox.
"""
# Python imports
import unittest

from cdr_cleaner.cleaning_rules.rdr_observation_source_concept_id_suppression import \
    ObservationSourceConceptIDRowSuppression, SAVE_TABLE_NAME, DROP_SELECTION_QUERY, DROP_QUERY
# Project imports
from constants.bq_utils import WRITE_TRUNCATE
from constants.cdr_cleaner import clean_cdr as clean_consts

# Third party imports


class ObservationSourceConceptIDRowSuppressionTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.project_id = 'foo'
        self.dataset_id = 'bar'
        self.sandbox_id = 'baz'

        self.query_class = ObservationSourceConceptIDRowSuppression(
            self.project_id, self.dataset_id, self.sandbox_id)

        self.assertEqual(self.query_class.get_project_id(), self.project_id)
        self.assertEqual(self.query_class.get_dataset_id(), self.dataset_id)
        self.assertEqual(self.query_class.get_sandbox_dataset_id(),
                         self.sandbox_id)

    def test_setup_rule(self):
        # test
        self.query_class.setup_rule()

    def test_get_affected_tables(self):
        self.query_class.get_affected_tables()

    def test_setup_validation(self):
        self.query_class.setup_validation()

    def test_validate_rule(self):
        self.query_class.validate_rule()

    # no errors are raised, nothing happens

    def test_get_query_specs(self):
        # pre-conditions
        self.assertEqual(self.query_class.get_affected_datasets(),
                         [clean_consts.RDR])

        # test
        result_list = self.query_class.get_query_specs()

        # post conditions
        expected_list = [{
            clean_consts.QUERY:
                DROP_SELECTION_QUERY.format(project=self.project_id,
                                            dataset=self.dataset_id,
                                            sandbox=self.sandbox_id,
                                            drop_table=SAVE_TABLE_NAME)
        }, {
            clean_consts.QUERY:
                DROP_QUERY.format(project=self.project_id,
                                  dataset=self.dataset_id),
            clean_consts.DESTINATION_TABLE:
                'observation',
            clean_consts.DESTINATION_DATASET:
                self.dataset_id,
            clean_consts.DISPOSITION:
                WRITE_TRUNCATE
        }]

        self.assertEqual(result_list, expected_list)

    def test_log_queries(self):
        # pre-conditions
        self.assertEqual(self.query_class.get_affected_datasets(),
                         [clean_consts.RDR])

        store_drops = DROP_SELECTION_QUERY.format(project=self.project_id,
                                                  dataset=self.dataset_id,
                                                  sandbox=self.sandbox_id,
                                                  drop_table=SAVE_TABLE_NAME)
        select_saves = DROP_QUERY.format(project=self.project_id,
                                         dataset=self.dataset_id)
        # test
        with self.assertLogs(level='INFO') as cm:
            self.query_class.log_queries()

            expected = [
                'INFO:cdr_cleaner.cleaning_rules.base_cleaning_rule:Generated SQL Query:\n'
                + store_drops,
                'INFO:cdr_cleaner.cleaning_rules.base_cleaning_rule:Generated SQL Query:\n'
                + select_saves
            ]

            # post condition
            self.assertEqual(cm.output, expected)
