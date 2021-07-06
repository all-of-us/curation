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

# Third party imports

# Project imports
from common import OBSERVATION
from cdr_cleaner.cleaning_rules.rdr_observation_source_concept_id_suppression import (
    ObservationSourceConceptIDRowSuppression, DROP_SELECTION_QUERY_TMPL,
    DROP_QUERY_TMPL, OBS_SRC_CONCEPTS)
from constants.bq_utils import WRITE_TRUNCATE
from constants.cdr_cleaner import clean_cdr as clean_consts


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
        self.client = None

        self.rule_instance = ObservationSourceConceptIDRowSuppression(
            self.project_id, self.dataset_id, self.sandbox_id)

        self.assertEqual(self.rule_instance.project_id, self.project_id)
        self.assertEqual(self.rule_instance.dataset_id, self.dataset_id)
        self.assertEqual(self.rule_instance.sandbox_dataset_id, self.sandbox_id)

    def test_setup_rule(self):
        # test
        self.rule_instance.setup_rule(self.client)

        # no errors are raised, nothing happens

    def test_get_query_specs(self):
        # pre-conditions
        self.assertEqual(self.rule_instance.affected_datasets,
                         [clean_consts.RDR])

        # test
        result_list = self.rule_instance.get_query_specs()

        # post conditions
        expected_list = [{
            clean_consts.QUERY:
                DROP_SELECTION_QUERY_TMPL.render(
                    project=self.project_id,
                    dataset=self.dataset_id,
                    sandbox=self.sandbox_id,
                    drop_table=self.rule_instance.get_sandbox_tablenames()[0],
                    obs_concepts=OBS_SRC_CONCEPTS)
        }, {
            clean_consts.QUERY:
                DROP_QUERY_TMPL.render(project=self.project_id,
                                       dataset=self.dataset_id,
                                       obs_concepts=OBS_SRC_CONCEPTS)
        }]

        self.assertEqual(result_list, expected_list)

    def test_log_queries(self):
        # pre-conditions
        self.assertEqual(self.rule_instance.affected_datasets,
                         [clean_consts.RDR])

        store_drops = DROP_SELECTION_QUERY_TMPL.render(
            project=self.project_id,
            dataset=self.dataset_id,
            sandbox=self.sandbox_id,
            drop_table=self.rule_instance.get_sandbox_tablenames()[0],
            obs_concepts=OBS_SRC_CONCEPTS)
        select_saves = DROP_QUERY_TMPL.render(project=self.project_id,
                                              dataset=self.dataset_id,
                                              obs_concepts=OBS_SRC_CONCEPTS)
        # test
        with self.assertLogs(level='INFO') as cm:
            self.rule_instance.log_queries()

            expected = [
                'INFO:cdr_cleaner.cleaning_rules.base_cleaning_rule:Generated SQL Query:\n'
                + store_drops,
                'INFO:cdr_cleaner.cleaning_rules.base_cleaning_rule:Generated SQL Query:\n'
                + select_saves
            ]

            # post condition
            self.assertEqual(cm.output, expected)
