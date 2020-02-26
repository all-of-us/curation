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
from constants.bq_utils import WRITE_TRUNCATE
from constants.cdr_cleaner import clean_cdr as clean_consts
from cdr_cleaner.cleaning_rules.rdr_observation_source_concept_id_suppression import ObservationSourceConceptIDRowSuppression, SAVE_TABLE_NAME, DROP_SELECTION_QUERY, DROP_QUERY


class ObservationSourceConceptIDRowSuppressionTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.query_class = ObservationSourceConceptIDRowSuppression(
            'foo', 'bar', 'baz')

    def test_get_query_dictionary_list(self):
        # pre-conditions
        self.assertEqual(self.query_class.get_affected_datasets(),
                         [clean_consts.RDR])

        # test
        result_list = self.query_class.get_query_dictionary_list()

        # post conditions
        expected_list = [{
            clean_consts.QUERY:
                DROP_SELECTION_QUERY.format(project='foo',
                                            dataset='bar',
                                            sandbox='baz',
                                            drop_table=SAVE_TABLE_NAME)
        }, {
            clean_consts.QUERY: DROP_QUERY.format(project='foo', dataset='bar'),
            clean_consts.DESTINATION_TABLE: 'observation',
            clean_consts.DESTINATION_DATASET: 'bar',
            clean_consts.DISPOSITION: WRITE_TRUNCATE
        }]

        self.assertEqual(result_list, expected_list)
