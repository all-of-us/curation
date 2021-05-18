"""
Unit test for generalize_state_by_population module

This cleaning rule will generalize participant states that do not meet a
threshold of participant size.


Original Issue: DC-1614
"""

# Python imports
import unittest

# Project imports
from cdr_cleaner.cleaning_rules.generalize_state_by_population import (
    GeneralizeStateByPopulation)
from constants.cdr_cleaner import clean_cdr as clean_consts
from common import JINJA_ENV, OBSERVATION

PARTICIPANT_THRESH = 200

STATE_GENERALIZATION_QUERY = JINJA_ENV.from_string("""
    UPDATE `{{project_id}}.{{dataset_id}}.observation`
    SET value_source_concept_id = 2000000011,
        value_as_concept_id = 2000000011
    WHERE value_source_concept_id IN (
        SELECT
            value_source_concept_id
        FROM `{{project_id}}.{{dataset_id}}.observation`
        WHERE observation_source_concept_id = 1585249
        GROUP BY value_source_concept_id
        HAVING COUNT(*) < {{threshold}}
    )
""")


class GeneralizeStateByPopulationTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.project_id = 'foo_project'
        self.dataset_id = 'bar_dataset'
        self.sandbox_id = 'baz_sandbox'

        self.rule_instance = GeneralizeStateByPopulation(
            self.project_id, self.dataset_id, self.sandbox_id)

        self.assertEqual(self.rule_instance.project_id, self.project_id)
        self.assertEqual(self.rule_instance.dataset_id, self.dataset_id)
        self.assertEqual(self.rule_instance.sandbox_dataset_id, self.sandbox_id)

    def test_get_query_specs(self):
        # Pre conditions
        self.assertEqual(self.rule_instance.affected_datasets,
                         [clean_consts.REGISTERED_TIER_DEID])

        #Test
        results_list = self.rule_instance.get_query_specs()

        # Post conditions
        expected_list = [{
            clean_consts.QUERY:
                STATE_GENERALIZATION_QUERY.render(project_id=self.project_id,
                                                  dataset_id=self.dataset_id,
                                                  threshold=PARTICIPANT_THRESH)
        }]

        self.assertEqual(results_list, expected_list)
