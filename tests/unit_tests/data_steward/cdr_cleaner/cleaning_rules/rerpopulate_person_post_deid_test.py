# Python imports
import unittest

import constants.cdr_cleaner.clean_cdr as cdr_consts
# Project imports
from cdr_cleaner.cleaning_rules.repopulate_person_post_deid import (
    RepopulatePersonPostDeid, REPOPULATE_PERSON_QUERY, GENDER_CONCEPT_ID,
    AOU_NONE_INDICATED_CONCEPT_ID, AOU_NON_INDICATED_SOURCE_VALUE)
from common import PERSON
from constants.bq_utils import WRITE_TRUNCATE
from constants.cdr_cleaner import clean_cdr as clean_consts


class RepopulatePersonPostDeidTest(unittest.TestCase):

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

        self.rule_instance = RepopulatePersonPostDeid(self.project_id,
                                                      self.dataset_id,
                                                      self.sandbox_id)

        self.assertEqual(self.rule_instance.project_id, self.project_id)
        self.assertEqual(self.rule_instance.dataset_id, self.dataset_id)
        self.assertEqual(self.rule_instance.sandbox_dataset_id, self.sandbox_id)

    def test_setup_rule(self):
        # Test
        self.rule_instance.setup_rule(self.client)

    def test_get_query_specs(self):
        # Pre conditions
        self.assertEqual(self.rule_instance.affected_datasets,
                         [clean_consts.REGISTERED_TIER_DEID_BASE])

        # Test
        results_list = self.rule_instance.get_query_specs()

        repopulate_query_dict = {
            cdr_consts.QUERY:
                REPOPULATE_PERSON_QUERY.render(
                    project=self.project_id,
                    dataset=self.dataset_id,
                                        gender_concept_id=GENDER_CONCEPT_ID,
                    aou_custom_concept=AOU_NONE_INDICATED_CONCEPT_ID,
                    aou_custom_value=AOU_NON_INDICATED_SOURCE_VALUE),
            cdr_consts.DESTINATION_TABLE:
                PERSON,
            cdr_consts.DESTINATION_DATASET:
                self.dataset_id,
            cdr_consts.DISPOSITION:
                WRITE_TRUNCATE
        }

        self.assertEqual(results_list, [repopulate_query_dict])
