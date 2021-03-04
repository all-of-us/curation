# Python imports
import unittest

# Project imports
from cdr_cleaner.cleaning_rules.null_person_birthdate import (
    NullPersonBirthdate, NULL_DATE_QUERY)
from constants.bq_utils import WRITE_TRUNCATE
from constants.cdr_cleaner import clean_cdr as clean_consts
from common import PERSON


class NullPersonBirthdateTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.project_id = 'foo_project'
        self.dataset_id = 'bar_dataset'
        self.sandbox_id = 'baz_sandbox'

        self.rule_instance = NullPersonBirthdate(self.project_id,
                                                 self.dataset_id,
                                                 self.sandbox_id)

        self.assertEqual(self.rule_instance.project_id, self.project_id)
        self.assertEqual(self.rule_instance.dataset_id, self.dataset_id)
        self.assertEqual(self.rule_instance.sandbox_dataset_id, self.sandbox_id)

    def test_get_query_specs(self):
        # Pre conditions
        self.assertEqual(self.rule_instance.affected_datasets,
                         [clean_consts.CONTROLLED_TIER_DEID])

        #Test
        results_list = self.rule_instance.get_query_specs()

        # Post conditions
        expected_list = [{
            clean_consts.QUERY:
                NULL_DATE_QUERY.render(project_id=self.project_id,
                                       dataset_id=self.dataset_id,
                                       person_table=PERSON)
        }]

        self.assertEqual(results_list, expected_list)
