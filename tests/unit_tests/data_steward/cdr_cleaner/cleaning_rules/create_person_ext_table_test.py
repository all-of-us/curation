"""
Unit test for create_person_ext_table cleaning rule

Original Issues: DC-1012

Background
In order to avoid further changes to the standard OMOP person table, two non-standard fields will be housed in a
person_ext table.

Cleaning rule script to run AFTER deid.
This cleaning rule will populate the person_ext table
The following fields will need to be copied from the observation table:
src_id (from observation_ext, should all be “PPI/PM”)
state_of_residence_concept_id: the value_source_concept_id field in the OBSERVATION table row where
observation_source_concept_id  = 1585249 (StreetAddress_PIIState)
state_of_residence_source_value: the concept_name from the concept table for the state_of_residence_concept_id
person_id (as research_id) can be pulled from the person table
"""

# Python imports
import unittest

# Project imports
from cdr_cleaner.cleaning_rules.create_person_ext_table import CreatePersonExtTable, PERSON_EXT_TABLE_QUERY
from constants.cdr_cleaner import clean_cdr as clean_consts


class CreatePersonExtTableTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.project_id = 'test_project'
        self.dataset_id = 'test_dataset'
        self.sandbox_id = 'test_sandbox'
        self.table_namer = 'test_tablenamer'
        self.client = None

        self.rule_instance = CreatePersonExtTable(self.project_id,
                                                  self.dataset_id,
                                                  self.sandbox_id,
                                                  self.table_namer)

        self.assertEqual(self.rule_instance.project_id, self.project_id)
        self.assertEqual(self.rule_instance.dataset_id, self.dataset_id)
        self.assertEqual(self.rule_instance.sandbox_dataset_id, self.sandbox_id)

    def test_setup_rule(self):
        # Test
        self.rule_instance.setup_rule(self.client)

    def test_get_query_specs(self):
        # Pre conditions
        self.assertEqual(self.rule_instance.affected_datasets, [
            clean_consts.REGISTERED_TIER_DEID_BASE,
            clean_consts.CONTROLLED_TIER_DEID_BASE
        ])

        # Test
        results_list = self.rule_instance.get_query_specs()

        # Post conditions
        expected_query_list = []

        expected_query_list.append({
            clean_consts.QUERY:
                PERSON_EXT_TABLE_QUERY.render(
                    project=self.project_id,
                    dataset=self.dataset_id,
                )
        })

        self.assertEqual(results_list, expected_query_list)
