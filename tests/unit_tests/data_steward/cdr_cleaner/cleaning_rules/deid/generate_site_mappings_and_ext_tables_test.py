"""
Unit test for generate_site_mappings_and_ext_tables

Original Issue: DC-1351

create non-deterministic site ids for ehr sites and create the ext_tables
"""

# Python imports
import unittest
import mock

# Project imports
from cdr_cleaner.cleaning_rules.deid.generate_site_mappings_and_ext_tables import (
    GenerateSiteMappingsAndExtTables, SITE_MASKINGS_QUERY,
    PIPELINE_TABLES_DATASET, SITE_MASKING_TABLE_ID)
from constants.cdr_cleaner import clean_cdr as clean_consts
import common
import constants.bq_utils as bq_consts
import constants.cdr_cleaner.clean_cdr as cdr_consts
import tools.generate_ext_tables as gen_ext


class GenerateSiteMappingsTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.project_id = 'foo_project'
        self.dataset_id = 'foo_dataset'
        self.sandbox_id = 'foo_sandbox_dataset'
        self.mapping_dataset_id = 'foo_mapping_dataset'
        self.mapping_tables = [
            gen_ext.MAPPING_PREFIX + cdm_table
            for cdm_table in common.AOU_REQUIRED
            if cdm_table not in
            [common.PERSON, common.DEATH, common.FACT_RELATIONSHIP]
        ]
        self.client = None

        self.rule_instance = GenerateSiteMappingsAndExtTables(
            self.project_id, self.dataset_id, self.sandbox_id,
            self.mapping_dataset_id)

        self.assertEqual(self.rule_instance.project_id, self.project_id)
        self.assertEqual(self.rule_instance.dataset_id, self.dataset_id)
        self.assertEqual(self.rule_instance.sandbox_dataset_id, self.sandbox_id)

    def test_setup_rule(self):
        # Test
        self.rule_instance.setup_rule(self.client)

    @mock.patch(
        'cdr_cleaner.cleaning_rules.deid.generate_site_mappings_and_ext_tables.get_generate_ext_table_queries'
    )
    def test_get_query_specs(self, mock_get_generate_ext_table_queries):
        self.assertEqual(self.rule_instance.affected_datasets,
                         [clean_consts.CONTROLLED_TIER_DEID])
        fake_query_dict = {
            clean_consts.QUERY: 'Fake query',
            cdr_consts.DESTINATION_TABLE: 'fake_table',
            cdr_consts.DESTINATION_DATASET: self.dataset_id,
            cdr_consts.DISPOSITION: bq_consts.WRITE_EMPTY
        }
        mock_get_generate_ext_table_queries.return_value = [fake_query_dict]
        # Test
        actual_list = self.rule_instance.get_query_specs()
        expected_list = [{
            clean_consts.QUERY:
                SITE_MASKINGS_QUERY.render(
                    project_id=self.project_id,
                    dataset_id=self.sandbox_id,
                    pipelines_dataset=PIPELINE_TABLES_DATASET,
                    site_masking_table=SITE_MASKING_TABLE_ID)
        }, fake_query_dict]

        self.assertEqual(actual_list, expected_list)
