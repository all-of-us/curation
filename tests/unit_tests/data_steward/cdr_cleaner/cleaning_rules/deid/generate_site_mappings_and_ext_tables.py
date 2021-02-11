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
    GenerateSiteMappingsAndExtTables, SITE_MAPPINGS_QUERY, SITE_TABLE_ID,
    HPO_SITE_ID_MAPPINGS_TABLE_ID, LOOKUP_TABLES_DATASET_ID, EHR_SITE_PREFIX,
    RDR, PPI_PM)
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
        self.mapping_dataset_id = 'foo_dataset'
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

    @mock.patch('bq_utils.create_table')
    @mock.patch('tools.generate_ext_tables.get_mapping_table_ids')
    def test_get_query_specs(
        self,
        mock_mapping_tables,
        mock_create_table,
    ):
        mock_mapping_tables.return_value = self.mapping_tables
        self.assertEqual(self.rule_instance.affected_datasets,
                         [clean_consts.CONTROLLED_TIER_DEID])

        # Test
        actual_list = self.rule_instance.get_query_specs()

        mock_mapping_tables.return_value = self.mapping_tables
        expected_list = [{
            clean_consts.QUERY:
                SITE_MAPPINGS_QUERY.render(
                    project_id=self.project_id,
                    dataset_id=self.sandbox_id,
                    table_id=SITE_TABLE_ID,
                    site_prefix=EHR_SITE_PREFIX,
                    lookup_tabels_dataset=LOOKUP_TABLES_DATASET_ID,
                    hpo_site_id_mappings_table=HPO_SITE_ID_MAPPINGS_TABLE_ID,
                    ppi_pm=PPI_PM,
                    rdr=RDR)
        }]

        for cdm_table in common.AOU_REQUIRED:
            if cdm_table not in [
                    common.PERSON, common.DEATH, common.FACT_RELATIONSHIP
            ]:
                query = dict()
                query[cdr_consts.QUERY] = gen_ext.REPLACE_SRC_QUERY.render(
                    project_id=self.project_id,
                    dataset_id=self.dataset_id,
                    mapping_dataset_id=self.dataset_id,
                    sandbox_dataset_id=self.sandbox_id,
                    mapping_table_id=gen_ext.MAPPING_PREFIX + cdm_table,
                    site_mappings_table_id=SITE_TABLE_ID,
                    cdm_table_id=cdm_table)
                query[cdr_consts.
                      DESTINATION_TABLE] = cdm_table + gen_ext.EXT_TABLE_SUFFIX
                query[cdr_consts.DESTINATION_DATASET] = self.dataset_id
                query[cdr_consts.DISPOSITION] = bq_consts.WRITE_EMPTY
                expected_list.append(query)

        # Post conditions
        expected_list = [{
            clean_consts.QUERY:
                SITE_MAPPINGS_QUERY.render(
                    project_id=self.project_id,
                    dataset_id=self.sandbox_id,
                    table_id=SITE_TABLE_ID,
                    site_prefix=EHR_SITE_PREFIX,
                    lookup_tabels_dataset=LOOKUP_TABLES_DATASET_ID,
                    hpo_site_id_mappings_table=HPO_SITE_ID_MAPPINGS_TABLE_ID,
                    ppi_pm=PPI_PM,
                    rdr=RDR)
        }]

        self.assertEqual(actual_list, expected_list)
