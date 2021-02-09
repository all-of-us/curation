"""
Unit test for remove_fitbit_data_if_max_age_exceeded module

Original Issue: DC-1001, DC-1037

Ensures any participant with FitBit data who is over the age of 89 is dropped from
activity_summary, steps_intraday, heart_rate_summary, and heart_rate_minute_level
FitBit tables.
"""

# Python imports
import unittest

# Project imports
from cdr_cleaner.cleaning_rules.deid.generate_site_mappings import (
    GenerateSiteMappings, SITE_MAPPINGS_QUERY, SITE_TABLE_ID,
    HPO_SITE_ID_MAPPINGS_TABLE_ID, LOOKUP_TABLES_DATASET_ID, EHR_SITE_PREFIX,
    RDR, PPI_PM)
from constants.cdr_cleaner import clean_cdr as clean_consts


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
        self.client = None

        self.rule_instance = GenerateSiteMappings(self.project_id,
                                                  self.dataset_id,
                                                  self.sandbox_id)

        self.assertEqual(self.rule_instance.project_id, self.project_id)
        self.assertEqual(self.rule_instance.dataset_id, self.dataset_id)
        self.assertEqual(self.rule_instance.sandbox_dataset_id, self.sandbox_id)

    def test_setup_rule(self):
        # Test
        self.rule_instance.setup_rule(self.client)

    def test_get_query_specs(self):
        self.assertEqual(self.rule_instance.affected_datasets,
                         [clean_consts.CONTROLLED_TIER_DEID])

        # Test
        results_list = self.rule_instance.get_query_specs()

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

        self.assertEqual(results_list, expected_list)

    def test_log_queries(self):
        # Pre conditions
        self.assertEqual(self.rule_instance.affected_datasets,
                         [clean_consts.CONTROLLED_TIER_DEID])

        # Test
        with self.assertLogs(level='INFO') as cm:
            self.rule_instance.log_queries()

            site_id_query = SITE_MAPPINGS_QUERY.render(
                project_id=self.project_id,
                dataset_id=self.sandbox_id,
                table_id=SITE_TABLE_ID,
                site_prefix=EHR_SITE_PREFIX,
                lookup_tabels_dataset=LOOKUP_TABLES_DATASET_ID,
                hpo_site_id_mappings_table=HPO_SITE_ID_MAPPINGS_TABLE_ID,
                ppi_pm=PPI_PM,
                rdr=RDR)

            expected = [
                'INFO:cdr_cleaner.cleaning_rules.base_cleaning_rule:Generated SQL Query:\n'
                + site_id_query,
            ]

            # Post condition
            self.assertEqual(cm.output, expected)
