import unittest

import common
from retraction import participant_row_counts as prc
from tests.unit_tests.data_steward.retraction.retract_utils_test import RetractUtilsTest
from constants.retraction import participant_row_counts as consts


class ParticipantPrevalenceTest(RetractUtilsTest, unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        super(ParticipantPrevalenceTest, self).setUp()

    def test_get_combined_deid_query(self):
        actual = prc.get_combined_deid_query(self.project_id,
                                             self.dataset_id,
                                             self.pid_table_str,
                                             self.table_df,
                                             for_deid=False)
        queries = actual.split(consts.UNION_ALL)

        for query in queries:
            self.assertIn('SELECT person_id\nFROM', query)
            self.assertNotIn('SELECT research_id\nFROM', query)
            tables = set(self.TABLE_REGEX.findall(query))
            self.assertLessEqual(len(tables), 2)
            self.assertGreaterEqual(len(tables), 1)
            if len(tables) == 2:
                table = tables.pop()
                if common.MAPPING_PREFIX in table:
                    map_table = table
                    table = tables.pop()
                else:
                    map_table = tables.pop()
                self.assertIn(table, self.mapped_cdm_tables)
                self.assertNotIn(table, self.unmapped_cdm_tables)
                self.assertIn(map_table, self.mapping_tables + self.ext_tables)
            elif len(tables) == 1:
                table = tables.pop()
                self.assertIn(table, self.unmapped_cdm_tables)
                self.assertNotIn(table, self.mapped_cdm_tables)
                if table == common.PERSON:
                    self.assertIn('0 AS ehr_count', query)
                elif table == common.DEATH:
                    self.assertIn('COUNT(*) AS ehr_count', query)

        actual = prc.get_combined_deid_query(self.project_id,
                                             self.dataset_id,
                                             self.pid_table_str,
                                             self.table_df,
                                             for_deid=True)
        queries = actual.split(consts.UNION_ALL)

        for query in queries:
            self.assertIn('SELECT research_id\nFROM', query)
            self.assertNotIn('SELECT person_id\nFROM', query)
            tables = set(self.TABLE_REGEX.findall(query))
            self.assertLessEqual(len(tables), 2)
            self.assertGreaterEqual(len(tables), 1)
            if len(tables) == 2:
                table = tables.pop()
                if common.MAPPING_PREFIX in table:
                    map_table = table
                    table = tables.pop()
                else:
                    map_table = tables.pop()
                self.assertIn(table, self.mapped_cdm_tables)
                self.assertNotIn(table, self.unmapped_cdm_tables)
                self.assertIn(map_table, self.mapping_tables + self.ext_tables)
            elif len(tables) == 1:
                table = tables.pop()
                self.assertIn(table, self.unmapped_cdm_tables)
                self.assertNotIn(table, self.mapped_cdm_tables)
                if table == common.PERSON:
                    self.assertIn('0 AS ehr_count', query)
                elif table == common.DEATH:
                    self.assertIn('COUNT(*) AS ehr_count', query)

    def test_get_dataset_query(self):
        actual = prc.get_dataset_query(self.project_id, self.dataset_id,
                                       self.pid_table_str, self.table_df)
        queries = actual.split(consts.UNION_ALL)

        for query in queries:
            tables = self.TABLE_REGEX.findall(query)
            self.assertEqual(len(tables), 1)
            table = tables[0]
            self.assertIn(table, self.cdm_tables)
            self.assertIn('COUNT(*) AS ehr_count', query)

    def test_get_ehr_query(self):
        actual = prc.get_ehr_query(self.project_id, self.dataset_id,
                                   self.pid_table_str, self.hpo_id,
                                   self.ehr_table_df)
        queries = actual.split(consts.UNION_ALL)

        for query in queries:
            tables = self.TABLE_REGEX.findall(query)
            self.assertEqual(len(tables), 1)
            table = tables[0]
            self.assertIn(table, self.ehr_tables)
            self.assertIn('COUNT(*) AS ehr_count', query)
