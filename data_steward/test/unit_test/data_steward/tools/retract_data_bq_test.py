import unittest
import mock

import bq_utils
import cdm
import common
import resources
from tools import retract_data_bq
from validation import ehr_union


class RetractDataBqTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.hpo_id = 'fake_hpo'
        self.project_id = 'fake-project-id'
        self.ehr_dataset_id = 'ehr20190801'
        self.combined_dataset_id = 'combined20190801'
        self.person_ids = [1, 2, 1234567]
        self.tables_to_retract = retract_data_bq.TABLES_WITH_PID | {common.FACT_RELATIONSHIP}

    def test_is_combined_dataset(self):
        actual_result = retract_data_bq.is_combined_dataset('combined20190801')
        self.assertTrue(actual_result)
        actual_result = retract_data_bq.is_combined_dataset('combined20190801_base')
        self.assertTrue(actual_result)
        actual_result = retract_data_bq.is_combined_dataset('combined20190801_clean')
        self.assertTrue(actual_result)
        actual_result = retract_data_bq.is_combined_dataset('combined20190801_deid')
        self.assertFalse(actual_result)
        actual_result = retract_data_bq.is_combined_dataset('combined20190801_deid_v1')
        self.assertFalse(actual_result)

    @mock.patch('bq_utils.get_dataset_id')
    def test_is_ehr_dataset(self, mock_get_dataset_id):
        mock_get_dataset_id.return_value = 'fake_dataset_id'
        actual_result = retract_data_bq.is_ehr_dataset('fake_dataset_id')
        self.assertTrue(actual_result)
        actual_result = retract_data_bq.is_ehr_dataset('ehr20190801')
        self.assertTrue(actual_result)
        actual_result = retract_data_bq.is_ehr_dataset('unioned_ehr20190801')
        self.assertFalse(actual_result)
        actual_result = retract_data_bq.is_ehr_dataset('unioned_ehr_20190801')
        self.assertFalse(actual_result)

    @mock.patch('tools.retract_data_bq.list_existing_tables')
    def test_queries_to_retract_from_ehr_dataset(self, mock_list_existing_tables):
        # TODO test inclusion of person for ehr and exclusion after combine
        hpo_person = bq_utils.get_table_id(self.hpo_id, common.PERSON)
        hpo_death = bq_utils.get_table_id(self.hpo_id, common.DEATH)

        # hpo tables
        existing_table_ids = [hpo_person, hpo_death]
        for table in self.tables_to_retract:
            table_id = bq_utils.get_table_id(self.hpo_id, table)
            existing_table_ids.append(table_id)

        # unioned tables
        ignored_tables = []
        for cdm_table in resources.CDM_TABLES:
            unioned_table_id = retract_data_bq.UNIONED_EHR + cdm_table
            existing_table_ids.append(unioned_table_id)

            if cdm_table not in self.tables_to_retract:
                ignored_tables.append(unioned_table_id)

        mapped_tables = cdm.tables_to_map()

        # fact_relationship does not have pid, is handled separate from other mapped tables
        for mapped_table in mapped_tables:
            mapping_table = ehr_union.mapping_table_for(mapped_table)
            existing_table_ids.append(mapping_table)
            legacy_mapping_table = retract_data_bq.UNIONED_EHR + mapping_table
            existing_table_ids.append(legacy_mapping_table)
            if mapped_table not in self.tables_to_retract:
                ignored_tables.append(mapping_table)
                ignored_tables.append(legacy_mapping_table)

        mock_list_existing_tables.return_value = existing_table_ids
        qs = retract_data_bq.queries_to_retract_from_ehr_dataset(self.project_id, self.ehr_dataset_id, self.hpo_id,
                                                                 self.person_ids)
        actual_dest_tables = set(q[retract_data_bq.DEST_TABLE] for q in qs)
        expected_dest_tables = set(existing_table_ids) - set(hpo_person) - set(ignored_tables)
        self.assertSetEqual(expected_dest_tables, actual_dest_tables)

    @mock.patch('tools.retract_data_bq.list_existing_tables')
    def test_queries_to_retract_from_combined_or_deid_dataset(self, mock_list_existing_tables):
        existing_table_ids = []
        ignored_tables = []
        for cdm_table in resources.CDM_TABLES:
            existing_table_ids.append(cdm_table)
            if cdm_table not in self.tables_to_retract:
                ignored_tables.append(cdm_table)

        mapped_tables = cdm.tables_to_map()
        for mapped_table in mapped_tables:
            mapping_table = ehr_union.mapping_table_for(mapped_table)
            existing_table_ids.append(mapping_table)
            if mapped_table not in self.tables_to_retract:
                ignored_tables.append(mapping_table)

        mock_list_existing_tables.return_value = existing_table_ids
        qs = retract_data_bq.queries_to_retract_from_combined_or_deid_dataset(self.project_id, self.combined_dataset_id,
                                                                              self.person_ids)
        actual_dest_tables = set(q[retract_data_bq.DEST_TABLE] for q in qs)
        expected_dest_tables = set(existing_table_ids) - set(ignored_tables)
        self.assertSetEqual(expected_dest_tables, actual_dest_tables)

        # death query should use person_id as-is (no constant factor)
        constant_factor = common.RDR_ID_CONSTANT + common.ID_CONSTANT_FACTOR
        for q in qs:
            if q[retract_data_bq.DEST_TABLE] == common.DEATH:
                self.assertNotIn(str(constant_factor), q[retract_data_bq.QUERY])

    def tearDown(self):
        pass
