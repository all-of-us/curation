import unittest
import mock
from google.appengine.ext import testbed

import common
import os
import gcs_utils
import bq_utils
import test_util
from validation import ehr_merge
from validation.export import query_result_to_payload

FAKE_HPO_ID = 'fake'
PITT_HPO_ID = 'pitt'
CHS_HPO_ID = 'chs'
CONDITION_OCCURRENCE_COUNT = 45
DRUG_EXPOSURE_COUNT = 47
MEASUREMENT_COUNT = 234
PERSON_COUNT = 5
PROCEDURE_OCCURRENCE_COUNT = 50
VISIT_OCCURRENCE_COUNT = 50


class EhrMergeTest(unittest.TestCase):
    def setUp(self):
        super(EhrMergeTest, self).setUp()
        self.testbed = testbed.Testbed()
        self.testbed.activate()
        self.testbed.init_app_identity_stub()
        self.testbed.init_memcache_stub()
        self.testbed.init_urlfetch_stub()
        self.testbed.init_blobstore_stub()
        self.testbed.init_datastore_v3_stub()
        self.hpo_bucket = gcs_utils.get_hpo_bucket(test_util.FAKE_HPO_ID)
        self.pitt_bucket = gcs_utils.get_hpo_bucket(PITT_HPO_ID)
        self.chs_bucket = gcs_utils.get_hpo_bucket(CHS_HPO_ID)
        self.project_id = bq_utils.app_identity.get_application_id()
        self._empty_bucket = test_util.empty_bucket
        self._empty_bucket(self.hpo_bucket)

    def _load_datasets(self):
        load_jobs = []
        for cdm_table in common.CDM_TABLES:
            cdm_file_name = os.path.join(test_util.FIVE_PERSONS_PATH, cdm_table + '.csv')
            if os.path.exists(cdm_file_name):
                test_util.write_cloud_file(self.chs_bucket, cdm_file_name)
                test_util.write_cloud_file(self.pitt_bucket, cdm_file_name)
            else:
                test_util.write_cloud_str(self.chs_bucket, cdm_table + '.csv', 'dummy\n')
                test_util.write_cloud_str(self.pitt_bucket, cdm_table + '.csv', 'dummy\n')
            chs_load_results = bq_utils.load_cdm_csv(CHS_HPO_ID, cdm_table)
            pitt_load_results = bq_utils.load_cdm_csv(PITT_HPO_ID, cdm_table)
            chs_load_job_id = chs_load_results['jobReference']['jobId']
            pitt_load_job_id = pitt_load_results['jobReference']['jobId']
            load_jobs.append(chs_load_job_id)
            load_jobs.append(pitt_load_job_id)
        incomplete_jobs = bq_utils.wait_on_jobs(load_jobs, retry_count=7)
        if len(incomplete_jobs) > 0:
            raise RuntimeError('BigQuery jobs %s failed to complete' % incomplete_jobs)

    @mock.patch('api_util.check_cron')
    def test_merge_EHR(self, mock_check_cron):
        self._load_datasets()
        # enable exception propagation as described at https://goo.gl/LqDgnj
        old_dataset_items = bq_utils.list_dataset_contents(bq_utils.get_dataset_id())
        expected_items = ['visit_id_mapping_table']
        expected_items.extend(['unioned_ehr_' + table_name for table_name in common.CDM_TABLES])

        ehr_merge.merge(bq_utils.get_dataset_id(), self.project_id)
        # check the result files were placed in bucket
        dataset_items = bq_utils.list_dataset_contents(bq_utils.get_dataset_id())
        for table_name in common.CDM_TABLES:
            cmd = 'SELECT COUNT(1) FROM unioned_ehr_{}'.format(table_name)
            result = bq_utils.query(cmd)
            self.assertEqual(int(result['rows'][0]['f'][0]['v']),
                             2*globals().get(table_name.upper() + '_COUNT', 0),
                             msg='failed for table unioned_ehr_{}'.format(table_name))
        self.assertSetEqual(set(old_dataset_items + expected_items), set(dataset_items))

        table_name = 'condition_occurrence'
        cmd_union = 'SELECT * FROM unioned_ehr_{}'.format(table_name)
        cmd_pitt = 'SELECT * FROM pitt_{}'.format(table_name)
        cmd_visit_mapping = "SELECT global_visit_id, mapping_visit_id FROM visit_id_mapping_table where hpo='pitt'"
        qr_union = bq_utils.query(cmd_union)
        qr_pitt = bq_utils.query(cmd_pitt)
        qr_visit_mapping = bq_utils.query(cmd_visit_mapping)

        union_result = query_result_to_payload(qr_union)
        pitt_result = query_result_to_payload(qr_pitt)
        visit_mapping_result = query_result_to_payload(qr_visit_mapping)

        def get_element_from_list_of_lists(index, list_of_lists):
            return [list_item[index] for list_item in list_of_lists]

        for ind, pitt_visit_id in enumerate(pitt_result['VISIT_OCCURRENCE_ID']):
            if pitt_visit_id not in visit_mapping_result['MAPPING_VISIT_ID']:
                continue
            global_visit_id_index = visit_mapping_result['MAPPING_VISIT_ID'].index(pitt_visit_id)
            global_visit_id = visit_mapping_result['GLOBAL_VISIT_ID'][global_visit_id_index]
            union_visit_id_index = union_result['VISIT_OCCURRENCE_ID'].index(global_visit_id)
            pitt_cols_without_id = [values for key, values in pitt_result.items()
                                    if key not in [u'VISIT_OCCURRENCE_ID', u'CONDITION_OCCURRENCE_ID']]
            union_cols_without_id = [values for key, values in union_result.items()
                                     if key not in [u'VISIT_OCCURRENCE_ID', u'CONDITION_OCCURRENCE_ID']]
            self.assertListEqual(get_element_from_list_of_lists(ind, pitt_cols_without_id),
                                 get_element_from_list_of_lists(union_visit_id_index, union_cols_without_id))

    def test_query_construction(self):
        table_name = 'condition_occurrence'
        hpos_to_merge = ['chs', 'pitt', 'chci']
        hpos_with_visit = ['chci', 'pitt']
        project_id = 'dummy_project'
        dataset_id = 'dummy_dataset'

        # checks whther the required blocks exits
        q = ehr_merge.construct_query(table_name, hpos_to_merge, hpos_with_visit, project_id, dataset_id)
        self.assertIn('global_visit_id', q)  # condition table requires a join
        self.assertIn('visit_id_mapping_table', q)
        self.assertIn("visit_id_map.hpo = 'pitt'", q)
        self.assertIn("visit_id_map.hpo = 'chci'", q)
        self.assertNotIn("visit_id_map.hpo = 'chs'", q)  # chs visit doesn't exist

        # checks that the visit blocks do not exist
        q = ehr_merge.construct_query('person', hpos_to_merge, hpos_with_visit, project_id, dataset_id)
        self.assertNotIn('global_visit_id', q)  # condition table requires a join
        self.assertNotIn('visit_id_mapping_table', q)
        self.assertNotIn("visit_id_map.hpo = 'pitt'", q)
        self.assertNotIn("visit_id_map.hpo = 'chci'", q)
        self.assertNotIn("visit_id_map.hpo = 'chs'", q)  # chs visit doesn't exist

    def tearDown(self):
        delete_list = ['visit_id_mapping_table'] + ['unioned_ehr_' + table_name for table_name in common.CDM_TABLES]
        existing_tables = bq_utils.list_dataset_contents(bq_utils.get_dataset_id())
        for table_id in delete_list:
            if table_id not in common.VOCABULARY_TABLES and table_id in existing_tables:
                bq_utils.delete_table(table_id)
        self._empty_bucket(self.hpo_bucket)
        self.testbed.deactivate()
