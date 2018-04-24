import unittest
import mock
from google.appengine.ext import testbed

import common
import os
import gcs_utils
import bq_utils
import test_util
from validation import ehr_merge

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
            raise RuntimeError('loading taking too long')

    @mock.patch('api_util.check_cron')
    def test_merge_EHR(self, mock_check_cron):
        self._load_datasets()
        # enable exception propagation as described at https://goo.gl/LqDgnj
        old_dataset_items = bq_utils.list_dataset_contents(bq_utils.get_dataset_id())
        expected_items = ['visit_id_mapping_table']
        expected_items.extend(['unioned_ehr_' + table_name for table_name in common.CDM_TABLES])

        return_string = ehr_merge.merge(bq_utils.get_dataset_id(), self.project_id)
        self.assertEqual(return_string, "success: " + ','.join([CHS_HPO_ID, PITT_HPO_ID]))
        # check the result files were placed in bucket
        dataset_items = bq_utils.list_dataset_contents(bq_utils.get_dataset_id())
        for table_name in common.CDM_TABLES:
            cmd = 'SELECT COUNT(1) FROM unioned_ehr_{}'.format(table_name)
            result = bq_utils.query(cmd)
            self.assertEqual(int(result['rows'][0]['f'][0]['v']),
                             2*globals().get(table_name.upper() + '_COUNT', 0),
                             msg='failed for table unioned_ehr_{}'.format(table_name))
        self.assertSetEqual(set(old_dataset_items + expected_items), set(dataset_items))

    def tearDown(self):
        delete_list = ['visit_id_mapping_table'] + ['unioned_ehr_' + table_name for table_name in common.CDM_TABLES]
        for table_id in delete_list:
            if table_id not in common.VOCABULARY_TABLES:
                bq_utils.delete_table(table_id)
        self._empty_bucket(self.hpo_bucket)
        self.testbed.deactivate()
