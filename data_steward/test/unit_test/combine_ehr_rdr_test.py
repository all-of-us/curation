import unittest
import bq_utils
import common
import gcs_utils
import test_util
import resources
import os

from google.appengine.api import app_identity
from google.appengine.ext import testbed


class CombineEhrRdrTest(unittest.TestCase):
    BUCKET = gcs_utils.get_hpo_bucket(test_util.FAKE_HPO_ID)
    EHR_DATASET_ID = bq_utils.get_dataset_id()
    RDR_DATASET_ID = bq_utils.get_rdr_dataset_id()
    APP_ID = app_identity.get_application_id()

    @classmethod
    def setUpClass(cls):
        # Ensure test EHR and RDR datasets
        cls._empty_bucket()
        cls._load_ehr_and_rdr_datasets()

    @classmethod
    def _empty_bucket(cls):
        bucket_items = gcs_utils.list_bucket(CombineEhrRdrTest.BUCKET)
        for bucket_item in bucket_items:
            gcs_utils.delete_object(CombineEhrRdrTest.BUCKET, bucket_item['name'])

    @classmethod
    def _load_dataset_from_files(cls, dataset_id, path):
        job_ids = []
        for cdm_table in common.CDM_TABLES:
            cdm_file_name = os.path.join(path, cdm_table + '.csv')
            if os.path.exists(cdm_file_name):
                test_util.write_cloud_file(CombineEhrRdrTest.BUCKET, cdm_file_name)
            else:
                # load empty table (headers are skipped)
                test_util.write_cloud_str(CombineEhrRdrTest.BUCKET, cdm_table + '.csv', '\n')
            fields_filename = os.path.join(resources.fields_path, cdm_table + '.json')
            gcs_object_path = 'gs://%s/%s.csv' % (CombineEhrRdrTest.BUCKET, cdm_table)
            load_results = bq_utils.load_csv(fields_filename, gcs_object_path, cls.APP_ID, dataset_id, cdm_table)
            job_ids.append(load_results['jobReference']['jobId'])
        bq_utils.wait_on_jobs(job_ids)

    @classmethod
    def _load_ehr_and_rdr_datasets(cls):
        cls._load_dataset_from_files(CombineEhrRdrTest.EHR_DATASET_ID, test_util.NYC_FIVE_PERSONS_PATH)
        cls._load_dataset_from_files(CombineEhrRdrTest.RDR_DATASET_ID, test_util.RDR_PATH)

    def setUp(self):
        super(CombineEhrRdrTest, self).setUp()
        self.testbed = testbed.Testbed()
        self.testbed.activate()
        self.testbed.init_app_identity_stub()
        self.testbed.init_memcache_stub()
        self.testbed.init_urlfetch_stub()
        self.testbed.init_blobstore_stub()
        self.testbed.init_datastore_v3_stub()
        self.app_id = app_identity.get_application_id()
        # TODO clear the combined dataset

    def test_copy_rdr_person(self):
        # person table from rdr is used
        pass

    def test_ehr_only_records_excluded(self):
        # any ehr records whose person_id is missing from rdr are excluded
        pass

    def test_rdr_only_records_included(self):
        # all rdr records are included whether or not there is corresponding ehr
        pass

    def test_ehr_person_to_observation(self):
        # ehr person table converts to observation records
        pass

    def tearDown(self):
        # TODO clear the combined dataset
        pass

    @classmethod
    def tearDownClass(cls):
        cls._empty_bucket()

