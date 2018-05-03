import unittest
import os
import common
import gcs_utils
import bq_utils
import resources
import test_util

from tools.combine_ehr_rdr import copy_rdr_person, consented_person, CONSENTED_PERSON_TABLE_ID
from google.appengine.ext import testbed


class CombineEhrRdrTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.testbed = testbed.Testbed()
        cls.testbed.activate()
        cls.testbed.init_app_identity_stub()
        cls.testbed.init_memcache_stub()
        cls.testbed.init_urlfetch_stub()
        cls.testbed.init_blobstore_stub()
        cls.testbed.init_datastore_v3_stub()
        ehr_dataset_id = bq_utils.get_dataset_id()
        rdr_dataset_id = bq_utils.get_rdr_dataset_id()
        test_util.delete_all_tables(ehr_dataset_id)
        test_util.delete_all_tables(rdr_dataset_id)
        cls.load_dataset_from_files(ehr_dataset_id, test_util.NYC_FIVE_PERSONS_PATH)
        cls.load_dataset_from_files(rdr_dataset_id, test_util.RDR_PATH)

    @staticmethod
    def load_dataset_from_files(dataset_id, path):
        app_id = bq_utils.app_identity.get_application_id()
        bucket = gcs_utils.get_hpo_bucket(test_util.FAKE_HPO_ID)
        test_util.empty_bucket(bucket)
        job_ids = []
        fs = test_util.list_files_in(path)
        for f in fs:
            filename = f.split(os.sep)[-1]
            assert filename in common.CDM_FILES
            table, _ = filename.split('.')
            schema = os.path.join(resources.fields_path, table + '.json')
            gcs_path = 'gs://{bucket}/{filename}'.format(bucket=bucket, filename=filename)
            with open(f, 'r') as fp:
                response = gcs_utils.upload_object(bucket, filename, fp)
            load_results = bq_utils.load_csv(schema, gcs_path, app_id, dataset_id, table)
            load_job_id = load_results['jobReference']['jobId']
            job_ids.append(load_job_id)
        incomplete_jobs = bq_utils.wait_on_jobs(job_ids)
        if len(incomplete_jobs) > 0:
            message = "Job id(s) %s failed to complete" % incomplete_jobs
            raise RuntimeError(message)
        test_util.empty_bucket(bucket)

    def setUp(self):
        super(CombineEhrRdrTest, self).setUp()
        self.APP_ID = bq_utils.app_identity.get_application_id()
        self.COMBINED_DATASET_ID = bq_utils.get_ehr_rdr_dataset_id()
        self.DRC_BUCKET = gcs_utils.get_drc_bucket()
        test_util.delete_all_tables(self.COMBINED_DATASET_ID)

    def test_consented_person_id(self):
        """
        Test observation data has seven (7) persons with consent records as described below
         1: No
         2: Yes
         3: NULL
         4: No  followed by Yes
         5: Yes followed by No
         6: Yes followed by NULL
         7: NULL and Yes with same date/time
        """
        # sanity check
        self.assertFalse(bq_utils.table_exists(CONSENTED_PERSON_TABLE_ID, self.COMBINED_DATASET_ID))
        consented_person()
        self.assertTrue(bq_utils.table_exists(CONSENTED_PERSON_TABLE_ID, self.COMBINED_DATASET_ID),
                        'Table {dataset}.{table} created by consented_person'.format(dataset=self.COMBINED_DATASET_ID,
                                                                                     table=CONSENTED_PERSON_TABLE_ID))
        response = bq_utils.query('SELECT * FROM {dataset}.{table}'.format(dataset=self.COMBINED_DATASET_ID,
                                                                           table=CONSENTED_PERSON_TABLE_ID))
        rows = test_util.response2rows(response)
        expected = {2, 4}
        actual = set(row['person_id'] for row in rows)
        self.assertSetEqual(expected,
                            actual,
                            'Records in {dataset}.{table}'.format(dataset=self.COMBINED_DATASET_ID,
                                                                  table=CONSENTED_PERSON_TABLE_ID))

    def test_copy_rdr_person(self):
        consented_person()
        # person records from rdr with consent
        self.assertFalse(bq_utils.table_exists('person', self.COMBINED_DATASET_ID))  # sanity check
        copy_rdr_person()
        self.assertTrue(bq_utils.table_exists('person', self.COMBINED_DATASET_ID))

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
        test_util.delete_all_tables(self.COMBINED_DATASET_ID)

    @classmethod
    def tearDownClass(cls):
        ehr_dataset_id = bq_utils.get_dataset_id()
        rdr_dataset_id = bq_utils.get_rdr_dataset_id()
        test_util.delete_all_tables(ehr_dataset_id)
        test_util.delete_all_tables(rdr_dataset_id)
        cls.testbed.deactivate()
