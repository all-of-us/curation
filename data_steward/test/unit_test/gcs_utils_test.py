import unittest

import os

import gcs_utils
import resources
from google.appengine.ext import testbed

TEST_DATA_PATH = os.path.join(resources.base_path, 'test', 'test_data')
PERSON_5_CSV_PATH = os.path.join(TEST_DATA_PATH, 'person_5.csv')
FAKE_HPO = 'foo'


class GcsUtilsTest(unittest.TestCase):
    def setUp(self):
        super(GcsUtilsTest, self).setUp()
        self.testbed = testbed.Testbed()
        self.testbed.activate()
        self.testbed.init_app_identity_stub()
        self.testbed.init_memcache_stub()
        self.testbed.init_urlfetch_stub()
        self.testbed.init_blobstore_stub()
        self.testbed.init_datastore_v3_stub()
        self.hpo_bucket = gcs_utils.get_hpo_bucket(FAKE_HPO)
        self.gcs_path = '/'.join([self.hpo_bucket, 'dummy'])
        self._empty_bucket()

    def _empty_bucket(self):
        bucket_items = gcs_utils.list_bucket(self.hpo_bucket)
        for bucket_item in bucket_items:
            gcs_utils.delete_object(self.hpo_bucket, bucket_item['name'])

    def test_upload_object(self):
        bucket_items = gcs_utils.list_bucket(self.hpo_bucket)
        self.assertEqual(len(bucket_items), 0)
        with open(PERSON_5_CSV_PATH, 'rb') as fp:
            gcs_utils.upload_object(self.hpo_bucket, 'person.csv', fp)
        bucket_items = gcs_utils.list_bucket(self.hpo_bucket)
        self.assertEqual(len(bucket_items), 1)
        bucket_item = bucket_items[0]
        self.assertEqual(bucket_item['name'], 'person.csv')

    def test_get_object(self):
        with open(PERSON_5_CSV_PATH, 'rb') as fp:
            expected = fp.read()
        with open(PERSON_5_CSV_PATH, 'rb') as fp:
            gcs_utils.upload_object(self.hpo_bucket, 'person.csv', fp)
        result = gcs_utils.get_object(self.hpo_bucket, 'person.csv')
        self.assertEqual(expected, result)

    def tearDown(self):
        self._empty_bucket()
        self.testbed.deactivate()
