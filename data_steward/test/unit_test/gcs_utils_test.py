import unittest

from google.appengine.ext import testbed
from googleapiclient.errors import HttpError

import gcs_utils
from test_util import FIVE_PERSONS_PERSON_CSV, FAKE_HPO_ID


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
        self.hpo_bucket = gcs_utils.get_hpo_bucket(FAKE_HPO_ID)
        self.gcs_path = '/'.join([self.hpo_bucket, 'dummy'])
        self._empty_bucket()

    def _empty_bucket(self):
        bucket_items = gcs_utils.list_bucket(self.hpo_bucket)
        for bucket_item in bucket_items:
            gcs_utils.delete_object(self.hpo_bucket, bucket_item['name'])

    def test_upload_object(self):
        bucket_items = gcs_utils.list_bucket(self.hpo_bucket)
        self.assertEqual(len(bucket_items), 0)
        with open(FIVE_PERSONS_PERSON_CSV, 'rb') as fp:
            gcs_utils.upload_object(self.hpo_bucket, 'person.csv', fp)
        bucket_items = gcs_utils.list_bucket(self.hpo_bucket)
        self.assertEqual(len(bucket_items), 1)
        bucket_item = bucket_items[0]
        self.assertEqual(bucket_item['name'], 'person.csv')

    def test_get_object(self):
        with open(FIVE_PERSONS_PERSON_CSV, 'rb') as fp:
            expected = fp.read()
        with open(FIVE_PERSONS_PERSON_CSV, 'rb') as fp:
            gcs_utils.upload_object(self.hpo_bucket, 'person.csv', fp)
        result = gcs_utils.get_object(self.hpo_bucket, 'person.csv')
        self.assertEqual(expected, result)

    def test_get_metadata_on_existing_file(self):
        expected_file_name = 'person.csv'
        with open(FIVE_PERSONS_PERSON_CSV, 'rb') as fp:
            gcs_utils.upload_object(self.hpo_bucket, expected_file_name, fp)
        metadata = gcs_utils.get_metadata(self.hpo_bucket, expected_file_name)
        self.assertIsNotNone(metadata)
        self.assertEqual(metadata['name'], expected_file_name)

    def test_get_metadata_on_not_existing_file(self):
        expected = 100
        actual = gcs_utils.get_metadata(self.hpo_bucket, 'this_file_does_not_exist', expected)
        self.assertEqual(expected, actual)

    def test_list_bucket_404_when_bucket_does_not_exist(self):
        with self.assertRaises(HttpError) as cm:
            gcs_utils.list_bucket('some-bucket-which-does-not-exist-123')
        self.assertEqual(cm.exception.resp.status, 404)

    def tearDown(self):
        self._empty_bucket()
        self.testbed.deactivate()
