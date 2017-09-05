import unittest

import bq_utils
import gcs_utils
from google.appengine.ext import testbed
from test_util import FAKE_HPO_ID, FIVE_PERSONS_PERSON_CSV

PERSON = 'person'


class BqUtilsTest(unittest.TestCase):
    def setUp(self):
        super(BqUtilsTest, self).setUp()
        self.testbed = testbed.Testbed()
        self.testbed.activate()
        self.testbed.init_app_identity_stub()
        self.testbed.init_memcache_stub()
        self.testbed.init_urlfetch_stub()
        self.testbed.init_blobstore_stub()
        self.testbed.init_datastore_v3_stub()
        self.hpo_bucket = gcs_utils.get_hpo_bucket(FAKE_HPO_ID)
        self.person_table_id = bq_utils.get_table_id(FAKE_HPO_ID, PERSON)
        self._drop_tables()
        self._empty_bucket()

    def _empty_bucket(self):
        bucket_items = gcs_utils.list_bucket(self.hpo_bucket)
        for bucket_item in bucket_items:
            gcs_utils.delete_object(self.hpo_bucket, bucket_item['name'])

    def _drop_tables(self):
        if bq_utils.table_exists(self.person_table_id):
            bq_utils.delete_table(self.person_table_id)

    def test_load_table_from_bucket(self):
        with open(FIVE_PERSONS_PERSON_CSV, 'rb') as fp:
            gcs_utils.upload_object(self.hpo_bucket, 'person.csv', fp)
        result = bq_utils.load_table_from_bucket(FAKE_HPO_ID, PERSON)
        self.assertEqual(result['status']['state'], 'RUNNING')

    def test_load_table_from_bucket_error_on_bad_table_name(self):
        with self.assertRaises(ValueError) as cm:
            bq_utils.load_table_from_bucket(FAKE_HPO_ID, 'not_a_cdm_table')

    def tearDown(self):
        self._drop_tables()
        self._empty_bucket()
        self.testbed.deactivate()
