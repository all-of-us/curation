import unittest

import os
import StringIO
import mock
from google.appengine.ext import testbed

import gcs_utils
from validation import main
import resources

_FAKE_HPO = 'foo'

TEST_DATA_PATH = os.path.join(resources.base_path, 'test', 'test_data')
EMPTY_VALIDATION_RESULT = os.path.join(TEST_DATA_PATH, 'empty_validation_result.csv')


class ValidationTest(unittest.TestCase):
    def setUp(self):
        super(ValidationTest, self).setUp()
        self.testbed = testbed.Testbed()
        self.testbed.activate()
        self.testbed.init_app_identity_stub()
        self.testbed.init_memcache_stub()
        self.testbed.init_urlfetch_stub()
        self.testbed.init_blobstore_stub()
        self.testbed.init_datastore_v3_stub()
        self.hpo_bucket = gcs_utils.get_hpo_bucket(_FAKE_HPO)
        self._empty_bucket()

    def _empty_bucket(self):
        bucket_items = gcs_utils.list_bucket(self.hpo_bucket)
        for bucket_item in bucket_items:
            gcs_utils.delete_object(self.hpo_bucket, bucket_item['name'])

    def _write_cloud_csv(self, bucket, name, contents_str):
        fp = StringIO.StringIO(contents_str)
        return gcs_utils.upload_object(bucket, name, fp)

    def _read_cloud_file(self, bucket, name):
        return gcs_utils.get_object(bucket, name)

    @mock.patch('api_util.check_cron')
    def test_validation_check_cron(self, mock_check_cron):
        main.validate_hpo_files(_FAKE_HPO)
        self.assertEquals(mock_check_cron.call_count, 1)

    def test_find_cdm_files(self):
        self._write_cloud_csv(self.hpo_bucket, 'person.csv', 'a,b,c,d')
        self._write_cloud_csv(self.hpo_bucket, 'visit_occurrence.csv', '1,2,3,4')
        cdm_files = main._find_cdm_files(self.hpo_bucket)
        self.assertEquals(len(cdm_files), 2)

    @mock.patch('api_util.check_cron')
    def test_validate_missing_files_output(self, mock_check_cron):
        validate_hpo_files_url = main.PREFIX + 'ValidateHpoFiles/' + _FAKE_HPO

        # enable exception propagation as described at https://goo.gl/LqDgnj
        main.app.testing = True
        with main.app.test_client() as c:
            c.get(validate_hpo_files_url)

            # check the result file was put in bucket
            bucket_items = gcs_utils.list_bucket(self.hpo_bucket)
            self.assertEquals(1, len(bucket_items))
            self.assertEquals(main.RESULT_CSV, bucket_items[0]['name'])

            # check content of the file is correct
            actual = self._read_cloud_file(self.hpo_bucket, main.RESULT_CSV)
            with open(EMPTY_VALIDATION_RESULT, 'r') as f:
                expected = f.read()
                self.assertEqual(expected, actual)

    def tearDown(self):
        self._empty_bucket()
        self.testbed.deactivate()
