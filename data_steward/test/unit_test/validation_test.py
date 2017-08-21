import unittest

import mock
import cloudstorage
from cloudstorage import cloudstorage_api  # stubbed by testbed
from google.appengine.ext import testbed

from validation import main

_FAKE_BUCKET = 'report_fake_bucket'


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

    def _write_cloud_csv(self, file_name, contents_str):
        with cloudstorage_api.open('/%s/%s' % (_FAKE_BUCKET, file_name), mode='w') as cloud_file:
            cloud_file.write(contents_str.encode('utf-8'))

    def _read_cloud_file(self, filename):
        with cloudstorage.open(filename) as cloudstorage_file:
            return cloudstorage_file.read()

    @mock.patch('api_util.check_cron')
    def test_validation_check_cron(self, mock_check_cron):
        main.validate_hpo_files('dummy')
        self.assertEquals(mock_check_cron.call_count, 1)

    def test_find_cdm_files(self):
        self._write_cloud_csv('person.csv', 'a,b,c,d')
        self._write_cloud_csv('visit_occurrence.csv', '1,2,3,4')
        cdm_files = main._find_cdm_files(_FAKE_BUCKET)
        self.assertEquals(len(cdm_files), 2)

    @mock.patch('api_util.check_cron')
    def test_validate_missing_files_output(self, mock_check_cron):
        bucket_name = main.hpo_gcs_path('foo')
        url = main.PREFIX + 'ValidateHpoFiles/foo'
        expected_result_file = '/' + bucket_name + '/result.csv'
        main.app.testing = True
        with main.app.test_client() as c:
            c.get(url)
            bucket_stat_list = list(cloudstorage_api.listbucket('/' + bucket_name))
            self.assertEquals(1, len(bucket_stat_list))
            bucket_stat = bucket_stat_list[0]
            self.assertEquals(expected_result_file, bucket_stat.filename)
            cloud_file = self._read_cloud_file(expected_result_file)
            print cloud_file

    def tearDown(self):
        self.testbed.deactivate()
