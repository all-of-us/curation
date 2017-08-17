import unittest
from google.appengine.ext import testbed
import mock

from cloudstorage import cloudstorage_api  # stubbed by testbed

import main

_FAKE_BUCKET = 'report_fake_bucket'


class ReportTest(unittest.TestCase):
    def setUp(self):
        super(ReportTest, self).setUp()
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

    @mock.patch('api_util.check_cron')
    def test_report_check_cron(self, mock_check_cron):
        main.run_report()
        self.assertEquals(mock_check_cron.call_count, 1)

    def test_find_cdm_files(self):
        self._write_cloud_csv('person.csv', 'a,b,c,d')
        self._write_cloud_csv('visit_occurrence.csv', '1,2,3,4')
        cdm_files = main._find_cdm_files(_FAKE_BUCKET)
        self.assertEquals(len(cdm_files), 2)

    def tearDown(self):
        self.testbed.deactivate()
