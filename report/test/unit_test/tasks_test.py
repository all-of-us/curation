import unittest
from google.appengine.ext import testbed
import mock

import cloudstorage  # stubbed by testbed
import tasks

_FAKE_DRC_SHARE_BUCKET = 'fake-drc'


class TasksTest(unittest.TestCase):
    def setUp(self):
        super(TasksTest, self).setUp()
        self.testbed = testbed.Testbed()
        self.testbed.activate()
        self.testbed.init_app_identity_stub()
        self.testbed.init_memcache_stub()
        self.testbed.init_urlfetch_stub()
        self.testbed.init_blobstore_stub()
        self.testbed.init_datastore_v3_stub()

    @mock.patch('api_util.check_cron')
    def test_report_check_cron(self, mock_check_cron):
        with tasks.app.test_request_context():
            tasks._generate_site()
            self.assertEquals(mock_check_cron.call_count, 1)

    @mock.patch('api_util.check_cron')
    def test_site_generation(self, mock_check_cron):
        with tasks.app.test_request_context():
            result = tasks._generate_site(_FAKE_DRC_SHARE_BUCKET)
            self.assertEquals(result, 'okay')

            # verify that page worked
            bucket = '/' + _FAKE_DRC_SHARE_BUCKET
            file_count = 0
            for stat in cloudstorage.listbucket(bucket, delimiter='/'):
                assert (not stat.is_dir)
                filename = stat.filename.split('/')[-1]
                assert (filename in
                        [name + '.html' for name in ['report', 'data_model', 'file_transfer_procedures', 'index']])
                file_count += 1
            self.assertEquals(file_count, 4)

    def tearDown(self):
        self.testbed.deactivate()
