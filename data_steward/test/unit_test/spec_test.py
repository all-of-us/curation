import unittest

import cloudstorage  # stubbed by testbed
import mock
from google.appengine.ext import testbed
from google.appengine.api import urlfetch

from spec import main

_FAKE_DRC_SHARE_BUCKET = 'fake-drc'


class SpecTest(unittest.TestCase):
    def setUp(self):
        super(SpecTest, self).setUp()
        self.testbed = testbed.Testbed()
        self.testbed.activate()
        self.testbed.init_app_identity_stub()
        self.testbed.init_memcache_stub()
        self.testbed.init_urlfetch_stub()
        self.testbed.init_blobstore_stub()
        self.testbed.init_datastore_v3_stub()

    @mock.patch('api_util.check_cron')
    def test_spec_check_cron(self, mock_check_cron):
        with main.app.test_request_context():
            main._generate_site()
            self.assertEquals(mock_check_cron.call_count, 1)

    @mock.patch('api_util.check_cron')
    def test_site_generation(self, mock_check_cron):
        with main.app.test_request_context():
            result = main._generate_site(_FAKE_DRC_SHARE_BUCKET)
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

    @mock.patch('spec.main.pages.get_or_404')
    def test_variable_population(self, mock_get_or_404):
        def dummy():
            return
        dummy.meta = {'title': 'foo', 'template': 'test_empty'}
        dummy.title = 'foo'
        dummy.body = 'bar'
        mock_get_or_404.return_value = dummy
        result = main._page('dummy')
        self.assertEquals(u'<span>foo</span>', result)

    def tearDown(self):
        self.testbed.deactivate()
