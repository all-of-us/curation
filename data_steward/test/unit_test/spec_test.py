import unittest

import cloudstorage  # stubbed by testbed
import mock
from google.appengine.ext import testbed

import jinja2
import unicodedata
from spec import main

_FAKE_DRC_SHARE_BUCKET = 'fake-drc'

# move to some utils function
def unicode_to_str(string):
    return unicodedata.normalize('NFKD', string).encode('ascii','ignore')


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
    
    @mock.patch('api_util.check_cron')
    def test_variable_population(self, mock_check_cron):

        pages = main.pages
        page =  pages.get('index')

        self.assertEquals(page.meta['title'], 'Home')
        self.assertEquals(page.meta['foo'], 'foo')
        self.assertEquals(page.meta['bar'], 'lolstillfoo')
        assert(page.meta.get('usehtml',None) is None)

        for page in pages:
            template = "{{page.path}},{{page.title}}"
            out = jinja2.Template(template).render(page=page)
            self.assertEquals(unicode_to_str(out), "{},{}".format(page.path, page.meta['title']))

    def tearDown(self):
        self.testbed.deactivate()
