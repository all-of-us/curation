import unittest

import mock
from google.appengine.ext import testbed

import gcs_utils
import resources
import common
import json
from spec import main
from test_util import FIVE_PERSONS_SUCCESS_RESULT_CSV, FIVE_PERSONS_SUCCESS_RESULT_NO_HPO_JSON
from test_util import ALL_FILES_UNPARSEABLE_VALIDATION_RESULT, ALL_FILES_UNPARSEABLE_VALIDATION_RESULT_NO_HPO_JSON


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
            result = main._generate_site()
            self.assertEquals(result, 'okay')

            # verify that page worked
            bucket = gcs_utils.get_drc_bucket()
            file_count = 0
            for stat in gcs_utils.list_bucket(bucket):
                filename = stat['name']
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

    def _empty_bucket(self, bucket):
        bucket_items = gcs_utils.list_bucket(bucket)
        for bucket_item in bucket_items:
            gcs_utils.delete_object(bucket, bucket_item['name'])

    def _empty_hpo_buckets(self):
        hpos = resources.hpo_csv()
        for hpo in hpos:
            hpo_id = hpo['hpo_id']
            bucket = gcs_utils.get_hpo_bucket(hpo_id)
            self._empty_bucket(bucket)

    def test_hpo_log_item_to_obj(self):
        hpo_id = 'foo'
        log_item = dict(cdm_file_name='person.csv', found='0', parsed='0', loaded='0')
        expected = dict(hpo_id=hpo_id, file_name='person.csv', table_name='person', found=False, parsed=False, loaded=False)
        actual = main.hpo_log_item_to_obj(hpo_id, log_item)
        self.assertDictEqual(expected, actual)

        log_item = dict(cdm_file_name='person.csv', found='1', parsed='1', loaded='1')
        expected = dict(hpo_id=hpo_id, file_name='person.csv', table_name='person', found=True, parsed=True, loaded=True)
        actual = main.hpo_log_item_to_obj(hpo_id, log_item)
        self.assertDictEqual(expected, actual)

    def assertResultLogItemsEqual(self, l, r):
        def sort_key(item):
            return item['hpo_id'], item['file_name']
        l.sort(key=sort_key)
        r.sort(key=sort_key)
        pairs = zip(l, r)
        for e, a in pairs:
            self.assertDictEqual(e, a)

    def test_get_full_result_log_when_all_exist(self):
        self._empty_hpo_buckets()
        hpos = resources.hpo_csv()
        hpo_0 = hpos[0]
        hpo_0_bucket = gcs_utils.get_hpo_bucket(hpo_0['hpo_id'])
        with open(FIVE_PERSONS_SUCCESS_RESULT_CSV, 'r') as fp:
            gcs_utils.upload_object(hpo_0_bucket, common.RESULT_CSV, fp)

        with open(FIVE_PERSONS_SUCCESS_RESULT_NO_HPO_JSON, 'r') as fp:
            hpo_0_expected_items = json.load(fp)
            for item in hpo_0_expected_items:
                item['hpo_id'] = hpo_0['hpo_id']

        hpo_1 = hpos[1]
        hpo_1_bucket = gcs_utils.get_hpo_bucket(hpo_1['hpo_id'])
        with open(ALL_FILES_UNPARSEABLE_VALIDATION_RESULT, 'r') as fp:
            gcs_utils.upload_object(hpo_1_bucket, common.RESULT_CSV, fp)

        with open(ALL_FILES_UNPARSEABLE_VALIDATION_RESULT_NO_HPO_JSON, 'r') as fp:
            hpo_1_expected_items = json.load(fp)
            for item in hpo_1_expected_items:
                item['hpo_id'] = hpo_1['hpo_id']

        expected = hpo_0_expected_items + hpo_1_expected_items
        actual = main.get_full_result_log()
        self.assertResultLogItemsEqual(expected, actual)

    def test_get_full_result_log_when_one_does_not_exist(self):
        self._empty_hpo_buckets()
        hpos = resources.hpo_csv()
        hpo_0 = hpos[0]
        hpo_0_bucket = gcs_utils.get_hpo_bucket(hpo_0['hpo_id'])
        with open(FIVE_PERSONS_SUCCESS_RESULT_CSV, 'r') as fp:
            gcs_utils.upload_object(hpo_0_bucket, common.RESULT_CSV, fp)
        with open(FIVE_PERSONS_SUCCESS_RESULT_NO_HPO_JSON, 'r') as fp:
            expected = json.load(fp)
            for item in expected:
                item['hpo_id'] = hpo_0['hpo_id']
        actual = main.get_full_result_log()
        self.assertResultLogItemsEqual(expected, actual)

    def tearDown(self):
        self._empty_hpo_buckets()
        self.testbed.deactivate()
