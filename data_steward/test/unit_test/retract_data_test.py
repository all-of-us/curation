import unittest

import mock
from google.appengine.ext import testbed

import gcs_utils
import test_util
from tools import retract_data


class RetractionTest(unittest.TestCase):
    def setUp(self):
        super(RetractionTest, self).setUp()
        self.testbed = testbed.Testbed()
        self.testbed.activate()
        self.testbed.init_app_identity_stub()
        self.testbed.init_memcache_stub()
        self.testbed.init_urlfetch_stub()
        self.testbed.init_blobstore_stub()
        self.testbed.init_datastore_v3_stub()
        self.hpo_bucket = gcs_utils.get_hpo_bucket(test_util.FAKE_HPO_ID)
        self._empty_bucket()

    def _empty_bucket(self):
        bucket_items = gcs_utils.list_bucket(self.hpo_bucket)
        for bucket_item in bucket_items:
            gcs_utils.delete_object(self.hpo_bucket, bucket_item['name'])

    def test_five_person_data_retraction(self):
        folder_prefix = 'dummy-prefix-2018-03-22/'
        pid = 7
        expected_result = {}
        for file_path in test_util.FIVE_PERSONS_FILES:
            # generate results files
            file_name = file_path.split('/')[-1]
            expected_result[file_name] = []
            with open(file_path) as f:
                for line in f:
                    line = line.strip()
                    if line != '':
                        if (file_name in retract_data.PID_IN_COL1 and line.split(',')[0] != pid) or \
                                (file_name in retract_data.PID_IN_COL2 and line.split(',')[1] != pid):
                                expected_result[file_name].append(line)

            # write file to cloud for testing
            test_util.write_cloud_file(self.hpo_bucket, file_path, prefix=folder_prefix)

        with mock.patch('__builtin__.raw_input', return_value='Y') as _raw_input:
            retract_data.retract_from_bucket(pid, self.hpo_bucket, folder_path=folder_prefix, force=True)

        actual_result = {}
        for file_path in test_util.FIVE_PERSONS_FILES:
            file_name = file_path.split('/')[-1]
            actual_result_contents = test_util.read_cloud_file(self.hpo_bucket, folder_prefix + file_name)
            # convert to list and remove last list item since it is a newline
            actual_result[file_name] = actual_result_contents.split('\n')[:-1]

        for key in expected_result.keys():
            self.assertListEqual(expected_result[key], actual_result[key])

    def tearDown(self):
        self._empty_bucket()
        bucket_nyc = gcs_utils.get_hpo_bucket('nyc')
        test_util.empty_bucket(bucket_nyc)
        test_util.empty_bucket(gcs_utils.get_drc_bucket())
        self.testbed.deactivate()
