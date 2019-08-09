import unittest

import mock
from google.appengine.ext import testbed

import gcs_utils
from test.unit_test import test_util
from tools import retract_data_gcs as rd


class RetractionTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

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
        self.pids = [1, 2]
        self._empty_bucket()

    def _empty_bucket(self):
        bucket_items = gcs_utils.list_bucket(self.hpo_bucket)
        for bucket_item in bucket_items:
            gcs_utils.delete_object(self.hpo_bucket, bucket_item['name'])

    def test_five_person_data_retraction(self):
        folder_prefix = 'dummy-prefix-2018-03-22/'
        lines_to_remove = {}
        total_lines_prior = {}
        for file_path in test_util.FIVE_PERSONS_FILES:
            # generate results files
            file_name = file_path.split('/')[-1]
            lines_to_remove[file_name] = 0
            total_lines_prior[file_name] = 0
            with open(file_path) as f:
                for line in f:
                    line = line.strip()
                    if line != '':
                        if (file_name in rd.PID_IN_COL1 and rd.get_integer(line.split(",")[0]) in self.pids) or \
                                (file_name in rd.PID_IN_COL2 and rd.get_integer(line.split(",")[1]) in self.pids):
                            lines_to_remove[file_name] += 1
                        total_lines_prior[file_name] += 1

            # write file to cloud for testing
            test_util.write_cloud_file(self.hpo_bucket, file_path, prefix=folder_prefix)

        with mock.patch('__builtin__.raw_input', return_value='Y') as _raw_input:
            retract_result = rd.run_retraction(self.pids, self.hpo_bucket, folder=folder_prefix, force_flag=True)

        total_lines_post = {}
        for file_path in test_util.FIVE_PERSONS_FILES:
            file_name = file_path.split('/')[-1]
            actual_result_contents = test_util.read_cloud_file(self.hpo_bucket, folder_prefix + file_name)
            # convert to list and remove last list item since it is a newline
            total_lines_post[file_name] = len(actual_result_contents.split('\n')[:-1])

        for key in total_lines_prior.keys():
            if key in lines_to_remove:
                self.assertEqual(lines_to_remove[key], total_lines_prior[key] - total_lines_post[key])
            else:
                self.assertEqual(total_lines_prior[key], total_lines_post[key])

        # metadata for each updated file is returned
        # TODO test that files lacking records for PID are not updated
        self.assertEqual(len(retract_result[folder_prefix]), len(lines_to_remove.keys()))

    def tearDown(self):
        self._empty_bucket()
        bucket_nyc = gcs_utils.get_hpo_bucket('nyc')
        test_util.empty_bucket(bucket_nyc)
        test_util.empty_bucket(gcs_utils.get_drc_bucket())
        self.testbed.deactivate()
