import unittest

from google.appengine.ext import testbed

import gcs_utils
from test.unit_test import test_util
from tools import retract_data_gcs as rd


class RetractDataGcsTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        super(RetractDataGcsTest, self).setUp()
        self.testbed = testbed.Testbed()
        self.testbed.activate()
        self.testbed.init_app_identity_stub()
        self.testbed.init_memcache_stub()
        self.testbed.init_urlfetch_stub()
        self.testbed.init_blobstore_stub()
        self.testbed.init_datastore_v3_stub()
        self.hpo_id = test_util.FAKE_HPO_ID
        self.bucket = gcs_utils.get_hpo_bucket(self.hpo_id)
        self.site_bucket = 'test_bucket'
        self.folder_1 = '2019-01-01-v1/'
        self.folder_2 = '2019-02-02-v2/'
        self.pids = [17, 20]
        self.skip_pids = [10, 25]
        self._empty_bucket()

    def _empty_bucket(self):
        bucket_items = gcs_utils.list_bucket(self.bucket)
        for bucket_item in bucket_items:
            gcs_utils.delete_object(self.bucket, bucket_item['name'])

    def test_integration_five_person_data_retraction_skip(self):
        folder_prefix_1 = self.hpo_id+'/'+self.site_bucket+'/'+self.folder_1
        folder_prefix_2 = self.hpo_id+'/'+self.site_bucket+'/'+self.folder_2
        lines_to_remove = {}
        total_lines_prior = {}
        for file_path in test_util.FIVE_PERSONS_FILES:
            # generate results files
            file_name = file_path.split('/')[-1]
            table_name = file_name.split('.')[0]
            lines_to_remove[file_name] = 0
            total_lines_prior[file_name] = 0
            with open(file_path) as f:
                for line in f:
                    line = line.strip()
                    if line != '':
                        if (table_name in rd.PID_IN_COL1 and rd.get_integer(line.split(",")[0]) in self.skip_pids) or \
                                (table_name in rd.PID_IN_COL2 and rd.get_integer(line.split(",")[1]) in self.skip_pids):
                            lines_to_remove[file_name] += 1
                        total_lines_prior[file_name] += 1

            # write file to cloud for testing
            test_util.write_cloud_file(self.bucket, file_path, prefix=folder_prefix_1)
            test_util.write_cloud_file(self.bucket, file_path, prefix=folder_prefix_2)

        retract_result = rd.run_retraction(self.skip_pids,
                                           self.bucket,
                                           self.hpo_id,
                                           self.site_bucket,
                                           folder=None,
                                           force_flag=True)

        total_lines_post = {}
        for file_path in test_util.FIVE_PERSONS_FILES:
            file_name = file_path.split('/')[-1]
            actual_result_contents = test_util.read_cloud_file(self.bucket, folder_prefix_1 + file_name)
            # convert to list and remove last list item since it is a newline
            total_lines_post[file_name] = len(actual_result_contents.split('\n')[:-1])

        for key in total_lines_prior.keys():
            if key in lines_to_remove:
                self.assertEqual(lines_to_remove[key], total_lines_prior[key] - total_lines_post[key])
            else:
                self.assertEqual(total_lines_prior[key], total_lines_post[key])

        # metadata for each updated file is returned
        for key, val in lines_to_remove.items():
            if val == 0:
                del lines_to_remove[key]
        self.assertEqual(len(retract_result[folder_prefix_1]), len(lines_to_remove.keys()))

    def test_integration_five_person_data_retraction(self):
        folder_prefix_1 = self.hpo_id+'/'+self.site_bucket+'/'+self.folder_1
        folder_prefix_2 = self.hpo_id+'/'+self.site_bucket+'/'+self.folder_2
        lines_to_remove = {}
        total_lines_prior = {}
        for file_path in test_util.FIVE_PERSONS_FILES:
            # generate results files
            file_name = file_path.split('/')[-1]
            table_name = file_name.split('.')[0]
            lines_to_remove[file_name] = 0
            total_lines_prior[file_name] = 0
            with open(file_path) as f:
                for line in f:
                    line = line.strip()
                    if line != '':
                        if (table_name in rd.PID_IN_COL1 and rd.get_integer(line.split(",")[0]) in self.pids) or \
                                (table_name in rd.PID_IN_COL2 and rd.get_integer(line.split(",")[1]) in self.pids):
                            lines_to_remove[file_name] += 1
                        total_lines_prior[file_name] += 1

            # write file to cloud for testing
            test_util.write_cloud_file(self.bucket, file_path, prefix=folder_prefix_1)
            test_util.write_cloud_file(self.bucket, file_path, prefix=folder_prefix_2)

        retract_result = rd.run_retraction(self.pids,
                                           self.bucket,
                                           self.hpo_id,
                                           self.site_bucket,
                                           folder=None,
                                           force_flag=True)

        total_lines_post = {}
        for file_path in test_util.FIVE_PERSONS_FILES:
            file_name = file_path.split('/')[-1]
            actual_result_contents = test_util.read_cloud_file(self.bucket, folder_prefix_1 + file_name)
            # convert to list and remove last list item since it is a newline
            total_lines_post[file_name] = len(actual_result_contents.split('\n')[:-1])

        for key in total_lines_prior.keys():
            if key in lines_to_remove:
                self.assertEqual(lines_to_remove[key], total_lines_prior[key] - total_lines_post[key])
            else:
                self.assertEqual(total_lines_prior[key], total_lines_post[key])

        # metadata for each updated file is returned
        self.assertEqual(len(retract_result[folder_prefix_1]), len(lines_to_remove.keys()))

    def tearDown(self):
        self._empty_bucket()
        test_util.empty_bucket(self.bucket)
        self.testbed.deactivate()
