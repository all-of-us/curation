# Pyton imports
import os
import unittest
from io import open

# Third party imports
from googleapiclient.errors import HttpError
import mock

# Project imports
import app_identity
import gcs_utils
from gcloud.gcs import StorageClient
from tests import test_util
from tests.test_util import FIVE_PERSONS_PERSON_CSV, FAKE_HPO_ID


class GcsUtilsTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')
        cls.env_patcher = mock.patch.dict(
            os.environ,
            {"GAE_SERVICE": test_util.get_unique_service_name(cls.__name__)})
        cls.env_patcher.start()

        # Run delete before insert in case old entries are not successfully
        # deleted in hpo_id_bucket_name from previous test runs
        test_util.delete_hpo_id_bucket_name(os.environ.get("GAE_SERVICE"))
        test_util.insert_hpo_id_bucket_name(os.environ.get("GAE_SERVICE"))

    def setUp(self):
        self.hpo_bucket = gcs_utils.get_hpo_bucket(FAKE_HPO_ID)
        self.gcs_path = '/'.join([self.hpo_bucket, 'dummy'])
        self.project_id = app_identity.get_application_id()
        self.storage_client = StorageClient(self.project_id)
        self.storage_client.empty_bucket(self.hpo_bucket)

    def test_upload_object(self):
        bucket_items = gcs_utils.list_bucket(self.hpo_bucket)
        self.assertEqual(len(bucket_items), 0)
        with open(FIVE_PERSONS_PERSON_CSV, 'rb') as fp:
            gcs_utils.upload_object(self.hpo_bucket, 'person.csv', fp)
        bucket_items = gcs_utils.list_bucket(self.hpo_bucket)
        self.assertEqual(len(bucket_items), 1)
        bucket_item = bucket_items[0]
        self.assertEqual(bucket_item['name'], 'person.csv')

    def test_get_metadata_on_existing_file(self):
        expected_file_name = 'person.csv'
        with open(FIVE_PERSONS_PERSON_CSV, 'rb') as fp:
            gcs_utils.upload_object(self.hpo_bucket, expected_file_name, fp)
        metadata = gcs_utils.get_metadata(self.hpo_bucket, expected_file_name)
        self.assertIsNotNone(metadata)
        self.assertEqual(metadata['name'], expected_file_name)

    def test_get_metadata_on_not_existing_file(self):
        expected = 100
        actual = gcs_utils.get_metadata(self.hpo_bucket,
                                        'this_file_does_not_exist', expected)
        self.assertEqual(expected, actual)

    def test_list_bucket_404_when_bucket_does_not_exist(self):
        with self.assertRaises(HttpError) as cm:
            gcs_utils.list_bucket('some-bucket-which-does-not-exist-123')
        self.assertEqual(cm.exception.resp.status, 404)

    def tearDown(self):
        self.storage_client.empty_bucket(self.hpo_bucket)

    @classmethod
    def tearDownClass(cls):
        test_util.delete_hpo_id_bucket_name(os.environ.get("GAE_SERVICE"))
        cls.env_patcher.stop()
