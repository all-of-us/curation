from __future__ import print_function
import unittest

from tools.consolidated_reports import get_all_achilles_reports as achilles_report


class GetAllAchillesReportsTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.drc_bucket_name = 'drc-curation-internal-test'
        self.hpo_id = 'hpo-id'
        self.bucket_id = 'id-bar'
        self.search_dir = 'curation_report'
        self.filepath = 'gs://{}/{}/{}/{}'.format(
            self.drc_bucket_name, self.hpo_id, self.bucket_id, self.search_dir
        )
        self.original_drc_bucket_path = achilles_report.DRC_BUCKET_PATH
        achilles_report.DRC_BUCKET_PATH = 'gs://{}/'.format(self.drc_bucket_name)

    def tearDown(self):
        achilles_report.DRC_BUCKET_PATH = self.original_drc_bucket_path

    def test_get_hpo_id(self):
        # pre conditions

        # test
        result = achilles_report.get_hpo_id(self.filepath)

        # post conditions
        self.assertEqual(self.hpo_id, result)

    @unittest.expectedFailure
    def test_get_hpo_id_with_bad_bucket(self):
        # pre conditions
        filepath = self.filepath.replace(self.drc_bucket_name,
                                         self.drc_bucket_name + 'bunk')

        # test
        result = achilles_report.get_hpo_id(filepath)

        # post conditions
        self.assertEqual(self.hpo_id, result)

    def test_get_submission_name(self):
        # pre conditions

        # test
        result = achilles_report.get_submission_name(self.filepath)

        # post conditions
        self.assertEqual(self.bucket_id, result)

    def test_get_submission_name_error(self):
        # pre conditions
        filepath = self.filepath.replace(self.search_dir, 'bunk')

        # test
        self.assertRaises(
            RuntimeError,
            achilles_report.get_submission_name,
            filepath
        )

    def test_transform_bq_list(self):
        # pre conditions
        uploads_list = [
            {
                'upload_timestamp': '2019-01-01',
                'file_path': self.filepath,
            }
        ]

        # test
        result = achilles_report.transform_bq_list(uploads_list)

        # post conditions
        expected = [
            {
                'hpo_id': self.hpo_id,
                'updated': '2019-01-01',
                'report_path': self.filepath,
                'name': self.bucket_id,
            },
        ]
        self.assertEqual(result, expected)
