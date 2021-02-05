import unittest
from datetime import datetime

import mock

import bq_utils
from constants import bq_utils as bq_utils_consts


class BqUtilsTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.hpo_id = 'fake-hpo'

    def test_load_cdm_csv_error_on_bad_table_name(self):
        self.assertRaises(ValueError, bq_utils.load_cdm_csv, self.hpo_id,
                          'not_a_cdm_table')

    @mock.patch('bq_utils.job_status_done', lambda x: True)
    def test_wait_on_jobs_already_done(self):
        job_ids = range(3)
        actual = bq_utils.wait_on_jobs(job_ids)
        expected = []
        self.assertEqual(actual, expected)

    @mock.patch('time.sleep', return_value=None)
    @mock.patch('bq_utils.job_status_done', return_value=False)
    def test_wait_on_jobs_all_fail(self, mock_job_status_done, mock_time_sleep):
        job_ids = list(range(3))
        actual = bq_utils.wait_on_jobs(job_ids)
        expected = job_ids
        self.assertEqual(actual, expected)
        # TODO figure out how to count this
        # self.assertEquals(mock_time_sleep.call_count, bq_utils.BQ_DEFAULT_RETRY_COUNT)

    @mock.patch('time.sleep', return_value=None)
    @mock.patch(
        'bq_utils.job_status_done',
        side_effect=[False, False, False, True, False, False, True, True, True])
    def test_wait_on_jobs_get_done(self, mock_job_status_done, mock_time_sleep):
        job_ids = list(range(3))
        actual = bq_utils.wait_on_jobs(job_ids)
        expected = []
        self.assertEqual(actual, expected)

    @mock.patch('time.sleep', return_value=None)
    @mock.patch('bq_utils.job_status_done',
                side_effect=[
                    False, False, True, False, False, False, False, False,
                    False, False, False, False
                ])
    def test_wait_on_jobs_some_fail(self, mock_job_status_done,
                                    mock_time_sleep):
        job_ids = list(range(2))
        actual = bq_utils.wait_on_jobs(job_ids)
        expected = [1]
        self.assertEqual(actual, expected)

    @mock.patch('bq_utils.sleeper')
    @mock.patch('bq_utils.job_status_done')
    def test_wait_on_jobs_retry_count(self, mock_job_status, mock_sleep):
        max_sleep_interval = 512
        mock_job_status.return_value = False
        job_ids = ["job_1", "job_2"]
        bq_utils.wait_on_jobs(job_ids)
        mock_sleep.assert_called_with(max_sleep_interval)

    @mock.patch('bq_utils.os.environ.get')
    def test_get_validation_results_dataset_id_not_existing(self, mock_env_var):
        # preconditions
        mock_env_var.return_value = bq_utils_consts.BLANK

        # test
        result_id = bq_utils.get_validation_results_dataset_id()

        # post conditions
        date_string = datetime.now().strftime(bq_utils_consts.DATE_FORMAT)
        expected = bq_utils_consts.VALIDATION_DATASET_FORMAT.format(date_string)
        self.assertEqual(result_id, expected)

    @mock.patch('bq_utils.os.environ.get')
    def test_get_validation_results_dataset_id_existing(self, mock_env_var):
        # preconditions
        mock_env_var.return_value = 'dataset_foo'

        # test
        result_id = bq_utils.get_validation_results_dataset_id()

        # post conditions
        expected = 'dataset_foo'
        self.assertEqual(result_id, expected)
