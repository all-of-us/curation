from __future__ import print_function
import os
import unittest


import bq_utils
import common
import resources
from test.unit_test import test_util
from tools.top_heel_errors import top_heel_errors, RESULT_LIMIT, FIELD_ANALYSIS_ID, FIELD_RECORD_COUNT, \
    FIELD_DATASET_NAME, FIELD_ACHILLES_HEEL_WARNING

HPO_NYC = 'nyc_cu'
HPO_PITT = 'pitt'


def is_error(r):
    return r.get(FIELD_ACHILLES_HEEL_WARNING, '').startswith('ERROR:')


def get_int(r, key):
    return int(r.get(key) or 0)


def sort_key(r):
    return get_int(r, FIELD_RECORD_COUNT)


def top_n_errors(rs):
    """
    In-memory impl of common heel errors

    :param rs: list of achilles_heel_results records
    :return: top N errors from `rs` with highest record count
    """
    errors = filter(is_error, rs)
    sorted_errors = sorted(errors, key=sort_key, reverse=True)
    return sorted_errors[:RESULT_LIMIT]


def comparison_view(rs):
    """
    Compact view on list of achilles_heel_results records to use for comparison

    :param rs: list of achilles_heel_results records
    :return: list of records with dataset_name, analysis_id, record_count keys
    """
    return [(r[FIELD_DATASET_NAME], get_int(r, FIELD_ANALYSIS_ID), get_int(r, FIELD_RECORD_COUNT)) for r in rs]


class TopHeelErrorsTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.app_id = os.environ.get('APPLICATION_ID')
        self.dataset_id = os.environ.get('BIGQUERY_DATASET_ID')
        self.bucket = os.environ.get('DRC_BUCKET_NAME')
        test_util.empty_bucket(self.bucket)
        test_util.delete_all_tables(self.dataset_id)
        self.load_test_data(hpo_id=HPO_NYC)

    def load_test_data(self, hpo_id=None):
        """
        Load to bq test achilles heel results data from csv file

        :param hpo_id: if specified, prefix to use on csv test file and bq table, otherwise no prefix is used
        :return: contents of the file as list of objects
        """
        schema_path = os.path.join(resources.fields_path, common.ACHILLES_HEEL_RESULTS + '.json')
        table_id = common.ACHILLES_HEEL_RESULTS
        if hpo_id is not None:
            table_id = bq_utils.get_table_id(hpo_id, common.ACHILLES_HEEL_RESULTS)
        test_file_name = table_id + '.csv'
        test_file_path = os.path.join(test_util.TEST_DATA_PATH, test_file_name)
        test_util.write_cloud_file(self.bucket, test_file_path)
        gcs_path = 'gs://' + self.bucket + '/' + test_file_name
        load_results = bq_utils.load_csv(schema_path, gcs_path, self.app_id, self.dataset_id, table_id)
        job_id = load_results['jobReference']['jobId']
        bq_utils.wait_on_jobs([job_id])
        return resources._csv_to_list(test_file_path)

    def test_top_heel_errors_no_hpo_prefix(self):
        rows = self.load_test_data()
        for row in rows:
            row[FIELD_DATASET_NAME] = self.dataset_id
        errors = top_n_errors(rows)
        expected_results = comparison_view(errors)
        dataset_errors = top_heel_errors(self.app_id, self.dataset_id)
        actual_results = comparison_view(dataset_errors)
        self.assertItemsEqual(actual_results, expected_results)

    def test_top_heel_errors_all_hpo(self):
        expected_results = []
        for hpo_id in [HPO_NYC, HPO_PITT]:
            rows = self.load_test_data(hpo_id)
            for row in rows:
                row[FIELD_DATASET_NAME] = hpo_id
            errors = top_n_errors(rows)
            expected_results += comparison_view(errors)
        dataset_errors = top_heel_errors(self.app_id, self.dataset_id, all_hpo=True)
        actual_results = comparison_view(dataset_errors)
        self.assertItemsEqual(actual_results, expected_results)

    def tearDown(self):
        test_util.empty_bucket(self.bucket)
        test_util.delete_all_tables(self.dataset_id)
