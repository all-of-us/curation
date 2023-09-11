# Python imports
import os
from unittest import mock, TestCase

# Project imports
import app_identity
import bq_utils
import common
from gcloud.bq import BigQueryClient
from gcloud.gcs import StorageClient
import resources
from tests import test_util
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
    errors = [r for r in rs if is_error(r)]
    sorted_errors = sorted(errors, key=sort_key, reverse=True)
    return sorted_errors[:RESULT_LIMIT]


def comparison_view(rs):
    """
    Compact view on list of achilles_heel_results records to use for comparison

    :param rs: list of achilles_heel_results records
    :return: list of records with dataset_name, analysis_id, record_count keys
    """
    return [(r[FIELD_DATASET_NAME], get_int(r, FIELD_ANALYSIS_ID),
             get_int(r, FIELD_RECORD_COUNT)) for r in rs]


class TopHeelErrorsTest(TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.project_id = app_identity.get_application_id()
        self.dataset_id = common.BIGQUERY_DATASET_ID
        self.storage_client = StorageClient(self.project_id)
        self.bq_client = BigQueryClient(self.project_id)
        self.drc_bucket = self.storage_client.get_drc_bucket()

        self.storage_client.empty_bucket(self.drc_bucket)
        test_util.delete_all_tables(self.bq_client, self.dataset_id)
        self.load_test_data(hpo_id=HPO_NYC)

    def load_test_data(self, hpo_id: str = None):
        """
        Load to bq test achilles heel results data from csv file

        :param hpo_id: if specified, prefix to use on csv test file and bq table, otherwise no prefix is used
        :return: contents of the file as list of objects
        """

        heel_results: str = common.ACHILLES_HEEL_RESULTS
        if hpo_id:
            table_id: str = resources.get_table_id(heel_results, hpo_id=hpo_id)
        else:
            table_id: str = heel_results
        table = self.drc_bucket.blob(f'{table_id}.csv')
        table_path: str = os.path.join(test_util.TEST_DATA_PATH, table.name)

        table.upload_from_filename(table_path)

        gcs_path: str = f'gs://{self.drc_bucket.name}/{table.name}'
        load_results = bq_utils.load_csv(heel_results, gcs_path,
                                         self.project_id, self.dataset_id,
                                         table_id)
        job_id = load_results['jobReference']['jobId']
        bq_utils.wait_on_jobs([job_id])
        return resources.csv_to_list(table_path)

    def test_top_heel_errors_no_hpo_prefix(self):
        rows = self.load_test_data()
        for row in rows:
            row[FIELD_DATASET_NAME] = self.dataset_id
        errors = top_n_errors(rows)
        expected_results = comparison_view(errors)
        dataset_errors = top_heel_errors(self.bq_client, self.dataset_id)
        actual_results = comparison_view(dataset_errors)
        self.assertCountEqual(actual_results, expected_results)

    @mock.patch('tools.top_heel_errors.get_hpo_ids')
    def test_top_heel_errors_all_hpo(self, mock_hpo_ids):
        hpo_ids = [HPO_NYC, HPO_PITT]
        mock_hpo_ids.return_value = hpo_ids
        expected_results = []
        for hpo_id in [HPO_NYC, HPO_PITT]:
            rows = self.load_test_data(hpo_id)
            for row in rows:
                row[FIELD_DATASET_NAME] = hpo_id
            errors = top_n_errors(rows)
            expected_results += comparison_view(errors)
        dataset_errors = top_heel_errors(self.bq_client,
                                         self.dataset_id,
                                         all_hpo=True)
        actual_results = comparison_view(dataset_errors)
        self.assertCountEqual(actual_results, expected_results)

    def tearDown(self):
        self.storage_client.empty_bucket(self.drc_bucket)
        test_util.delete_all_tables(self.bq_client, self.dataset_id)
