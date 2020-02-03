import unittest
import mock
import os
import gcs_utils
import bq_utils
import common
import app_identity
import resources
from tests import test_util
from tests.test_util import (FAKE_HPO_ID)
from validation.metrics import required_labs as required_labs
import validation.sql_wrangle as sql_wrangle
from validation import main
from tests.integration_tests.data_steward.validation import main_test as main_test
from bs4 import BeautifulSoup


class RequiredLabsTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.hpo_bucket = gcs_utils.get_hpo_bucket(FAKE_HPO_ID)
        self.project_id = app_identity.get_application_id()
        self.dataset_id = bq_utils.get_dataset_id()
        self.folder_prefix = '2019-01-01/'
        test_util.delete_all_tables(self.dataset_id)
        test_util.empty_bucket(self.hpo_bucket)

        mock_get_hpo_name = mock.patch('validation.main.get_hpo_name')

        self.mock_get_hpo_name = mock_get_hpo_name.start()
        self.mock_get_hpo_name.return_value = 'Fake HPO'
        self.addCleanup(mock_get_hpo_name.stop)

        self._load_data()

    def tearDown(self):
        test_util.delete_all_tables(bq_utils.get_dataset_id())
        test_util.empty_bucket(self.hpo_bucket)

    def _load_data(self):

        # Load measurement_concept_sets
        required_labs.load_required_lab_table(
            project_id=app_identity.get_application_id(),
            dataset_id=bq_utils.get_dataset_id())
        # Load measurement_concept_sets_descendants
        required_labs.load_measurement_concept_sets_descendants_table(
            project_id=self.project_id, dataset_id=self.dataset_id)

        # Create concept and concept_ancestor empty tables if not exist
        if not bq_utils.table_exists(common.CONCEPT, self.dataset_id):
            bq_utils.create_standard_table(common.CONCEPT, common.CONCEPT)
        if not bq_utils.table_exists(common.CONCEPT, self.dataset_id):
            bq_utils.create_standard_table(common.CONCEPT_ANCESTOR,
                                           common.CONCEPT_ANCESTOR)

        # Load the test measurement data
        test_util.write_cloud_file(self.hpo_bucket,
                                   test_util.TEST_MEASUREMENT_CSV,
                                   prefix=self.folder_prefix)
        results = bq_utils.load_cdm_csv(FAKE_HPO_ID, common.MEASUREMENT, source_folder_prefix=self.folder_prefix)
        query_job_id = results['jobReference']['jobId']
        bq_utils.wait_on_jobs([query_job_id])

        # Load the drug_class.csv dependency otherwise the generate_metrics in main.py will fail
        main_test.ValidationMainTest.create_drug_class_table()

    def test_load_required_lab_table(self):

        query = sql_wrangle.qualify_tables(
            """SELECT * FROM {table_id}""".format(
                table_id=required_labs.MEASUREMENT_CONCEPT_SETS_TABLE))
        response = bq_utils.query(query)

        actual_fields = [{
            'name': field['name'].lower(),
            'type': field['type'].lower()
        } for field in response['schema']['fields']]

        expected_fields = [{
            'name': field['name'].lower(),
            'type': field['type'].lower()
        } for field in required_labs.MEASUREMENT_CONCEPT_SETS_FIELDS]

        self.assertListEqual(expected_fields, actual_fields)

        measurement_concept_sets_table_path = os.path.join(
            resources.resource_path,
            required_labs.MEASUREMENT_CONCEPT_SETS_TABLE + '.csv')
        expected_total_rows = len(
            resources.csv_to_list(measurement_concept_sets_table_path))
        self.assertEqual(expected_total_rows, int(response['totalRows']))

    def test_load_measurement_concept_sets_descendants_table(self):

        query = sql_wrangle.qualify_tables(
            """SELECT * FROM {table_id}""".format(
                table_id=required_labs.
                MEASUREMENT_CONCEPT_SETS_DESCENDANTS_TABLE))
        response = bq_utils.query(query)

        actual_fields = [{
            'name': field['name'].lower(),
            'type': field['type'].lower()
        } for field in response['schema']['fields']]

        expected_fields = [{
            'name': field['name'].lower(),
            'type': field['type'].lower()
        } for field in required_labs.MEASUREMENT_CONCEPT_SETS_DESCENDANTS_FIELDS
                          ]

        self.assertListEqual(expected_fields, actual_fields)

    def test_get_lab_concept_summary_query(self):
        summary_query = required_labs.get_lab_concept_summary_query(FAKE_HPO_ID)
        summary_response = bq_utils.query(summary_query)
        summary_rows = bq_utils.response2rows(summary_response)
        submitted_labs = [
            row for row in summary_rows if row['measurement_concept_id_exists'] == 1
        ]
        actual_total_labs = summary_response['totalRows']

        # Count the total number of labs required, this number should be equal to the total number of rows in the
        # results generated by get_lab_concept_summary_query including the submitted and missing labs.
        unique_ancestor_concept_query = sql_wrangle.qualify_tables(
            """SELECT DISTINCT ancestor_concept_id FROM {table_id}""".format(
                table_id=required_labs.
                MEASUREMENT_CONCEPT_SETS_DESCENDANTS_TABLE))
        unique_ancestor_cocnept_response = bq_utils.query(
            unique_ancestor_concept_query)
        expected_total_labs = unique_ancestor_cocnept_response['totalRows']

        # Count the number of labs in the measurement table, this number should be equal to the number of labs
        # submitted by the fake site
        unique_measurement_concept_id_query = sql_wrangle.qualify_tables(
            """SELECT DISTINCT measurement_concept_id FROM {table_id}""".format(
                table_id=bq_utils.get_table_id(FAKE_HPO_ID, common.MEASUREMENT)
                ))
        unique_measurement_concept_id_response = bq_utils.query(
            unique_measurement_concept_id_query)
        unique_measurement_concept_id_total_labs = unique_measurement_concept_id_response['totalRows']

        self.assertEqual(int(expected_total_labs), int(actual_total_labs))
        self.assertEqual(int(unique_measurement_concept_id_total_labs), len(submitted_labs))

    @mock.patch('api_util.check_cron')
    def test_required_labs_html_page(self, mock_check_cron):
        main.app.testing = True
        with main.app.test_client() as c:
            c.get(test_util.VALIDATE_HPO_FILES_URL)
            actual_result = test_util.read_cloud_file(
                self.hpo_bucket, self.folder_prefix + common.RESULTS_HTML)
            soup = BeautifulSoup(actual_result, 'html.parser')
            required_lab_html_table = soup.find_all('table', class_='required-lab')[0]
            table_headers = required_lab_html_table.find_all('th')
            self.assertEqual(3, len(table_headers))
            self.assertEqual('Ancestor Concept ID', table_headers[0].get_text())
            self.assertEqual('Ancestor Concept Name', table_headers[1].get_text())
            self.assertEqual('Found', table_headers[2].get_text())

            table_rows = required_lab_html_table.find_next('tbody').find_all('tr')
            table_rows_last_column = [table_row.find_all('td')[-1] for table_row in table_rows]
            submitted_labs = [row for row in table_rows_last_column if 'result-1' in row.attrs['class']]
            missing_labs = [row for row in table_rows_last_column if 'result-0' in row.attrs['class']]
            self.assertTrue(len(table_rows) > 0)
            self.assertTrue(len(submitted_labs) > 0)
            self.assertTrue(len(missing_labs) > 0)
