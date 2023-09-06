# Python imports
import os
import unittest

# Third party imports
import mock

# Project imports
import app_identity
import bq_utils
import common
from gcloud.gcs import StorageClient
from gcloud.bq import BigQueryClient
import resources
import validation.sql_wrangle as sql_wrangle
from tests import test_util
from tests.test_util import FAKE_HPO_ID
from validation.metrics import required_labs as required_labs
from validation.metrics.required_labs import (
    MEASUREMENT_CONCEPT_SETS_TABLE, MEASUREMENT_CONCEPT_SETS_DESCENDANTS_TABLE)


class RequiredLabsTest(unittest.TestCase):

    dataset_id = common.BIGQUERY_DATASET_ID
    project_id = app_identity.get_application_id()
    bq_client = BigQueryClient(project_id)

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')
        test_util.setup_hpo_id_bucket_name_table(cls.bq_client, cls.dataset_id)

    @mock.patch("gcloud.gcs.LOOKUP_TABLES_DATASET_ID", dataset_id)
    def setUp(self):
        # Ids
        self.folder_prefix = '2019-01-01/'
        # Clients
        self.storage_client = StorageClient(self.project_id)
        self.hpo_bucket = self.storage_client.get_hpo_bucket(FAKE_HPO_ID)
        self.rdr_dataset_id = bq_utils.get_rdr_dataset_id()
        # Cleanup
        self.storage_client.empty_bucket(self.hpo_bucket)
        test_util.delete_all_tables(self.bq_client, self.dataset_id)

        mock_get_hpo_name = mock.patch('validation.main.get_hpo_name')
        self.mock_get_hpo_name = mock_get_hpo_name.start()
        self.mock_get_hpo_name.return_value = 'Fake HPO'
        self.addCleanup(mock_get_hpo_name.stop)
        # Data load
        self._load_data()

    def tearDown(self):
        self.storage_client.empty_bucket(self.hpo_bucket)
        test_util.delete_all_tables(self.bq_client, self.dataset_id)

    @classmethod
    def tearDownClass(cls):
        test_util.drop_hpo_id_bucket_name_table(cls.bq_client, cls.dataset_id)

    def _load_data(self):

        # Load measurement_concept_sets
        required_labs.load_measurement_concept_sets_table(
            client=self.bq_client, dataset_id=self.dataset_id)
        # Load measurement_concept_sets_descendants
        required_labs.load_measurement_concept_sets_descendants_table(
            client=self.bq_client, dataset_id=self.dataset_id)

        # we need to load measurement.csv into bigquery_dataset_id in advance for the other integration tests
        ehr_measurement_result = bq_utils.load_table_from_csv(
            project_id=self.project_id,
            dataset_id=self.dataset_id,
            table_name=resources.get_table_id(common.MEASUREMENT,
                                              hpo_id=FAKE_HPO_ID),
            csv_path=test_util.FIVE_PERSONS_MEASUREMENT_CSV,
            fields=resources.fields_for(common.MEASUREMENT))
        bq_utils.wait_on_jobs([ehr_measurement_result['jobReference']['jobId']])

    def test_check_and_copy_tables(self):
        """
        Test to ensure all the necessary tables for required_labs.py are copied and or created
        """
        # Preconditions
        descendants_table_name = f'{self.project_id}.{self.dataset_id}.{MEASUREMENT_CONCEPT_SETS_DESCENDANTS_TABLE}'
        concept_sets_table_name = f'{self.project_id}.{self.dataset_id}.{MEASUREMENT_CONCEPT_SETS_TABLE}'
        concept_table_name = f'{self.project_id}.{self.dataset_id}.{common.CONCEPT}'
        concept_ancestor_table_name = f'{self.project_id}.{self.dataset_id}.{common.CONCEPT_ANCESTOR}'

        actual_descendants_table = self.bq_client.get_table(
            descendants_table_name)
        actual_concept_sets_table = self.bq_client.get_table(
            concept_sets_table_name)
        actual_concept_table = self.bq_client.get_table(concept_table_name)
        actual_concept_ancestor_table = self.bq_client.get_table(
            concept_ancestor_table_name)

        # Test
        required_labs.check_and_copy_tables(self.bq_client, self.dataset_id)

        # Post conditions
        self.assertIsNotNone(actual_descendants_table.created)
        self.assertIsNotNone(actual_concept_sets_table.created)
        self.assertIsNotNone(actual_concept_table.created)
        self.assertIsNotNone(actual_concept_ancestor_table.created)

    def test_measurement_concept_sets_table(self):

        query = sql_wrangle.qualify_tables(
            '''SELECT * FROM {dataset_id}.{table_id}'''.format(
                dataset_id=self.dataset_id,
                table_id=MEASUREMENT_CONCEPT_SETS_TABLE))
        response = bq_utils.query(query)

        actual_fields = [{
            'name': field['name'].lower(),
            'type': field['type'].lower()
        } for field in response['schema']['fields']]

        expected_fields = [{
            'name': field['name'].lower(),
            'type': field['type'].lower()
        } for field in resources.fields_for(MEASUREMENT_CONCEPT_SETS_TABLE)]

        self.assertListEqual(expected_fields, actual_fields)

        measurement_concept_sets_table_path = os.path.join(
            resources.resource_files_path,
            f'{MEASUREMENT_CONCEPT_SETS_TABLE}.csv')
        expected_total_rows = len(
            resources.csv_to_list(measurement_concept_sets_table_path))
        self.assertEqual(expected_total_rows, int(response['totalRows']))

    def test_load_measurement_concept_sets_descendants_table(self):

        query = sql_wrangle.qualify_tables(
            """SELECT * FROM {dataset_id}.{table_id}""".format(
                dataset_id=self.dataset_id,
                table_id=MEASUREMENT_CONCEPT_SETS_DESCENDANTS_TABLE))
        response = bq_utils.query(query)

        actual_fields = [{
            'name': field['name'].lower(),
            'type': field['type'].lower()
        } for field in response['schema']['fields']]

        expected_fields = [{
            'name': field['name'].lower(),
            'type': field['type'].lower()
        } for field in resources.fields_for(
            MEASUREMENT_CONCEPT_SETS_DESCENDANTS_TABLE)]

        self.assertListEqual(expected_fields, actual_fields)

    def test_get_lab_concept_summary_query(self):
        summary_query = required_labs.get_lab_concept_summary_query(
            self.bq_client, FAKE_HPO_ID)
        summary_response = bq_utils.query(summary_query)
        summary_rows = bq_utils.response2rows(summary_response)
        submitted_labs = [
            row for row in summary_rows
            if row['measurement_concept_id_exists'] == 1
        ]
        actual_total_labs = summary_response['totalRows']

        # Count the total number of labs required, this number should be equal to the total number of rows in the
        # results generated by get_lab_concept_summary_query including the submitted and missing labs.
        unique_ancestor_concept_query = sql_wrangle.qualify_tables(
            """SELECT DISTINCT ancestor_concept_id FROM `{project_id}.{dataset_id}.{table_id}`"""
            .format(project_id=self.project_id,
                    dataset_id=self.dataset_id,
                    table_id=MEASUREMENT_CONCEPT_SETS_DESCENDANTS_TABLE))
        unique_ancestor_concept_response = bq_utils.query(
            unique_ancestor_concept_query)
        expected_total_labs = unique_ancestor_concept_response['totalRows']

        # Count the number of labs in the measurement table, this number should be equal to the number of labs
        # submitted by the fake site
        unique_measurement_concept_id_query = '''
                SELECT
                  DISTINCT c.ancestor_concept_id
                FROM
                  `{project_id}.{dataset_id}.{measurement_concept_sets_descendants}` AS c
                JOIN
                  `{project_id}.{dataset_id}.{measurement}` AS m
                ON
                  c.descendant_concept_id = m.measurement_concept_id
                '''.format(project_id=self.project_id,
                           dataset_id=self.dataset_id,
                           measurement_concept_sets_descendants=
                           MEASUREMENT_CONCEPT_SETS_DESCENDANTS_TABLE,
                           measurement=resources.get_table_id(
                               common.MEASUREMENT, hpo_id=FAKE_HPO_ID))

        unique_measurement_concept_id_response = bq_utils.query(
            unique_measurement_concept_id_query)
        unique_measurement_concept_id_total_labs = unique_measurement_concept_id_response[
            'totalRows']

        self.assertEqual(int(expected_total_labs),
                         int(actual_total_labs),
                         msg='Compare the total number of labs')
        self.assertEqual(int(unique_measurement_concept_id_total_labs),
                         len(submitted_labs),
                         msg='Compare the number '
                         'of labs submitted '
                         'in the measurement')
