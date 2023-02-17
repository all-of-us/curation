from __future__ import print_function
import csv
import os
import time
import unittest
from io import open

import mock

import bq_utils
import common
import app_identity
from gcloud.gcs import StorageClient
from gcloud.bq import BigQueryClient
from google.cloud.storage.bucket import Bucket, Blob
import resources
from tests import test_util
from tests.test_util import (FAKE_HPO_ID, FIVE_PERSONS_PERSON_CSV, PITT_HPO_ID,
                             PITT_FIVE_PERSONS_OBSERVATION_CSV)
from validation.achilles import ACHILLES_TABLES


class BqUtilsTest(unittest.TestCase):

    dataset_id = bq_utils.get_dataset_id()
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
        self.person_table_id = bq_utils.get_table_id(FAKE_HPO_ID, common.PERSON)
        test_util.delete_all_tables(self.bq_client, self.dataset_id)
        self.TEST_FIELDS = [
            {
                "type": "integer",
                "name": "integer_field",
                "mode": "required",
                "description": "An integer field"
            },
            # DC-586 Import RDR rules should support null fields
            {
                "type": "integer",
                "name": "nullable_integer_field",
                "mode": "nullable",
                "description": "A nullable integer field"
            },
            {
                "type": "string",
                "name": "string_field",
                "mode": "required",
                "description": "A string field"
            },
            {
                "type": "date",
                "name": "date_field",
                "mode": "required",
                "description": "A date field"
            },
            {
                "type": "timestamp",
                "name": "timestamp_field",
                "mode": "required",
                "description": "A timestamp field"
            },
            {
                "type": "boolean",
                "name": "boolean_field",
                "mode": "required",
                "description": "A boolean field"
            },
            {
                "type": "float",
                "name": "float_field",
                "mode": "required",
                "description": "A float field"
            }
        ]
        self.DT_FORMAT = '%Y-%m-%d %H:%M:%S'
        self.storage_client = StorageClient(self.project_id)
        self.hpo_bucket: Bucket = self.storage_client.get_hpo_bucket(
            FAKE_HPO_ID)
        self.storage_client.empty_bucket(self.hpo_bucket)

    def _table_has_clustering(self, table_info):
        clustering = table_info.get('clustering')
        self.assertIsNotNone(clustering)
        fields = clustering.get('fields')
        self.assertSetEqual(set(fields), {'person_id'})
        time_partitioning = table_info.get('timePartitioning')
        self.assertIsNotNone(time_partitioning)
        tpe = time_partitioning.get('type')
        self.assertEqual(tpe, 'DAY')

    def test_load_csv(self):
        table_name = 'achilles_analysis'
        table_blob: Blob = self.hpo_bucket.blob(f'{table_name}.csv')
        local_csv_path = os.path.join(test_util.TEST_DATA_EXPORT_PATH,
                                      table_blob.name)
        with open(local_csv_path, 'rb') as fp:
            table_blob.upload_from_file(fp)
        gcs_object_path = f'gs://{self.hpo_bucket.name}/{table_blob.name}'
        load_results = bq_utils.load_csv(table_name, gcs_object_path,
                                         self.project_id, self.dataset_id,
                                         table_name)

        load_job_id = load_results['jobReference']['jobId']
        incomplete_jobs = bq_utils.wait_on_jobs([load_job_id])
        self.assertEqual(len(incomplete_jobs), 0,
                         'loading table {} timed out'.format(table_name))
        query_response = bq_utils.query('SELECT COUNT(1) FROM %(table_name)s' %
                                        locals())
        self.assertEqual(query_response['kind'], 'bigquery#queryResponse')

    @mock.patch("gcloud.gcs.LOOKUP_TABLES_DATASET_ID", dataset_id)
    def test_load_cdm_csv(self):
        table_blob: Blob = self.hpo_bucket.blob('person.csv')
        with open(FIVE_PERSONS_PERSON_CSV, 'rb') as fp:
            table_blob.upload_from_file(fp)
        result = bq_utils.load_cdm_csv(FAKE_HPO_ID, common.PERSON)
        self.assertEqual(result['status']['state'], 'RUNNING')
        load_job_id = result['jobReference']['jobId']
        table_id = result['configuration']['load']['destinationTable'][
            'tableId']
        incomplete_jobs = bq_utils.wait_on_jobs([load_job_id])
        self.assertEqual(len(incomplete_jobs), 0,
                         'loading table {} timed out'.format(table_id))
        table = self.bq_client.get_table(
            f'{os.environ.get("BIGQUERY_DATASET_ID")}.{table_id}')
        self.assertEqual(table.num_rows, 5)

    @mock.patch("gcloud.gcs.LOOKUP_TABLES_DATASET_ID", dataset_id)
    def test_query_result(self):
        table_blob: Blob = self.hpo_bucket.blob('person.csv')
        with open(FIVE_PERSONS_PERSON_CSV, 'rb') as fp:
            table_blob.upload_from_file(fp)
        result = bq_utils.load_cdm_csv(FAKE_HPO_ID, common.PERSON)
        load_job_id = result['jobReference']['jobId']
        incomplete_jobs = bq_utils.wait_on_jobs([load_job_id])
        self.assertEqual(len(incomplete_jobs), 0,
                         'loading table {} timed out'.format(common.PERSON))
        table_id = bq_utils.get_table_id(FAKE_HPO_ID, common.PERSON)
        q = 'SELECT person_id FROM %s' % table_id
        result = bq_utils.query(q)
        self.assertEqual(5, int(result['totalRows']))

    def test_create_table(self):
        table_id = 'some_random_table_id'
        fields = [
            dict(name='person_id', type='integer', mode='required'),
            dict(name='name', type='string', mode='nullable')
        ]
        result = bq_utils.create_table(table_id, fields)
        self.assertTrue('kind' in result)
        self.assertEqual(result['kind'], 'bigquery#table')
        table = self.bq_client.get_table(
            f'{os.environ.get("BIGQUERY_DATASET_ID")}.{table_id}')
        self._table_has_clustering(table)

    def test_create_existing_table_without_drop_raises_error(self):
        table_id = 'some_random_table_id'
        fields = [
            dict(name='id', type='integer', mode='required'),
            dict(name='name', type='string', mode='nullable')
        ]
        bq_utils.create_table(table_id, fields)
        with self.assertRaises(bq_utils.InvalidOperationError):
            bq_utils.create_table(table_id, fields, drop_existing=False)

    def test_create_table_drop_existing_success(self):
        table_id = 'some_random_table_id'
        fields = [
            dict(name='id', type='integer', mode='required'),
            dict(name='name', type='string', mode='nullable')
        ]
        result_1 = bq_utils.create_table(table_id, fields)
        # sanity check
        table_id = result_1['tableReference']['tableId']
        self.assertTrue(bq_utils.table_exists(table_id))
        result_2 = bq_utils.create_table(table_id, fields, drop_existing=True)
        # same id and second one created after first one
        self.assertEqual(result_1['id'], result_2['id'])
        self.assertTrue(result_2['creationTime'] > result_1['creationTime'])

    def test_create_standard_table(self):
        standard_tables = list(resources.CDM_TABLES) + ACHILLES_TABLES
        for standard_table in standard_tables:
            table_id = f'prefix_for_test_{standard_table}'
            result = bq_utils.create_standard_table(standard_table, table_id)
            self.assertTrue('kind' in result)
            self.assertEqual(result['kind'], 'bigquery#table')
            # sanity check
            self.assertTrue(bq_utils.table_exists(table_id))

    @mock.patch("gcloud.gcs.LOOKUP_TABLES_DATASET_ID", dataset_id)
    def test_load_ehr_observation(self):
        hpo_id = PITT_HPO_ID
        dataset_id = self.dataset_id
        table_id = bq_utils.get_table_id(hpo_id, table_name='observation')
        q = 'SELECT observation_id FROM {dataset_id}.{table_id} ORDER BY observation_id'.format(
            dataset_id=dataset_id, table_id=table_id)
        expected_observation_ids = [
            int(row['observation_id'])
            for row in resources.csv_to_list(PITT_FIVE_PERSONS_OBSERVATION_CSV)
        ]
        bucket: Bucket = self.storage_client.get_hpo_bucket(hpo_id)
        table_blob: Blob = bucket.blob('observation.csv')
        with open(PITT_FIVE_PERSONS_OBSERVATION_CSV, 'rb') as fp:
            table_blob.upload_from_file(fp)
        result = bq_utils.load_cdm_csv(hpo_id, 'observation')
        job_id = result['jobReference']['jobId']
        incomplete_jobs = bq_utils.wait_on_jobs([job_id])
        self.assertEqual(len(incomplete_jobs), 0,
                         'pitt_observation load job did not complete')
        load_job_result = bq_utils.get_job_details(job_id)
        load_job_result_status = load_job_result['status']
        load_job_errors = load_job_result_status.get('errors')
        self.assertIsNone(
            load_job_errors,
            msg=f'pitt_observation load job failed: {str(load_job_errors)}')
        query_results_response = bq_utils.query(q)
        query_job_errors = query_results_response.get('errors')
        self.assertIsNone(query_job_errors)
        actual_result = [
            int(row['f'][0]['v']) for row in query_results_response['rows']
        ]
        self.assertCountEqual(actual_result, expected_observation_ids)

    def test_load_table_from_csv(self):
        table_id = 'test_csv_table'
        csv_file = 'load_csv_test_data.csv'
        csv_path = os.path.join(test_util.TEST_DATA_PATH, csv_file)
        with open(csv_path, 'r') as f:
            expected = list(csv.DictReader(f))
        bq_utils.load_table_from_csv(self.project_id, self.dataset_id, table_id,
                                     csv_path, self.TEST_FIELDS)
        q = """ SELECT *
                FROM `{project_id}.{dataset_id}.{table_id}`""".format(
            project_id=self.project_id,
            dataset_id=self.dataset_id,
            table_id=table_id)
        r = bq_utils.query(q)
        actual = bq_utils.response2rows(r)

        # Convert the epoch times to datetime with time zone
        for row in actual:
            row['timestamp_field'] = time.strftime(
                self.DT_FORMAT + ' UTC', time.gmtime(row['timestamp_field']))
        expected.sort(key=lambda row: row['integer_field'])
        actual.sort(key=lambda row: row['integer_field'])
        for i, _ in enumerate(expected):
            self.assertCountEqual(expected[i], actual[i])

    def test_get_hpo_info(self):
        hpo_info = bq_utils.get_hpo_info()
        self.assertGreater(len(hpo_info), 0)

    def test_csv_line_to_sql_row_expr(self):
        fields = [{
            'name': 'nullable_date_col',
            'type': 'date',
            'mode': 'nullable',
            'description': ''
        }, {
            'name': 'nullable_float_col',
            'type': 'float',
            'mode': 'nullable',
            'description': ''
        }, {
            'name': 'nullable_integer_col',
            'type': 'integer',
            'mode': 'nullable',
            'description': ''
        }, {
            'name': 'nullable_string_col',
            'type': 'string',
            'mode': 'nullable',
            'description': ''
        }, {
            'name': 'nullable_timestamp_col',
            'type': 'timestamp',
            'mode': 'nullable',
            'description': ''
        }, {
            'name': 'required_date_col',
            'type': 'date',
            'mode': 'required',
            'description': ''
        }, {
            'name': 'required_float_col',
            'type': 'float',
            'mode': 'required',
            'description': ''
        }, {
            'name': 'required_integer_col',
            'type': 'integer',
            'mode': 'required',
            'description': ''
        }, {
            'name': 'required_string_col',
            'type': 'string',
            'mode': 'required',
            'description': ''
        }, {
            'name': 'required_timestamp_col',
            'type': 'timestamp',
            'mode': 'required',
            'description': ''
        }]

        # dummy values for each type
        flt_str = "3.14"
        int_str = "1234"
        str_str = "abc"
        dt_str = "2019-01-01"
        ts_str = "2019-01-01 14:00:00.0"
        row = {
            'nullable_date_col': dt_str,
            'nullable_float_col': flt_str,
            'nullable_integer_col': int_str,
            'nullable_string_col': str_str,
            'nullable_timestamp_col': ts_str,
            'required_date_col': dt_str,
            'required_float_col': flt_str,
            'required_integer_col': int_str,
            'required_string_col': str_str,
            'required_timestamp_col': ts_str
        }
        # all fields populated
        expected_expr = f"('{dt_str}',{flt_str},{int_str},'{str_str}','{ts_str}','{dt_str}',{flt_str},{int_str},'{str_str}','{ts_str}')"
        actual_expr = bq_utils.csv_line_to_sql_row_expr(row, fields)
        self.assertEqual(expected_expr, actual_expr)

        # nullable int zero is converted
        row['nullable_integer_col'] = '0'
        expected_expr = f"('{dt_str}',{flt_str},0,'{str_str}','{ts_str}','{dt_str}',{flt_str},{int_str},'{str_str}','{ts_str}')"
        actual_expr = bq_utils.csv_line_to_sql_row_expr(row, fields)
        self.assertEqual(expected_expr, actual_expr)

        # empty nullable is converted null
        row['nullable_date_col'] = ''
        row['nullable_float_col'] = ''
        row['nullable_integer_col'] = ''
        row['nullable_string_col'] = ''
        row['nullable_timestamp_col'] = ''
        expected_expr = f"(NULL,NULL,NULL,NULL,NULL,'{dt_str}',{flt_str},{int_str},'{str_str}','{ts_str}')"
        actual_expr = bq_utils.csv_line_to_sql_row_expr(row, fields)
        self.assertEqual(expected_expr, actual_expr)

        # empty required string converted to empty string value
        row['required_string_col'] = ''
        actual_expr = bq_utils.csv_line_to_sql_row_expr(row, fields)
        expected_expr = f"(NULL,NULL,NULL,NULL,NULL,'{dt_str}',{flt_str},{int_str},'','{ts_str}')"
        self.assertEqual(expected_expr, actual_expr)

        # empty required field raises error
        row['required_integer_col'] = ''
        with self.assertRaises(bq_utils.InvalidOperationError) as c:
            bq_utils.csv_line_to_sql_row_expr(row, fields)
        self.assertEqual(
            c.exception.msg,
            f'Value not provided for required field required_integer_col')

    def tearDown(self):
        test_util.delete_all_tables(self.bq_client, self.dataset_id)
        self.storage_client.empty_bucket(self.hpo_bucket)

    @classmethod
    def tearDownClass(cls):
        test_util.drop_hpo_id_bucket_name_table(cls.bq_client, cls.dataset_id)
