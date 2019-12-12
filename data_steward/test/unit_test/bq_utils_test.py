from __future__ import print_function
import csv
import os
import time
import unittest
from datetime import datetime
from io import open

import mock

import bq_utils
import common
import app_identity
from constants import bq_utils as bq_utils_consts
import gcs_utils
import resources
from test.unit_test import test_util
from test.unit_test.test_util import FAKE_HPO_ID, FIVE_PERSONS_PERSON_CSV, \
    NYC_FIVE_PERSONS_MEASUREMENT_CSV, NYC_FIVE_PERSONS_PERSON_CSV, PITT_FIVE_PERSONS_PERSON_CSV, \
    PITT_FIVE_PERSONS_OBSERVATION_CSV
from validation.achilles import ACHILLES_TABLES


class BqUtilsTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.hpo_bucket = gcs_utils.get_hpo_bucket(FAKE_HPO_ID)
        self.person_table_id = bq_utils.get_table_id(FAKE_HPO_ID, common.PERSON)
        self.dataset_id = bq_utils.get_dataset_id()
        test_util.delete_all_tables(self.dataset_id)
        self.project_id = app_identity.get_application_id()
        self.TEST_FIELDS = [
            {
                "type": "integer",
                "name": "integer_field",
                "mode": "required",
                "description": "An integer field"
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
        self._empty_bucket()

    def _empty_bucket(self):
        bucket_items = gcs_utils.list_bucket(self.hpo_bucket)
        for bucket_item in bucket_items:
            gcs_utils.delete_object(self.hpo_bucket, bucket_item['name'])

    def _drop_tables(self):
        tables = bq_utils.list_tables()
        for table in tables:
            table_id = table['tableReference']['tableId']
            if table_id not in common.VOCABULARY_TABLES:
                bq_utils.delete_table(table_id)

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
        import app_identity

        app_id = app_identity.get_application_id()
        table_name = 'achilles_analysis'
        schema_file_name = table_name + '.json'
        csv_file_name = table_name + '.csv'
        schema_path = os.path.join(resources.fields_path, schema_file_name)
        local_csv_path = os.path.join(test_util.TEST_DATA_EXPORT_PATH, csv_file_name)
        with open(local_csv_path, 'rb') as fp:
            response = gcs_utils.upload_object(self.hpo_bucket, csv_file_name, fp)
        hpo_bucket = self.hpo_bucket
        gcs_object_path = 'gs://%(hpo_bucket)s/%(csv_file_name)s' % locals()
        dataset_id = self.dataset_id
        load_results = bq_utils.load_csv(schema_path, gcs_object_path, app_id, dataset_id, table_name)

        load_job_id = load_results['jobReference']['jobId']
        incomplete_jobs = bq_utils.wait_on_jobs([load_job_id])
        self.assertEqual(len(incomplete_jobs), 0, 'loading table {} timed out'.format(table_name))
        query_response = bq_utils.query('SELECT COUNT(1) FROM %(table_name)s' % locals())
        self.assertEqual(query_response['kind'], 'bigquery#queryResponse')

    def test_load_cdm_csv(self):
        with open(FIVE_PERSONS_PERSON_CSV, 'rb') as fp:
            gcs_utils.upload_object(self.hpo_bucket, 'person.csv', fp)
        result = bq_utils.load_cdm_csv(FAKE_HPO_ID, common.PERSON)
        self.assertEqual(result['status']['state'], 'RUNNING')

        load_job_id = result['jobReference']['jobId']
        table_id = result['configuration']['load']['destinationTable']['tableId']
        incomplete_jobs = bq_utils.wait_on_jobs([load_job_id])
        self.assertEqual(len(incomplete_jobs), 0, 'loading table {} timed out'.format(table_id))
        table_info = bq_utils.get_table_info(table_id)
        num_rows = table_info.get('numRows')
        self.assertEqual(num_rows, '5')

    def test_load_cdm_csv_error_on_bad_table_name(self):
        with self.assertRaises(ValueError) as cm:
            bq_utils.load_cdm_csv(FAKE_HPO_ID, 'not_a_cdm_table')

    def test_query_result(self):
        with open(FIVE_PERSONS_PERSON_CSV, 'rb') as fp:
            gcs_utils.upload_object(self.hpo_bucket, 'person.csv', fp)
        result = bq_utils.load_cdm_csv(FAKE_HPO_ID, common.PERSON)

        load_job_id = result['jobReference']['jobId']
        incomplete_jobs = bq_utils.wait_on_jobs([load_job_id])
        self.assertEqual(len(incomplete_jobs), 0, 'loading table {} timed out'.format(common.PERSON))

        table_id = bq_utils.get_table_id(FAKE_HPO_ID, common.PERSON)
        q = 'SELECT person_id FROM %s' % table_id
        result = bq_utils.query(q)
        self.assertEqual(5, int(result['totalRows']))

    def test_merge_with_good_data(self):
        running_jobs = []
        with open(NYC_FIVE_PERSONS_PERSON_CSV, 'rb') as fp:
            gcs_utils.upload_object(gcs_utils.get_hpo_bucket('nyc'), 'person.csv', fp)
        result = bq_utils.load_cdm_csv('nyc', 'person')
        running_jobs.append(result['jobReference']['jobId'])

        with open(PITT_FIVE_PERSONS_PERSON_CSV, 'rb') as fp:
            gcs_utils.upload_object(gcs_utils.get_hpo_bucket('pitt'), 'person.csv', fp)
        result = bq_utils.load_cdm_csv('pitt', 'person')
        running_jobs.append(result['jobReference']['jobId'])

        nyc_person_ids = [int(row['person_id'])
                          for row in
                          resources.csv_to_list(NYC_FIVE_PERSONS_PERSON_CSV)]
        pitt_person_ids = [int(row['person_id'])
                           for row in resources.csv_to_list(
                PITT_FIVE_PERSONS_PERSON_CSV
            )]
        expected_result = nyc_person_ids + pitt_person_ids
        expected_result.sort()

        incomplete_jobs = bq_utils.wait_on_jobs(running_jobs)
        self.assertEqual(len(incomplete_jobs), 0, 'loading tables {},{} timed out'.format('nyc_person', 'pitt_person'))

        dataset_id = self.dataset_id
        table_ids = ['nyc_person', 'pitt_person']
        merged_table_id = 'merged_nyc_pitt'
        success_flag, error = bq_utils.merge_tables(dataset_id,
                                                    table_ids,
                                                    dataset_id,
                                                    merged_table_id)

        self.assertTrue(success_flag)
        self.assertEqual(error, "")

        query_string = 'SELECT person_id FROM {dataset_id}.{table_id}'.format(dataset_id=dataset_id,
                                                                              table_id=merged_table_id)
        merged_query_job_result = bq_utils.query(query_string)

        self.assertIsNone(merged_query_job_result.get('errors', None))
        actual_result = [int(row['f'][0]['v']) for row in merged_query_job_result['rows']]
        actual_result.sort()
        self.assertCountEqual(expected_result, actual_result)

    def test_merge_bad_table_names(self):
        table_ids = ['nyc_person_foo', 'pitt_person_foo']
        success_flag, error_msg = bq_utils.merge_tables(
            self.dataset_id,
            table_ids,
            self.dataset_id,
            'merged_nyc_pitt'
        )

        # print error_msg
        assert (not success_flag)

    def test_merge_with_unmatched_schema(self):
        running_jobs = []
        with open(NYC_FIVE_PERSONS_MEASUREMENT_CSV, 'rb') as fp:
            gcs_utils.upload_object(gcs_utils.get_hpo_bucket('nyc'), 'measurement.csv', fp)
        result = bq_utils.load_cdm_csv('nyc', 'measurement')
        running_jobs.append(result['jobReference']['jobId'])

        with open(PITT_FIVE_PERSONS_PERSON_CSV, 'rb') as fp:
            gcs_utils.upload_object(gcs_utils.get_hpo_bucket('pitt'), 'person.csv', fp)
        result = bq_utils.load_cdm_csv('pitt', 'person')
        running_jobs.append(result['jobReference']['jobId'])

        incomplete_jobs = bq_utils.wait_on_jobs(running_jobs)
        self.assertEqual(len(incomplete_jobs), 0,
                         'loading tables {},{} timed out'.format('nyc_measurement', 'pitt_person'))

        table_names = ['nyc_measurement', 'pitt_person']
        success, error = bq_utils.merge_tables(
            self.dataset_id,
            table_names,
            self.dataset_id,
            'merged_nyc_pitt'
        )
        self.assertFalse(success)

    def test_create_table(self):
        table_id = 'some_random_table_id'
        fields = [dict(name='person_id', type='integer', mode='required'),
                  dict(name='name', type='string', mode='nullable')]
        result = bq_utils.create_table(table_id, fields)
        self.assertTrue('kind' in result)
        self.assertEqual(result['kind'], 'bigquery#table')
        table_info = bq_utils.get_table_info(table_id)
        self._table_has_clustering(table_info)

    def test_create_existing_table_without_drop_raises_error(self):
        table_id = 'some_random_table_id'
        fields = [dict(name='id', type='integer', mode='required'),
                  dict(name='name', type='string', mode='nullable')]
        bq_utils.create_table(table_id, fields)
        with self.assertRaises(bq_utils.InvalidOperationError) as cm:
            bq_utils.create_table(table_id, fields, drop_existing=False)

    def test_create_table_drop_existing_success(self):
        table_id = 'some_random_table_id'
        fields = [dict(name='id', type='integer', mode='required'),
                  dict(name='name', type='string', mode='nullable')]
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
            table_id = 'prefix_for_test_' + standard_table
            result = bq_utils.create_standard_table(standard_table, table_id)
            self.assertTrue('kind' in result)
            self.assertEqual(result['kind'], 'bigquery#table')
            # sanity check
            self.assertTrue(bq_utils.table_exists(table_id))

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
    @mock.patch('bq_utils.job_status_done', side_effect=[False, False, False, True, False, False, True, True, True])
    def test_wait_on_jobs_get_done(self, mock_job_status_done, mock_time_sleep):
        job_ids = list(range(3))
        actual = bq_utils.wait_on_jobs(job_ids)
        expected = []
        self.assertEqual(actual, expected)

    @mock.patch('time.sleep', return_value=None)
    @mock.patch('bq_utils.job_status_done', side_effect=[False, False, True, False, False, False, False,
                                                         False, False, False, False, False])
    def test_wait_on_jobs_some_fail(self, mock_job_status_done, mock_time_sleep):
        job_ids = list(range(2))
        actual = bq_utils.wait_on_jobs(job_ids)
        expected = [1]
        self.assertEqual(actual, expected)

    def test_wait_on_jobs_retry_count(self):
        # TODO figure out how to count this
        # self.assertEquals(mock_time_sleep.call_count, bq_utils.BQ_DEFAULT_RETRY_COUNT)
        pass

    def test_load_ehr_observation(self):
        hpo_id = 'pitt'
        dataset_id = self.dataset_id
        table_id = bq_utils.get_table_id(hpo_id, table_name='observation')
        q = 'SELECT observation_id FROM {dataset_id}.{table_id} ORDER BY observation_id'.format(
            dataset_id=dataset_id,
            table_id=table_id)
        expected_observation_ids = [int(row['observation_id'])
                                    for row in resources.csv_to_list(PITT_FIVE_PERSONS_OBSERVATION_CSV)]
        with open(PITT_FIVE_PERSONS_OBSERVATION_CSV, 'rb') as fp:
            gcs_utils.upload_object(gcs_utils.get_hpo_bucket(hpo_id), 'observation.csv', fp)
        result = bq_utils.load_cdm_csv(hpo_id, 'observation')
        job_id = result['jobReference']['jobId']
        incomplete_jobs = bq_utils.wait_on_jobs([job_id])
        self.assertEqual(len(incomplete_jobs), 0, 'pitt_observation load job did not complete')
        load_job_result = bq_utils.get_job_details(job_id)
        load_job_result_status = load_job_result['status']
        load_job_errors = load_job_result_status.get('errors')
        self.assertIsNone(load_job_errors, msg='pitt_observation load job failed: ' + str(load_job_errors))
        query_results_response = bq_utils.query(q)
        query_job_errors = query_results_response.get('errors')
        self.assertIsNone(query_job_errors)
        actual_result = [int(row['f'][0]['v']) for row in query_results_response['rows']]
        self.assertCountEqual(actual_result, expected_observation_ids)

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

    def test_load_table_from_csv(self):
        table_id = 'test_csv_table'
        csv_file = 'load_csv_test_data.csv'
        csv_path = os.path.join(test_util.TEST_DATA_PATH, csv_file)
        with open(csv_path, 'r') as f:
            expected = list(csv.DictReader(f))
        bq_utils.load_table_from_csv(self.project_id, self.dataset_id, table_id, csv_path, self.TEST_FIELDS)
        q = """ SELECT *
                FROM `{project_id}.{dataset_id}.{table_id}`""".format(project_id=self.project_id,
                                                                      dataset_id=self.dataset_id,
                                                                      table_id=table_id)
        r = bq_utils.query(q)
        actual = bq_utils.response2rows(r)

        # Convert the epoch times to datetime with time zone
        for row in actual:
            row['timestamp_field'] = time.strftime(self.DT_FORMAT + ' UTC', time.gmtime(row['timestamp_field']))
        expected.sort(key=lambda row: row['integer_field'])
        actual.sort(key=lambda row: row['integer_field'])
        for i, _ in enumerate(expected):
            self.assertCountEqual(expected[i], actual[i])

    def test_load_smoking_lookup(self):
        # DC-586 Import RDR rules should support null fields
        from cdr_cleaner.manual_cleaning_rules.clean_smoking_ppi import SMOKING_LOOKUP_TABLE, SMOKING_LOOKUP_FIELDS
        bq_utils.load_table_from_csv('fake-project',
                                     'fake-dataset',
                                     SMOKING_LOOKUP_TABLE,
                                     csv_path=None,
                                     fields=SMOKING_LOOKUP_FIELDS)

    def tearDown(self):
        test_util.delete_all_tables(self.dataset_id)
        self._empty_bucket()
