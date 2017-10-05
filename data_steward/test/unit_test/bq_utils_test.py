import unittest
import os
import bq_utils
import gcs_utils
import resources
import common
from validation.achilles import ACHILLES_TABLES
from google.appengine.ext import testbed
from test_util import FAKE_HPO_ID, FIVE_PERSONS_PERSON_CSV
import test_util
import time

PERSON = 'person'
BQ_TIMEOUT_SECONDS = 5


class BqUtilsTest(unittest.TestCase):
    def setUp(self):
        super(BqUtilsTest, self).setUp()
        self.testbed = testbed.Testbed()
        self.testbed.activate()
        self.testbed.init_app_identity_stub()
        self.testbed.init_memcache_stub()
        self.testbed.init_urlfetch_stub()
        self.testbed.init_blobstore_stub()
        self.testbed.init_datastore_v3_stub()
        self.hpo_bucket = gcs_utils.get_hpo_bucket(FAKE_HPO_ID)
        self.person_table_id = bq_utils.get_table_id(FAKE_HPO_ID, PERSON)
        self._drop_tables()
        self._empty_bucket()

    def _empty_bucket(self):
        bucket_items = gcs_utils.list_bucket(self.hpo_bucket)
        for bucket_item in bucket_items:
            gcs_utils.delete_object(self.hpo_bucket, bucket_item['name'])

    def _drop_tables(self):
        result = bq_utils.list_tables()
        if result['totalItems'] > 0:
            for table in result['tables']:
                table_id = table['tableReference']['tableId']
                bq_utils.delete_table(table_id)

    def test_load_csv(self):
        from google.appengine.api import app_identity

        app_id = app_identity.get_application_id()
        table_name = 'achilles_analysis'
        schema_file_name = table_name + '.json'
        csv_file_name = table_name + '.csv'
        schema_path = os.path.join(resources.fields_path, schema_file_name)
        local_csv_path = os.path.join(test_util.TEST_DATA_PATH, csv_file_name)
        with open(local_csv_path, 'r') as fp:
            response = gcs_utils.upload_object(self.hpo_bucket, csv_file_name, fp)
        hpo_bucket = self.hpo_bucket
        gcs_object_path = 'gs://%(hpo_bucket)s/%(csv_file_name)s' % locals()
        dataset_id = bq_utils.get_dataset_id()
        result = bq_utils.load_csv(schema_path, gcs_object_path, app_id, dataset_id, table_name)
        time.sleep(BQ_TIMEOUT_SECONDS)
        query_response = bq_utils.query('SELECT COUNT(1) FROM %(table_name)s' % locals())
        self.assertEqual(query_response['kind'], 'bigquery#queryResponse')

    def test_load_cdm_csv(self):
        with open(FIVE_PERSONS_PERSON_CSV, 'rb') as fp:
            gcs_utils.upload_object(self.hpo_bucket, 'person.csv', fp)
        result = bq_utils.load_cdm_csv(FAKE_HPO_ID, PERSON)
        self.assertEqual(result['status']['state'], 'RUNNING')
        time.sleep(BQ_TIMEOUT_SECONDS)
        table_id = result['configuration']['load']['destinationTable']['tableId']
        query_response = bq_utils.query('SELECT 1 FROM %(table_id)s' % locals())
        self.assertEqual(query_response['totalRows'], '5')

    def test_load_cdm_csv_error_on_bad_table_name(self):
        with self.assertRaises(ValueError) as cm:
            bq_utils.load_cdm_csv(FAKE_HPO_ID, 'not_a_cdm_table')

    def test_query_result(self):
        with open(FIVE_PERSONS_PERSON_CSV, 'rb') as fp:
            gcs_utils.upload_object(self.hpo_bucket, 'person.csv', fp)
        bq_utils.load_cdm_csv(FAKE_HPO_ID, PERSON)
        time.sleep(BQ_TIMEOUT_SECONDS)

        table_id = bq_utils.get_table_id(FAKE_HPO_ID, PERSON)
        q = 'SELECT person_id FROM %s' % table_id
        result = bq_utils.query(q)
        self.assertEqual(5, int(result['totalRows']))

    def test_create_table(self):
        table_id = 'some_random_table_id'
        fields = [dict(name='id', type='integer', mode='required'),
                  dict(name='name', type='string', mode='nullable')]
        result = bq_utils.create_table(table_id, fields)
        self.assertTrue('kind' in result)
        self.assertEqual(result['kind'], 'bigquery#table')
        # sanity check
        self.assertTrue(bq_utils.table_exists(table_id))

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
        standard_tables = list(common.CDM_TABLES) + ACHILLES_TABLES
        for standard_table in standard_tables:
            table_id = 'prefix_for_test_' + standard_table
            result = bq_utils.create_standard_table(standard_table, table_id)
            self.assertTrue('kind' in result)
            self.assertEqual(result['kind'], 'bigquery#table')
            # sanity check
            self.assertTrue(bq_utils.table_exists(table_id))

    def tearDown(self):
        self._drop_tables()
        self._empty_bucket()
        self.testbed.deactivate()
