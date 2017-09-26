import unittest
import time
import bq_utils
import gcs_utils
from google.appengine.ext import testbed
from test_util import FAKE_HPO_ID, FIVE_PERSONS_PERSON_CSV
import resources
import common


PERSON = 'person'


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
        self.tables_to_drop = []
        self._drop_tables()
        self._empty_bucket()

    def _empty_bucket(self):
        bucket_items = gcs_utils.list_bucket(self.hpo_bucket)
        for bucket_item in bucket_items:
            gcs_utils.delete_object(self.hpo_bucket, bucket_item['name'])

    def _drop_tables(self):
        if bq_utils.table_exists(self.person_table_id):
            bq_utils.delete_table(self.person_table_id)

    def _drop_created_test_tables(self):
        for table_id in self.tables_to_drop:
            if table_id not in common.VOCABULARY_TABLES:
                bq_utils.delete_table(table_id)

    def test_load_table_from_bucket(self):
        with open(FIVE_PERSONS_PERSON_CSV, 'rb') as fp:
            gcs_utils.upload_object(self.hpo_bucket, 'person.csv', fp)
        result = bq_utils.load_table_from_bucket(FAKE_HPO_ID, PERSON)
        self.assertEqual(result['status']['state'], 'RUNNING')

    def test_load_table_from_bucket_error_on_bad_table_name(self):
        with self.assertRaises(ValueError) as cm:
            bq_utils.load_table_from_bucket(FAKE_HPO_ID, 'not_a_cdm_table')

    def test_query_result(self):
        with open(FIVE_PERSONS_PERSON_CSV, 'rb') as fp:
            gcs_utils.upload_object(self.hpo_bucket, 'person.csv', fp)
        bq_utils.load_table_from_bucket(FAKE_HPO_ID, PERSON)
        time.sleep(2)

        table_id = bq_utils.get_table_id(FAKE_HPO_ID, PERSON)
        q = 'SELECT person_id FROM %s' % table_id
        result = bq_utils.query(q)
        self.assertEqual(5, int(result['totalRows']))

    def test_merge_with_good_data(self):
        with open(NYC_FIVE_PERSONS_PERSON_CSV, 'rb') as fp:
            gcs_utils.upload_object(gcs_utils.get_hpo_bucket('nyc'), 'person.csv', fp)
        bq_utils.load_table_from_bucket('nyc', 'person')
        with open(PITT_FIVE_PERSONS_PERSON_CSV, 'rb') as fp:
            gcs_utils.upload_object(gcs_utils.get_hpo_bucket('pitt'), 'person.csv', fp)
        bq_utils.load_table_from_bucket('pitt', 'person')

        nyc_person_ids = [int(row['person_id'])
                          for row in
                          resources._csv_to_list(NYC_FIVE_PERSONS_PERSON_CSV)]
        pitt_person_ids = [int(row['person_id'])
                           for row in resources._csv_to_list(
                               PITT_FIVE_PERSONS_PERSON_CSV
                           )]
        expected_result = nyc_person_ids + pitt_person_ids
        expected_result.sort()

        time.sleep(5)

        table_ids = ['nyc_person', 'pitt_person']
        success_flag, error = bq_utils.merge_tables(bq_utils.get_dataset_id(),
                                                    table_ids,
                                                    bq_utils.get_dataset_id(),
                                                    'merged_nyc_pitt')

        self.assertTrue(success_flag)
        self.assertEqual(error, "")

        query_string = "SELECT person_id FROM {}.{} LIMIT 1000".format(bq_utils.get_dataset_id(), 'merged_nyc_pitt')

        merged_query_job_result = bq_utils.query_table(query_string)

        self.assertIsNone(merged_query_job_result.get('errors', None))
        actual_result = [int(row['f'][0]['v']) for row in merged_query_job_result['rows']]
        actual_result.sort()

        self.assertListEqual(expected_result, actual_result)

        for table_name in table_ids + ['merged_nyc_pitt']:
            if table_name not in self.tables_to_drop:
                self.tables_to_drop.append(table_name)

    def test_merge_bad_table_names(self):
        table_ids = ['nyc_person_foo', 'pitt_person_foo']
        success_flag, error_msg = bq_utils.merge_tables(
            bq_utils.get_dataset_id(),
            table_ids,
            bq_utils.get_dataset_id(),
            'merged_nyc_pitt'
        )

        # print error_msg
        assert(not success_flag)

    def test_merge_with_unmatched_schema(self):
        with open(NYC_FIVE_PERSONS_MEASUREMENT_CSV, 'rb') as fp:
            gcs_utils.upload_object(gcs_utils.get_hpo_bucket('nyc'), 'measurement.csv', fp)
        bq_utils.load_table_from_bucket('nyc', 'measurement')
        with open(PITT_FIVE_PERSONS_PERSON_CSV, 'rb') as fp:
            gcs_utils.upload_object(gcs_utils.get_hpo_bucket('pitt'), 'person.csv', fp)
        bq_utils.load_table_from_bucket('pitt', 'person')

        time.sleep(5)

        table_names = ['nyc_measurement', 'pitt_person']
        success_flag, error = bq_utils.merge_tables(
          bq_utils.get_dataset_id(),
          table_names,
          bq_utils.get_dataset_id(),
          'merged_nyc_pitt'
        )
        self.assertFalse(success_flag)

    def tearDown(self):
        self._drop_tables()
        self._drop_created_test_tables()
        self._empty_bucket()
        self.testbed.deactivate()
