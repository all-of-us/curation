import unittest
import os
import common
import gcs_utils
import bq_utils
import resources
import test_util
import logging
import sys

from tools.combine_ehr_rdr import copy_rdr_table, ehr_consent, main, mapping_table_for, create_cdm_tables
from tools.combine_ehr_rdr import copy_ehr_table, move_ehr_person_to_observation
from tools.combine_ehr_rdr import DOMAIN_TABLES, EHR_CONSENT_TABLE_ID, RDR_TABLES_TO_COPY
from google.appengine.ext import testbed
from tools.combine_ehr_rdr import logger
from validation.export import


class CombineEhrRdrTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # TODO base class this
        logger.level = logging.INFO
        stream_handler = logging.StreamHandler(sys.stdout)
        logger.addHandler(stream_handler)
        cls.testbed = testbed.Testbed()
        cls.testbed.activate()
        cls.testbed.init_app_identity_stub()
        cls.testbed.init_memcache_stub()
        cls.testbed.init_urlfetch_stub()
        cls.testbed.init_blobstore_stub()
        cls.testbed.init_datastore_v3_stub()
        ehr_dataset_id = bq_utils.get_dataset_id()
        rdr_dataset_id = bq_utils.get_rdr_dataset_id()
        test_util.delete_all_tables(ehr_dataset_id)
        test_util.delete_all_tables(rdr_dataset_id)
        cls.load_dataset_from_files(ehr_dataset_id, test_util.NYC_FIVE_PERSONS_PATH)
        cls.load_dataset_from_files(rdr_dataset_id, test_util.RDR_PATH)

    @staticmethod
    def load_dataset_from_files(dataset_id, path):
        app_id = bq_utils.app_identity.get_application_id()
        bucket = gcs_utils.get_hpo_bucket(test_util.FAKE_HPO_ID)
        test_util.empty_bucket(bucket)
        job_ids = []
        for table in common.CDM_TABLES:
            filename = table + '.csv'
            schema = os.path.join(resources.fields_path, table + '.json')
            f = os.path.join(path, filename)
            if os.path.exists(os.path.join(path, filename)):
                with open(f, 'r') as fp:
                    gcs_utils.upload_object(bucket, filename, fp)
            else:
                test_util.write_cloud_str(bucket, filename, '\n')
            gcs_path = 'gs://{bucket}/{filename}'.format(bucket=bucket, filename=filename)
            load_results = bq_utils.load_csv(schema, gcs_path, app_id, dataset_id, table, allow_jagged_rows=True)
            load_job_id = load_results['jobReference']['jobId']
            job_ids.append(load_job_id)
        incomplete_jobs = bq_utils.wait_on_jobs(job_ids)
        if len(incomplete_jobs) > 0:
            message = "Job id(s) %s failed to complete" % incomplete_jobs
            raise RuntimeError(message)
        test_util.empty_bucket(bucket)

    def setUp(self):
        super(CombineEhrRdrTest, self).setUp()
        self.APP_ID = bq_utils.app_identity.get_application_id()
        self.ehr_dataset_id = bq_utils.get_dataset_id()
        self.rdr_dataset_id = bq_utils.get_rdr_dataset_id()
        self.combined_dataset_id = bq_utils.get_ehr_rdr_dataset_id()
        self.drc_bucket = gcs_utils.get_drc_bucket()
        test_util.delete_all_tables(self.combined_dataset_id)

    def _test_consented_person_id(self):
        """
        Test observation data has seven (7) persons with consent records as described below
         1: No
         2: Yes
         3: NULL
         4: No  followed by Yes
         5: Yes followed by No
         6: Yes followed by NULL
         7: NULL and Yes with same date/time
        """
        # sanity check
        self.assertFalse(bq_utils.table_exists(EHR_CONSENT_TABLE_ID, self.combined_dataset_id))
        ehr_consent()
        self.assertTrue(bq_utils.table_exists(EHR_CONSENT_TABLE_ID, self.combined_dataset_id),
                        'Table {dataset}.{table} created by consented_person'.format(dataset=self.combined_dataset_id,
                                                                                     table=EHR_CONSENT_TABLE_ID))
        response = bq_utils.query('SELECT * FROM {dataset}.{table}'.format(dataset=self.combined_dataset_id,
                                                                           table=EHR_CONSENT_TABLE_ID))
        rows = test_util.response2rows(response)
        expected = {2, 4}
        actual = set(row['person_id'] for row in rows)
        self.assertSetEqual(expected,
                            actual,
                            'Records in {dataset}.{table}'.format(dataset=self.combined_dataset_id,
                                                                  table=EHR_CONSENT_TABLE_ID))

    def _test_copy_rdr_tables(self):
        for table in RDR_TABLES_TO_COPY:
            self.assertFalse(bq_utils.table_exists(table, self.combined_dataset_id))  # sanity check
            copy_rdr_table(table)
            actual = bq_utils.table_exists(table, self.combined_dataset_id)
            self.assertTrue(actual, msg='RDR table {table} should be copied'.format(table=table))

            # Check that row count in combined is same as rdr
            q = '''
              WITH rdr AS
               (SELECT COUNT(1) n FROM {rdr_dataset_id}.{table}),
              combined AS
               (SELECT COUNT(1) n FROM {combined_dataset_id}.{table})
              SELECT 
                rdr.n      AS rdr_count, 
                combined.n AS combined_count 
              FROM rdr, combined
            '''.format(rdr_dataset_id=self.rdr_dataset_id, combined_dataset_id=self.combined_dataset_id, table=table)
            response = bq_utils.query(q)
            rows = test_util.response2rows(response)
            self.assertTrue(len(rows) == 1)  # sanity check
            row = rows[0]
            rdr_count, combined_count = row['rdr_count'], row['combined_count']
            msg_fmt = 'Table {table} has {rdr_count} in rdr and {combined_count} in combined (expected to be equal)'
            self.assertEqual(rdr_count, combined_count,
                             msg_fmt.format(table=table, rdr_count=rdr_count, combined_count=combined_count))

    def test_person_move(self):
        move_ehr_person_to_observation()
        for table in RDR_TABLES_TO_COPY:
            # person table query
            q_person = '''
                SELECT (person_id,
                        gender_concept_id,
                        gender_source_value,
                        race_concept_id,
                        race_source_value,
                        birth_datetime,
                        ethnicity_concept_id,
                        ethnicity_source_value)
                FROM {ehr_dataset_id}.person
            '''.format(ehr_dataset_id=self.ehr_dataset_id)
            response_person = bq_utils.query(q_person)
            print response
            q_obs = '''
                SELECT (person_id,
                        observation_concept_id,
                        observation_type_concept_id,
                        observation_datetime,
                        value_as_concept_id,
                        value_as_string,
                        observation_source_value,
                        observation_source_concept_id)
                FROM {ehr_dataset_id}.observation
            '''.format(ehr_dataset_id=self.ehr_dataset_id)
            response_obs = bq_utils.query(q_obs)
            print response
            return
            # Check 
            q = '''
              WITH rdr AS
               (SELECT COUNT(1) n FROM {rdr_dataset_id}.{table}),
              combined AS
               (SELECT COUNT(1) n FROM {combined_dataset_id}.{table})
              SELECT 
                rdr.n      AS rdr_count, 
                combined.n AS combined_count 
              FROM rdr, combined
            '''.format(rdr_dataset_id=self.rdr_dataset_id, combined_dataset_id=self.combined_dataset_id, table=table)
            response = bq_utils.query(q)
            print response
            rows = test_util.response2rows(response)
            self.assertTrue(len(rows) == 1)  # sanity check
            row = rows[0]
            rdr_count, combined_count = row['rdr_count'], row['combined_count']
            msg_fmt = 'Table {table} has {rdr_count} in rdr and {combined_count} in combined (expected to be equal)'
            self.assertEqual(rdr_count, combined_count,
                             msg_fmt.format(table=table, rdr_count=rdr_count, combined_count=combined_count))


    def _ehr_only_records_excluded(self):
        """
        EHR person records which are missing from RDR are excluded from combined
        """
        q = '''
        WITH ehr_only AS
        (SELECT person_id 
         FROM {ehr_dataset_id}.person ep
         WHERE NOT EXISTS 
           (SELECT 1 
            FROM {rdr_dataset_id}.person rp 
            WHERE rp.person_id = ep.person_id)
        )
        SELECT 
          ehr_only.person_id AS ehr_person_id,
          p.person_id        AS combined_person_id
        FROM ehr_only 
          LEFT JOIN {ehr_rdr_dataset_id}.person p
            ON ehr_only.person_id = p.person_id
        '''.format(ehr_dataset_id=self.ehr_dataset_id,
                   rdr_dataset_id=self.rdr_dataset_id,
                   ehr_rdr_dataset_id=self.combined_dataset_id)
        response = bq_utils.query(q)
        rows = test_util.response2rows(response)
        self.assertGreater(len(rows), 0, 'Test data is missing EHR-only records')
        for row in rows:
            combined_person_id = row['combined_person_id']
            self.assertIsNone(combined_person_id,
                              'EHR-only person_id `{ehr_person_id}` found in combined when it should be excluded')

    def _all_rdr_records_included(self):
        """
        All rdr records are included whether or not there is corresponding ehr record
        """
        for domain_table in DOMAIN_TABLES:
            mapping_table = mapping_table_for(domain_table)
            q = '''SELECT rt.{domain_table}_id as id
               FROM {rdr_dataset_id}.{domain_table} rt
               LEFT JOIN {ehr_rdr_dataset_id}.{mapping_table} m
               ON rt.{domain_table}_id = m.src_{domain_table}_id
               WHERE
                 m.{domain_table}_id IS NULL 
               OR NOT EXISTS
                 (SELECT 1 FROM {ehr_rdr_dataset_id}.{domain_table} t
                  WHERE t.{domain_table}_id = m.{domain_table}_id)'''.format(
                domain_table=domain_table,
                rdr_dataset_id=bq_utils.get_rdr_dataset_id(),
                ehr_rdr_dataset_id=bq_utils.get_ehr_rdr_dataset_id(),
                mapping_table=mapping_table)
            response = bq_utils.query(q)
            rows = test_util.response2rows(response)
            self.assertEqual(0, len(rows), "RDR records should map to records in mapping and combined tables")

    def test_create_cdm_tables(self):
        # Sanity check
        for table in common.CDM_TABLES:
            self.assertFalse(bq_utils.table_exists(table, self.combined_dataset_id))
        create_cdm_tables()
        for table in common.CDM_TABLES:
            actual = bq_utils.table_exists(table, self.combined_dataset_id)
            self.assertTrue(actual, 'Table {table} not created in combined dataset'.format(table=table))

    def _fact_relationship_loaded(self):
        # TODO
        # All fact_id_1 where domain_concept_id_1==21 map to measurement
        # All fact_id_2 where domain_concept_id_2==27 map to observation
        pass

    def test_main(self):
        main()
        self._ehr_only_records_excluded()
        self._all_rdr_records_included()

    def test_ehr_person_to_observation(self):
        # ehr person table converts to observation records
        pass

    def tearDown(self):
        test_util.delete_all_tables(self.combined_dataset_id)

    @classmethod
    def tearDownClass(cls):
        ehr_dataset_id = bq_utils.get_dataset_id()
        rdr_dataset_id = bq_utils.get_rdr_dataset_id()
        # test_util.delete_all_tables(ehr_dataset_id)
        # test_util.delete_all_tables(rdr_dataset_id)
        cls.testbed.deactivate()
        logger.handlers = []
