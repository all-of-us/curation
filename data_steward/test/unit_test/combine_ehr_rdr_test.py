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
from tools.combine_ehr_rdr import move_ehr_person_to_observation, mapping_query
from tools.combine_ehr_rdr import DOMAIN_TABLES, EHR_CONSENT_TABLE_ID, RDR_TABLES_TO_COPY
from google.appengine.ext import testbed
from tools.combine_ehr_rdr import logger
from validation.export import query_result_to_payload


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
        cls.load_dataset_from_files(ehr_dataset_id, test_util.NYC_FIVE_PERSONS_PATH, True)
        cls.load_dataset_from_files(rdr_dataset_id, test_util.RDR_PATH)

    @staticmethod
    def load_dataset_from_files(dataset_id, path, mappings=False):
        bucket = gcs_utils.get_hpo_bucket(test_util.FAKE_HPO_ID)
        test_util.empty_bucket(bucket)
        job_ids = []
        for table in common.CDM_TABLES:
            job_ids.append(CombineEhrRdrTest._upload_file_to_bucket(bucket, dataset_id, path, table))
            if mappings and table in DOMAIN_TABLES:
                mapping_table = '_mapping_{table}'.format(table=table)
                job_ids.append(CombineEhrRdrTest._upload_file_to_bucket(bucket, dataset_id, path, mapping_table))
        incomplete_jobs = bq_utils.wait_on_jobs(job_ids)
        if len(incomplete_jobs) > 0:
            message = "Job id(s) %s failed to complete" % incomplete_jobs
            raise RuntimeError(message)
        test_util.empty_bucket(bucket)

    @staticmethod
    def _upload_file_to_bucket(bucket, dataset_id, path, table):
        app_id = bq_utils.app_identity.get_application_id()
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
        return load_job_id

    def setUp(self):
        super(CombineEhrRdrTest, self).setUp()
        self.APP_ID = bq_utils.app_identity.get_application_id()
        self.ehr_dataset_id = bq_utils.get_dataset_id()
        self.rdr_dataset_id = bq_utils.get_rdr_dataset_id()
        self.combined_dataset_id = bq_utils.get_ehr_rdr_dataset_id()
        self.drc_bucket = gcs_utils.get_drc_bucket()
        test_util.delete_all_tables(self.combined_dataset_id)

    def test_consented_person_id(self):
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

    def test_copy_rdr_tables(self):
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

    def test_ehr_person_to_observation(self):
        # ehr person table converts to observation records
        create_cdm_tables()
        copy_rdr_table('person')
        move_ehr_person_to_observation()
        # person table query
        q_person = '''
            SELECT (person_id,
                    gender_concept_id,
                    gender_source_value,
                    race_concept_id,
                    race_source_value,
                    CAST(birth_datetime as STRING),
                    ethnicity_concept_id,
                    ethnicity_source_value,
                    EXTRACT(DATE FROM birth_datetime))
            FROM {ehr_dataset_id}.person
        '''.format(ehr_dataset_id=self.ehr_dataset_id)
        response_ehr_person = [[item['v'] for item in row['f']] for row in
                               query_result_to_payload(bq_utils.query(q_person))['F0_']]
        q_obs = '''
            SELECT (person_id,
                    observation_concept_id,
                    value_as_concept_id,
                    value_as_string,
                    observation_source_value,
                    observation_date)
            FROM {ehr_dataset_id}.observation obs
            WHERE   obs.observation_concept_id=4013886 -- Race - 4013886
                OR  obs.observation_concept_id=4271761 -- Ethnic group - 4271761
                OR  obs.observation_concept_id=4135376 -- Gender - 4135376
                OR  obs.observation_concept_id=4083587 -- DOB - 4083587
        '''.format(ehr_dataset_id=self.combined_dataset_id)
        response_obs = [[item['v'] for item in row['f']] for row in
                        query_result_to_payload(bq_utils.query(q_obs))['F0_']]
        # concept ids
        gender_concept_id = '4135376'
        race_concept_id = '4013886'
        dob_concept_id = '4083587'
        ethnicity_concept_id = '4271761'

        # expected lists
        expected_gender_list = [(row[0], gender_concept_id, row[1], row[8]) for row in response_ehr_person]
        expected_race_list = [(row[0], race_concept_id, row[3], row[8]) for row in response_ehr_person]
        expected_dob_list = [(row[0], dob_concept_id, row[5], row[8]) for row in response_ehr_person]
        expected_ethnicity_list = [(row[0], ethnicity_concept_id, row[6], row[8]) for row in response_ehr_person]

        # actual lists
        actual_gender_list = [(row[0], row[1], row[2], row[5]) for row in response_obs if row[1] == gender_concept_id]
        actual_race_list = [(row[0], row[1], row[2], row[5]) for row in response_obs if row[1] == race_concept_id]
        actual_dob_list = [(row[0], row[1], row[3], row[5]) for row in response_obs if row[1] == dob_concept_id]
        actual_ethnicity_list = [(row[0], row[1], row[2], row[5]) for row in response_obs if
                                 row[1] == ethnicity_concept_id]

        self.assertListEqual(sorted(expected_gender_list), sorted(actual_gender_list), 'gender check fails')
        self.assertListEqual(sorted(expected_race_list), sorted(actual_race_list), 'race check fails')
        self.assertListEqual(sorted(expected_dob_list), sorted(actual_dob_list), 'dob check fails')
        self.assertListEqual(sorted(expected_ethnicity_list), sorted(actual_ethnicity_list), 'ethnicity check fails')

        person_ehr_row_count = int(bq_utils.get_table_info('person', self.ehr_dataset_id)['numRows'])
        obs_row_count = int(bq_utils.get_table_info('observation', self.combined_dataset_id)['numRows'])

        self.assertEqual(person_ehr_row_count * 4, obs_row_count)

    def test_mapping_query(self):
        table = 'visit_occurrence'
        q = mapping_query(table)
        expected_query = '''
    WITH all_records AS
    (
        SELECT
          '{rdr_dataset_id}'  AS src_dataset_id, 
          {domain_table}_id AS src_{domain_table}_id,
          NULL as src_hpo_id 
        FROM {rdr_dataset_id}.{domain_table}

        UNION ALL

        SELECT
          '{ehr_dataset_id}'  AS src_dataset_id, 
          t.{domain_table}_id AS src_{domain_table}_id,
          v.src_hpo_id AS src_hpo_id
        FROM {ehr_dataset_id}.{domain_table} t
        JOIN {ehr_dataset_id}._mapping_{domain_table}  v on t.{domain_table}_id = v.{domain_table}_id 
        WHERE EXISTS
           (SELECT 1 FROM {ehr_rdr_dataset_id}.{ehr_consent_table_id} c 
            WHERE t.person_id = c.person_id)
    )
    SELECT 
      ROW_NUMBER() OVER (ORDER BY src_dataset_id, src_{domain_table}_id) AS {domain_table}_id,
      src_dataset_id,
      src_{domain_table}_id,
      src_hpo_id
    FROM all_records
    '''.format(rdr_dataset_id=self.rdr_dataset_id, domain_table=table, ehr_dataset_id=self.ehr_dataset_id,
                   ehr_consent_table_id=EHR_CONSENT_TABLE_ID, ehr_rdr_dataset_id=self.combined_dataset_id)

        self.assertEqual(expected_query, q, "Mapping query for \n {q} \n to is not as expected".format(q=q))

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

    def _mapping_table_checks(self):
        """
        Check mapping tables exist, have correct schema, have expected number of records
        """
        where = '''
                WHERE EXISTS
                   (SELECT 1 FROM {ehr_rdr_dataset_id}.{ehr_consent_table_id} c 
                    WHERE t.person_id = c.person_id)
                '''.format(ehr_rdr_dataset_id=self.combined_dataset_id, ehr_consent_table_id=EHR_CONSENT_TABLE_ID)
        ehr_counts = test_util.get_table_counts(self.ehr_dataset_id, DOMAIN_TABLES, where)
        rdr_counts = test_util.get_table_counts(self.rdr_dataset_id)
        combined_counts = test_util.get_table_counts(self.combined_dataset_id)
        output_tables = combined_counts.keys()
        expected_counts = dict()
        expected_diffs = ['observation']
        self.maxDiff = None

        for t in DOMAIN_TABLES:
            expected_mapping_table = mapping_table_for(t)
            self.assertIn(expected_mapping_table, output_tables)
            expected_fields = resources.fields_for(expected_mapping_table)
            actual_table_info = bq_utils.get_table_info(expected_mapping_table, self.combined_dataset_id)
            actual_fields = actual_table_info.get('schema', dict()).get('fields', [])
            actual_fields_norm = map(test_util.normalize_field_payload, actual_fields)
            self.assertItemsEqual(expected_fields, actual_fields_norm)

            # Count should be sum of EHR and RDR
            # (except for tables like observation where extra records are created for demographics)
            actual_count = combined_counts[expected_mapping_table]
            expected_count = actual_count if t in expected_diffs else ehr_counts[t] + rdr_counts[t]
            expected_counts[expected_mapping_table] = expected_count
        self.assertDictContainsSubset(expected=expected_counts, actual=combined_counts)

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
        tables_before = bq_utils.list_tables(self.combined_dataset_id)
        table_names_before = [t['tableReference']['tableId'] for t in tables_before]
        for table in common.CDM_TABLES:
            self.assertNotIn(table, table_names_before)
        create_cdm_tables()
        tables_after = bq_utils.list_tables(self.combined_dataset_id)
        table_names_after = [t['tableReference']['tableId'] for t in tables_after]
        for table in common.CDM_TABLES:
            self.assertIn(table, table_names_after)

    def _fact_relationship_loaded(self):
        # TODO
        # All fact_id_1 where domain_concept_id_1==21 map to measurement
        # All fact_id_2 where domain_concept_id_2==27 map to observation
        pass

    def _check_ehr_person_observation(self):
        q = '''SELECT * FROM {dataset_id}.person'''.format(dataset_id=self.ehr_dataset_id)
        person_response = bq_utils.query(q)
        person_rows = test_util.response2rows(person_response)
        q = '''SELECT * 
               FROM {ehr_rdr_dataset_id}.observation
               WHERE observation_type_concept_id = 38000280'''.format(ehr_rdr_dataset_id=self.combined_dataset_id)
        # observation should contain 4 records per person of type EHR
        expected = len(person_rows) * 4
        observation_response = bq_utils.query(q)
        observation_rows = test_util.response2rows(observation_response)
        # TODO check row content is as expected
        actual = len(observation_rows)
        self.assertEqual(actual, expected,
                         'Expected %s EHR person records in observation but found %s' % (expected, actual))

    def test_main(self):
        main()
        self._mapping_table_checks()
        self._ehr_only_records_excluded()
        self._all_rdr_records_included()
        self._check_ehr_person_observation()

    def tearDown(self):
        test_util.delete_all_tables(self.combined_dataset_id)

    @classmethod
    def tearDownClass(cls):
        ehr_dataset_id = bq_utils.get_dataset_id()
        rdr_dataset_id = bq_utils.get_rdr_dataset_id()
        test_util.delete_all_tables(ehr_dataset_id)
        test_util.delete_all_tables(rdr_dataset_id)
        cls.testbed.deactivate()
        logger.handlers = []
