# Python imports
import os
import unittest
from io import open

# Project imports
import bq_utils
import gcs_utils
import resources
from tests import test_util
from constants.tools.combine_ehr_rdr import EHR_CONSENT_TABLE_ID, RDR_TABLES_TO_COPY, DOMAIN_TABLES
from tools.combine_ehr_rdr import (copy_rdr_table, ehr_consent, main,
                                   mapping_table_for, create_cdm_tables)

UNCONSENTED_EHR_COUNTS_QUERY = (
    '  select \'{domain_table}\' as table_id, count(1) as n from (SELECT DISTINCT'
    '  v.src_hpo_id AS src_hpo_id,'
    '  t.{domain_table}_id  AS {domain_table}_id'
    '  FROM `{ehr_dataset_id}.{domain_table}` AS t'
    '  JOIN `{ehr_dataset_id}._mapping_{domain_table}` AS v'
    '  ON t.{domain_table}_id = v.{domain_table}_id'
    '  WHERE NOT EXISTS'
    '  (SELECT 1 FROM `{combined_dataset_id}.{ehr_consent_table_id}` AS c'
    '  WHERE t.person_id = c.person_id))')


class CombineEhrRdrTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')
        # TODO base class this
        ehr_dataset_id = bq_utils.get_dataset_id()
        rdr_dataset_id = bq_utils.get_rdr_dataset_id()
        test_util.delete_all_tables(ehr_dataset_id)
        test_util.delete_all_tables(rdr_dataset_id)
        cls.load_dataset_from_files(ehr_dataset_id,
                                    test_util.NYC_FIVE_PERSONS_PATH, True)
        cls.load_dataset_from_files(rdr_dataset_id, test_util.RDR_PATH)

    @staticmethod
    def load_dataset_from_files(dataset_id, path, mappings=False):
        bucket = gcs_utils.get_hpo_bucket(test_util.FAKE_HPO_ID)
        test_util.empty_bucket(bucket)
        job_ids = []
        for table in resources.CDM_TABLES:
            job_ids.append(
                CombineEhrRdrTest._upload_file_to_bucket(
                    bucket, dataset_id, path, table))
            if mappings and table in DOMAIN_TABLES:
                mapping_table = '_mapping_{table}'.format(table=table)
                job_ids.append(
                    CombineEhrRdrTest._upload_file_to_bucket(
                        bucket, dataset_id, path, mapping_table))
        incomplete_jobs = bq_utils.wait_on_jobs(job_ids)
        if len(incomplete_jobs) > 0:
            message = "Job id(s) %s failed to complete" % incomplete_jobs
            raise RuntimeError(message)
        test_util.empty_bucket(bucket)

    @staticmethod
    def _upload_file_to_bucket(bucket, dataset_id, path, table):
        app_id = bq_utils.app_identity.get_application_id()
        filename = table + '.csv'

        file_path = os.path.join(path, filename)
        try:
            with open(file_path, 'rb') as filepath:
                gcs_utils.upload_object(bucket, filename, filepath)
        except OSError:
            test_util.write_cloud_str(bucket, filename, '\n')

        gcs_path = 'gs://{bucket}/{filename}'.format(bucket=bucket,
                                                     filename=filename)
        load_results = bq_utils.load_csv(table,
                                         gcs_path,
                                         app_id,
                                         dataset_id,
                                         table,
                                         allow_jagged_rows=True)
        load_job_id = load_results['jobReference']['jobId']
        return load_job_id

    def setUp(self):
        self.ehr_dataset_id = bq_utils.get_dataset_id()
        self.rdr_dataset_id = bq_utils.get_rdr_dataset_id()
        self.combined_dataset_id = bq_utils.get_combined_dataset_id()
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
        # pre-conditions
        self.assertFalse(
            bq_utils.table_exists(EHR_CONSENT_TABLE_ID,
                                  self.combined_dataset_id))

        # test
        ehr_consent()

        # post conditions
        self.assertTrue(
            bq_utils.table_exists(EHR_CONSENT_TABLE_ID,
                                  self.combined_dataset_id),
            'Table {dataset}.{table} created by consented_person'.format(
                dataset=self.combined_dataset_id, table=EHR_CONSENT_TABLE_ID))
        response = bq_utils.query('SELECT * FROM {dataset}.{table}'.format(
            dataset=self.combined_dataset_id, table=EHR_CONSENT_TABLE_ID))
        rows = bq_utils.response2rows(response)
        expected = {2, 4}
        actual = set(row['person_id'] for row in rows)
        self.assertSetEqual(
            expected, actual, 'Records in {dataset}.{table}'.format(
                dataset=self.combined_dataset_id, table=EHR_CONSENT_TABLE_ID))

    def test_copy_rdr_tables(self):
        for table in RDR_TABLES_TO_COPY:
            self.assertFalse(
                bq_utils.table_exists(table,
                                      self.combined_dataset_id))  # sanity check
            copy_rdr_table(table)
            actual = bq_utils.table_exists(table, self.combined_dataset_id)
            self.assertTrue(
                actual,
                msg='RDR table {table} should be copied'.format(table=table))

            # Check that row count in combined is same as rdr
            query = (
                'WITH rdr AS '
                ' (SELECT COUNT(1) n FROM `{rdr_dataset_id}.{table}`), '
                'combined AS '
                ' (SELECT COUNT(1) n FROM `{combined_dataset_id}.{table}`) '
                'SELECT '
                'rdr.n AS rdr_count, '
                'combined.n AS combined_count '
                'FROM rdr, combined ').format(
                    rdr_dataset_id=self.rdr_dataset_id,
                    combined_dataset_id=self.combined_dataset_id,
                    table=table)
            response = bq_utils.query(query)
            rows = bq_utils.response2rows(response)
            self.assertTrue(len(rows) == 1)  # sanity check
            row = rows[0]
            rdr_count, combined_count = row['rdr_count'], row['combined_count']
            msg_fmt = 'Table {table} has {rdr_count} in rdr and {combined_count} in combined (expected to be equal)'
            self.assertEqual(
                rdr_count, combined_count,
                msg_fmt.format(table=table,
                               rdr_count=rdr_count,
                               combined_count=combined_count))

    def _ehr_only_records_excluded(self):
        """
        EHR person records which are missing from RDR are excluded from combined
        """
        query = ('WITH ehr_only AS '
                 ' (SELECT person_id '
                 '  FROM `{ehr_dataset_id}.person` AS ep '
                 '  WHERE NOT EXISTS '
                 '    (SELECT 1 '
                 '     FROM `{rdr_dataset_id}.person` AS rp '
                 '     WHERE rp.person_id = ep.person_id) '
                 ' ) '
                 'SELECT '
                 'ehr_only.person_id AS ehr_person_id, '
                 'p.person_id AS combined_person_id '
                 'FROM ehr_only '
                 'LEFT JOIN `{combined_dataset_id}.person` AS p '
                 'ON ehr_only.person_id = p.person_id').format(
                     ehr_dataset_id=self.ehr_dataset_id,
                     rdr_dataset_id=self.rdr_dataset_id,
                     combined_dataset_id=self.combined_dataset_id)
        response = bq_utils.query(query)
        rows = bq_utils.response2rows(response)
        self.assertGreater(len(rows), 0,
                           'Test data is missing EHR-only records')
        for row in rows:
            combined_person_id = row['combined_person_id']
            self.assertIsNone(
                combined_person_id,
                'EHR-only person_id `{ehr_person_id}` found in combined when it should be excluded'
            )

    def get_unconsented_ehr_records_count(self, table_name):
        query = UNCONSENTED_EHR_COUNTS_QUERY.format(
            rdr_dataset_id=bq_utils.get_rdr_dataset_id(),
            ehr_dataset_id=bq_utils.get_dataset_id(),
            combined_dataset_id=self.combined_dataset_id,
            domain_table=table_name,
            ehr_consent_table_id='_ehr_consent')
        response = bq_utils.query(query)
        rows = bq_utils.response2rows(response)
        return rows[0]['n']

    def _mapping_table_checks(self):
        """
        Check mapping tables exist, have correct schema, have expected number of records
        """
        where = (
            'WHERE EXISTS '
            '  (SELECT 1 FROM `{combined_dataset_id}.{ehr_consent_table_id}` AS c '
            '   WHERE t.person_id = c.person_id)').format(
                combined_dataset_id=self.combined_dataset_id,
                ehr_consent_table_id=EHR_CONSENT_TABLE_ID)
        ehr_counts = test_util.get_table_counts(self.ehr_dataset_id,
                                                DOMAIN_TABLES, where)
        rdr_counts = test_util.get_table_counts(self.rdr_dataset_id)
        combined_counts = test_util.get_table_counts(self.combined_dataset_id)
        output_tables = combined_counts.keys()
        expected_counts = dict()
        expected_diffs = ['observation']

        for table in DOMAIN_TABLES:
            expected_mapping_table = mapping_table_for(table)
            self.assertIn(expected_mapping_table, output_tables)
            expected_fields = resources.fields_for(expected_mapping_table)
            actual_table_info = bq_utils.get_table_info(
                expected_mapping_table, self.combined_dataset_id)
            actual_fields = actual_table_info.get('schema',
                                                  dict()).get('fields', [])
            actual_fields_norm = map(test_util.normalize_field_payload,
                                     actual_fields)
            self.assertCountEqual(expected_fields, actual_fields_norm)

            # Count should be sum of EHR and RDR
            # (except for tables like observation where extra records are created for demographics)
            if 'person_id' in [
                    field.get('name', '')
                    for field in resources.fields_for(table)
            ]:
                unconsented_ehr_records = self.get_unconsented_ehr_records_count(
                    table)
            else:
                unconsented_ehr_records = 0

            actual_count = combined_counts[expected_mapping_table]

            if table in expected_diffs:
                expected_count = actual_count
            else:
                expected_count = (ehr_counts[table] -
                                  unconsented_ehr_records) + rdr_counts[table]
            expected_counts[expected_mapping_table] = expected_count

        self.assertDictContainsSubset(expected_counts, combined_counts)

    def _all_rdr_records_included(self):
        """
        All rdr records are included whether or not there is corresponding ehr record
        """
        for domain_table in DOMAIN_TABLES:
            mapping_table = mapping_table_for(domain_table)
            query = (
                'SELECT rt.{domain_table}_id as id '
                'FROM `{rdr_dataset_id}.{domain_table}` AS rt '
                'LEFT JOIN `{combined_dataset_id}.{mapping_table}` AS m '
                'ON rt.{domain_table}_id = m.src_{domain_table}_id '
                'WHERE '
                '  m.{domain_table}_id IS NULL '
                'OR NOT EXISTS '
                ' (SELECT 1 FROM `{combined_dataset_id}.{domain_table}` AS t '
                '  WHERE t.{domain_table}_id = m.{domain_table}_id)').format(
                    domain_table=domain_table,
                    rdr_dataset_id=bq_utils.get_rdr_dataset_id(),
                    combined_dataset_id=bq_utils.get_combined_dataset_id(),
                    mapping_table=mapping_table)
            response = bq_utils.query(query)
            rows = bq_utils.response2rows(response)
            self.assertEqual(
                0, len(rows),
                "RDR records should map to records in mapping and combined tables"
            )

    def test_create_cdm_tables(self):
        # pre-conditions
        # Sanity check
        tables_before = bq_utils.list_tables(self.combined_dataset_id)
        table_names_before = [
            t['tableReference']['tableId'] for t in tables_before
        ]
        for table in resources.CDM_TABLES:
            self.assertNotIn(table, table_names_before)

        # test
        create_cdm_tables()

        # post conditions
        tables_after = bq_utils.list_tables(self.combined_dataset_id)
        table_names_after = [
            t['tableReference']['tableId'] for t in tables_after
        ]
        for table in resources.CDM_TABLES:
            self.assertIn(table, table_names_after)

    def _fact_relationship_loaded(self):
        # TODO
        # All fact_id_1 where domain_concept_id_1==21 map to measurement
        # All fact_id_2 where domain_concept_id_2==27 map to observation
        pass

    def test_main(self):
        # test
        main()

        # post conditions
        self._mapping_table_checks()
        self._ehr_only_records_excluded()
        self._all_rdr_records_included()

    def tearDown(self):
        test_util.delete_all_tables(self.combined_dataset_id)

    @classmethod
    def tearDownClass(cls):
        ehr_dataset_id = bq_utils.get_dataset_id()
        rdr_dataset_id = bq_utils.get_rdr_dataset_id()
        test_util.delete_all_tables(ehr_dataset_id)
        test_util.delete_all_tables(rdr_dataset_id)
