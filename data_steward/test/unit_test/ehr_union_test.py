import unittest
import json
import os
from google.appengine.ext import testbed

import common
import resources
import gcs_utils
import bq_utils
import test_util
from validation import ehr_union

PITT_HPO_ID = 'pitt'
CHS_HPO_ID = 'chs'


class EhrUnionTest(unittest.TestCase):
    def setUp(self):
        super(EhrUnionTest, self).setUp()
        self.testbed = testbed.Testbed()
        self.testbed.activate()
        self.testbed.init_app_identity_stub()
        self.testbed.init_memcache_stub()
        self.testbed.init_urlfetch_stub()
        self.testbed.init_blobstore_stub()
        self.testbed.init_datastore_v3_stub()
        self.project_id = bq_utils.app_identity.get_application_id()
        self.hpo_ids = [CHS_HPO_ID, PITT_HPO_ID]
        self.input_dataset_id = bq_utils.get_dataset_id()
        self.output_dataset_id = bq_utils.get_unioned_dataset_id()
        self._empty_hpo_buckets()
        test_util.delete_all_tables(self.input_dataset_id)
        test_util.delete_all_tables(self.output_dataset_id)

    def _empty_hpo_buckets(self):
        for hpo_id in self.hpo_ids:
            bucket = gcs_utils.get_hpo_bucket(hpo_id)
            test_util.empty_bucket(bucket)

    def _create_hpo_table(self, hpo_id, table, dataset_id):
        table_id = bq_utils.get_table_id(hpo_id, table)
        bq_utils.create_table(table_id, resources.fields_for(table), dataset_id=dataset_id)
        return table_id

    def _load_datasets(self):
        """
        Load five persons data for each test hpo
        """
        # expected_tables is for testing output
        # it maps table name to list of expected records ex: "unioned_ehr_visit_occurrence" -> [{}, {}, ...]
        expected_tables = dict()
        running_jobs = []
        for cdm_table in common.CDM_TABLES:
            cdm_file_name = os.path.join(test_util.FIVE_PERSONS_PATH, cdm_table + '.csv')
            output_table = ehr_union.output_table_for(cdm_table)
            expected_tables[output_table] = []
            for hpo_id in self.hpo_ids:
                # upload csv into hpo bucket
                bucket = gcs_utils.get_hpo_bucket(hpo_id)
                if os.path.exists(cdm_file_name):
                    test_util.write_cloud_file(bucket, cdm_file_name)
                    csv_rows = resources._csv_to_list(cdm_file_name)
                else:
                    # results in empty table
                    test_util.write_cloud_str(bucket, cdm_table + '.csv', 'dummy\n')
                    csv_rows = []
                # load table from csv
                result = bq_utils.load_cdm_csv(hpo_id, cdm_table)
                running_jobs.append(result['jobReference']['jobId'])
                expected_tables[output_table] += list(csv_rows)
        incomplete_jobs = bq_utils.wait_on_jobs(running_jobs)
        if len(incomplete_jobs) > 0:
            message = "Job id(s) %s failed to complete" % incomplete_jobs
            raise RuntimeError(message)
        self.expected_tables = expected_tables

    def _table_has_clustering(self, table_info):
        clustering = table_info.get('clustering')
        self.assertIsNotNone(clustering)
        fields = clustering.get('fields')
        self.assertSetEqual(set(fields), {'person_id'})
        time_partitioning = table_info.get('timePartitioning')
        self.assertIsNotNone(time_partitioning)
        tpe = time_partitioning.get('type')
        self.assertEqual(tpe, 'DAY')

    def _dataset_tables(self, dataset_id):
        """
        Get names of existing tables in specified dataset

        :param dataset_id: identifies the dataset
        :return: list of table_ids
        """
        result = bq_utils.list_tables(dataset_id)
        tables = result.get('tables', [])
        return [table['tableReference']['tableId'] for table in tables]

    def test_union_ehr(self):
        self._load_datasets()
        input_tables_before = set(self._dataset_tables(self.input_dataset_id))

        # output should be mapping tables and cdm tables
        output_tables_before = self._dataset_tables(self.output_dataset_id)
        mapping_tables = [ehr_union.mapping_table_for(table) for table in ehr_union.tables_to_map()]
        output_cdm_tables = [ehr_union.output_table_for(table) for table in common.CDM_TABLES]
        expected_output = set(output_tables_before + mapping_tables + output_cdm_tables)

        # perform ehr union
        ehr_union.main(self.input_dataset_id, self.output_dataset_id, self.project_id, self.hpo_ids)

        # input dataset should be unchanged
        input_tables_after = set(self._dataset_tables(self.input_dataset_id))
        self.assertSetEqual(input_tables_before, input_tables_after)

        # check for each output table
        for table_name in common.CDM_TABLES:
            # output table exists and row count is sum of those submitted by hpos
            result_table = ehr_union.output_table_for(table_name)
            expected_rows = self.expected_tables[result_table]
            expected_count = len(expected_rows)
            table_info = bq_utils.get_table_info(result_table, dataset_id=self.output_dataset_id)
            actual_count = int(table_info.get('numRows'))
            msg = 'Unexpected row count in table {result_table} after ehr union'.format(result_table=result_table)
            self.assertEqual(expected_count, actual_count, msg)
            # TODO Compare table rows to expected accounting for the new ids and ignoring field types
            # q = 'SELECT * FROM {dataset}.{table}'.format(dataset=self.output_dataset_id, table=result_table)
            # query_response = bq_utils.query(q)
            # actual_rows = test_util.response2rows(query_response)

            # output table has clustering on person_id where applicable
            fields_file = os.path.join(resources.fields_path, table_name + '.json')
            with open(fields_file, 'r') as fp:
                fields = json.load(fp)
                field_names = [field['name'] for field in fields]
                if 'person_id' in field_names:
                    self._table_has_clustering(table_info)

        actual_output = set(self._dataset_tables(self.output_dataset_id))
        self.assertSetEqual(expected_output, actual_output)

    def test_subqueries(self):
        hpo_ids = ['chs', 'pitt']
        project_id = bq_utils.app_identity.get_application_id()
        dataset_id = bq_utils.get_dataset_id()
        table = 'measurement'
        mapping_msg = 'Expected mapping subquery count %s but got %s'
        union_msg = 'Expected union subquery count %s but got %s'

        # Should not generate subqueries when HPO tables do not exist
        pitt_table_id = self._create_hpo_table('pitt', table, dataset_id)
        expected_count = 1

        subqueries = ehr_union._mapping_subqueries(table, hpo_ids, dataset_id, project_id)
        actual_count = len(subqueries)
        self.assertEqual(expected_count, actual_count, mapping_msg % (expected_count, actual_count))
        subquery = subqueries[0]
        self.assertTrue(pitt_table_id in subquery)

        subqueries = ehr_union._union_subqueries(table, hpo_ids, dataset_id, self.output_dataset_id)
        self.assertEqual(expected_count, actual_count, union_msg % (expected_count, actual_count))
        subquery = subqueries[0]
        self.assertTrue(pitt_table_id in subquery)

        # After adding measurement table for chs, should generate subqueries for both
        chs_table_id = self._create_hpo_table('chs', table, dataset_id)
        expected_count = 2
        subqueries = ehr_union._mapping_subqueries(table, hpo_ids, dataset_id, project_id)
        actual_count = len(subqueries)
        self.assertEqual(expected_count, actual_count, mapping_msg % (expected_count, actual_count))
        self.assertTrue(any(sq for sq in subqueries if pitt_table_id in sq))
        self.assertTrue(any(sq for sq in subqueries if chs_table_id in sq))

        subqueries = ehr_union._union_subqueries(table, hpo_ids, dataset_id, self.output_dataset_id)
        actual_count = len(subqueries)
        self.assertEqual(expected_count, actual_count, union_msg % (expected_count, actual_count))
        self.assertTrue(any(sq for sq in subqueries if pitt_table_id in sq))
        self.assertTrue(any(sq for sq in subqueries if chs_table_id in sq))

    # TODO Figure out a good way to test query structure
    # One option may be for each query under test to generate an abstract syntax tree
    # (using e.g. https://github.com/andialbrecht/sqlparse) and compare it to an expected tree fragment.
    # Functions below are for reference

    def _test_mapping_query(self):
        table = 'measurement'
        hpo_ids = ['chs', 'pitt']
        project_id = bq_utils.app_identity.get_application_id()
        dataset_id = bq_utils.get_dataset_id()
        created_tables = []
        for hpo_id in hpo_ids:
            hpo_table = self._create_hpo_table(hpo_id, table, dataset_id)
            created_tables.append(hpo_table)
        q = ehr_union.mapping_query(table, hpo_ids, dataset_id, project_id)

    def _test_table_hpo_subquery(self):
        # person is a simple select, no ids should be mapped
        person = ehr_union.table_hpo_subquery(
            'person', hpo_id=CHS_HPO_ID, input_dataset_id='input', output_dataset_id='output')

        # _mapping_visit_occurrence(src_table_id, src_visit_occurrence_id, visit_occurrence_id)
        # visit_occurrence_id should be mapped
        visit_occurrence = ehr_union.table_hpo_subquery(
            'visit_occurrence', hpo_id=CHS_HPO_ID, input_dataset_id='input', output_dataset_id='output')

        # visit_occurrence_id and condition_occurrence_id should be mapped
        condition_occurrence = ehr_union.table_hpo_subquery(
            'condition_occurrence', hpo_id=CHS_HPO_ID, input_dataset_id='input', output_dataset_id='output')

    def _test_table_union_query(self):
        measurement = ehr_union.table_union_query(
            'measurement', self.hpo_ids, self.input_dataset_id, self.output_dataset_id)
        # person is a simple union without has no mapping
        person = ehr_union.table_union_query(
            'person', self.hpo_ids, self.input_dataset_id, self.output_dataset_id)
        visit_occurrence = ehr_union.table_union_query(
            'visit_occurrence', self.hpo_ids, self.input_dataset_id, self.output_dataset_id)
        death = ehr_union.table_union_query(
            'death', self.hpo_ids, self.input_dataset_id, self.output_dataset_id)
        care_site = ehr_union.table_union_query(
            'care_site', self.hpo_ids, self.input_dataset_id, self.output_dataset_id)
        location = ehr_union.table_union_query(
            'location', self.hpo_ids, self.input_dataset_id, self.output_dataset_id)

    def tearDown(self):
        self._empty_hpo_buckets()
        test_util.delete_all_tables(self.input_dataset_id)
        test_util.delete_all_tables(self.output_dataset_id)
        self.testbed.deactivate()
