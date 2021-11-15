import json
import os
import re
import unittest

import dpath
import mock
import moz_sql_parser

import bq_utils
import cdm
import common
from constants.tools import combine_ehr_rdr
from constants.validation import ehr_union as eu_constants
import gcs_utils
import resources
import tests.test_util as test_util
from validation import ehr_union
from io import open

PITT_HPO_ID = 'pitt'
NYC_HPO_ID = 'nyc'
SUBQUERY_FAIL_MSG = '''
Test {expr} in {table} subquery 
 Expected: {expected}
 Actual: {actual}

{subquery}
'''


def first_or_none(l):
    return next(iter(l or []), None)


class EhrUnionTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):

        self.project_id = bq_utils.app_identity.get_application_id()
        self.hpo_ids = [NYC_HPO_ID, PITT_HPO_ID]
        self.input_dataset_id = bq_utils.get_dataset_id()
        self.output_dataset_id = bq_utils.get_unioned_dataset_id()
        # Done in tearDown().  this is redundant.
        self._empty_hpo_buckets()
        test_util.delete_all_tables(self.input_dataset_id)
        test_util.delete_all_tables(self.output_dataset_id)

        # TODO Generalize to work for all foreign key references
        # Collect all primary key fields in CDM tables
        mapped_fields = []
        for table in cdm.tables_to_map():
            field = table + '_id'
            mapped_fields.append(field)
        self.mapped_fields = mapped_fields
        self.implemented_foreign_keys = [
            eu_constants.VISIT_OCCURRENCE_ID, eu_constants.CARE_SITE_ID,
            eu_constants.LOCATION_ID
        ]

    def _empty_hpo_buckets(self):
        for hpo_id in self.hpo_ids:
            bucket = gcs_utils.get_hpo_bucket(hpo_id)
            test_util.empty_bucket(bucket)

    def _create_hpo_table(self, hpo_id, table, dataset_id):
        table_id = bq_utils.get_table_id(hpo_id, table)
        bq_utils.create_table(table_id,
                              resources.fields_for(table),
                              dataset_id=dataset_id)
        return table_id

    def _load_datasets(self):
        """
        Load five persons data for each test hpo
        # expected_tables is for testing output
        # it maps table name to list of expected records ex: "unioned_ehr_visit_occurrence" -> [{}, {}, ...]
        """
        expected_tables = dict()
        running_jobs = []
        for cdm_table in resources.CDM_TABLES:
            output_table = ehr_union.output_table_for(cdm_table)
            expected_tables[output_table] = []
            for hpo_id in self.hpo_ids:
                # upload csv into hpo bucket
                if hpo_id == NYC_HPO_ID:
                    cdm_file_name = os.path.join(test_util.FIVE_PERSONS_PATH,
                                                 cdm_table + '.csv')
                else:
                    cdm_file_name = os.path.join(
                        test_util.PITT_FIVE_PERSONS_PATH, cdm_table + '.csv')
                bucket = gcs_utils.get_hpo_bucket(hpo_id)
                if os.path.exists(cdm_file_name):
                    test_util.write_cloud_file(bucket, cdm_file_name)
                    csv_rows = resources.csv_to_list(cdm_file_name)
                else:
                    # results in empty table
                    test_util.write_cloud_str(bucket, cdm_table + '.csv',
                                              'dummy\n')
                    csv_rows = []
                # load table from csv
                result = bq_utils.load_cdm_csv(hpo_id, cdm_table)
                running_jobs.append(result['jobReference']['jobId'])
                expected_tables[output_table] += list(csv_rows)
        # ensure person to observation output is as expected
        output_table_person = ehr_union.output_table_for(common.PERSON)
        output_table_observation = ehr_union.output_table_for(
            common.OBSERVATION)
        expected_tables[output_table_observation] += 4 * expected_tables[
            output_table_person]

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
        tables = bq_utils.list_tables(dataset_id)
        return [table['tableReference']['tableId'] for table in tables]

    def test_union_ehr(self):
        self._load_datasets()
        input_tables_before = set(self._dataset_tables(self.input_dataset_id))

        # output should be mapping tables and cdm tables
        output_tables_before = self._dataset_tables(self.output_dataset_id)
        mapping_tables = [
            ehr_union.mapping_table_for(table)
            for table in cdm.tables_to_map() + [combine_ehr_rdr.PERSON_TABLE]
        ]
        output_cdm_tables = [
            ehr_union.output_table_for(table) for table in resources.CDM_TABLES
        ]
        expected_output = set(output_tables_before + mapping_tables +
                              output_cdm_tables)

        # perform ehr union
        ehr_union.main(self.input_dataset_id, self.output_dataset_id,
                       self.project_id, self.hpo_ids)

        # input dataset should be unchanged
        input_tables_after = set(self._dataset_tables(self.input_dataset_id))
        self.assertSetEqual(input_tables_before, input_tables_after)

        # fact_relationship from pitt
        hpo_unique_identifiers = ehr_union.get_hpo_offsets(self.hpo_ids)
        pitt_offset = hpo_unique_identifiers[PITT_HPO_ID]
        q = '''SELECT fact_id_1, fact_id_2 
               FROM `{input_dataset}.{hpo_id}_fact_relationship`
               where domain_concept_id_1 = 21 and domain_concept_id_2 = 21'''.format(
            input_dataset=self.input_dataset_id, hpo_id=PITT_HPO_ID)
        response = bq_utils.query(q)
        result = bq_utils.response2rows(response)

        expected_fact_id_1 = result[0]["fact_id_1"] + pitt_offset
        expected_fact_id_2 = result[0]["fact_id_2"] + pitt_offset

        q = '''SELECT fr.fact_id_1, fr.fact_id_2 FROM `{dataset_id}.unioned_ehr_fact_relationship` fr
            join `{dataset_id}._mapping_measurement` mm on fr.fact_id_1 = mm.measurement_id
            and mm.src_hpo_id = "{hpo_id}"'''.format(
            dataset_id=self.output_dataset_id, hpo_id=PITT_HPO_ID)
        response = bq_utils.query(q)
        result = bq_utils.response2rows(response)
        actual_fact_id_1, actual_fact_id_2 = result[0]["fact_id_1"], result[0][
            "fact_id_2"]
        self.assertEqual(expected_fact_id_1, actual_fact_id_1)
        self.assertEqual(expected_fact_id_2, actual_fact_id_2)

        # mapping tables
        tables_to_map = cdm.tables_to_map()
        for table_to_map in tables_to_map:
            mapping_table = ehr_union.mapping_table_for(table_to_map)
            expected_fields = {
                'src_table_id',
                'src_%s_id' % table_to_map,
                '%s_id' % table_to_map, 'src_hpo_id', 'src_dataset_id'
            }
            mapping_table_info = bq_utils.get_table_info(
                mapping_table, dataset_id=self.output_dataset_id)
            mapping_table_fields = mapping_table_info.get('schema', dict()).get(
                'fields', [])
            actual_fields = set([f['name'] for f in mapping_table_fields])
            message = 'Table %s has fields %s when %s expected' % (
                mapping_table, actual_fields, expected_fields)
            self.assertSetEqual(expected_fields, actual_fields, message)
            result_table = ehr_union.output_table_for(table_to_map)
            expected_num_rows = len(self.expected_tables[result_table])
            actual_num_rows = int(mapping_table_info.get('numRows', -1))
            message = 'Table %s has %s rows when %s expected' % (
                mapping_table, actual_num_rows, expected_num_rows)
            self.assertEqual(expected_num_rows, actual_num_rows, message)

        # check for each output table
        for table_name in resources.CDM_TABLES:
            # output table exists and row count is sum of those submitted by hpos
            result_table = ehr_union.output_table_for(table_name)
            expected_rows = self.expected_tables[result_table]
            expected_count = len(expected_rows)
            table_info = bq_utils.get_table_info(
                result_table, dataset_id=self.output_dataset_id)
            actual_count = int(table_info.get('numRows'))
            msg = 'Unexpected row count in table {result_table} after ehr union'.format(
                result_table=result_table)
            self.assertEqual(expected_count, actual_count, msg)
            # TODO Compare table rows to expected accounting for the new ids and ignoring field types
            # q = 'SELECT * FROM {dataset}.{table}'.format(dataset=self.output_dataset_id, table=result_table)
            # query_response = bq_utils.query(q)
            # actual_rows = bq_utils.response2rows(query_response)

            # output table has clustering on person_id where applicable
            fields = resources.fields_for(table_name)
            field_names = [field['name'] for field in fields]
            if 'person_id' in field_names:
                self._table_has_clustering(table_info)

        actual_output = set(self._dataset_tables(self.output_dataset_id))
        self.assertSetEqual(expected_output, actual_output)

        # explicit check that output person_ids are same as input
        nyc_person_table_id = bq_utils.get_table_id(NYC_HPO_ID, 'person')
        pitt_person_table_id = bq_utils.get_table_id(PITT_HPO_ID, 'person')
        q = '''SELECT DISTINCT person_id FROM (
           SELECT person_id FROM {dataset_id}.{nyc_person_table_id}
           UNION ALL
           SELECT person_id FROM {dataset_id}.{pitt_person_table_id}
        ) ORDER BY person_id ASC'''.format(
            dataset_id=self.input_dataset_id,
            nyc_person_table_id=nyc_person_table_id,
            pitt_person_table_id=pitt_person_table_id)
        response = bq_utils.query(q)
        expected_rows = bq_utils.response2rows(response)
        person_table_id = ehr_union.output_table_for('person')
        q = '''SELECT DISTINCT person_id 
               FROM {dataset_id}.{table_id} 
               ORDER BY person_id ASC'''.format(
            dataset_id=self.output_dataset_id, table_id=person_table_id)
        response = bq_utils.query(q)
        actual_rows = bq_utils.response2rows(response)
        self.assertCountEqual(expected_rows, actual_rows)

    # TODO Figure out a good way to test query structure
    # One option may be for each query under test to generate an abstract syntax tree
    # (using e.g. https://github.com/andialbrecht/sqlparse) and compare it to an expected tree fragment.
    # Functions below are for reference

    def convert_ehr_person_to_observation(self, person_row):
        obs_rows = []
        dob_row = {
            'observation_concept_id': eu_constants.DOB_CONCEPT_ID,
            'observation_source_value': None,
            'value_as_string': person_row['birth_datetime'],
            'person_id': person_row['person_id'],
            'observation_date': person_row['birth_date'],
            'value_as_concept_id': None
        }
        gender_row = {
            'observation_concept_id': eu_constants.GENDER_CONCEPT_ID,
            'observation_source_value': person_row['gender_source_value'],
            'value_as_string': None,
            'person_id': person_row['person_id'],
            'observation_date': person_row['birth_date'],
            'value_as_concept_id': person_row['gender_concept_id']
        }
        race_row = {
            'observation_concept_id': eu_constants.RACE_CONCEPT_ID,
            'observation_source_value': person_row['race_source_value'],
            'value_as_string': None,
            'person_id': person_row['person_id'],
            'observation_date': person_row['birth_date'],
            'value_as_concept_id': person_row['race_concept_id']
        }
        ethnicity_row = {
            'observation_concept_id': eu_constants.ETHNICITY_CONCEPT_ID,
            'observation_source_value': person_row['ethnicity_source_value'],
            'value_as_string': None,
            'person_id': person_row['person_id'],
            'observation_date': person_row['birth_date'],
            'value_as_concept_id': person_row['ethnicity_concept_id']
        }
        obs_rows.extend([dob_row, gender_row, race_row, ethnicity_row])
        return obs_rows

    @mock.patch('resources.CDM_TABLES', [
        common.PERSON, common.OBSERVATION, common.LOCATION, common.CARE_SITE,
        common.VISIT_OCCURRENCE
    ])
    @mock.patch('cdm.tables_to_map')
    def test_ehr_person_to_observation(self, mock_tables_map):
        # ehr person table converts to observation records
        self._load_datasets()
        mock_tables_map.return_value = [
            common.OBSERVATION, common.LOCATION, common.CARE_SITE,
            common.VISIT_OCCURRENCE
        ]

        # perform ehr union
        ehr_union.main(self.input_dataset_id, self.output_dataset_id,
                       self.project_id, self.hpo_ids)

        person_query = '''
            SELECT
                p.person_id,
                gender_concept_id,
                gender_source_value,
                race_concept_id,
                race_source_value,
                CAST(birth_datetime AS STRING) AS birth_datetime,
                ethnicity_concept_id,
                ethnicity_source_value,
                EXTRACT(DATE FROM birth_datetime) AS birth_date
            FROM {output_dataset_id}.unioned_ehr_person p
            JOIN {output_dataset_id}._mapping_person AS mp
                ON mp.person_id = p.person_id
            '''.format(output_dataset_id=self.output_dataset_id)
        person_response = bq_utils.query(person_query)
        person_rows = bq_utils.response2rows(person_response)

        # construct dicts of expected values
        expected = []
        for person_row in person_rows:
            expected.extend(self.convert_ehr_person_to_observation(person_row))

        # query for observation table records
        query = '''
            SELECT person_id,
                    observation_concept_id,
                    value_as_concept_id,
                    value_as_string,
                    observation_source_value,
                    observation_date
            FROM {output_dataset_id}.unioned_ehr_observation AS obs
            WHERE obs.observation_concept_id IN ({gender_concept_id},{race_concept_id},{dob_concept_id},
            {ethnicity_concept_id})
            '''

        obs_query = query.format(
            output_dataset_id=self.output_dataset_id,
            gender_concept_id=eu_constants.GENDER_CONCEPT_ID,
            race_concept_id=eu_constants.RACE_CONCEPT_ID,
            dob_concept_id=eu_constants.DOB_CONCEPT_ID,
            ethnicity_concept_id=eu_constants.ETHNICITY_CONCEPT_ID)
        obs_response = bq_utils.query(obs_query)
        obs_rows = bq_utils.response2rows(obs_response)
        actual = obs_rows

        self.assertCountEqual(expected, actual)

    @mock.patch('resources.CDM_TABLES', [
        common.PERSON, common.OBSERVATION, common.LOCATION, common.CARE_SITE,
        common.VISIT_OCCURRENCE
    ])
    @mock.patch('cdm.tables_to_map')
    def test_ehr_person_to_observation_counts(self, mock_tables_map):
        self._load_datasets()
        mock_tables_map.return_value = [
            common.OBSERVATION, common.LOCATION, common.CARE_SITE,
            common.VISIT_OCCURRENCE
        ]

        # perform ehr union
        ehr_union.main(self.input_dataset_id, self.output_dataset_id,
                       self.project_id, self.hpo_ids)

        q_person = '''
                    SELECT p.*
                    FROM {output_dataset_id}.unioned_ehr_person AS p
                    JOIN {output_dataset_id}._mapping_person AS mp
                        ON mp.person_id = p.person_id
                    '''.format(output_dataset_id=self.output_dataset_id)
        person_response = bq_utils.query(q_person)
        person_rows = bq_utils.response2rows(person_response)
        q_observation = '''
                    SELECT *
                    FROM {output_dataset_id}.unioned_ehr_observation
                    WHERE observation_type_concept_id = 38000280
                    '''.format(output_dataset_id=self.output_dataset_id)
        # observation should contain 4 records of type EHR per person per hpo
        expected = len(person_rows) * 4
        observation_response = bq_utils.query(q_observation)
        observation_rows = bq_utils.response2rows(observation_response)
        actual = len(observation_rows)
        self.assertEqual(
            actual, expected,
            'Expected %s EHR person records in observation but found %s' %
            (expected, actual))

    def _test_table_hpo_subquery(self):
        # person is a simple select, no ids should be mapped
        person = ehr_union.table_hpo_subquery('person',
                                              hpo_id=NYC_HPO_ID,
                                              input_dataset_id='input',
                                              output_dataset_id='output')

        # _mapping_visit_occurrence(src_table_id, src_visit_occurrence_id, visit_occurrence_id)
        # visit_occurrence_id should be mapped
        visit_occurrence = ehr_union.table_hpo_subquery(
            'visit_occurrence',
            hpo_id=NYC_HPO_ID,
            input_dataset_id='input',
            output_dataset_id='output')

        # visit_occurrence_id and condition_occurrence_id should be mapped
        condition_occurrence = ehr_union.table_hpo_subquery(
            'condition_occurrence',
            hpo_id=NYC_HPO_ID,
            input_dataset_id='input',
            output_dataset_id='output')

    def get_table_hpo_subquery_error(self, table, dataset_in, dataset_out):
        subquery = ehr_union.table_hpo_subquery(table, NYC_HPO_ID, dataset_in,
                                                dataset_out)

        # moz-sql-parser doesn't support the ROW_NUMBER() OVER() a analytical function of sql we are removing
        # that statement from the returned query for the parser be able to parse out the query without erroring out.

        subquery = re.sub(
            r",\s+ROW_NUMBER\(\) OVER \(PARTITION BY nm\..+?_id\) AS row_num",
            " ", subquery)
        stmt = moz_sql_parser.parse(subquery)

        # Sanity check it is a select statement
        if 'select' not in stmt:
            return SUBQUERY_FAIL_MSG.format(expr='query type',
                                            table=table,
                                            expected='select',
                                            actual=str(stmt),
                                            subquery=subquery)

        # Input table should be first in FROM expression
        actual_from = first_or_none(
            dpath.util.values(stmt, 'from/0/value/from/value') or
            dpath.util.values(stmt, 'from'))
        expected_from = dataset_in + '.' + bq_utils.get_table_id(
            NYC_HPO_ID, table)
        if expected_from != actual_from:
            return SUBQUERY_FAIL_MSG.format(expr='first object in FROM',
                                            table=table,
                                            expected=expected_from,
                                            actual=actual_from,
                                            subquery=subquery)

        # Ensure all key fields (primary or foreign) yield joins with their associated mapping tables
        # Note: ordering of joins in the subquery is assumed to be consistent with field order in the json file
        fields = resources.fields_for(table)
        id_field = table + '_id'
        key_ind = 0
        expected_join = None
        actual_join = None
        for field in fields:
            if field['name'] in self.mapped_fields:
                # key_ind += 1  # TODO use this increment when we generalize solution for all foreign keys
                if field['name'] == id_field:
                    # Primary key, mapping table associated with this one should be INNER joined
                    key_ind += 1
                    expr = 'inner join on primary key'
                    actual_join = first_or_none(
                        dpath.util.values(stmt, 'from/%s/join/value' % key_ind))
                    expected_join = dataset_out + '.' + ehr_union.mapping_table_for(
                        table)
                elif field['name'] in self.implemented_foreign_keys:
                    # Foreign key, mapping table associated with the referenced table should be LEFT joined
                    key_ind += 1
                    expr = 'left join on foreign key'
                    actual_join = first_or_none(
                        dpath.util.values(stmt,
                                          'from/%s/left join/value' % key_ind))
                    joined_table = field['name'].replace('_id', '')
                    expected_join = dataset_out + '.' + ehr_union.mapping_table_for(
                        joined_table)
                if expected_join != actual_join:
                    return SUBQUERY_FAIL_MSG.format(expr=expr,
                                                    table=table,
                                                    expected=expected_join,
                                                    actual=actual_join,
                                                    subquery=subquery)

    def test_hpo_subquery(self):
        input_dataset_id = 'input'
        output_dataset_id = 'output'
        subquery_fails = []

        # Key fields should be populated using associated mapping tables
        for table in resources.CDM_TABLES:
            # This condition is to exempt person table from table hpo sub query
            if table != common.PERSON:
                subquery_fail = self.get_table_hpo_subquery_error(
                    table, input_dataset_id, output_dataset_id)
                if subquery_fail is not None:
                    subquery_fails.append(subquery_fail)

        if len(subquery_fails) > 0:
            self.fail('\n\n'.join(subquery_fails))

    def tearDown(self):
        self._empty_hpo_buckets()
        test_util.delete_all_tables(self.input_dataset_id)
        test_util.delete_all_tables(self.output_dataset_id)
