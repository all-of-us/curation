# Python imports
import os
import re
import unittest
import mock

# Third party imports
import dpath
import moz_sql_parser

# Project imports
import app_identity
import bq_utils
import cdm
import common
from constants.validation import ehr_union as eu_constants
from gcloud.bq import BigQueryClient
from gcloud.gcs import StorageClient
import resources
import tests.test_util as test_util
from tests.test_util import FAKE_HPO_ID, NYC_HPO_ID, PITT_HPO_ID
from validation import ehr_union

EXCLUDED_HPO_ID = FAKE_HPO_ID
SUBQUERY_FAIL_MSG = '''
Test {expr} in {table} subquery
 Expected: {expected}
 Actual: {actual}

{subquery}
'''


def first_or_none(l):
    return next(iter(l or []), None)


class EhrUnionTest(unittest.TestCase):
    dataset_id = bq_utils.get_dataset_id()
    project_id = app_identity.get_application_id()
    bq_client = BigQueryClient(project_id)

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')
        test_util.setup_hpo_id_bucket_name_table(cls.bq_client, cls.dataset_id)

    def setUp(self):
        self.hpo_ids = [PITT_HPO_ID, NYC_HPO_ID, EXCLUDED_HPO_ID]
        self.input_dataset_id = bq_utils.get_dataset_id()
        self.output_dataset_id = bq_utils.get_unioned_dataset_id()
        self.storage_client = StorageClient(self.project_id)
        self.tearDown()

        # TODO Generalize to work for all foreign key references
        # Collect all primary key fields in CDM tables
        mapped_fields = []
        for table in cdm.tables_to_map():
            field = f'{table}_id'
            mapped_fields.append(field)
        self.mapped_fields = mapped_fields + [
            eu_constants.PRECEDING_VISIT_DETAIL_ID,
            eu_constants.PRECEDING_VISIT_OCCURRENCE_ID,
            eu_constants.VISIT_DETAIL_PARENT_ID
        ]
        self.implemented_foreign_keys = [
            eu_constants.VISIT_OCCURRENCE_ID, eu_constants.VISIT_DETAIL_ID,
            eu_constants.CARE_SITE_ID, eu_constants.LOCATION_ID,
            eu_constants.PRECEDING_VISIT_DETAIL_ID,
            eu_constants.PRECEDING_VISIT_OCCURRENCE_ID,
            eu_constants.VISIT_DETAIL_PARENT_ID
        ]
        self.self_reference_keys = {
            eu_constants.PRECEDING_VISIT_DETAIL_ID: 'visit_detail',
            eu_constants.VISIT_DETAIL_PARENT_ID: 'visit_detail',
            eu_constants.PRECEDING_VISIT_OCCURRENCE_ID: 'visit_occurrence'
        }

        self.ehr_cutoff_date = '2022-01-05'

    @mock.patch("gcloud.gcs.LOOKUP_TABLES_DATASET_ID", dataset_id)
    def _empty_hpo_buckets(self):

        for hpo_id in self.hpo_ids:
            bucket = self.storage_client.get_hpo_bucket(hpo_id)
            self.storage_client.empty_bucket(bucket)

    def _create_hpo_table(self, hpo_id, table, dataset_id):
        table_id = resources.get_table_id(table, hpo_id=hpo_id)
        bq_utils.create_table(table_id,
                              resources.fields_for(table),
                              dataset_id=dataset_id)
        return table_id

    @mock.patch("gcloud.gcs.LOOKUP_TABLES_DATASET_ID", dataset_id)
    def _load_datasets(self):
        """
        Load five persons data for nyc and pitt test hpo and rdr data for the excluded_hpo
        # expected_tables is for testing output
        # it maps table name to list of expected records ex: "unioned_ehr_visit_occurrence" -> [{}, {}, ...]
        """
        expected_tables: dict = {}
        running_jobs: list = []
        for cdm_table in resources.CDM_TABLES:
            output_table: str = ehr_union.output_table_for(cdm_table)
            expected_tables[output_table] = []
            for hpo_id in self.hpo_ids:
                # upload csv into hpo bucket
                cdm_filename: str = f'{cdm_table}.csv'
                cdm_filepath = self._get_cdm_filepath(cdm_table, hpo_id)

                hpo_bucket = self.storage_client.get_hpo_bucket(hpo_id)
                if os.path.exists(cdm_filepath):

                    csv_rows = resources.csv_to_list(cdm_filepath)
                    cdm_blob = hpo_bucket.blob(cdm_filename)
                    cdm_blob.upload_from_filename(cdm_filepath)

                else:
                    # results in empty table
                    cdm_blob = hpo_bucket.blob(cdm_filename)
                    cdm_blob.upload_from_string('dummy\n')
                    csv_rows: list = []

                # load table from csv
                result = bq_utils.load_cdm_csv(hpo_id, cdm_table)
                running_jobs.append(result['jobReference']['jobId'])

                if hpo_id != EXCLUDED_HPO_ID and cdm_table != common.VISIT_DETAIL:
                    expected_tables[output_table] += list(csv_rows)
                elif hpo_id != EXCLUDED_HPO_ID and cdm_table == common.VISIT_DETAIL:
                    # All rows are included in the mapping table for visit_detail,
                    # but the rows with invalid visit_occurrence_id are excluded
                    # from the unioned visit_detail table
                    mapping_table: str = ehr_union.mapping_table_for(cdm_table)
                    if mapping_table not in expected_tables:
                        expected_tables[mapping_table] = []

                    expected_tables[mapping_table] += list(csv_rows)
                    visit_occurrence_ids = self._get_valid_visit_occurrence_ids(
                        hpo_id)
                    csv_rows = [
                        row for row in csv_rows
                        if row['visit_occurrence_id'] in visit_occurrence_ids
                    ]
                    expected_tables[output_table] += list(csv_rows)

        # ensure person to observation output is as expected
        output_table_person: str = ehr_union.output_table_for(common.PERSON)
        output_table_observation: str = ehr_union.output_table_for(
            common.OBSERVATION)
        expected_tables[output_table_observation] += 4 * expected_tables[
            output_table_person]

        incomplete_jobs: list = bq_utils.wait_on_jobs(running_jobs)
        if len(incomplete_jobs) > 0:
            message: str = "Job id(s) %s failed to complete" % incomplete_jobs
            raise RuntimeError(message)

        self.expected_tables = expected_tables

    def _get_cdm_filepath(self, cdm_table, hpo_id) -> str:
        """
        Get the path of the specified CDM table's data file for the HPO.

        :param cdm_table: name of the CDM table (e.g. 'person', 'visit_occurrence', 'death')
        :param hpo_ids: identifies which HPOs to include in union
        :return: str. Path of the data file, including the file name.
        """
        cdm_filename: str = f'{cdm_table}.csv'
        cdm_filepath: str = ''

        if hpo_id == NYC_HPO_ID:
            cdm_filepath = os.path.join(test_util.FIVE_PERSONS_PATH,
                                        cdm_filename)
        elif hpo_id == PITT_HPO_ID:
            cdm_filepath = os.path.join(test_util.PITT_FIVE_PERSONS_PATH,
                                        cdm_filename)
        elif hpo_id == EXCLUDED_HPO_ID and cdm_table in [
                'observation', 'person', 'visit_occurrence'
        ]:
            cdm_filepath = os.path.join(test_util.RDR_PATH, cdm_filename)

        return cdm_filepath

    def _get_valid_visit_occurrence_ids(self, hpo_id) -> list:
        """
        Get the list of the valid visit_occurrence_ids for the HPO.
        
        :param hpo_ids: identifies which HPOs to include in union
        :return: list of valid occurrence IDs.
        """
        cdm_filepath = self._get_cdm_filepath(common.VISIT_OCCURRENCE, hpo_id)

        if not os.path.exists(cdm_filepath):
            return []

        csv_rows = resources.csv_to_list(cdm_filepath)
        return [row['visit_occurrence_id'] for row in csv_rows]

    def _table_has_clustering(self, table_obj):
        self.assertIsNotNone(table_obj.clustering_fields)
        self.assertSetEqual(set(table_obj.clustering_fields), {'person_id'})
        self.assertIsNotNone(table_obj.time_partitioning)
        self.assertEqual(table_obj.time_partitioning.type_, 'DAY')

    def _dataset_tables(self, dataset_id):
        """
        Get names of existing tables in specified dataset

        :param dataset_id: identifies the dataset
        :return: list of table_ids
        """
        tables = self.bq_client.list_tables(dataset_id)
        return [table.table_id for table in tables]

    @mock.patch('bq_utils.get_hpo_info')
    def test_union_ehr(self, mock_hpo_info):
        self._load_datasets()
        input_tables_before = set(self._dataset_tables(self.input_dataset_id))

        # output should be mapping tables and cdm tables
        output_tables_before = self._dataset_tables(self.output_dataset_id)
        mapping_tables = [
            ehr_union.mapping_table_for(table)
            for table in cdm.tables_to_map() + [common.PERSON]
            if not table == common.SURVEY_CONDUCT
        ]
        output_cdm_tables = [
            ehr_union.output_table_for(table) for table in resources.CDM_TABLES
        ]
        sandbox_tables = [
            f'{self.output_dataset_id}_dc2340_unioned_ehr_observation'
        ]
        expected_output = set(output_tables_before + mapping_tables +
                              output_cdm_tables + sandbox_tables)

        mock_hpo_info.return_value = [{
            'hpo_id': hpo_id
        } for hpo_id in self.hpo_ids]

        # perform ehr union
        ehr_union.main(self.input_dataset_id, self.output_dataset_id,
                       self.project_id, [EXCLUDED_HPO_ID])

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
            if not table_to_map == common.SURVEY_CONDUCT:
                mapping_table = ehr_union.mapping_table_for(table_to_map)
                expected_fields = {
                    'src_table_id',
                    'src_%s_id' % table_to_map,
                    '%s_id' % table_to_map, 'src_hpo_id', 'src_dataset_id'
                }
                mapping_table_obj = self.bq_client.get_table(
                    f'{self.output_dataset_id}.{mapping_table}')
                actual_fields = set(
                    [field.name for field in mapping_table_obj.schema])
                message = 'Table %s has fields %s when %s expected' % (
                    mapping_table, actual_fields, expected_fields)
                self.assertSetEqual(expected_fields, actual_fields, message)

                if table_to_map == common.VISIT_DETAIL:
                    expected_num_rows = len(self.expected_tables[mapping_table])
                else:
                    result_table = ehr_union.output_table_for(table_to_map)
                    expected_num_rows = len(self.expected_tables[result_table])

                actual_num_rows = int(mapping_table_obj.num_rows)
                message = 'Table %s has %s rows when %s expected' % (
                    mapping_table, actual_num_rows, expected_num_rows)
                self.assertEqual(expected_num_rows, actual_num_rows, message)

        # check for each output table
        for table_name in resources.CDM_TABLES:
            if not table_name == common.SURVEY_CONDUCT:
                # output table exists and row count is sum of those submitted by hpos
                result_table = ehr_union.output_table_for(table_name)
                expected_rows = self.expected_tables[result_table]
                expected_count = len(expected_rows)
                table_obj = self.bq_client.get_table(
                    f'{self.output_dataset_id}.{result_table}')
                actual_count = int(table_obj.num_rows)
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
                    self._table_has_clustering(table_obj)

            actual_output = set(self._dataset_tables(self.output_dataset_id))
            self.assertSetEqual(expected_output, actual_output)

        # explicit check that output person_ids are same as input
        nyc_person_table_id = resources.get_table_id('person',
                                                     hpo_id=NYC_HPO_ID)
        pitt_person_table_id = resources.get_table_id('person',
                                                      hpo_id=PITT_HPO_ID)
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
            'observation_date': self.ehr_cutoff_date,
            'value_as_concept_id': None
        }
        gender_row = {
            'observation_concept_id': eu_constants.GENDER_CONCEPT_ID,
            'observation_source_value': person_row['gender_source_value'],
            'value_as_string': None,
            'person_id': person_row['person_id'],
            'observation_date': self.ehr_cutoff_date,
            'value_as_concept_id': person_row['gender_concept_id']
        }
        race_row = {
            'observation_concept_id': eu_constants.RACE_CONCEPT_ID,
            'observation_source_value': person_row['race_source_value'],
            'value_as_string': None,
            'person_id': person_row['person_id'],
            'observation_date': self.ehr_cutoff_date,
            'value_as_concept_id': person_row['race_concept_id']
        }
        ethnicity_row = {
            'observation_concept_id': eu_constants.ETHNICITY_CONCEPT_ID,
            'observation_source_value': person_row['ethnicity_source_value'],
            'value_as_string': None,
            'person_id': person_row['person_id'],
            'observation_date': self.ehr_cutoff_date,
            'value_as_concept_id': person_row['ethnicity_concept_id']
        }
        obs_rows.extend([dob_row, gender_row, race_row, ethnicity_row])
        return obs_rows

    @mock.patch('bq_utils.get_hpo_info')
    @mock.patch('resources.CDM_TABLES', [
        common.PERSON, common.OBSERVATION, common.LOCATION, common.CARE_SITE,
        common.VISIT_OCCURRENCE, common.VISIT_DETAIL
    ])
    @mock.patch('cdm.tables_to_map')
    def test_ehr_person_to_observation(self, mock_tables_map, mock_hpo_info):
        # ehr person table converts to observation records
        self._load_datasets()
        mock_tables_map.return_value = [
            common.OBSERVATION, common.LOCATION, common.CARE_SITE,
            common.VISIT_OCCURRENCE, common.VISIT_DETAIL
        ]

        mock_hpo_info.return_value = [{
            'hpo_id': hpo_id
        } for hpo_id in self.hpo_ids]

        # perform ehr union
        ehr_union.main(self.input_dataset_id,
                       self.output_dataset_id,
                       self.project_id,
                       ehr_cutoff_date=self.ehr_cutoff_date)

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

    @mock.patch('bq_utils.get_hpo_info')
    @mock.patch('resources.CDM_TABLES', [
        common.PERSON, common.OBSERVATION, common.LOCATION, common.CARE_SITE,
        common.VISIT_OCCURRENCE, common.VISIT_DETAIL
    ])
    @mock.patch('cdm.tables_to_map')
    def test_ehr_person_to_observation_counts(self, mock_tables_map,
                                              mock_hpo_info):
        self._load_datasets()
        mock_tables_map.return_value = [
            common.OBSERVATION, common.LOCATION, common.CARE_SITE,
            common.VISIT_OCCURRENCE, common.VISIT_DETAIL
        ]

        mock_hpo_info.return_value = [{
            'hpo_id': hpo_id
        } for hpo_id in self.hpo_ids]

        # perform ehr union
        ehr_union.main(self.input_dataset_id, self.output_dataset_id,
                       self.project_id)

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
        # offset is being used as a column-name in note_nlp table.
        # Although, BigQuery does not throw any errors for this, moz_sql_parser indentifies as a SQL Keyword.
        # So, change required only in Test Script as a workaround.
        if 'offset,' in subquery:
            subquery = subquery.replace('offset,', '"offset",')
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
        expected_from = f'{dataset_in}.{resources.get_table_id(table, hpo_id=NYC_HPO_ID)}'
        if expected_from != actual_from:
            return SUBQUERY_FAIL_MSG.format(expr='first object in FROM',
                                            table=table,
                                            expected=expected_from,
                                            actual=actual_from,
                                            subquery=subquery)

        # Ensure all key fields (primary or foreign) yield joins with their associated mapping tables
        # Note: ordering of joins in the subquery is assumed to be consistent with field order in the json file
        fields = resources.fields_for(table)
        id_field = f'{table}_id'
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
                    # Visit_detail table has 'visit_occurrence' column after 'care_site', which is different from
                    # other cdm tables, where 'visit_occurrence' comes before other foreign_keys.
                    # The test expects the same order as other cmd tables, so the expected-query has
                    # 'visit_occurrence' before 'care_site'. The following reorder is required to match the sequence
                    # to the actual-query.
                    if table == 'visit_detail' and key_ind == 2:
                        stmt['from'][2], stmt['from'][3], stmt['from'][4], stmt[
                            'from'][5] = stmt['from'][3], stmt['from'][4], stmt[
                                'from'][5], stmt['from'][2]
                    actual_join = first_or_none(
                        dpath.util.values(stmt,
                                          'from/%s/left join/value' % key_ind))
                    joined_table = field['name'].replace('_id', '')

                    if field['name'] in self.self_reference_keys:
                        expected_join = f'{dataset_out}.{ehr_union.mapping_table_for(self.self_reference_keys[field["name"]])}'
                    else:
                        expected_join = f'{dataset_out}.{ehr_union.mapping_table_for(joined_table)}'

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
        test_util.delete_all_tables(self.bq_client, self.input_dataset_id)
        test_util.delete_all_tables(self.bq_client, self.output_dataset_id)

    @classmethod
    def tearDownClass(cls):
        test_util.drop_hpo_id_bucket_name_table(cls.bq_client, cls.dataset_id)
