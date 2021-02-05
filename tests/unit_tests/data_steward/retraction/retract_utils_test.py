import re
import unittest

import mock
import pandas as pd
from google.cloud.bigquery import DatasetReference as data_ref

import common
from retraction import retract_utils as ru
from constants.retraction import retract_utils as ru_consts
from constants.utils import bq as bq_consts


class RetractUtilsTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.project_id = 'project_id'
        self.dataset_id = 'dataset_id'
        self.ehr_dataset_id = 'ehr_dataset_id'
        self.pid_table_str = 'pid_project_id.sandbox_dataset_id.pid_table_id'
        self.hpo_id = 'fake'
        self.TABLE_REGEX = re.compile(
            r'`' + self.project_id + r'\.' + self.dataset_id + r'\.(.*)`',
            re.MULTILINE | re.IGNORECASE)

        # common tables across datasets
        self.mapped_cdm_tables = [
            common.OBSERVATION, common.VISIT_OCCURRENCE, common.MEASUREMENT,
            common.OBSERVATION_PERIOD, common.CONDITION_OCCURRENCE
        ]
        self.unmapped_cdm_tables = [common.PERSON, common.DEATH]
        self.mapping_tables = [
            common.MAPPING_PREFIX + table for table in self.mapped_cdm_tables
        ]
        self.ext_tables = [
            table + common.EXT_SUFFIX for table in self.mapped_cdm_tables
        ]
        self.cdm_tables = self.mapped_cdm_tables + self.unmapped_cdm_tables
        self.other_tables = [
            common.MAPPING_PREFIX + 'drug_source',
            'site' + common.MAPPING_PREFIX
        ]

        self.list_of_dicts = [{
            bq_consts.TABLE_NAME: table,
            bq_consts.COLUMN_NAME: ru_consts.PERSON_ID
        } for table in self.cdm_tables] + [{
            bq_consts.TABLE_NAME: table,
            bq_consts.COLUMN_NAME: ru_consts.TABLE_ID
        } for table in self.mapping_tables + self.ext_tables] + [{
            bq_consts.TABLE_NAME: table,
            bq_consts.COLUMN_NAME: ru_consts.TABLE_ID
        } for table in self.other_tables]

        self.table_df = pd.DataFrame(self.list_of_dicts)

        self.pids_list = [1, 2, 3, 4]

        # ehr dataset tables
        self.participant_tables = [
            common.PII_NAME, common.PARTICIPANT_MATCH, common.PII_ADDRESS
        ]
        self.unioned_ehr_tables = [
            common.UNIONED_EHR + '_' + table for table in self.cdm_tables
        ]
        self.hpo_tables = [
            self.hpo_id + '_' + table
            for table in self.cdm_tables + self.participant_tables
        ]
        self.ehr_tables = self.hpo_tables + self.unioned_ehr_tables

        self.ehr_list_of_dicts = [{
            bq_consts.TABLE_NAME: table,
            bq_consts.COLUMN_NAME: ru_consts.PERSON_ID
        } for table in self.ehr_tables] + [{
            bq_consts.TABLE_NAME: table,
            bq_consts.COLUMN_NAME: ru_consts.TABLE_ID
        } for table in self.mapping_tables]
        self.ehr_table_df = pd.DataFrame(self.ehr_list_of_dicts)

    def test_get_pid_sql_expr(self):
        expected = '({pids})'.format(
            pids=', '.join([str(pid) for pid in self.pids_list]))
        actual = ru.get_pid_sql_expr(self.pids_list)
        self.assertEqual(expected, actual)

        self.assertRaises(ValueError, ru.get_pid_sql_expr, self.project_id)
        self.assertRaises(ValueError, ru.get_pid_sql_expr,
                          self.project_id + '.' + self.dataset_id)
        self.assertRaises(ValueError, ru.get_pid_sql_expr,
                          self.project_id + '.' + self.pid_table_str)

    def test_get_cdm_table(self):
        expected = common.AOU_REQUIRED
        mapping_tables = [
            common.MAPPING_PREFIX + table for table in common.AOU_REQUIRED
        ]
        ext_tables = [
            table + common.EXT_SUFFIX for table in common.AOU_REQUIRED
        ]
        for table in mapping_tables:
            cdm_table = ru.get_cdm_table(table)
            self.assertIn(cdm_table, expected)
        for table in ext_tables:
            cdm_table = ru.get_cdm_table(table)
            self.assertIn(cdm_table, expected)

    def test_get_cdm_and_mapping_tables(self):
        expected = dict((table, common.MAPPING_PREFIX + table)
                        for table in self.mapped_cdm_tables)
        actual = ru.get_cdm_and_mapping_tables(self.mapping_tables,
                                               self.mapped_cdm_tables)
        self.assertEqual(expected, actual)

        expected = dict((table, table + common.EXT_SUFFIX)
                        for table in self.mapped_cdm_tables)
        actual = ru.get_cdm_and_mapping_tables(self.ext_tables,
                                               self.mapped_cdm_tables)
        self.assertEqual(expected, actual)

    def test_get_tables(self):
        self.assertEqual(
            ru.get_tables(self.table_df), self.cdm_tables +
            self.mapping_tables + self.ext_tables + self.other_tables)
        self.assertEqual(ru.get_tables(self.ehr_table_df),
                         self.ehr_tables + self.mapping_tables)

    def test_get_pid_tables(self):
        self.assertEqual(ru.get_pid_tables(self.table_df), self.cdm_tables)
        self.assertEqual(ru.get_pid_tables(self.ehr_table_df), self.ehr_tables)

    def test_get_mapping_type(self):
        self.assertEqual(ru.get_mapping_type(self.cdm_tables), common.MAPPING)
        self.assertEqual(
            ru.get_mapping_type(self.cdm_tables + self.mapping_tables),
            common.MAPPING)
        self.assertEqual(ru.get_mapping_type(self.cdm_tables + self.ext_tables),
                         common.EXT)
        self.assertEqual(
            ru.get_mapping_type(self.cdm_tables + self.mapping_tables +
                                self.ext_tables), common.MAPPING)
        self.assertEqual(
            ru.get_mapping_type(self.cdm_tables + self.ext_tables +
                                self.other_tables), common.EXT)
        self.assertEqual(ru.get_mapping_type([]), common.MAPPING)

    def test_get_mapping_tables(self):
        self.assertEqual(ru.get_mapping_tables(common.MAPPING, self.cdm_tables),
                         [])
        self.assertEqual(
            ru.get_mapping_tables(common.MAPPING,
                                  self.cdm_tables + self.mapping_tables),
            self.mapping_tables)
        self.assertEqual(
            ru.get_mapping_tables(common.EXT,
                                  self.cdm_tables + self.ext_tables),
            self.ext_tables)
        self.assertEqual(
            ru.get_mapping_tables(common.MAPPING,
                                  self.mapping_tables + self.ext_tables),
            self.mapping_tables)
        self.assertEqual(
            ru.get_mapping_tables(common.MAPPING,
                                  self.mapping_tables + self.other_tables),
            self.mapping_tables + self.other_tables)
        self.assertEqual(
            ru.get_mapping_tables(common.EXT,
                                  self.mapping_tables + self.other_tables), [])

    @mock.patch('utils.bq.list_datasets')
    def test_get_datasets_list(self, mock_all_datasets):
        #pre-conditions
        removed_datasets = [
            data_ref('foo', 'vocabulary20201010'),
            data_ref('foo', 'R2019q4r1_deid_sandbox')
        ]
        expected_datasets = [
            data_ref('foo', '2021q1r1_rdr'),
            data_ref('foo', 'C2020q1r1_deid'),
            data_ref('foo', 'R2019q4r1_deid'),
            data_ref('foo', '2018q4r1_rdr')
        ]
        expected_list = [dataset.dataset_id for dataset in expected_datasets]
        mock_all_datasets.return_value = removed_datasets + expected_datasets

        # test all_datasets flag
        ds_list = ru.get_datasets_list('foo', ['all_datasets'])

        # post conditions
        self.assertCountEqual(expected_list, ds_list)

        # test specific dataset
        ds_list = ru.get_datasets_list('foo', ['C2020q1r1_deid'])

        # post conditions
        self.assertEqual(['C2020q1r1_deid'], ds_list)

        # test None dataset
        ds_list = ru.get_datasets_list('foo', None)

        # post conditions
        self.assertEqual([], ds_list)

        # test empty list dataset
        ds_list = ru.get_datasets_list('foo', [])

        # post conditions
        self.assertEqual([], ds_list)

    def test_is_combined_dataset(self):
        self.assertTrue(ru.is_combined_dataset('combined20190801'))
        self.assertFalse(ru.is_combined_dataset('combined20190801_deid'))
        self.assertTrue(ru.is_combined_dataset('combined20190801_base'))
        self.assertTrue(ru.is_combined_dataset('combined20190801_clean'))
        self.assertFalse(ru.is_combined_dataset('combined20190801_deid_v1'))

    def test_is_deid_dataset(self):
        self.assertFalse(ru.is_deid_dataset('combined20190801'))
        self.assertTrue(ru.is_deid_dataset('combined20190801_deid'))
        self.assertFalse(ru.is_deid_dataset('combined20190801_base'))
        self.assertFalse(ru.is_deid_dataset('combined20190801_clean'))
        self.assertTrue(ru.is_deid_dataset('combined20190801_deid_v1'))

    @mock.patch('retraction.retract_utils.os.environ.get')
    def test_is_ehr_dataset(self, mock_get_dataset_id):
        # pre-conditions
        dataset_id = 'fake_dataset_id'
        mock_get_dataset_id.return_value = dataset_id

        # tests
        self.assertTrue(ru.is_ehr_dataset('ehr20190801'))
        self.assertTrue(ru.is_ehr_dataset('ehr_20190801'))
        self.assertFalse(ru.is_ehr_dataset('unioned_ehr_20190801_base'))
        self.assertFalse(ru.is_ehr_dataset('unioned_ehr20190801_clean'))
        self.assertTrue(ru.is_ehr_dataset(dataset_id))

    def test_is_unioned_dataset(self):
        self.assertFalse(ru.is_unioned_dataset('ehr20190801'))
        self.assertFalse(ru.is_unioned_dataset('ehr_20190801'))
        self.assertTrue(ru.is_unioned_dataset('unioned_ehr_20190801_base'))
        self.assertTrue(ru.is_unioned_dataset('unioned_ehr20190801_clean'))

    def test_get_dataset_type(self):
        self.assertEqual(ru.get_dataset_type('unioned_ehr_4023498'),
                         common.UNIONED_EHR)
        self.assertNotEqual(ru.get_dataset_type('unioned_ehr_4023498'),
                            common.COMBINED)
        self.assertNotEqual(ru.get_dataset_type('unioned_ehr_4023498'),
                            common.DEID)
        self.assertNotEqual(ru.get_dataset_type('unioned_ehr_4023498'),
                            common.EHR)
        self.assertNotEqual(ru.get_dataset_type('unioned_ehr_4023498'),
                            common.OTHER)

        self.assertEqual(ru.get_dataset_type('5349850'), common.OTHER)
        self.assertNotEqual(ru.get_dataset_type('5349850'), common.COMBINED)
        self.assertNotEqual(ru.get_dataset_type('5349850'), common.DEID)
        self.assertNotEqual(ru.get_dataset_type('5349850'), common.EHR)
        self.assertNotEqual(ru.get_dataset_type('5349850'), common.UNIONED_EHR)

        self.assertEqual(ru.get_dataset_type('combined_deid_53521'),
                         common.DEID)
        self.assertNotEqual(ru.get_dataset_type('combined_deid_53521'),
                            common.COMBINED)
        self.assertNotEqual(ru.get_dataset_type('combined_deid_53521'),
                            common.UNIONED_EHR)
        self.assertNotEqual(ru.get_dataset_type('combined_deid_53521'),
                            common.EHR)
        self.assertNotEqual(ru.get_dataset_type('combined_deid_53521'),
                            common.OTHER)

        self.assertEqual(ru.get_dataset_type('combined_dbrowser_562'),
                         common.COMBINED)
        self.assertNotEqual(ru.get_dataset_type('combined_dbrowser_562'),
                            common.DEID)
        self.assertNotEqual(ru.get_dataset_type('combined_dbrowser_562'),
                            common.UNIONED_EHR)
        self.assertNotEqual(ru.get_dataset_type('combined_dbrowser_562'),
                            common.EHR)
        self.assertNotEqual(ru.get_dataset_type('combined_dbrowser_562'),
                            common.OTHER)

        self.assertEqual(ru.get_dataset_type('ehr_43269'), common.EHR)
        self.assertNotEqual(ru.get_dataset_type('ehr_43269'), common.DEID)
        self.assertNotEqual(ru.get_dataset_type('ehr_43269'),
                            common.UNIONED_EHR)
        self.assertNotEqual(ru.get_dataset_type('ehr_43269'), common.COMBINED)
        self.assertNotEqual(ru.get_dataset_type('ehr_43269'), common.OTHER)

    @mock.patch('utils.bq.list_datasets')
    def test_get_dataset_ids_to_target(self, mock_datasets_list):
        dataset_id_1 = 'dataset_id_1'
        dataset_id_2 = 'dataset_id_2'
        dataset_1 = mock.Mock(spec=['dataset_1'], dataset_id=dataset_id_1)
        dataset_2 = mock.Mock(spec=['dataset_2'], dataset_id=dataset_id_2)
        mock_datasets_list.return_value = [dataset_1, dataset_2]

        expected = [dataset_id_1, dataset_id_2]
        actual = ru.get_dataset_ids_to_target(self.project_id)
        self.assertListEqual(expected, actual)

        dataset_ids = [dataset_id_1]
        expected = [dataset_id_1]
        actual = ru.get_dataset_ids_to_target(self.project_id, dataset_ids)
        self.assertListEqual(expected, actual)

        # a dataset which is not found is skipped
        dataset_ids = [dataset_id_1, dataset_id_2, 'missing_dataset']
        expected = [dataset_id_1, dataset_id_2]
        actual = ru.get_dataset_ids_to_target(self.project_id, dataset_ids)
        self.assertListEqual(expected, actual)

    def test_check_dataset_ids_for_sentinel(self):
        dataset_id_1 = 'dataset_id_1'
        dataset_id_2 = 'dataset_id_2'

        dataset_ids = [dataset_id_1, dataset_id_2]
        expected = dataset_ids
        actual = ru.check_dataset_ids_for_sentinel(dataset_ids)
        self.assertListEqual(expected, actual)

        dataset_ids = [ru_consts.ALL_DATASETS]
        actual = ru.check_dataset_ids_for_sentinel(dataset_ids)
        self.assertIsNone(actual)

        dataset_ids = [ru_consts.ALL_DATASETS, dataset_id_1]
        self.assertRaises(ValueError, ru.check_dataset_ids_for_sentinel,
                          dataset_ids)

    def test_fetch_args(self):
        parser = ru.fetch_parser()

        expected = self.pids_list
        args = parser.parse_args([
            '-p', self.project_id, '-d', self.dataset_id, self.ehr_dataset_id,
            '-o', self.hpo_id, '--pid_list', '1', '2', '3', '4'
        ])
        actual = args.pid_source
        self.assertEqual(expected, actual)
        expected_datasets = [self.dataset_id, self.ehr_dataset_id]
        actual_datasets = args.dataset_ids
        self.assertEqual(expected_datasets, actual_datasets)

        expected = self.pid_table_str
        args = parser.parse_args([
            '-p', self.project_id, '-d', ru_consts.ALL_DATASETS, '-o',
            self.hpo_id, '--pid_table', self.pid_table_str
        ])
        actual = args.pid_source
        self.assertEqual(expected, actual)

        expected = self.pid_table_str
        args = parser.parse_args([
            '-p', self.project_id, '-d', ru_consts.ALL_DATASETS, '-o',
            self.hpo_id, '--pid_table', self.pid_table_str
        ])
        actual = args.pid_source
        self.assertEqual(expected, actual)

        args_list = [
            '-p', self.project_id, '-d', ru_consts.ALL_DATASETS, '-o',
            self.hpo_id, '--pid_table', self.pid_table_str, '--pid_list', '1',
            '2', '3', '4'
        ]
        self.assertRaises((ru.argparse.ArgumentError, SystemExit),
                          parser.parse_args, args_list)

    @mock.patch('retraction.retract_utils.is_labeled_deid')
    def test_is_deid_label_or_id(self, mock_labeled_deid):
        client = mock.MagicMock()
        mock_labeled_deid.return_value = True
        actual = ru.is_deid_label_or_id(client, self.project_id,
                                        self.dataset_id)
        self.assertTrue(actual)

        mock_labeled_deid.return_value = True
        actual = ru.is_deid_label_or_id(client, self.project_id,
                                        self.dataset_id)
        self.assertTrue(actual)

        mock_labeled_deid.return_value = None
        dataset_id = 'deid_dataset'
        actual = ru.is_deid_label_or_id(client, self.project_id, dataset_id)
        self.assertTrue(actual)

        mock_labeled_deid.return_value = None
        dataset_id = 'dataset'
        actual = ru.is_deid_label_or_id(client, self.project_id, dataset_id)
        self.assertFalse(actual)
