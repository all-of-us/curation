"""
Unit test components of data_steward.validation.main
"""
from __future__ import print_function
import datetime
import os
import unittest
from time import sleep

import mock
from bs4 import BeautifulSoup as bs

import bq_utils
import app_identity
import common
from constants import bq_utils as bq_consts
from constants.validation import main as main_consts
import gcs_utils
from gcloud.gcs import StorageClient
import resources
from tests import test_util
from validation import main
from validation.metrics import required_labs


class ValidationMainTest(unittest.TestCase):

    dataset_id = bq_utils.get_dataset_id()

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')
        test_util.setup_hpo_id_bucket_name_table(cls.dataset_id)

    @mock.patch("gcs_utils.LOOKUP_TABLES_DATASET_ID", dataset_id)
    @mock.patch("gcloud.gcs.LOOKUP_TABLES_DATASET_ID", dataset_id)
    def setUp(self):
        self.hpo_id = test_util.FAKE_HPO_ID
        self.hpo_bucket = gcs_utils.get_hpo_bucket(self.hpo_id)
        self.project_id = app_identity.get_application_id()
        self.rdr_dataset_id = bq_utils.get_rdr_dataset_id()
        mock_get_hpo_name = mock.patch('validation.main.get_hpo_name')

        self.mock_get_hpo_name = mock_get_hpo_name.start()
        self.mock_get_hpo_name.return_value = 'Fake HPO'
        self.addCleanup(mock_get_hpo_name.stop)

        self.folder_prefix = '2019-01-01-v1/'

        self.storage_client = StorageClient(self.project_id)
        self.storage_bucket = self.storage_client.get_bucket(self.hpo_bucket)
        self.storage_client.empty_bucket(self.hpo_bucket)

        test_util.delete_all_tables(self.dataset_id)
        self._create_drug_class_table(self.dataset_id)

    @staticmethod
    def _create_drug_class_table(bigquery_dataset_id):

        table_name = 'drug_class'
        fields = [{
            "type": "integer",
            "name": "concept_id",
            "mode": "required"
        }, {
            "type": "string",
            "name": "concept_name",
            "mode": "required"
        }, {
            "type": "string",
            "name": "drug_class_name",
            "mode": "required"
        }]
        bq_utils.create_table(table_id=table_name,
                              fields=fields,
                              drop_existing=True,
                              dataset_id=bigquery_dataset_id)

        bq_utils.query(q=main_consts.DRUG_CLASS_QUERY.format(
            dataset_id=bigquery_dataset_id),
                       use_legacy_sql=False,
                       destination_table_id='drug_class',
                       retry_count=bq_consts.BQ_DEFAULT_RETRY_COUNT,
                       write_disposition='WRITE_TRUNCATE',
                       destination_dataset_id=bigquery_dataset_id)

        # ensure concept ancestor table exists
        if not bq_utils.table_exists(common.CONCEPT_ANCESTOR):
            bq_utils.create_standard_table(common.CONCEPT_ANCESTOR,
                                           common.CONCEPT_ANCESTOR)
            q = """INSERT INTO {dataset}.concept_ancestor
            SELECT * FROM {vocab}.concept_ancestor""".format(
                dataset=bigquery_dataset_id, vocab=common.VOCABULARY_DATASET)
            bq_utils.query(q)

    def table_has_clustering(self, table_info):
        clustering = table_info.get('clustering')
        self.assertIsNotNone(clustering)
        fields = clustering.get('fields')
        self.assertSetEqual(set(fields), {'person_id'})
        time_partitioning = table_info.get('timePartitioning')
        self.assertIsNotNone(time_partitioning)
        tpe = time_partitioning.get('type')
        self.assertEqual(tpe, 'DAY')

    @mock.patch("gcs_utils.LOOKUP_TABLES_DATASET_ID", dataset_id)
    @mock.patch("gcloud.gcs.LOOKUP_TABLES_DATASET_ID", dataset_id)
    def test_all_files_unparseable_output(self):
        # TODO possible bug: if no pre-existing table, results in bq table not found error
        for cdm_table in common.SUBMISSION_FILES:
            cdm_blob = self.storage_bucket.blob(
                f'{self.folder_prefix}{cdm_table}')
            cdm_blob.upload_from_string('.\n .')

        bucket_items = gcs_utils.list_bucket(self.hpo_bucket)
        folder_items = main.get_folder_items(bucket_items, self.folder_prefix)
        expected_results = [(f, 1, 0, 0) for f in common.SUBMISSION_FILES]
        r = main.validate_submission(self.hpo_id, self.hpo_bucket, folder_items,
                                     self.folder_prefix)
        self.assertSetEqual(set(expected_results), set(r['results']))

    def test_bad_file_names(self):
        bad_file_names: list = [
            "avisit_occurrence.csv",
            "condition_occurence.csv",  # misspelled
            "person_final.csv",
            "procedure_occurrence.tsv"
        ]  # unsupported file extension
        expected_warnings: list = []
        for file_name in bad_file_names:
            bad_blob = self.storage_bucket.blob(
                f'{self.folder_prefix}{file_name}')
            bad_blob.upload_from_string('.')

            expected_item: tuple = (file_name, common.UNKNOWN_FILE)
            expected_warnings.append(expected_item)
        bucket_items = gcs_utils.list_bucket(self.hpo_bucket)
        folder_items = main.get_folder_items(bucket_items, self.folder_prefix)
        r = main.validate_submission(self.hpo_id, self.hpo_bucket, folder_items,
                                     self.folder_prefix)
        self.assertCountEqual(expected_warnings, r['warnings'])

    @mock.patch("gcs_utils.LOOKUP_TABLES_DATASET_ID", dataset_id)
    @mock.patch("gcloud.gcs.LOOKUP_TABLES_DATASET_ID", dataset_id)
    @mock.patch('api_util.check_cron')
    def test_validate_five_persons_success(self, mock_check_cron):
        expected_results: list = []
        test_file_names: list = [
            os.path.basename(f) for f in test_util.FIVE_PERSONS_FILES
        ]

        for cdm_filename in common.SUBMISSION_FILES:
            if cdm_filename in test_file_names:
                expected_result: tuple = (cdm_filename, 1, 1, 1)
                test_filepath: str = os.path.join(test_util.FIVE_PERSONS_PATH,
                                                  cdm_filename)
                test_blob = self.storage_bucket.blob(
                    f'{self.folder_prefix}{cdm_filename}')
                test_blob.upload_from_filename(test_filepath)

            else:
                expected_result: tuple = (cdm_filename, 0, 0, 0)
            expected_results.append(expected_result)
        bucket_items = gcs_utils.list_bucket(self.hpo_bucket)
        folder_items = main.get_folder_items(bucket_items, self.folder_prefix)
        r = main.validate_submission(self.hpo_id, self.hpo_bucket, folder_items,
                                     self.folder_prefix)
        self.assertSetEqual(set(r['results']), set(expected_results))

        # check tables exist and are clustered as expected
        for table in resources.CDM_TABLES + common.PII_TABLES:
            table_id = bq_utils.get_table_id(test_util.FAKE_HPO_ID, table)
            table_info = bq_utils.get_table_info(table_id)
            fields = resources.fields_for(table)
            field_names = [field['name'] for field in fields]
            if 'person_id' in field_names:
                self.table_has_clustering(table_info)

    @mock.patch('validation.main.updated_datetime_object')
    def test_check_processed(self, mock_updated_datetime_object):

        mock_updated_datetime_object.return_value = datetime.datetime.today(
        ) - datetime.timedelta(minutes=7)

        for fname in common.AOU_REQUIRED_FILES:
            blob_name: str = f'{self.folder_prefix}{fname}'
            test_blob = self.storage_bucket.blob(blob_name)
            test_blob.upload_from_string('\n')

            sleep(1)

        blob_name: str = f'{self.folder_prefix}{common.PROCESSED_TXT}'
        test_blob = self.storage_bucket.blob(blob_name)
        test_blob.upload_from_string('\n')

        bucket_items = self.storage_client.get_bucket_items_metadata(
            self.storage_bucket)
        result = main._get_submission_folder(self.hpo_bucket,
                                             bucket_items,
                                             force_process=False)
        self.assertIsNone(result)
        result = main._get_submission_folder(self.hpo_bucket,
                                             bucket_items,
                                             force_process=True)
        self.assertEqual(result, self.folder_prefix)

    @mock.patch("gcloud.gcs.LOOKUP_TABLES_DATASET_ID", dataset_id)
    @mock.patch('api_util.check_cron')
    def test_copy_five_persons(self, mock_check_cron):
        # upload all five_persons files
        for cdm_pathfile in test_util.FIVE_PERSONS_FILES:
            test_filename: str = os.path.basename(cdm_pathfile)

            blob_name: str = f'{self.folder_prefix}{test_filename}'
            test_blob = self.storage_bucket.blob(blob_name)
            test_blob.upload_from_filename(cdm_pathfile)

            blob_name: str = f'{self.folder_prefix}{self.folder_prefix}{test_filename}'
            test_blob = self.storage_bucket.blob(blob_name)
            test_blob.upload_from_filename(cdm_pathfile)

        main.app.testing = True
        with main.app.test_client() as c:
            c.get(test_util.COPY_HPO_FILES_URL)
            prefix: str = f'{test_util.FAKE_HPO_ID}/{self.hpo_bucket}/{self.folder_prefix}'
            expected_bucket_items = [
                f'{prefix}{item.split(os.sep)[-1]}'
                for item in test_util.FIVE_PERSONS_FILES
            ]
            expected_bucket_items.extend([
                f'{prefix}{self.folder_prefix}{item.split(os.sep)[-1]}'
                for item in test_util.FIVE_PERSONS_FILES
            ])

            list_bucket_result = gcs_utils.list_bucket(
                gcs_utils.get_drc_bucket())
            actual_bucket_items = [item['name'] for item in list_bucket_result]
            self.assertSetEqual(set(expected_bucket_items),
                                set(actual_bucket_items))

    @mock.patch("gcloud.gcs.LOOKUP_TABLES_DATASET_ID", dataset_id)
    @mock.patch("gcs_utils.LOOKUP_TABLES_DATASET_ID", dataset_id)
    def test_target_bucket_upload(self):
        bucket_nyc = gcs_utils.get_hpo_bucket(test_util.NYC_HPO_ID)
        folder_prefix = 'test-folder-fake/'
        self.storage_client.empty_bucket(bucket_nyc)

        main._upload_achilles_files(hpo_id=None,
                                    folder_prefix=folder_prefix,
                                    target_bucket=bucket_nyc)
        actual_bucket_files = set(
            [item['name'] for item in gcs_utils.list_bucket(bucket_nyc)])
        expected_bucket_files = set([
            f'test-folder-fake/{item}'
            for item in resources.ALL_ACHILLES_INDEX_FILES
        ])
        self.assertSetEqual(expected_bucket_files, actual_bucket_files)

    @mock.patch("gcs_utils.LOOKUP_TABLES_DATASET_ID", dataset_id)
    @mock.patch("gcloud.gcs.LOOKUP_TABLES_DATASET_ID", dataset_id)
    @mock.patch('api_util.check_cron')
    def test_pii_files_loaded(self, mock_check_cron):
        # tests if pii files are loaded
        test_file_paths: list = [
            test_util.PII_NAME_FILE, test_util.PII_MRN_BAD_PERSON_ID_FILE
        ]
        test_file_names: list = [os.path.basename(f) for f in test_file_paths]

        blob_name: str = f'{self.folder_prefix}{os.path.basename(test_util.PII_NAME_FILE)}'
        test_blob = self.storage_bucket.blob(blob_name)
        test_blob.upload_from_filename(test_util.PII_NAME_FILE)

        blob_name: str = f'{self.folder_prefix}{os.path.basename(test_util.PII_MRN_BAD_PERSON_ID_FILE)}'
        test_blob = self.storage_bucket.blob(blob_name)
        test_blob.upload_from_filename(test_util.PII_MRN_BAD_PERSON_ID_FILE)

        rs = resources.csv_to_list(test_util.PII_FILE_LOAD_RESULT_CSV)
        expected_results = [(r['file_name'], int(r['found']), int(r['parsed']),
                             int(r['loaded'])) for r in rs]
        for f in common.SUBMISSION_FILES:
            if f not in test_file_names:
                expected_result = (f, 0, 0, 0)
                expected_results.append(expected_result)

        bucket_items = gcs_utils.list_bucket(self.hpo_bucket)
        folder_items = main.get_folder_items(bucket_items, self.folder_prefix)
        r = main.validate_submission(self.hpo_id, self.hpo_bucket, folder_items,
                                     self.folder_prefix)
        self.assertSetEqual(set(expected_results), set(r['results']))

    @mock.patch('validation.main.updated_datetime_object')
    @mock.patch('validation.main._has_all_required_files')
    @mock.patch('validation.main.all_required_files_loaded')
    @mock.patch('validation.main.is_first_validation_run')
    @mock.patch('api_util.check_cron')
    def test_html_report_five_person(self, mock_check_cron, mock_first_run,
                                     mock_required_files_loaded,
                                     mock_has_all_required_files,
                                     mock_updated_datetime_object):
        mock_required_files_loaded.return_value = False
        mock_first_run.return_value = False
        mock_has_all_required_files.return_value = True
        mock_updated_datetime_object.return_value = datetime.datetime.today(
        ) - datetime.timedelta(minutes=7)

        for cdm_file in test_util.FIVE_PERSONS_FILES:
            blob_name = f'{self.folder_prefix}{os.path.basename(cdm_file)}'
            test_blob = self.storage_bucket.blob(blob_name)
            test_blob.upload_from_filename(cdm_file)

        # load person table in RDR
        bq_utils.load_table_from_csv(self.project_id, self.rdr_dataset_id,
                                     common.PERSON,
                                     test_util.FIVE_PERSONS_PERSON_CSV)

        # Load measurement_concept_sets
        required_labs.load_measurement_concept_sets_table(
            project_id=self.project_id, dataset_id=self.dataset_id)
        # Load measurement_concept_sets_descendants
        required_labs.load_measurement_concept_sets_descendants_table(
            project_id=self.project_id, dataset_id=self.dataset_id)

        main.app.testing = True
        with main.app.test_client() as c:
            c.get(test_util.VALIDATE_HPO_FILES_URL)
            actual_result = self.storage_bucket.get_blob(
                f'{self.folder_prefix}{common.RESULTS_HTML}').download_as_text(
                )

        # ensure emails are not sent
        bucket_items = gcs_utils.list_bucket(self.hpo_bucket)
        folder_items = main.get_folder_items(bucket_items, self.folder_prefix)
        self.assertFalse(main.is_first_validation_run(folder_items))

        # parse html
        soup = bs(actual_result, parser="lxml", features="lxml")
        missing_pii_html_table = soup.find('table', id='missing_pii')
        table_headers = missing_pii_html_table.find_all('th')
        self.assertEqual('Missing Participant Record Type',
                         table_headers[0].get_text())
        self.assertEqual('Count', table_headers[1].get_text())

        table_rows = missing_pii_html_table.find_next('tbody').find_all('tr')
        missing_record_types = [
            table_row.find('td').text for table_row in table_rows
        ]
        self.assertIn(main_consts.EHR_NO_PII, missing_record_types)
        self.assertIn(main_consts.PII_NO_EHR, missing_record_types)

        # the missing from RDR component is obsolete (see DC-1932)
        # this is to confirm it was removed successfully from the report
        rdr_date = '2020-01-01'
        self.assertNotIn(main_consts.EHR_NO_RDR.format(date=rdr_date),
                         missing_record_types)
        self.assertIn(main_consts.EHR_NO_PARTICIPANT_MATCH,
                      missing_record_types)

        required_lab_html_table = soup.find('table', id='required-lab')
        table_headers = required_lab_html_table.find_all('th')
        self.assertEqual(3, len(table_headers))
        self.assertEqual('Ancestor Concept ID', table_headers[0].get_text())
        self.assertEqual('Ancestor Concept Name', table_headers[1].get_text())
        self.assertEqual('Found', table_headers[2].get_text())

        table_rows = required_lab_html_table.find_next('tbody').find_all('tr')
        table_rows_last_column = [
            table_row.find_all('td')[-1] for table_row in table_rows
        ]
        submitted_labs = [
            row for row in table_rows_last_column
            if 'result-1' in row.attrs['class']
        ]
        missing_labs = [
            row for row in table_rows_last_column
            if 'result-0' in row.attrs['class']
        ]
        self.assertTrue(len(table_rows) > 0)
        self.assertTrue(len(submitted_labs) > 0)
        self.assertTrue(len(missing_labs) > 0)

    @mock.patch("gcs_utils.LOOKUP_TABLES_DATASET_ID", dataset_id)
    @mock.patch("gcloud.gcs.LOOKUP_TABLES_DATASET_ID", dataset_id)
    def tearDown(self):
        self.storage_client.empty_bucket(self.hpo_bucket)
        bucket_nyc = gcs_utils.get_hpo_bucket(test_util.NYC_HPO_ID)
        self.storage_client.empty_bucket(bucket_nyc)
        self.storage_client.empty_bucket(gcs_utils.get_drc_bucket())
        test_util.delete_all_tables(self.dataset_id)

    @classmethod
    def tearDownClass(cls):
        test_util.drop_hpo_id_bucket_name_table(cls.dataset_id)
