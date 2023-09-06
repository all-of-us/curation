"""
Unit test components of data_steward.validation.main
"""
# Python imports
import datetime
from time import sleep
import os
import unittest
import mock

# Third party imports
from bs4 import BeautifulSoup as bs

# Project imports
import bq_utils
import app_identity
import common
from constants import bq_utils as bq_consts
from constants.validation import main as main_consts
from gcloud.gcs import StorageClient
from gcloud.bq import BigQueryClient
import resources
from tests import test_util
from validation import main
from validation.metrics import required_labs


class ValidationMainTest(unittest.TestCase):
    dataset_id = common.BIGQUERY_DATASET_ID
    project_id = app_identity.get_application_id()
    bq_client = BigQueryClient(project_id)

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')
        test_util.setup_hpo_id_bucket_name_table(cls.bq_client, cls.dataset_id)

    @mock.patch("gcloud.gcs.LOOKUP_TABLES_DATASET_ID", dataset_id)
    def setUp(self):
        self.hpo_id: str = test_util.FAKE_HPO_ID
        self.rdr_dataset_id: str = bq_utils.get_rdr_dataset_id()

        mock_get_hpo_name = mock.patch('validation.main.get_hpo_name')
        self.mock_get_hpo_name = mock_get_hpo_name.start()
        self.mock_get_hpo_name.return_value = 'Fake HPO'
        self.addCleanup(mock_get_hpo_name.stop)

        self.bigquery_dataset_id: str = common.BIGQUERY_DATASET_ID
        self.folder_prefix: str = '2019-01-01-v1/'

        self.storage_client = StorageClient(self.project_id)
        self.hpo_bucket = self.storage_client.get_hpo_bucket(self.hpo_id)
        self.drc_bucket = self.storage_client.get_drc_bucket()

        self.storage_client.empty_bucket(self.drc_bucket)
        self.storage_client.empty_bucket(self.hpo_bucket)

        test_util.delete_all_tables(self.bq_client, self.dataset_id)
        self.create_drug_class_table()

    def create_drug_class_table(self):

        table_name: str = 'drug_class'
        fields: list = [{
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
                              dataset_id=self.dataset_id)

        bq_utils.query(
            q=main_consts.DRUG_CLASS_QUERY.format(dataset_id=self.dataset_id),
            use_legacy_sql=False,
            destination_table_id='drug_class',
            retry_count=bq_consts.BQ_DEFAULT_RETRY_COUNT,
            write_disposition='WRITE_TRUNCATE',
            destination_dataset_id=self.dataset_id)

        # ensure concept ancestor table exists
        if not self.bq_client.table_exists(common.CONCEPT_ANCESTOR):
            bq_utils.create_standard_table(common.CONCEPT_ANCESTOR,
                                           common.CONCEPT_ANCESTOR)
            q = """INSERT INTO {dataset}.concept_ancestor
            SELECT * FROM {vocab}.concept_ancestor""".format(
                dataset=self.dataset_id, vocab=common.VOCABULARY_DATASET)
            bq_utils.query(q)

    def _table_has_clustering(self, table_obj):
        self.assertIsNotNone(table_obj.clustering_fields)
        self.assertSetEqual(set(table_obj.clustering_fields), {'person_id'})
        self.assertIsNotNone(table_obj.time_partitioning)
        self.assertEqual(table_obj.time_partitioning.type_, 'DAY')

    @mock.patch("gcloud.gcs.LOOKUP_TABLES_DATASET_ID", dataset_id)
    def test_all_files_unparseable_output(self):
        # TODO possible bug: if no pre-existing table, results in bq table not found error
        for cdm_table in common.SUBMISSION_FILES:
            cdm_blob = self.hpo_bucket.blob(f'{self.folder_prefix}{cdm_table}')
            cdm_blob.upload_from_string('.\n .')

        item_metadata: list = self.storage_client.get_bucket_items_metadata(
            self.hpo_bucket)
        folder_items: list = main.get_folder_items(item_metadata,
                                                   self.folder_prefix)
        expected_results: list = [(f, 1, 0, 0) for f in common.SUBMISSION_FILES]
        actual: list = main.validate_submission(self.hpo_id, self.hpo_bucket,
                                                folder_items,
                                                self.folder_prefix)
        self.assertSetEqual(set(expected_results), set(actual['results']))

    def test_bad_file_names(self):
        bad_file_names: list = [
            "avisit_occurrence.csv",
            "condition_occurence.csv",  # misspelled
            "person_final.csv",
            "procedure_occurrence.tsv"
        ]  # unsupported file extension
        expected_warnings: list = []
        for file_name in bad_file_names:
            bad_blob = self.hpo_bucket.blob(f'{self.folder_prefix}{file_name}')
            bad_blob.upload_from_string('.')
            expected_item: tuple = (file_name, common.UNKNOWN_FILE)
            expected_warnings.append(expected_item)

        items_metadata: list = self.storage_client.get_bucket_items_metadata(
            self.hpo_bucket)
        folder_items: list = main.get_folder_items(items_metadata,
                                                   self.folder_prefix)
        actual: dict = main.validate_submission(self.hpo_id, self.hpo_bucket,
                                                folder_items,
                                                self.folder_prefix)
        self.assertCountEqual(expected_warnings, actual['warnings'])

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
                test_blob = self.hpo_bucket.blob(
                    f'{self.folder_prefix}{cdm_filename}')
                test_blob.upload_from_filename(test_filepath)

            else:
                expected_result: tuple = (cdm_filename, 0, 0, 0)
            expected_results.append(expected_result)
        items_metadata: list = self.storage_client.get_bucket_items_metadata(
            self.hpo_bucket)
        folder_items: list = main.get_folder_items(items_metadata,
                                                   self.folder_prefix)
        actual: dict = main.validate_submission(self.hpo_id, self.hpo_bucket,
                                                folder_items,
                                                self.folder_prefix)
        self.assertSetEqual(set(actual['results']), set(expected_results))

        # check tables exist and are clustered as expected
        for table in resources.CDM_TABLES + common.PII_TABLES:
            table_id: str = resources.get_table_id(table,
                                                   hpo_id=test_util.FAKE_HPO_ID)
            table_obj = self.bq_client.get_table(
                f'{os.environ.get("BIGQUERY_DATASET_ID")}.{table_id}')
            field_names: list = [field.name for field in table_obj.schema]
            if 'person_id' in field_names:
                self._table_has_clustering(table_obj)

    def test_check_processed(self):

        for fname in common.AOU_REQUIRED_FILES:
            blob_name: str = f'{self.folder_prefix}{fname}'
            test_blob = self.hpo_bucket.blob(blob_name)
            test_blob.upload_from_string('\n')
            sleep(1)

        blob_name: str = f'{self.folder_prefix}{common.PROCESSED_TXT}'
        test_blob = self.hpo_bucket.blob(blob_name)
        test_blob.upload_from_string('\n')

        items_metadata: list = self.storage_client.get_bucket_items_metadata(
            self.hpo_bucket)

        for item in items_metadata:
            item['updated'].replace(tzinfo=None)
            item['updated'] = datetime.datetime.today() - datetime.timedelta(
                minutes=7)

        result = main._get_submission_folder(self.hpo_bucket,
                                             items_metadata,
                                             force_process=False)
        self.assertIsNone(result)
        result = main._get_submission_folder(self.hpo_bucket,
                                             items_metadata,
                                             force_process=True)
        self.assertEqual(result, self.folder_prefix)

    @mock.patch("gcloud.gcs.LOOKUP_TABLES_DATASET_ID", dataset_id)
    @mock.patch('api_util.check_cron')
    def test_copy_five_persons(self, mock_check_cron):
        # upload all five_persons files
        for cdm_pathfile in test_util.FIVE_PERSONS_FILES:
            test_filename: str = os.path.basename(cdm_pathfile)

            blob_name: str = f'{self.folder_prefix}{test_filename}'
            test_blob = self.hpo_bucket.blob(blob_name)
            test_blob.upload_from_filename(cdm_pathfile)

            blob_name: str = f'{self.folder_prefix}{self.folder_prefix}{test_filename}'
            test_blob = self.hpo_bucket.blob(blob_name)
            test_blob.upload_from_filename(cdm_pathfile)

        main.app.testing = True
        with main.app.test_client() as c:
            c.get(test_util.COPY_HPO_FILES_URL)
            prefix: str = f'{test_util.FAKE_HPO_ID}/{self.hpo_bucket.name}/{self.folder_prefix}'
            expected_metadata: list = [
                f'{prefix}{item.split(os.sep)[-1]}'
                for item in test_util.FIVE_PERSONS_FILES
            ]
            expected_metadata.extend([
                f'{prefix}{self.folder_prefix}{item.split(os.sep)[-1]}'
                for item in test_util.FIVE_PERSONS_FILES
            ])

            raw_metadata: list = self.storage_client.get_bucket_items_metadata(
                self.drc_bucket)
            actual_metadata: list = [item['name'] for item in raw_metadata]
            self.assertSetEqual(set(expected_metadata), set(actual_metadata))

    @mock.patch("gcloud.gcs.LOOKUP_TABLES_DATASET_ID", dataset_id)
    def test_target_bucket_upload(self):
        bucket_nyc = self.storage_client.get_hpo_bucket('nyc')
        folder_prefix: str = 'test-folder-fake/'
        self.storage_client.empty_bucket(bucket_nyc)

        main._upload_achilles_files(hpo_id=None,
                                    folder_prefix=folder_prefix,
                                    target_bucket=bucket_nyc.name)
        actual_bucket_files = set([
            item['name'] for item in
            self.storage_client.get_bucket_items_metadata(bucket_nyc)
        ])
        expected_bucket_files = set([
            f'test-folder-fake/{item}'
            for item in resources.ALL_ACHILLES_INDEX_FILES
        ])
        self.assertSetEqual(expected_bucket_files, actual_bucket_files)

    @mock.patch("gcloud.gcs.LOOKUP_TABLES_DATASET_ID", dataset_id)
    @mock.patch('api_util.check_cron')
    def test_pii_files_loaded(self, mock_check_cron):
        # tests if pii files are loaded
        test_file_paths: list = [
            test_util.PII_NAME_FILE, test_util.PII_MRN_BAD_PERSON_ID_FILE
        ]
        test_file_names: list = [os.path.basename(f) for f in test_file_paths]

        blob_name: str = f'{self.folder_prefix}{os.path.basename(test_util.PII_NAME_FILE)}'
        test_blob = self.hpo_bucket.blob(blob_name)
        test_blob.upload_from_filename(test_util.PII_NAME_FILE)

        blob_name: str = f'{self.folder_prefix}{os.path.basename(test_util.PII_MRN_BAD_PERSON_ID_FILE)}'
        test_blob = self.hpo_bucket.blob(blob_name)
        test_blob.upload_from_filename(test_util.PII_MRN_BAD_PERSON_ID_FILE)

        rs: list = resources.csv_to_list(test_util.PII_FILE_LOAD_RESULT_CSV)
        expected_results: list = [(r['file_name'], int(r['found']),
                                   int(r['parsed']), int(r['loaded']))
                                  for r in rs]
        for f in common.SUBMISSION_FILES:
            if f not in test_file_names:
                expected_result: tuple = (f, 0, 0, 0)
                expected_results.append(expected_result)

        bucket_items: list = self.storage_client.get_bucket_items_metadata(
            self.hpo_bucket)
        folder_items = main.get_folder_items(bucket_items, self.folder_prefix)
        actual: dict = main.validate_submission(self.hpo_id, self.hpo_bucket,
                                                folder_items,
                                                self.folder_prefix)
        self.assertSetEqual(set(expected_results), set(actual['results']))

    @mock.patch('constants.validation.main.SUBMISSION_LAG_TIME_MINUTES', 0)
    @mock.patch("gcloud.gcs.LOOKUP_TABLES_DATASET_ID", dataset_id)
    @mock.patch('validation.main.get_participant_validation_summary_query')
    @mock.patch('validation.main.setup_and_validate_participants')
    @mock.patch('validation.main._has_all_required_files')
    @mock.patch('validation.main.all_required_files_loaded')
    @mock.patch('validation.main.is_first_validation_run')
    @mock.patch('api_util.check_cron')
    def test_html_report_five_person(self, mock_check_cron, mock_first_run,
                                     mock_required_files_loaded,
                                     mock_has_all_required_files,
                                     mock_setup_validate_participants,
                                     mock_part_val_summary_query):
        mock_required_files_loaded.return_value = False
        mock_first_run.return_value = False
        mock_has_all_required_files.return_value = True

        for cdm_file in test_util.FIVE_PERSONS_FILES:
            blob_name: str = f'{self.folder_prefix}{os.path.basename(cdm_file)}'
            test_blob = self.hpo_bucket.blob(blob_name)
            test_blob.upload_from_filename(cdm_file)

        # load person table in RDR
        bq_utils.load_table_from_csv(self.project_id, self.rdr_dataset_id,
                                     common.PERSON,
                                     test_util.FIVE_PERSONS_PERSON_CSV)

        # Load measurement_concept_sets
        required_labs.load_measurement_concept_sets_table(
            client=self.bq_client, dataset_id=self.dataset_id)
        # Load measurement_concept_sets_descendants
        required_labs.load_measurement_concept_sets_descendants_table(
            client=self.bq_client, dataset_id=self.dataset_id)

        main.app.testing = True
        with main.app.test_client() as test_client:
            test_client.get(test_util.VALIDATE_HPO_FILES_URL)
            actual_result = self.hpo_bucket.get_blob(
                f'{self.folder_prefix}{common.RESULTS_HTML}').download_as_text(
                )

        # ensure emails are not sent
        items_metadata: list = self.storage_client.get_bucket_items_metadata(
            self.hpo_bucket)

        for item in items_metadata:
            item['updated'].replace(tzinfo=None)
            item['updated'] = datetime.datetime.today() - datetime.timedelta(
                minutes=7)
        folder_items: list = main.get_folder_items(items_metadata,
                                                   self.folder_prefix)
        self.assertFalse(main.is_first_validation_run(folder_items))

        # parse html
        soup = bs(actual_result, parser="lxml", features="lxml")
        missing_pii_html_table = soup.find('table', id='missing_pii')
        table_headers = missing_pii_html_table.find_all('th')
        self.assertEqual('Missing Participant Record Type',
                         table_headers[0].get_text())
        self.assertEqual('Count', table_headers[1].get_text())

        table_rows = missing_pii_html_table.find_next('tbody').find_all('tr')
        missing_record_types: list = [
            table_row.find('td').text for table_row in table_rows
        ]
        self.assertIn(main_consts.EHR_NO_PII, missing_record_types)
        self.assertIn(main_consts.PII_NO_EHR, missing_record_types)

        # the missing from RDR component is obsolete (see DC-1932)
        # this is to confirm it was removed successfully from the report
        rdr_date: str = '2020-01-01'
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
        table_rows_last_column: list = [
            table_row.find_all('td')[-1] for table_row in table_rows
        ]
        submitted_labs: list = [
            row for row in table_rows_last_column
            if 'result-1' in row.attrs['class']
        ]
        missing_labs: list = [
            row for row in table_rows_last_column
            if 'result-0' in row.attrs['class']
        ]

        self.assertGreater(len(table_rows), 0)
        self.assertGreater(len(submitted_labs), 0)
        self.assertGreater(len(missing_labs), 0)

    @mock.patch("gcloud.gcs.LOOKUP_TABLES_DATASET_ID", dataset_id)
    def tearDown(self):
        nyc_bucket = self.storage_client.get_hpo_bucket('nyc')
        self.storage_client.empty_bucket(nyc_bucket)
        self.storage_client.empty_bucket(self.hpo_bucket)
        self.storage_client.empty_bucket(self.drc_bucket)

        test_util.delete_all_tables(self.bq_client, self.dataset_id)
        test_util.delete_all_tables(self.bq_client, self.rdr_dataset_id)

    @classmethod
    def tearDownClass(cls):
        test_util.drop_hpo_id_bucket_name_table(cls.bq_client, cls.dataset_id)
