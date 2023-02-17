"""
Unit test components of data_steward.validation.main
"""
# Python imports
import datetime
import re
from unittest import TestCase, mock

# Project imports
import common
import resources
from constants.validation import hpo_report as report_consts
from constants.validation import main as main_consts
from constants.validation.participants import identity_match as id_match_consts
from validation import main
from tests.test_util import mock_google_http_error, mock_google_cloud_error


class ValidationMainTest(TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.mock_bq_client_patcher = mock.patch(
            'validation.main.BigQueryClient')
        self.mock_bq_client = self.mock_bq_client_patcher.start()
        self.addCleanup(self.mock_bq_client_patcher.stop)

        self.hpo_id = 'fake_hpo_id'
        self.hpo_bucket = 'fake_aou_000'
        self.project_id = 'fake_project_id'
        self.bigquery_dataset_id = 'fake_dataset_id'
        mock_get_hpo_name = mock.patch('validation.main.get_hpo_name')
        self.mock_get_hpo_name = mock_get_hpo_name.start()
        self.mock_get_hpo_name.return_value = 'Fake HPO'
        self.addCleanup(mock_get_hpo_name.stop)
        self.folder_prefix = '2019-01-01-v1/'

    def _create_dummy_bucket_items(self,
                                   time_created,
                                   updated,
                                   file_exclusions=[],
                                   folder="2018-09-01"):
        bucket_items = []
        for file_name in common.AOU_REQUIRED_FILES:
            if file_name not in file_exclusions:
                bucket_items.append({
                    'name': f'{folder}/{file_name}',
                    'timeCreated': time_created,
                    'updated': updated
                })

        return bucket_items

    def test_retention_checks_list_submitted_bucket_items(self):
        # Define times to use
        within_retention = datetime.datetime.now(tz=None) - datetime.timedelta(
            days=25)
        after_lag_time = datetime.datetime.now(tz=None) - datetime.timedelta(
            minutes=7)
        stale_lag_time = datetime.datetime.now(tz=None) - datetime.timedelta(
            minutes=200)

        # If any required files are missing, nothing should be returned
        bucket_items = self._create_dummy_bucket_items(
            within_retention,
            after_lag_time,
            file_exclusions=['visit_occurrence.csv'])
        actual_result = main.list_submitted_bucket_items(bucket_items)
        expected_result = []
        self.maxDiff = None
        self.assertCountEqual(expected_result, actual_result)

        # If all required files are present and files within retention period, files should be returned
        bucket_items = self._create_dummy_bucket_items(within_retention,
                                                       after_lag_time)
        actual_result = main.list_submitted_bucket_items(bucket_items)
        expected_result = bucket_items
        self.assertCountEqual(expected_result, actual_result)

        actual_result = main.list_submitted_bucket_items([])
        self.assertCountEqual([], actual_result)

        # If unknown item and all other conditions met, return the folder
        bucket_items = self._create_dummy_bucket_items(within_retention,
                                                       after_lag_time)
        unknown_item = {
            'name': '2018-09-01/nyc_cu_person.csv',
            'timeCreated': within_retention,
            'updated': after_lag_time
        }
        bucket_items.append(unknown_item)

        actual_result = main.list_submitted_bucket_items(bucket_items)
        self.assertCountEqual(actual_result, bucket_items)

        # If unknown item and it replaces a file, return empty folder if fresh
        bucket_items = self._create_dummy_bucket_items(
            within_retention, after_lag_time, file_exclusions=['person.csv'])
        unknown_item = {
            'name': '2018-09-01/nyc_cu_person.csv',
            'timeCreated': within_retention,
            'updated': after_lag_time
        }
        bucket_items.append(unknown_item)

        actual_result = main.list_submitted_bucket_items(bucket_items)
        expected_result = []
        self.assertCountEqual(actual_result, expected_result)

        # If unknown item and it replaces a file, return the folder only after stale
        bucket_items = self._create_dummy_bucket_items(
            within_retention, stale_lag_time, file_exclusions=['person.csv'])
        unknown_item = {
            'name': '2018-09-01/nyc_cu_person.csv',
            'timeCreated': within_retention,
            'updated': stale_lag_time
        }
        bucket_items.append(unknown_item)

        actual_result = main.list_submitted_bucket_items(bucket_items)
        self.assertCountEqual(actual_result, bucket_items)

        # If ignored item and all other conditions met, only exclude the ignored item
        bucket_items = self._create_dummy_bucket_items(within_retention,
                                                       after_lag_time)
        bucket_items_with_ignored_item = bucket_items.copy()
        ignored_item = dict(name='2018-09-01/' + common.RESULTS_HTML,
                            timeCreated=within_retention,
                            updated=within_retention)
        bucket_items_with_ignored_item.append(ignored_item)
        actual_result = main.list_submitted_bucket_items(
            bucket_items_with_ignored_item)
        expected_result = bucket_items
        self.assertCountEqual(expected_result, actual_result)

        # If any file is missing but submission is fresh, skip it
        bucket_items = self._create_dummy_bucket_items(
            within_retention,
            after_lag_time,
            file_exclusions=['observation.csv'])

        actual_result = main.list_submitted_bucket_items(bucket_items)
        expected_result = []
        self.assertCountEqual(expected_result, actual_result)

        # If any file is missing but submission is stale, process it
        bucket_items = self._create_dummy_bucket_items(
            within_retention,
            stale_lag_time,
            file_exclusions=['observation.csv'])

        actual_result = main.list_submitted_bucket_items(bucket_items)
        expected_result = bucket_items
        self.assertCountEqual(expected_result, actual_result)

    def test_folder_list(self):
        now = datetime.datetime.now()
        t0 = now - datetime.timedelta(days=3)
        t1 = now - datetime.timedelta(days=2)
        t2 = now - datetime.timedelta(days=1)
        t3 = now - datetime.timedelta(hours=1)
        expected = 't2/'

        bucket_items = self._create_dummy_bucket_items(
            t2, t2, file_exclusions=["person.csv"], folder="t2")
        bucket_items.extend([{
            'name': 't0/person.csv',
            'updated': t0,
            'timeCreated': t0
        }, {
            'name': 't1/person.csv',
            'updated': t1,
            'timeCreated': t1
        }, {
            'name': '%sperson.csv' % expected,
            'updated': t2,
            'timeCreated': t2
        }])

        # mock bypasses api call and says no folders were processed
        with mock.patch(
                'validation.main._validation_done') as mock_validation_done:
            mock_validation_done.return_value = False

            # should be bucket_item with latest timestamp
            submission_folder = main._get_submission_folder(
                self.hpo_bucket, bucket_items)
            self.assertEqual(submission_folder, expected)

            # report dir should be ignored despite being more recent than t2
            report_dir = id_match_consts.REPORT_DIRECTORY.format(
                date=now.strftime('%Y%m%d'))
            # sanity check
            compiled_exp = re.compile(id_match_consts.REPORT_DIRECTORY_REGEX)
            assert (compiled_exp.match(report_dir))
            report_item = {
                'name': '%s/id-validation.csv' % report_dir,
                'updated': t3,
                'timeCreated': t3
            }
            submission_folder = main._get_submission_folder(
                self.hpo_bucket, bucket_items + [report_item])
            self.assertEqual(submission_folder, 't2/')

            # participant dir should be ignored despite being more recent than t2
            partipant_item = {
                'name': '%s/person.csv' % common.PARTICIPANT_DIR,
                'updated': t3,
                'timeCreated': t3
            }
            submission_folder = main._get_submission_folder(
                self.hpo_bucket, bucket_items + [partipant_item])
            self.assertEqual(submission_folder, 't2/')

    @mock.patch('api_util.check_cron')
    def test_categorize_folder_items(self, mock_check_cron):
        expected_cdm_files = ['person.csv']
        expected_pii_files = ['pii_email.csv']
        expected_unknown_files = ['random.csv']
        ignored_files = ['curation_report/index.html']
        folder_items = expected_cdm_files + expected_pii_files + expected_unknown_files + ignored_files
        cdm_files, pii_files, unknown_files = main.categorize_folder_items(
            folder_items)
        self.assertCountEqual(expected_cdm_files, cdm_files)
        self.assertCountEqual(expected_pii_files, pii_files)
        self.assertCountEqual(expected_unknown_files, unknown_files)

    @mock.patch('bq_utils.create_standard_table')
    @mock.patch('validation.main.perform_validation_on_file')
    @mock.patch('api_util.check_cron')
    def test_validate_submission(self, mock_check_cron,
                                 mock_perform_validation_on_file,
                                 mock_create_standard_table):
        """
        Checks the return value of validate_submission

        :param mock_check_cron:
        :param mock_perform_validation_on_file:
        :param mock_create_standard_table:
        :return:
        """
        folder_prefix = '2019-01-01/'
        folder_items = ['person.csv', 'invalid_file.csv']

        perform_validation_on_file_returns = dict()
        expected_results = []
        expected_errors = []
        expected_warnings = [('invalid_file.csv', 'Unknown file')]
        for file_name in sorted(resources.CDM_CSV_FILES) + [
                common.NOTE_JSONL
        ] + sorted(common.PII_FILES):
            result = []
            errors = []
            found = 0
            parsed = 0
            loaded = 0
            if file_name == 'person.csv':
                found = 1
                parsed = 1
                loaded = 1
            elif file_name == 'visit_occurrence.csv':
                found = 1
                error = (file_name, 'Fake parsing error')
                errors.append(error)
            elif file_name == 'note.jsonl':
                result.append((file_name, found, parsed, loaded))
                perform_validation_on_file_returns[file_name] = result, errors
                continue
            result.append((file_name, found, parsed, loaded))
            perform_validation_on_file_returns[file_name] = result, errors
            expected_results += result
            expected_errors += errors

        def perform_validation_on_file(cdm_file_name, found_cdm_files, hpo_id,
                                       folder_prefix, bucket):
            return perform_validation_on_file_returns.get(cdm_file_name)

        mock_perform_validation_on_file.side_effect = perform_validation_on_file

        mock_bucket = mock.MagicMock()
        type(mock_bucket).name = mock.PropertyMock(return_value=self.hpo_bucket)

        actual_result = main.validate_submission(self.hpo_id, mock_bucket,
                                                 folder_items, folder_prefix)
        self.assertCountEqual(expected_results, actual_result.get('results'))
        self.assertCountEqual(expected_errors, actual_result.get('errors'))
        self.assertCountEqual(expected_warnings, actual_result.get('warnings'))

    @mock.patch('validation.main.StorageClient')
    @mock.patch('bq_utils.get_hpo_info')
    @mock.patch('logging.exception')
    @mock.patch('api_util.check_cron')
    def test_validate_all_hpos_exception(self, check_cron, mock_logging_error,
                                         mock_hpo_csv, mock_storage_client):

        http_error_string = 'fake http error'
        mock_hpo_csv.return_value = [{'hpo_id': self.hpo_id}]
        mock_client = mock.MagicMock()
        mock_storage_client.return_value = mock_client
        mock_client.get_bucket_items_metadata.side_effect = mock_google_cloud_error(
            content=http_error_string.encode())
        with main.app.test_client() as c:
            c.get(main_consts.PREFIX + 'ValidateAllHpoFiles')
            expected_call = mock.call(
                f"Failed to process hpo_id '{self.hpo_id}' due to the following "
                f"HTTP error: {http_error_string}")
            self.assertIn(expected_call, mock_logging_error.mock_calls)

    @mock.patch('validation.main.setup_and_validate_participants',
                mock.MagicMock())
    @mock.patch('bq_utils.query')
    @mock.patch('validation.main.is_valid_folder_prefix_name')
    @mock.patch('validation.main.run_export')
    @mock.patch('validation.main.run_achilles')
    @mock.patch('validation.main.all_required_files_loaded')
    @mock.patch('validation.main.query_rows')
    @mock.patch('validation.main.get_duplicate_counts_query')
    @mock.patch('validation.main.get_hpo_name')
    @mock.patch('validation.main.validate_submission')
    @mock.patch('validation.main.get_folder_items')
    @mock.patch('validation.main._has_all_required_files')
    @mock.patch('validation.main.is_first_validation_run')
    @mock.patch('validation.main.is_valid_rdr')
    @mock.patch('validation.main.StorageClient')
    def test_process_hpo_ignore_dirs(
        self, mock_storage_client, mock_valid_rdr, mock_first_validation,
        mock_has_all_required_files, mock_folder_items, mock_validation,
        mock_get_hpo_name, mock_get_duplicate_counts_query, mock_query_rows,
        mock_all_required_files_loaded, mock_run_achilles, mock_export,
        mock_valid_folder_name, mock_query):
        """
        Test process_hpo with directories we want to ignore.

        This should process one directory whose case insensitive root
        does not match 'participant'.  Otherwise, process_hpo should work
        as before and only process items in directories and the most recent
        directory.  Checks to see if other functions are called with the
        correct argument lists.  Process_hpo calls _get_submission_folder,
        which is where the ignoring actually occurs.

        :param mock_hpo_bucket: mock the hpo bucket name.
        :param mock_bucket_list: mocks the list of items in the hpo bucket.
        :param mock_validation: mock performing validation
        :param mock_folder_items: mock get_folder_items
        :param mock_first_validation: mock first validation run
        :param mock_valid_rdr: mock valid rdr dataset
        :param mock_upload: mock uploading to a bucket
        :param mock_run_achilles: mock running the achilles reports
        :param mock_export: mock exporting the files
        """
        # pre-conditions
        fake_hpo = 'fake_hpo_id'
        submission_path = 'SUBMISSION/'
        mock_valid_folder_name.return_value = True
        mock_client = mock.MagicMock()
        mock_bucket = mock.MagicMock()
        mock_blob = mock.MagicMock()
        mock_storage_client.return_value = mock_client
        mock_client.get_hpo_bucket.return_value = mock_bucket
        type(mock_bucket).name = mock.PropertyMock(
            return_value='fake_bucket_name')
        mock_bucket.blob.return_value = mock_blob
        mock_all_required_files_loaded.return_value = True
        mock_has_all_required_files.return_value = True
        mock_query.return_value = {}
        mock_query_rows.return_value = []
        mock_get_duplicate_counts_query.return_value = ''
        mock_get_hpo_name.return_value = 'fake_hpo_name'
        mock_valid_rdr.return_value = True
        mock_first_validation.return_value = False
        yesterday = datetime.datetime.now() - datetime.timedelta(hours=24)
        now = datetime.datetime.now()

        after_lag_time = datetime.datetime.today() - datetime.timedelta(
            minutes=7)

        mock_client.get_bucket_items_metadata.return_value = [{
            'name': 'unknown.pdf',
            'timeCreated': now,
            'updated': after_lag_time
        }, {
            'name': 'participant/no-site/foo.pdf',
            'timeCreated': now,
            'updated': after_lag_time
        }, {
            'name': 'PARTICIPANT/siteone/foo.pdf',
            'timeCreated': now,
            'updated': after_lag_time
        }, {
            'name': 'Participant/sitetwo/foo.pdf',
            'timeCreated': now,
            'updated': after_lag_time
        }, {
            'name': f'{submission_path.lower()}person.csv',
            'timeCreated': yesterday,
            'updated': yesterday
        }, {
            'name': f'{submission_path}measurement.csv',
            'timeCreated': now,
            'updated': after_lag_time
        }]

        mock_validation.return_value = {
            'results': [(f'{submission_path}measurement.csv', 1, 1, 1)],
            'errors': [],
            'warnings': []
        }

        mock_folder_items.return_value = ['measurement.csv']

        # test
        main.process_hpo(fake_hpo, force_run=True)

        # post conditions
        mock_folder_items.assert_called()
        mock_folder_items.assert_called_once_with(
            mock_client.get_bucket_items_metadata.return_value, submission_path)
        mock_validation.assert_called()
        mock_validation.assert_called_once_with(fake_hpo, mock_bucket,
                                                mock_folder_items.return_value,
                                                submission_path)
        mock_run_achilles.assert_called()
        mock_export.assert_called()
        mock_export.assert_called_once_with(datasource_id=fake_hpo,
                                            folder_prefix=submission_path)
        # make sure upload is called for only the most recent
        # non-participant directory
        mock_client.get_hpo_bucket.assert_called()
        mock_bucket.blob.assert_called()
        self.assertEqual(mock_blob.upload_from_string.call_count, 2)
        mock_blob.upload_from_file.assert_called()
        for filepath in mock_bucket.blob.call_args_list:
            self.assertEqual('fake_bucket_name', mock_bucket.name)
            self.assertTrue(filepath.startswith(submission_path))
        mock_client.get_blob_metadata.assert_called()

    @mock.patch('validation.main.StorageClient')
    @mock.patch('api_util.check_cron')
    def test_copy_files_ignore_all(self, mock_check_cron, mock_storage_client):
        """
        Test copying files to the drc internal bucket.
        This should copy anything in the site's bucket except for files named
        participant.  Copy_files uses a case insensitive match, so any
        capitalization scheme should be detected and left out of the copy.
        Nothing should be copied.  Mocks are used to determine if the
        test ran as expected and the get and copy blobs methods were never called.

        :param mock_check_cron: mocks the cron decorator.
        :param mock_storage_client: mocks the StorageClient which has bucket and blob functionality.
        """
        # pre-conditions
        mock_client = mock.MagicMock()
        mock_hpo_bucket = mock.MagicMock()
        mock_drc_bucket = mock.MagicMock()
        mock_source_blob = mock.MagicMock()

        type(mock_hpo_bucket).name = mock.PropertyMock(
            return_value='fake_prefix')
        mock_hpo_bucket.get_blob.return_value = mock_source_blob
        mock_hpo_bucket.copy_blob.return_value = mock_source_blob

        mock_client.get_hpo_bucket.return_value = mock_hpo_bucket
        mock_client.get_drc_bucket.return_value = mock_drc_bucket
        mock_client.get_bucket_items_metadata.return_value = [{
            'name': 'participant/site_1/person.csv',
        }, {
            'name': 'PARTICIPANT/site_2/measurement.csv',
        }, {
            'name': 'Participant/site_3/person.csv',
        }]

        mock_storage_client.return_value = mock_client

        # test
        result = main.copy_files('fake_hpo_id')

        # post conditions
        expected = '{"copy-status": "done"}'
        self.assertEqual(result, expected)
        mock_check_cron.assert_called()

        # make sure we were given the data
        mock_client.get_hpo_bucket.assert_called()
        mock_client.get_drc_bucket.assert_called()
        mock_client.get_bucket_items_metadata.assert_called()

        # make sure get/copy was never called for participant directories
        mock_hpo_bucket.get_blob.assert_not_called()
        mock_hpo_bucket.copy_blob.assert_not_called()

    @mock.patch('validation.main.StorageClient')
    @mock.patch('api_util.check_cron')
    def test_copy_files_accept_all(self, mock_check_cron, mock_storage_client):
        """
        Test copying files to the drc internal bucket.
        This should copy anything in the site's bucket except for files named
        participant.  Copy_files uses a case insensitive match, so any
        capitalization scheme should be detected and left out of the copy.
        Everything should be copied.  Mocks are used to determine if the
        test ran as expected and all statements would execute in a production
        environment.

        :param mock_check_cron: mocks the cron decorator.
        :param mock_storage_client: mocks the StorageClient which has bucket and blob functionality.
        """
        # pre-conditions
        mock_client = mock.MagicMock()
        mock_hpo_bucket = mock.MagicMock()
        mock_drc_bucket = mock.MagicMock()
        mock_source_blob = mock.MagicMock()

        type(mock_hpo_bucket).name = mock.PropertyMock(
            return_value='fake_prefix')
        mock_hpo_bucket.get_blob.return_value = mock_source_blob
        mock_hpo_bucket.copy_blob.return_value = mock_source_blob

        mock_client.get_hpo_bucket.return_value = mock_hpo_bucket
        mock_client.get_drc_bucket.return_value = mock_drc_bucket
        mock_client.get_bucket_items_metadata.return_value = [{
            'name': 'submission/person.csv',
        }, {
            'name': 'SUBMISSION/measurement.csv',
        }]

        mock_storage_client.return_value = mock_client

        # test
        result = main.copy_files('fake_hpo_id')

        # post conditions
        expected = '{"copy-status": "done"}'
        self.assertEqual(result, expected)
        mock_check_cron.assert_called()
        # make sure we were given the data
        mock_client.get_hpo_bucket.assert_called()
        mock_client.get_drc_bucket.assert_called()
        mock_client.get_bucket_items_metadata.assert_called()

        expected: list = [
            mock.call(mock_source_blob, mock_drc_bucket,
                      'fake_hpo_id/fake_prefix/submission/person.csv'),
            mock.call(mock_source_blob, mock_drc_bucket,
                      'fake_hpo_id/fake_prefix/SUBMISSION/measurement.csv')
        ]
        # make sure copy is called for submission directories
        self.assertEqual(mock_hpo_bucket.get_blob.call_count, 2)
        self.assertEqual(mock_hpo_bucket.copy_blob.call_count, 2)
        mock_hpo_bucket.copy_blob.assert_has_calls(expected, any_order=True)

    @mock.patch('validation.main.setup_and_validate_participants',
                mock.MagicMock())
    @mock.patch('bq_utils.query', mock.MagicMock())
    def test_generate_metrics(self):
        summary = {
            report_consts.RESULTS_REPORT_KEY: [{
                'file_name': 'person.csv',
                'found': 1,
                'parsed': 1,
                'loaded': 1
            }],
            report_consts.ERRORS_REPORT_KEY: [],
            report_consts.WARNINGS_REPORT_KEY: []
        }

        def all_required_files_loaded(results):
            return False

        def query_rows(q):
            return []

        def query_rows_error(q):
            raise mock_google_http_error(status_code=500,
                                         reason='baz',
                                         content=b'bar')

        def get_duplicate_counts_query(bq_client, hpo_id):
            return ''

        def is_valid_rdr(rdr_dataset_id):
            return True

        with mock.patch.multiple(
                'validation.main',
                all_required_files_loaded=all_required_files_loaded,
                query_rows=query_rows,
                get_duplicate_counts_query=get_duplicate_counts_query,
                is_valid_rdr=is_valid_rdr):
            result = main.generate_metrics(self.project_id, self.hpo_id,
                                           self.hpo_bucket, self.folder_prefix,
                                           summary)
            self.assertIn(report_consts.RESULTS_REPORT_KEY, result)
            self.assertIn(report_consts.WARNINGS_REPORT_KEY, result)
            self.assertIn(report_consts.ERRORS_REPORT_KEY, result)
            self.assertNotIn(report_consts.HEEL_ERRORS_REPORT_KEY, result)
            self.assertIn(report_consts.NONUNIQUE_KEY_METRICS_REPORT_KEY,
                          result)
            self.assertIn(report_consts.COMPLETENESS_REPORT_KEY, result)
            self.assertIn(report_consts.DRUG_CLASS_METRICS_REPORT_KEY, result)

        # if error occurs (e.g. limit reached) error flag is set
        with mock.patch.multiple(
                'validation.main',
                all_required_files_loaded=all_required_files_loaded,
                query_rows=query_rows_error,
                get_duplicate_counts_query=get_duplicate_counts_query,
                is_valid_rdr=is_valid_rdr):
            result = main.generate_metrics(self.project_id, self.hpo_id,
                                           self.hpo_bucket, self.folder_prefix,
                                           summary)
            error_occurred = result.get(report_consts.ERROR_OCCURRED_REPORT_KEY)
            self.assertEqual(error_occurred, True)

    @mock.patch('bq_utils.get_hpo_info')
    def test_html_incorrect_folder_name(self, mock_hpo_csv):
        mock_hpo_csv.return_value = [{'hpo_id': self.hpo_id}]

        # validate folder name
        self.assertEqual(
            bool(main.is_valid_folder_prefix_name(self.folder_prefix)), True)
        incorrect_folder_prefix = '2020-01-01/'
        self.assertEqual(
            bool(main.is_valid_folder_prefix_name(incorrect_folder_prefix)),
            False)

        # validate report data
        report_data = main.generate_empty_report(self.hpo_id,
                                                 incorrect_folder_prefix)
        self.assertIn(report_consts.SUBMISSION_ERROR_REPORT_KEY, report_data)
        self.assertIn(incorrect_folder_prefix,
                      report_data[report_consts.SUBMISSION_ERROR_REPORT_KEY])

    @mock.patch('validation.main._upload_achilles_files')
    @mock.patch('validation.main.run_export')
    @mock.patch('validation.main.run_achilles')
    @mock.patch('validation.ehr_union.main')
    @mock.patch('bq_utils.get_unioned_dataset_id')
    @mock.patch('bq_utils.get_dataset_id')
    @mock.patch('bq_utils.app_identity.get_application_id')
    @mock.patch('api_util.check_cron')
    def test_union_ehr(self, mock_check_cron, mock_get_application_id,
                       mock_get_dataset_id, mock_get_unioned_dataset_id,
                       mock_ehr_union_main, mock_run_achilles, mock_run_export,
                       mock_upload_achilles_files):

        application_id = 'application_id'
        input_dataset = 'input_dataset'
        output_dataset = 'output_dataset'
        mock_client = mock.MagicMock()

        self.mock_bq_client.return_value = mock_client
        mock_check_cron.return_value = True
        mock_get_application_id.return_value = application_id
        mock_get_dataset_id.return_value = input_dataset
        mock_get_unioned_dataset_id.return_value = output_dataset

        main.app.testing = True
        main.before_first_request_funcs = []
        with main.app.test_client() as c:
            c.get(main_consts.PREFIX + 'UnionEHR')

            mock_ehr_union_main.assert_called_once_with(input_dataset,
                                                        output_dataset,
                                                        application_id)
            mock_run_achilles.assert_called_once_with(mock_client,
                                                      'unioned_ehr')

            self.assertEqual(mock_run_export.call_count, 1)
            self.assertEqual(mock_upload_achilles_files.call_count, 1)
