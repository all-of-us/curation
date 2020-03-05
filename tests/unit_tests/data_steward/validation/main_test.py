"""
Unit test components of data_steward.validation.main
"""
import datetime
import re
import unittest

import googleapiclient.errors
import mock

import common
from constants.validation import hpo_report as report_consts
from constants.validation import main as main_consts
from constants.validation.participants import identity_match as id_match_consts
import resources
from validation import main


class ValidationMainTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.hpo_id = 'fake_hpo_id'
        self.hpo_bucket = 'fake_aou_000'
        self.project_id = 'fake_project_id'
        self.bigquery_dataset_id = 'fake_dataset_id'
        mock_get_hpo_name = mock.patch('validation.main.get_hpo_name')
        self.mock_get_hpo_name = mock_get_hpo_name.start()
        self.mock_get_hpo_name.return_value = 'Fake HPO'
        self.addCleanup(mock_get_hpo_name.stop)
        self.folder_prefix = '2019-01-01-v1/'

    def test_retention_checks_list_submitted_bucket_items(self):
        outside_retention = datetime.datetime.today() - datetime.timedelta(
            days=29)
        outside_retention_str = outside_retention.strftime(
            '%Y-%m-%dT%H:%M:%S.%fZ')
        bucket_items = [{
            'name': '2018-09-01/person.csv',
            'timeCreated': outside_retention_str,
            'updated': outside_retention_str
        }]
        # if the file expires within a day it should not be returned
        actual_result = main.list_submitted_bucket_items(bucket_items)
        expected_result = []
        self.assertCountEqual(expected_result, actual_result)

        # if the file within retention period it should be returned
        within_retention = datetime.datetime.today() - datetime.timedelta(
            days=25)
        within_retention_str = within_retention.strftime(
            '%Y-%m-%dT%H:%M:%S.%fZ')
        item_2 = {
            'name': '2018-09-01/visit_occurrence.csv',
            'timeCreated': within_retention_str,
            'updated': within_retention_str
        }
        bucket_items.append(item_2)
        expected_result = [item_2]
        actual_result = main.list_submitted_bucket_items(bucket_items)
        self.assertCountEqual(expected_result, actual_result)

        actual_result = main.list_submitted_bucket_items([])
        self.assertCountEqual([], actual_result)

        unknown_item = {
            'name': '2018-09-01/nyc_cu_person.csv',
            'timeCreated': within_retention_str,
            'updated': within_retention_str
        }
        bucket_items = [unknown_item]
        actual_result = main.list_submitted_bucket_items(bucket_items)
        self.assertCountEqual(actual_result, bucket_items)

        ignored_item = dict(name='2018-09-01/' + common.RESULTS_HTML,
                            timeCreated=within_retention_str,
                            updated=within_retention_str)
        bucket_items = [ignored_item]
        actual_result = main.list_submitted_bucket_items(bucket_items)
        self.assertCountEqual([], actual_result)

    def test_folder_list(self):
        fmt = '%Y-%m-%dT%H:%M:%S.%fZ'
        now = datetime.datetime.now()
        t0 = (now - datetime.timedelta(days=3)).strftime(fmt)
        t1 = (now - datetime.timedelta(days=2)).strftime(fmt)
        t2 = (now - datetime.timedelta(days=1)).strftime(fmt)
        t3 = (now - datetime.timedelta(hours=1)).strftime(fmt)
        expected = 't2/'
        bucket_items = [{
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
        }]

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
        bucket_items = [{
            'name': folder_prefix + 'person.csv'
        }, {
            'name': folder_prefix + 'invalid_file.csv'
        }]

        perform_validation_on_file_returns = dict()
        expected_results = []
        expected_errors = []
        expected_warnings = [('invalid_file.csv', 'Unknown file')]
        for file_name in sorted(resources.CDM_FILES) + sorted(common.PII_FILES):
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
            result.append((file_name, found, parsed, loaded))
            perform_validation_on_file_returns[file_name] = result, errors
            expected_results += result
            expected_errors += errors

        def perform_validation_on_file(cdm_file_name, found_cdm_files, hpo_id,
                                       folder_prefix, bucket):
            return perform_validation_on_file_returns.get(cdm_file_name)

        mock_perform_validation_on_file.side_effect = perform_validation_on_file

        actual_result = main.validate_submission(self.hpo_id, self.hpo_bucket,
                                                 bucket_items, folder_prefix)
        self.assertCountEqual(expected_results, actual_result.get('results'))
        self.assertCountEqual(expected_errors, actual_result.get('errors'))
        self.assertCountEqual(expected_warnings, actual_result.get('warnings'))

    @mock.patch('validation.main.gcs_utils.get_hpo_bucket')
    @mock.patch('bq_utils.get_hpo_info')
    @mock.patch('validation.main.list_bucket')
    @mock.patch('logging.exception')
    @mock.patch('api_util.check_cron')
    def test_validate_all_hpos_exception(self, check_cron, mock_logging_error,
                                         mock_list_bucket, mock_hpo_csv,
                                         mock_hpo_bucket):
        mock_hpo_csv.return_value = [{'hpo_id': self.hpo_id}]
        mock_list_bucket.side_effect = googleapiclient.errors.HttpError(
            'fake http error', b'fake http error')
        with main.app.test_client() as c:
            c.get(main_consts.PREFIX + 'ValidateAllHpoFiles')
            expected_call = mock.call(
                'Failed to process hpo_id `{}` due to the following '
                'HTTP error: fake http error'.format(self.hpo_id))
            self.assertIn(expected_call, mock_logging_error.mock_calls)

    def test_extract_date_from_rdr(self):
        rdr_dataset_id = 'rdr20200201'
        bad_rdr_dataset_id = 'ehr2019-02-01'
        expected_date = '2020-02-01'
        rdr_date = main.extract_date_from_rdr_dataset_id(rdr_dataset_id)
        self.assertEqual(rdr_date, expected_date)
        self.assertRaises(ValueError, main.extract_date_from_rdr_dataset_id,
                          bad_rdr_dataset_id)

    @mock.patch('bq_utils.table_exists', mock.MagicMock())
    @mock.patch('bq_utils.query')
    @mock.patch('validation.main.is_valid_folder_prefix_name')
    @mock.patch('validation.main.run_export')
    @mock.patch('validation.main.run_achilles')
    @mock.patch('gcs_utils.upload_object')
    @mock.patch('validation.main.all_required_files_loaded')
    @mock.patch('validation.main.query_rows')
    @mock.patch('validation.main.get_duplicate_counts_query')
    @mock.patch('validation.main._write_string_to_file')
    @mock.patch('validation.main.get_hpo_name')
    @mock.patch('validation.main.validate_submission')
    @mock.patch('validation.main.is_valid_rdr')
    @mock.patch('gcs_utils.list_bucket')
    @mock.patch('gcs_utils.get_hpo_bucket')
    def test_process_hpo_ignore_dirs(
        self, mock_hpo_bucket, mock_bucket_list, mock_valid_rdr,
        mock_validation, mock_get_hpo_name, mock_write_string_to_file,
        mock_get_duplicate_counts_query, mock_query_rows,
        mock_all_required_files_loaded, mock_upload, mock_run_achilles,
        mock_export, mock_valid_folder_name, mock_query):
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
        :param mock_validation: mock generate metrics
        :param mock_valid_rdr: mock valid rdr dataset
        :param mock_upload: mock uploading to a bucket
        :param mock_run_achilles: mock running the achilles reports
        :param mock_export: mock exporting the files
        """
        # pre-conditions
        mock_valid_folder_name.return_value = True
        mock_hpo_bucket.return_value = 'noob'
        mock_all_required_files_loaded.return_value = True
        mock_query.return_value = {}
        mock_query_rows.return_value = []
        mock_get_duplicate_counts_query.return_value = ''
        mock_get_hpo_name.return_value = 'noob'
        mock_write_string_to_file.return_value = ''
        mock_valid_rdr.return_value = True
        yesterday = datetime.datetime.now() - datetime.timedelta(hours=24)
        yesterday = yesterday.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        moment = datetime.datetime.now()
        now = moment.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        mock_bucket_list.return_value = [{
            'name': 'unknown.pdf',
            'timeCreated': now,
            'updated': now
        }, {
            'name': 'participant/no-site/foo.pdf',
            'timeCreated': now,
            'updated': now
        }, {
            'name': 'PARTICIPANT/siteone/foo.pdf',
            'timeCreated': now,
            'updated': now
        }, {
            'name': 'Participant/sitetwo/foo.pdf',
            'timeCreated': now,
            'updated': now
        }, {
            'name': 'submission/person.csv',
            'timeCreated': yesterday,
            'updated': yesterday
        }, {
            'name': 'SUBMISSION/measurement.csv',
            'timeCreated': now,
            'updated': now
        }]

        mock_validation.return_value = {
            'results': [('SUBMISSION/measurement.csv', 1, 1, 1)],
            'errors': [],
            'warnings': []
        }

        # test
        main.process_hpo('noob', force_run=True)

        # post conditions
        self.assertTrue(mock_validation.called)
        self.assertEqual(
            mock_validation.assert_called_once_with(
                'noob', 'noob', mock_bucket_list.return_value, 'SUBMISSION/'),
            None)
        self.assertTrue(mock_run_achilles.called)
        self.assertTrue(mock_export.called)
        self.assertEqual(
            mock_export.assert_called_once_with(hpo_id='noob',
                                                folder_prefix='SUBMISSION/'),
            None)
        # make sure upload is called for only the most recent
        # non-participant directory
        self.assertTrue(mock_upload.called)
        for call in mock_upload.call_args_list:
            args, _ = call
            bucket = args[0]
            filepath = args[1]
            self.assertEqual('noob', bucket)
            self.assertTrue(filepath.startswith('SUBMISSION/'))

    @mock.patch('gcs_utils.copy_object')
    @mock.patch('gcs_utils.list_bucket')
    @mock.patch('gcs_utils.get_drc_bucket')
    @mock.patch('gcs_utils.get_hpo_bucket')
    @mock.patch('api_util.check_cron')
    def test_copy_files_ignore_dir(self, mock_check_cron, mock_hpo_bucket,
                                   mock_drc_bucket, mock_bucket_list,
                                   mock_copy):
        """
        Test copying files to the drc internal bucket.

        This should copy anything in the site's bucket except for files named
        participant.  Copy_files uses a case insensitive match, so any
        capitalization scheme should be detected and left out of the copy.
        Anything else should be copied.  Mocks are used to determine if the
        test ran as expected and all statements would execute in a producstion
        environment.

        :param mock_check_cron: mocks the cron decorator.
        :param mock_hpo_bucket: mock the hpo bucket name.
        :param mock_drc_bucket: mocks the internal drc bucket name.
        :param mock_bucket_list: mocks the list of items in the hpo bucket.
        :param mock_copy: mocks the utility call to actually perform the copy.
        """
        # pre-conditions
        mock_hpo_bucket.return_value = 'noob'
        mock_drc_bucket.return_value = 'unit_test_drc_internal'
        mock_bucket_list.return_value = [{
            'name': 'participant/no-site/foo.pdf',
        }, {
            'name': 'PARTICIPANT/siteone/foo.pdf',
        }, {
            'name': 'Participant/sitetwo/foo.pdf',
        }, {
            'name': 'submission/person.csv',
        }, {
            'name': 'SUBMISSION/measurement.csv',
        }]

        # test
        result = main.copy_files('noob')

        # post conditions
        expected = '{"copy-status": "done"}'
        self.assertEqual(result, expected)
        self.assertTrue(mock_check_cron.called)
        self.assertTrue(mock_hpo_bucket.called)
        self.assertTrue(mock_drc_bucket.called)
        self.assertTrue(mock_bucket_list.called)
        # make sure copy is called for only the non-participant directories
        expected_calls = [
            mock.call(source_bucket='noob',
                      source_object_id='submission/person.csv',
                      destination_bucket='unit_test_drc_internal',
                      destination_object_id='noob/noob/submission/person.csv'),
            mock.call(
                source_bucket='noob',
                source_object_id='SUBMISSION/measurement.csv',
                destination_bucket='unit_test_drc_internal',
                destination_object_id='noob/noob/SUBMISSION/measurement.csv')
        ]
        self.assertTrue(mock_copy.called)
        self.assertEqual(mock_copy.call_count, 2)
        self.assertEqual(
            mock_copy.assert_has_calls(expected_calls, any_order=True), None)

        unexpected_calls = [
            mock.call(
                source_bucket='noob',
                source_object_id='participant/no-site/foo.pdf',
                destination_bucket='unit_test_drc_internal',
                destination_object_id='noob/noob/participant/no-site/foo.pdf'),
            mock.call(
                source_bucket='noob',
                source_object_id='PARTICIPANT/siteone/foo.pdf',
                destination_bucket='unit_test_drc_internal',
                destination_object_id='noob/noob/PARTICIPANT/siteone/foo.pdf'),
            mock.call(
                source_bucket='noob',
                source_object_id='Participant/sitetwo/foo.pdf',
                destination_bucket='unit_test_drc_internal',
                destination_object_id='noob/noob/Participant/sitetwo/foo.pdf')
        ]
        # can't easily use assertRaises here.  3.5 has mock.assert_not_called
        # that should be used when we upgrade instead of this
        for call in unexpected_calls:
            try:
                mock_copy.assert_has_calls([call], any_order=True)
            except AssertionError:
                pass
            else:
                raise AssertionError(
                    "Unexpected call in mock_copy calls:  {}".format(call))

    @mock.patch('bq_utils.table_exists', mock.MagicMock())
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
            raise googleapiclient.errors.HttpError(500, b'bar', 'baz')

        def _write_string_to_file(bucket, filename, content):
            return True

        def get_duplicate_counts_query(hpo_id):
            return ''

        def is_valid_rdr(rdr_dataset_id):
            return True

        with mock.patch.multiple(
                'validation.main',
                all_required_files_loaded=all_required_files_loaded,
                query_rows=query_rows,
                get_duplicate_counts_query=get_duplicate_counts_query,
                _write_string_to_file=_write_string_to_file,
                is_valid_rdr=is_valid_rdr):
            result = main.generate_metrics(self.hpo_id, self.hpo_bucket,
                                           self.folder_prefix, summary)
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
                _write_string_to_file=_write_string_to_file,
                is_valid_rdr=is_valid_rdr):
            with self.assertRaises(googleapiclient.errors.HttpError):
                result = main.generate_metrics(self.hpo_id, self.hpo_bucket,
                                               self.folder_prefix, summary)
                error_occurred = result.get(
                    report_consts.ERROR_OCCURRED_REPORT_KEY)
                self.assertEqual(error_occurred, True)

    @mock.patch('bq_utils.get_hpo_info')
    @mock.patch('validation.main._write_string_to_file')
    def test_html_incorrect_folder_name(self, mock_string_to_file,
                                        mock_hpo_csv):
        mock_hpo_csv.return_value = [{'hpo_id': self.hpo_id}]

        # validate folder name
        self.assertEqual(
            bool(main.is_valid_folder_prefix_name(self.folder_prefix)), True)
        incorrect_folder_prefix = '2020-01-01/'
        self.assertEqual(
            bool(main.is_valid_folder_prefix_name(incorrect_folder_prefix)),
            False)

        # validate report data
        report_data = main.generate_empty_report(self.hpo_id, self.hpo_bucket,
                                                 incorrect_folder_prefix)
        self.assertIn(report_consts.SUBMISSION_ERROR_REPORT_KEY, report_data)
        self.assertIn(incorrect_folder_prefix,
                      report_data[report_consts.SUBMISSION_ERROR_REPORT_KEY])
