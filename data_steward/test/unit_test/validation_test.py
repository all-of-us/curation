"""
Unit test components of data_steward.validation.main
"""
import datetime
import json
import os
import StringIO
import unittest

from google.appengine.ext import testbed
import mock

import bq_utils
import common
import test.unit_test.test_util as test_util
import gcs_utils
import resources
from validation import main


class ValidationTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')


    def setUp(self):
        super(ValidationTest, self).setUp()
        self.testbed = testbed.Testbed()
        self.testbed.activate()
        self.testbed.init_app_identity_stub()
        self.testbed.init_memcache_stub()
        self.testbed.init_urlfetch_stub()
        self.testbed.init_blobstore_stub()
        self.testbed.init_datastore_v3_stub()
        self.hpo_id = test_util.FAKE_HPO_ID
        self.hpo_bucket = gcs_utils.get_hpo_bucket(self.hpo_id)
        self.folder_prefix = '2019-01-01/'
        self._empty_bucket()


    def _empty_bucket(self):
        bucket_items = gcs_utils.list_bucket(self.hpo_bucket)
        for bucket_item in bucket_items:
            gcs_utils.delete_object(self.hpo_bucket, bucket_item['name'])


    def test_all_files_unparseable_output(self):
        # TODO possible bug: if no pre-existing table, results in bq table not found error
        for cdm_table in common.SUBMISSION_FILES:
            test_util.write_cloud_str(self.hpo_bucket, self.folder_prefix + cdm_table, ".\n .")
        bucket_items = gcs_utils.list_bucket(self.hpo_bucket)
        expected_results = [(f, 1, 0, 0) for f in common.SUBMISSION_FILES]
        r = main.validate_submission(self.hpo_id, self.hpo_bucket, bucket_items, self.folder_prefix)
        self.assertSetEqual(set(expected_results), set(r['results']))


    def test_bad_file_names(self):
        bad_file_names = ["avisit_occurrence.csv",
                          "condition_occurence.csv",  # misspelled
                          "person_final.csv",
                          "procedure_occurrence.tsv"]  # unsupported file extension
        expected_warnings = []
        for file_name in bad_file_names:
            test_util.write_cloud_str(self.hpo_bucket, self.folder_prefix + file_name, ".")
            expected_item = (file_name, main.UNKNOWN_FILE)
            expected_warnings.append(expected_item)
        bucket_items = gcs_utils.list_bucket(self.hpo_bucket)
        r = main.validate_submission(self.hpo_id, self.hpo_bucket, bucket_items, self.folder_prefix)
        self.assertListEqual(expected_warnings, r['warnings'])


    def test_retention_checks_list_submitted_bucket_items(self):
        outside_retention = datetime.datetime.today() - datetime.timedelta(days=29)
        outside_retention_str = outside_retention.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        bucket_items = [{'name': '2018-09-01/person.csv',
                         'timeCreated': outside_retention_str,
                         'updated': outside_retention_str}]
        # if the file expires within a day it should not be returned
        actual_result = main.list_submitted_bucket_items(bucket_items)
        expected_result = []
        self.assertListEqual(expected_result, actual_result)

        # if the file within retention period it should be returned
        within_retention = datetime.datetime.today() - datetime.timedelta(days=25)
        within_retention_str = within_retention.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        item_2 = {'name': '2018-09-01/visit_occurrence.csv',
                  'timeCreated': within_retention_str,
                  'updated': within_retention_str}
        bucket_items.append(item_2)
        expected_result = [item_2]
        actual_result = main.list_submitted_bucket_items(bucket_items)
        self.assertListEqual(expected_result, actual_result)

        actual_result = main.list_submitted_bucket_items([])
        self.assertListEqual([], actual_result)

        unknown_item = {'name': '2018-09-01/nyc_cu_person.csv',
                        'timeCreated': within_retention_str,
                        'updated': within_retention_str}
        bucket_items = [unknown_item]
        actual_result = main.list_submitted_bucket_items(bucket_items)
        self.assertListEqual(actual_result, bucket_items)

        ignored_item = dict(name='2018-09-01/' + common.RESULTS_HTML,
                            timeCreated=within_retention_str,
                            updated=within_retention_str)
        bucket_items = [ignored_item]
        actual_result = main.list_submitted_bucket_items(bucket_items)
        self.assertListEqual([], actual_result)


    def table_has_clustering(self, table_info):
        clustering = table_info.get('clustering')
        self.assertIsNotNone(clustering)
        fields = clustering.get('fields')
        self.assertSetEqual(set(fields), {'person_id'})
        time_partitioning = table_info.get('timePartitioning')
        self.assertIsNotNone(time_partitioning)
        tpe = time_partitioning.get('type')
        self.assertEqual(tpe, 'DAY')

    @mock.patch('api_util.check_cron')
    def test_validate_five_persons_success(self, mock_check_cron):
        expected_results = []
        test_file_names = [os.path.basename(f) for f in test_util.FIVE_PERSONS_FILES]

        for cdm_file in common.SUBMISSION_FILES:
            if cdm_file in test_file_names:
                expected_result = (cdm_file, 1, 1, 1)
                test_file = os.path.join(test_util.FIVE_PERSONS_PATH, cdm_file)
                test_util.write_cloud_file(self.hpo_bucket, test_file, prefix=self.folder_prefix)
            else:
                expected_result = (cdm_file, 0, 0, 0)
            expected_results.append(expected_result)
        bucket_items = gcs_utils.list_bucket(self.hpo_bucket)
        r = main.validate_submission(self.hpo_id, self.hpo_bucket, bucket_items, self.folder_prefix)
        self.assertSetEqual(set(r['results']), set(expected_results))

        # check tables exist and are clustered as expected
        for table in common.CDM_TABLES + common.PII_TABLES:
            fields_file = os.path.join(resources.fields_path, table + '.json')
            table_id = bq_utils.get_table_id(test_util.FAKE_HPO_ID, table)
            table_info = bq_utils.get_table_info(table_id)
            with open(fields_file, 'r') as fp:
                fields = json.load(fp)
                field_names = [field['name'] for field in fields]
                if 'person_id' in field_names:
                    self.table_has_clustering(table_info)


    def test_folder_list(self):
        folder_prefix_1 = '2018-03-22-v1/'
        folder_prefix_2 = '2018-03-22-v2/'
        folder_prefix_3 = '2018-03-22-v3/'
        file_list = [folder_prefix_1 + 'person.csv',
                     folder_prefix_2 + 'blah.csv',
                     folder_prefix_3 + 'visit_occurrence.csv',
                     'person.csv']

        for filename in file_list:
            test_util.write_cloud_str(self.hpo_bucket, filename, ".\n .")

        bucket_items = gcs_utils.list_bucket(self.hpo_bucket)
        folder_prefix = main._get_submission_folder(self.hpo_bucket, bucket_items)
        self.assertEqual(folder_prefix, folder_prefix_3)


    def test_check_processed(self):
        test_util.write_cloud_str(self.hpo_bucket, self.folder_prefix + 'person.csv', '\n')
        test_util.write_cloud_str(self.hpo_bucket, self.folder_prefix + common.PROCESSED_TXT, '\n')

        bucket_items = gcs_utils.list_bucket(self.hpo_bucket)
        result = main._get_submission_folder(self.hpo_bucket, bucket_items, force_process=False)
        self.assertIsNone(result)
        result = main._get_submission_folder(self.hpo_bucket, bucket_items, force_process=True)
        self.assertEqual(result, self.folder_prefix)

    @mock.patch('api_util.check_cron')
    def test_copy_five_persons(self, mock_check_cron):
        # upload all five_persons files
        for cdm_file in test_util.FIVE_PERSONS_FILES:
            test_util.write_cloud_file(self.hpo_bucket, cdm_file, prefix=self.folder_prefix)
            test_util.write_cloud_file(self.hpo_bucket, cdm_file, prefix=self.folder_prefix + self.folder_prefix)

        main.app.testing = True
        with main.app.test_client() as c:
            c.get(test_util.COPY_HPO_FILES_URL)
            prefix = test_util.FAKE_HPO_ID + '/' + self.hpo_bucket + '/' + self.folder_prefix
            expected_bucket_items = [prefix + item.split(os.sep)[-1] for item in test_util.FIVE_PERSONS_FILES]
            expected_bucket_items.extend([prefix + self.folder_prefix + item.split(os.sep)[-1] for item in
                                          test_util.FIVE_PERSONS_FILES])

            list_bucket_result = gcs_utils.list_bucket(gcs_utils.get_drc_bucket())
            actual_bucket_items = [item['name'] for item in list_bucket_result]
            self.assertSetEqual(set(expected_bucket_items), set(actual_bucket_items))


    def test_target_bucket_upload(self):
        bucket_nyc = gcs_utils.get_hpo_bucket('nyc')
        folder_prefix = 'test-folder-fake/'
        test_util.empty_bucket(bucket_nyc)

        main._upload_achilles_files(hpo_id=None, folder_prefix=folder_prefix, target_bucket=bucket_nyc)
        actual_bucket_files = set([item['name'] for item in gcs_utils.list_bucket(bucket_nyc)])
        expected_bucket_files = set(['test-folder-fake/' + item for item in common.ALL_ACHILLES_INDEX_FILES])
        self.assertSetEqual(expected_bucket_files, actual_bucket_files)

    @mock.patch('api_util.check_cron')
    def test_curation_report_ignored(self, mock_check_cron):
        exclude_file_list = ["person.csv"]
        exclude_file_list = [self.folder_prefix + item for item in exclude_file_list]
        expected_result_items = []
        for file_name in exclude_file_list:
            test_util.write_cloud_str(self.hpo_bucket, file_name, ".")

        main.app.testing = True
        with main.app.test_client() as c:
            c.get(test_util.VALIDATE_HPO_FILES_URL)

        # check content of the bucket is correct
        expected_bucket_items = exclude_file_list + [self.folder_prefix + item for item in common.IGNORE_LIST]
        list_bucket_result = gcs_utils.list_bucket(self.hpo_bucket)
        actual_bucket_items = [item['name'] for item in list_bucket_result]
        actual_bucket_items = [item for item in actual_bucket_items
                               if not main._is_string_excluded_file(item[len(self.folder_prefix):])]
        self.assertSetEqual(set(expected_bucket_items), set(actual_bucket_items))

        # check that the errors file is empty
        bucket_items = gcs_utils.list_bucket(self.hpo_bucket)
        r = main.validate_submission(self.hpo_id, self.hpo_bucket, bucket_items, self.folder_prefix)
        self.assertListEqual(expected_result_items, r['errors'])

    @mock.patch('api_util.check_cron')
    def test_pii_files_loaded(self, mock_check_cron):
        # tests if pii files are loaded
        test_file_paths = [test_util.PII_NAME_FILE, test_util.PII_MRN_BAD_PERSON_ID_FILE]
        test_file_names = [os.path.basename(f) for f in test_file_paths]
        test_util.write_cloud_file(self.hpo_bucket, test_util.PII_NAME_FILE, prefix=self.folder_prefix)
        test_util.write_cloud_file(self.hpo_bucket, test_util.PII_MRN_BAD_PERSON_ID_FILE, prefix=self.folder_prefix)

        rs = resources._csv_to_list(test_util.PII_FILE_LOAD_RESULT_CSV)
        expected_results = [(r['file_name'], int(r['found']), int(r['parsed']), int(r['loaded'])) for r in rs]
        for f in common.SUBMISSION_FILES:
            if f not in test_file_names:
                expected_result = (f, 0, 0, 0)
                expected_results.append(expected_result)

        bucket_items = gcs_utils.list_bucket(self.hpo_bucket)
        r = main.validate_submission(self.hpo_id, self.hpo_bucket, bucket_items, self.folder_prefix)
        self.assertSetEqual(set(expected_results), set(r['results']))

    @mock.patch('api_util.check_cron')
    def test_html_report_person_only(self, mock_check_cron):
        folder_prefix = '2019-01-01/'
        test_util.write_cloud_str(self.hpo_bucket, folder_prefix + 'person.csv', ".\n .,.,.")

        with open(test_util.PERSON_ONLY_RESULTS_FILE, 'r') as f:
            expected_result_file = f.read()

        main.app.testing = True
        with main.app.test_client() as c:
            c.get(test_util.VALIDATE_HPO_FILES_URL)

            actual_result = test_util.read_cloud_file(self.hpo_bucket, folder_prefix + common.RESULTS_HTML)
            actual_result_file = StringIO.StringIO(actual_result).getvalue()
            self.assertEqual(expected_result_file, actual_result_file)

    @mock.patch('api_util.check_cron')
    def test_html_report_five_person(self, mock_check_cron):
        folder_prefix = '2019-01-01/'
        for cdm_file in test_util.FIVE_PERSONS_FILES:
            test_util.write_cloud_file(self.hpo_bucket, cdm_file, prefix=folder_prefix)
        with open(test_util.FIVE_PERSON_RESULTS_FILE, 'r') as f:
            expected_result = f.read()
        main.app.testing = True
        with main.app.test_client() as c:
            c.get(test_util.VALIDATE_HPO_FILES_URL)
            actual_result = test_util.read_cloud_file(self.hpo_bucket, folder_prefix + common.RESULTS_HTML)
            actual_result_file = StringIO.StringIO(actual_result).getvalue()
            self.assertEqual(expected_result, actual_result_file)


    @mock.patch('validation.main.run_export')
    @mock.patch('validation.main.run_achilles')
    @mock.patch('gcs_utils.upload_object')
    @mock.patch('validation.main.validate_submission')
    @mock.patch('gcs_utils.list_bucket')
    @mock.patch('gcs_utils.get_hpo_bucket')
    def test_process_hpo_ignore_dirs(
            self,
            mock_hpo_bucket,
            mock_bucket_list,
            mock_validation,
            mock_upload,
            mock_run_achilles,
            mock_export):
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
        :param mock_upload: mock uploading to a bucket
        :param mock_run_achilles: mock running the achilles reports
        :param mock_export: mock exporting the files
        """

        # pre-conditions
        mock_hpo_bucket.return_value = 'noob'
        yesterday = datetime.datetime.now() - datetime.timedelta(hours=24)
        yesterday = yesterday.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        moment = datetime.datetime.now()
        now = moment.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        mock_bucket_list.return_value = [
            {'name':  'unknown.pdf', 'timeCreated': now, 'updated': now},
            {'name':  'participant/no-site/foo.pdf', 'timeCreated': now, 'updated': now},
            {'name':  'PARTICIPANT/siteone/foo.pdf', 'timeCreated': now, 'updated': now},
            {'name':  'Participant/sitetwo/foo.pdf', 'timeCreated': now, 'updated': now},
            {'name':  'submission/person.csv', 'timeCreated': yesterday, 'updated': yesterday},
            {'name':  'SUBMISSION/measurement.csv', 'timeCreated': now, 'updated': now}
        ]

        mock_validation.return_value = {
            'results':[('SUBMISSION/measurement.csv', 1, 1, 1)],
            'errors': [],
            'warnings': []}
        mock_export.return_value = '{"success":  "true"}'

        # test
        main.process_hpo('noob', force_run=True)

        # post conditions
        self.assertTrue(mock_validation.called)
        self.assertEqual(
            mock_validation.assert_called_once_with(
                'noob', 'noob', mock_bucket_list.return_value, 'SUBMISSION/'
            ),
            None
        )
        self.assertTrue(mock_run_achilles.called)
        self.assertTrue(mock_export.called)
        self.assertEqual(
            mock_export.assert_called_once_with(
                hpo_id='noob', folder_prefix='SUBMISSION/'
            ),
            None
        )
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
    def test_copy_files_ignore_dir(
            self,
            mock_check_cron,
            mock_hpo_bucket,
            mock_drc_bucket,
            mock_bucket_list,
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
        mock_bucket_list.return_value = [
            {'name':  'participant/no-site/foo.pdf',},
            {'name':  'PARTICIPANT/siteone/foo.pdf',},
            {'name':  'Participant/sitetwo/foo.pdf',},
            {'name':  'submission/person.csv',},
            {'name':  'SUBMISSION/measurement.csv',}
        ]

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
            mock.call(source_bucket='noob',
                      source_object_id='SUBMISSION/measurement.csv',
                      destination_bucket='unit_test_drc_internal',
                      destination_object_id='noob/noob/SUBMISSION/measurement.csv')
        ]
        self.assertTrue(mock_copy.called)
        self.assertEqual(mock_copy.call_count, 2)
        self.assertEqual(mock_copy.assert_has_calls(expected_calls, any_order=True), None)

        unexpected_calls = [
            mock.call(source_bucket='noob',
                      source_object_id='participant/no-site/foo.pdf',
                      destination_bucket='unit_test_drc_internal',
                      destination_object_id='noob/noob/participant/no-site/foo.pdf'),
            mock.call(source_bucket='noob',
                      source_object_id='PARTICIPANT/siteone/foo.pdf',
                      destination_bucket='unit_test_drc_internal',
                      destination_object_id='noob/noob/PARTICIPANT/siteone/foo.pdf'),
            mock.call(source_bucket='noob',
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
                raise AssertionError("Unexpected call in mock_copy calls:  {}"
                                     .format(call))


    def tearDown(self):
        self._empty_bucket()
        bucket_nyc = gcs_utils.get_hpo_bucket('nyc')
        test_util.empty_bucket(bucket_nyc)
        test_util.empty_bucket(gcs_utils.get_drc_bucket())
        self.testbed.deactivate()
