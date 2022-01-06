# Python imports
import os
import unittest

# Third party imports
from mock import ANY, call, patch, MagicMock
import oauth2client

# Project imports
from constants.validation.participants import writers as consts
from validation.participants import writers as writer


class WritersTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.filename: str = 'fake_filename'
        self.project: str = 'fake_project'
        self.dataset: str = 'fake_dataset'
        self.site: str = 'fake_site'
        self.bucket_name: str = os.environ.get(f'BUCKET_NAME_FAKE')

    @patch('validation.participants.writers.StorageClient')
    @patch('validation.participants.writers.bq_utils.wait_on_jobs')
    @patch('validation.participants.writers.bq_utils.load_csv')
    def test_write_to_result_table(self, mock_load_csv, mock_wait,
                                   mock_storage_client):
        # pre-conditions
        mock_wait.return_value = []
        mock_client = MagicMock()
        mock_bucket = MagicMock()
        mock_blob = MagicMock()

        mock_bucket.name.return_value = self.bucket_name
        mock_bucket.blob.return_value = mock_blob

        mock_storage_client.return_value = mock_client
        mock_client.get_drc_bucket.return_value = mock_bucket

        match: dict = {}
        for field in consts.VALIDATION_FIELDS:
            match[field] = consts.MATCH

        matches: dict = {1: match}

        # test
        writer.write_to_result_table(self.project, self.dataset, self.site,
                                     matches)

        # post conditions
        self.assertEqual(mock_load_csv.call_count, 1)
        self.assertEqual(mock_wait.call_count, 1)
        self.assertEqual(mock_client.get_drc_bucket.call_count, 1)
        self.assertEqual(mock_bucket.blob.call_count, 1)
        self.assertEqual(mock_blob.upload_from_file.call_count, 1)

        upload_path = f'{self.dataset}/intermediate_results/{self.site}.csv'
        mock_bucket.blob.assert_called_with(upload_path)
        mock_blob.upload_from_file.assert_called_with(ANY)

        mock_load_csv.assert_called_with(
            ANY,
            f'gs://{mock_bucket.name}/{upload_path}',
            self.project,
            self.dataset,
            self.site + consts.VALIDATION_TABLE_SUFFIX,
            write_disposition=consts.WRITE_TRUNCATE)

    @patch('validation.participants.writers.StorageClient')
    @patch('validation.participants.writers.StorageClient.get_drc_bucket')
    @patch('validation.participants.writers.bq_utils.load_csv')
    def test_write_to_result_table_error(self, mock_load_csv, mock_bucket,
                                         mock_storage_client):
        # pre-conditions
        mock_client = MagicMock()
        mock_blob = MagicMock()
        mock_bucket.name.return_value = self.bucket_name
        mock_bucket.blob.return_value = mock_blob
        mock_load_csv.side_effect = oauth2client.client.HttpAccessTokenRefreshError(
        )

        mock_storage_client.return_value = mock_client
        mock_client.get_drc_bucket.return_value = mock_bucket

        match: dict = {}
        for field in consts.VALIDATION_FIELDS:
            match[field] = consts.MATCH
        matches: dict = {1: match}

        # test
        self.assertRaises(oauth2client.client.HttpAccessTokenRefreshError,
                          writer.write_to_result_table, self.project,
                          self.dataset, self.site, matches)

        # post conditions
        self.assertEqual(mock_load_csv.call_count, 1)
        self.assertEqual(mock_client.get_drc_bucket.call_count, 1)
        self.assertEqual(mock_bucket.blob.call_count, 1)
        self.assertEqual(mock_blob.upload_from_file.call_count, 1)

        upload_path = f'{self.dataset}/intermediate_results/{self.site}.csv'
        mock_bucket.blob.assert_called_with(upload_path)
        mock_blob.upload_from_file.assert_called_with(ANY)

        mock_load_csv.assert_called_with(
            ANY,
            f'gs://{mock_bucket.name}/{upload_path}',
            self.project,
            self.dataset,
            f'{self.site}{consts.VALIDATION_TABLE_SUFFIX}',
            write_disposition=consts.WRITE_TRUNCATE)

    def test_get_address_match(self):
        # pre conditions
        values: list = [
            consts.MATCH, consts.MATCH, consts.MATCH, consts.MATCH, consts.MATCH
        ]
        # test
        actual = writer.get_address_match(values)

        # post condition
        expected = consts.MATCH
        self.assertEqual(actual, expected)

    def test_get_address_match_and_mismatch(self):
        # pre conditions
        values: list = [
            consts.MATCH, consts.MATCH, consts.MATCH, consts.MATCH,
            consts.MISMATCH
        ]
        # test
        actual = writer.get_address_match(values)

        # post condition
        expected = consts.MISMATCH
        self.assertEqual(actual, expected)

    def test_get_address_match_and_missing(self):
        # pre conditions
        values: list = [
            consts.MATCH, consts.MATCH, consts.MATCH, consts.MATCH,
            consts.MISSING
        ]
        # test
        actual = writer.get_address_match(values)

        # post condition
        expected = consts.MISSING
        self.assertEqual(actual, expected)

    def test_get_address_match_mismatch_and_missing(self):
        # pre conditions
        values: list = [
            consts.MATCH, consts.MATCH, consts.MATCH, consts.MISMATCH,
            consts.MISSING
        ]
        # test
        actual = writer.get_address_match(values)

        # post condition
        expected = consts.MISMATCH
        self.assertEqual(actual, expected)

    @patch('validation.participants.writers.Blob')
    @patch('validation.participants.writers.bq_utils.large_response_to_rowlist')
    @patch('validation.participants.writers.bq_utils.query')
    @patch('validation.participants.writers.StringIO')
    def test_create_site_validation_report(self, mock_string_io, mock_query,
                                           mock_response, mock_blob):

        # preconditions
        mock_response.return_value = [
            {
                consts.ADDRESS_ONE_FIELD: consts.MATCH,
                consts.ADDRESS_TWO_FIELD: consts.MATCH,
                consts.CITY_FIELD: consts.MATCH,
                consts.STATE_FIELD: consts.MATCH,
                consts.ZIP_CODE_FIELD: consts.MATCH,
                consts.PERSON_ID_FIELD: 1,
                consts.FIRST_NAME_FIELD: consts.MATCH,
                consts.LAST_NAME_FIELD: consts.MATCH,
                consts.MIDDLE_NAME_FIELD: consts.MATCH,
                consts.BIRTH_DATE_FIELD: consts.MATCH,
                consts.PHONE_NUMBER_FIELD: consts.MATCH,
                consts.EMAIL_FIELD: consts.MATCH,
                consts.ALGORITHM_FIELD: consts.MATCH,
                consts.SEX_FIELD: consts.MATCH,
            },
            {
                consts.ADDRESS_ONE_FIELD: consts.MATCH,
                consts.ADDRESS_TWO_FIELD: consts.MATCH,
                consts.CITY_FIELD: consts.MATCH,
                consts.STATE_FIELD: consts.MATCH,
                consts.ZIP_CODE_FIELD: consts.MISMATCH,
                consts.PERSON_ID_FIELD: 2,
                consts.FIRST_NAME_FIELD: consts.MATCH,
                consts.LAST_NAME_FIELD: consts.MATCH,
                consts.MIDDLE_NAME_FIELD: consts.MATCH,
                consts.BIRTH_DATE_FIELD: consts.MISMATCH,
                consts.PHONE_NUMBER_FIELD: consts.MATCH,
                consts.EMAIL_FIELD: consts.MATCH,
                consts.ALGORITHM_FIELD: consts.MATCH,
                consts.SEX_FIELD: consts.MISSING,
            },
        ]

        mock_client = MagicMock()
        mock_client.project = self.project

        mock_string_io.return_value = MagicMock()

        mock_blob.name = self.filename
        mock_blob.bucket = self.bucket_name

        # test
        writer.create_site_validation_report(mock_client, self.dataset,
                                             [self.site], mock_blob)

        # post conditions
        self.assertEqual(mock_blob.upload_from_file.call_count, 1)
        self.assertEqual(mock_blob.upload_from_file.call_args,
                         call(mock_string_io.return_value))
        self.assertEqual(mock_query.call_count, len([self.site]))
        self.assertEqual(mock_response.call_count, len([self.site]))

        expected_query: str = consts.VALIDATION_RESULTS_VALUES.format(
            project=self.project,
            dataset=self.dataset,
            table=f'{self.site}{consts.VALIDATION_TABLE_SUFFIX}',
        )
        self.assertEqual(
            mock_query.assert_called_with(expected_query, batch=True), None)

        expected_string_io_calls: list = [
            call(),
            call().write(
                'person_id,first_name,last_name,birth_date,sex,address,phone_number,email,algorithm\n'
            ),
            call().write('1,match,match,match,match,match,match,match,match\n'),
            call().write(
                '2,match,match,no_match,missing,no_match,match,match,match\n'),
            call().seek(0),
            call().close()
        ]
        self.assertEqual(mock_string_io.mock_calls, expected_string_io_calls)

    @patch('validation.participants.writers.Blob')
    @patch('validation.participants.writers.bq_utils.query')
    @patch('validation.participants.writers.StringIO')
    def test_create_site_validation_report_with_errors(self, mock_string_io,
                                                       mock_query, mock_blob):
        # preconditions

        mock_client = MagicMock()
        mock_client.project = self.project

        mock_string_io.return_value = MagicMock()

        mock_query.side_effect = oauth2client.client.HttpAccessTokenRefreshError(
        )

        # test
        writer.create_site_validation_report(mock_client, self.dataset,
                                             [self.site], mock_blob)

        # post conditions
        self.assertEqual(mock_string_io.call_count, 1)
        self.assertEqual(mock_query.call_count, len([self.site]))

        expected_query = consts.VALIDATION_RESULTS_VALUES.format(
            project=self.project,
            dataset=self.dataset,
            table=f'{self.site}{consts.VALIDATION_TABLE_SUFFIX}',
        )
        self.assertEqual(
            mock_query.assert_called_with(expected_query, batch=True), None)

        self.assertEqual(mock_blob.upload_from_file.call_args,
                         call(mock_string_io.return_value))

        expected_string_io_calls: list = [
            call(),
            call().write(
                'person_id,first_name,last_name,birth_date,sex,address,phone_number,email,algorithm\n'
            ),
            call().write(
                "Unable to report id validation match records for site:\t{}.\n".
                format(self.site)),
            call().seek(0),
            call().close()
        ]
        self.assertEqual(mock_string_io.mock_calls, expected_string_io_calls)
