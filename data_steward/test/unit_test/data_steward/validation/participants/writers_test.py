# Python imports
import unittest

# Third party imports
from mock import ANY, call, patch
import oauth2client

# Project imports
import constants.validation.participants.writers as consts
import validation.participants.writers as writer


class WritersTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.project = 'foo'
        self.dataset = 'bar'
        self.site = 'rho'

    @patch('validation.participants.writers.gcs_utils.get_drc_bucket')
    @patch('validation.participants.writers.bq_utils.wait_on_jobs')
    @patch('validation.participants.writers.gcs_utils.upload_object')
    @patch('validation.participants.writers.bq_utils.load_csv')
    def test_append_to_result_table(self, mock_load_csv, mock_upload, mock_wait, mock_bucket):
        # pre-conditions
        bucket_name = 'mock_bucket'
        mock_wait.return_value = []
        mock_bucket.return_value = bucket_name
        matches = {1: consts.MATCH, 2: consts.MISMATCH, 3: consts.MISSING}
        field = 'alpha'
        field_list = ['person_id', 'alpha', 'beta', 'phi', 'algorithm']

        # test
        writer.append_to_result_table(
            self.site, matches, self.project, self.dataset, field, field_list
        )

        # post conditions
        self.assertEqual(mock_upload.call_count, 1)
        self.assertEqual(mock_load_csv.call_count, 1)
        self.assertEqual(mock_wait.call_count, 1)

        upload_path = self.dataset + '/intermediate_results/' + self.site + '_' + field + '.csv'
        self.assertEqual(
            mock_upload.assert_called_with(
                bucket_name, upload_path, ANY
            ),
            None
        )

        self.assertEqual(
            mock_load_csv.assert_called_with(
                ANY,
                'gs://' + bucket_name +'/' + upload_path,
                self.project,
                self.dataset,
                self.site + consts.VALIDATION_TABLE_SUFFIX,
                write_disposition=consts.WRITE_APPEND
            ),
            None
        )


    @patch('validation.participants.writers.gcs_utils.get_drc_bucket')
    @patch('validation.participants.writers.gcs_utils.upload_object')
    @patch('validation.participants.writers.bq_utils.load_csv')
    def test_append_to_result_table_error(self, mock_load_csv, mock_upload, mock_bucket):
        # pre-conditions
        bucket_name = 'mock_bucket'
        mock_bucket.return_value = bucket_name
        mock_load_csv.side_effect = oauth2client.client.HttpAccessTokenRefreshError()
        matches = {1: consts.MATCH, 2: consts.MISMATCH, 3: consts.MISSING}
        field = 'alpha'
        field_list = ['person_id', 'alpha', 'beta', 'phi', 'algorithm']

        # test
        self.assertRaises(
            oauth2client.client.HttpAccessTokenRefreshError,
            writer.append_to_result_table,
            self.site,
            matches,
            self.project,
            self.dataset,
            field,
            field_list
        )

        # post conditions
        self.assertEqual(mock_load_csv.call_count, 1)
        self.assertEqual(mock_upload.call_count, 1)
        self.assertEqual(mock_bucket.call_count, 1)

        upload_path = self.dataset + '/intermediate_results/' + self.site + '_' + field + '.csv'
        self.assertEqual(
            mock_upload.assert_called_with(
                bucket_name, upload_path, ANY
            ),
            None
        )

        self.assertEqual(
            mock_load_csv.assert_called_with(
                ANY,
                'gs://' + bucket_name +'/' + upload_path,
                self.project,
                self.dataset,
                self.site + consts.VALIDATION_TABLE_SUFFIX,
                write_disposition=consts.WRITE_APPEND
            ),
            None
        )

    @patch('validation.participants.writers.bq_utils.query')
    def test_remove_sparse_records(self, mock_query):
        # preconditions

        # test
        writer.remove_sparse_records(self.project, self.dataset, self.site)

        # post conditions
        expected_merge = consts.SELECT_FULL_RECORDS.format(
            project=self.project,
            dataset=self.dataset,
            table=self.site + consts.VALIDATION_TABLE_SUFFIX,
            field_one=consts.VALIDATION_FIELDS[0],
            field_two=consts.VALIDATION_FIELDS[1],
            field_three=consts.VALIDATION_FIELDS[2],
            field_four=consts.VALIDATION_FIELDS[3],
            field_five=consts.VALIDATION_FIELDS[4],
            field_six=consts.VALIDATION_FIELDS[5],
            field_seven=consts.VALIDATION_FIELDS[6],
            field_eight=consts.VALIDATION_FIELDS[7],
            field_nine=consts.VALIDATION_FIELDS[8],
            field_ten=consts.VALIDATION_FIELDS[9],
            field_eleven=consts.VALIDATION_FIELDS[10],
            field_twelve=consts.VALIDATION_FIELDS[11]
        )

        self.assertEqual(mock_query.call_count, 1)
        self.assertEqual(
            mock_query.assert_called_with(
                expected_merge,
                batch=True,
                destination_table_id=self.site + consts.VALIDATION_TABLE_SUFFIX,
                destination_dataset_id=self.dataset,
                write_disposition=consts.WRITE_TRUNCATE
            ),
            None
        )

    @patch('validation.participants.writers.bq_utils.query')
    def test_remove_sparse_records_with_errors(self, mock_query):
        # preconditions
        mock_query.side_effect = oauth2client.client.HttpAccessTokenRefreshError()

        # test
        self.assertRaises(
            oauth2client.client.HttpAccessTokenRefreshError,
            writer.remove_sparse_records, self.project, self.dataset, self.site
        )

        # post conditions
        expected_merge = consts.SELECT_FULL_RECORDS.format(
            project=self.project,
            dataset=self.dataset,
            table=self.site + consts.VALIDATION_TABLE_SUFFIX,
            field_one=consts.VALIDATION_FIELDS[0],
            field_two=consts.VALIDATION_FIELDS[1],
            field_three=consts.VALIDATION_FIELDS[2],
            field_four=consts.VALIDATION_FIELDS[3],
            field_five=consts.VALIDATION_FIELDS[4],
            field_six=consts.VALIDATION_FIELDS[5],
            field_seven=consts.VALIDATION_FIELDS[6],
            field_eight=consts.VALIDATION_FIELDS[7],
            field_nine=consts.VALIDATION_FIELDS[8],
            field_ten=consts.VALIDATION_FIELDS[9],
            field_eleven=consts.VALIDATION_FIELDS[10],
            field_twelve=consts.VALIDATION_FIELDS[11]
        )

        self.assertEqual(mock_query.call_count, 1)
        self.assertEqual(
            mock_query.assert_called_with(
                expected_merge,
                batch=True,
                destination_table_id=self.site + consts.VALIDATION_TABLE_SUFFIX,
                destination_dataset_id=self.dataset,
                write_disposition=consts.WRITE_TRUNCATE
            ),
            None
        )

    @patch('validation.participants.writers.bq_utils.query')
    def test_merge_fields_into_single_record(self, mock_query):
        # test
        writer.merge_fields_into_single_record(self.project, self.dataset, self.site)

        # post conditions
        expected_merge = consts.MERGE_UNIFY_SITE_RECORDS.format(
            project=self.project,
            dataset=self.dataset,
            table=self.site + consts.VALIDATION_TABLE_SUFFIX,
            field=consts.VALIDATION_FIELDS[11]
        )

        self.assertEqual(mock_query.call_count, 12)
        self.assertEqual(
            mock_query.assert_called_with(expected_merge, batch=True),
            None
        )

    @patch('validation.participants.writers.bq_utils.query')
    def test_merge_fields_into_single_record_with_errors(self, mock_query):
        # preconditions
        mock_query.side_effect = oauth2client.client.HttpAccessTokenRefreshError()

        # test
        self.assertRaises(
            oauth2client.client.HttpAccessTokenRefreshError,
            writer.merge_fields_into_single_record, self.project, self.dataset, self.site
        )

        # post conditions
        expected_merge = consts.MERGE_UNIFY_SITE_RECORDS.format(
            project=self.project,
            dataset=self.dataset,
            table=self.site + consts.VALIDATION_TABLE_SUFFIX,
            field=consts.VALIDATION_FIELDS[0]
        )

        self.assertEqual(mock_query.call_count, 1)
        self.assertEqual(
            mock_query.assert_called_with(expected_merge, batch=True),
            None
        )

    @patch('validation.participants.writers.bq_utils.query')
    def test_change_nulls_to_missing_value(self, mock_query):
        # preconditions

        # test
        writer.change_nulls_to_missing_value(self.project, self.dataset, self.site)

        # post conditions
        expected_merge = consts.SELECT_SET_MISSING_VALUE.format(
            project=self.project,
            dataset=self.dataset,
            table=self.site + consts.VALIDATION_TABLE_SUFFIX,
            field_one=consts.VALIDATION_FIELDS[0],
            field_two=consts.VALIDATION_FIELDS[1],
            field_three=consts.VALIDATION_FIELDS[2],
            field_four=consts.VALIDATION_FIELDS[3],
            field_five=consts.VALIDATION_FIELDS[4],
            field_six=consts.VALIDATION_FIELDS[5],
            field_seven=consts.VALIDATION_FIELDS[6],
            field_eight=consts.VALIDATION_FIELDS[7],
            field_nine=consts.VALIDATION_FIELDS[8],
            field_ten=consts.VALIDATION_FIELDS[9],
            field_eleven=consts.VALIDATION_FIELDS[10],
            field_twelve=consts.VALIDATION_FIELDS[11],
            value=consts.MISSING,
            person_id=consts.PERSON_ID_FIELD,
            algorithm=consts.ALGORITHM_FIELD
        )

        self.assertEqual(mock_query.call_count, 1)
        self.assertEqual(
            mock_query.assert_called_with(
                expected_merge,
                batch=True,
                destination_table_id=self.site + consts.VALIDATION_TABLE_SUFFIX,
                destination_dataset_id=self.dataset,
                write_disposition=consts.WRITE_TRUNCATE
            ),
            None
        )

    @patch('validation.participants.writers.bq_utils.query')
    def test_change_nulls_to_missing_value_with_errors(self, mock_query):
        # preconditions
        mock_query.side_effect = oauth2client.client.HttpAccessTokenRefreshError()

        # test
        self.assertRaises(
            oauth2client.client.HttpAccessTokenRefreshError,
            writer.change_nulls_to_missing_value, self.project, self.dataset, self.site
        )

        # post conditions
        expected_merge = consts.SELECT_SET_MISSING_VALUE.format(
            project=self.project,
            dataset=self.dataset,
            table=self.site + consts.VALIDATION_TABLE_SUFFIX,
            field_one=consts.VALIDATION_FIELDS[0],
            field_two=consts.VALIDATION_FIELDS[1],
            field_three=consts.VALIDATION_FIELDS[2],
            field_four=consts.VALIDATION_FIELDS[3],
            field_five=consts.VALIDATION_FIELDS[4],
            field_six=consts.VALIDATION_FIELDS[5],
            field_seven=consts.VALIDATION_FIELDS[6],
            field_eight=consts.VALIDATION_FIELDS[7],
            field_nine=consts.VALIDATION_FIELDS[8],
            field_ten=consts.VALIDATION_FIELDS[9],
            field_eleven=consts.VALIDATION_FIELDS[10],
            field_twelve=consts.VALIDATION_FIELDS[11],
            value=consts.MISSING,
            person_id=consts.PERSON_ID_FIELD,
            algorithm=consts.ALGORITHM_FIELD
        )

        self.assertEqual(mock_query.call_count, 1)
        self.assertEqual(
            mock_query.assert_called_with(
                expected_merge,
                batch=True,
                destination_table_id=self.site + consts.VALIDATION_TABLE_SUFFIX,
                destination_dataset_id=self.dataset,
                write_disposition=consts.WRITE_TRUNCATE
            ),
            None
        )

    def test_get_address_match(self):
        # pre conditions
        values = [consts.MATCH, consts.MATCH,
                  consts.MATCH, consts.MATCH, consts.MATCH]
        # test
        actual = writer.get_address_match(values)

        # post condition
        expected = consts.MATCH
        self.assertEqual(actual, expected)

    def test_get_address_match_and_mismatch(self):
        # pre conditions
        values = [consts.MATCH, consts.MATCH,
                  consts.MATCH, consts.MATCH, consts.MISMATCH]
        # test
        actual = writer.get_address_match(values)

        # post condition
        expected = consts.MISMATCH
        self.assertEqual(actual, expected)

    def test_get_address_match_and_missing(self):
        # pre conditions
        values = [consts.MATCH, consts.MATCH,
                  consts.MATCH, consts.MATCH, consts.MISSING]
        # test
        actual = writer.get_address_match(values)

        # post condition
        expected = consts.MISSING
        self.assertEqual(actual, expected)

    def test_get_address_match_mismatch_and_missing(self):
        # pre conditions
        values = [consts.MATCH, consts.MATCH,
                  consts.MATCH, consts.MISMATCH, consts.MISSING]
        # test
        actual = writer.get_address_match(values)

        # post condition
        expected = consts.MISMATCH
        self.assertEqual(actual, expected)

    @patch('validation.participants.writers.gcs_utils.upload_object')
    @patch('validation.participants.writers.bq_utils.large_response_to_rowlist')
    @patch('validation.participants.writers.bq_utils.query')
    @patch('validation.participants.writers.StringIO.StringIO')
    def test_create_site_validation_report(
            self,
            mock_report_file,
            mock_query,
            mock_response,
            mock_upload
    ):
        # preconditions
        bucket = 'abc'
        filename = 'output.csv'
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

        # test
        writer.create_site_validation_report(
            self.project, self.dataset, [self.site], bucket, filename
        )

        # post conditions
        self.assertEqual(mock_report_file.call_count, 1)
        self.assertEqual(mock_query.call_count, len([self.site]))
        self.assertEqual(mock_response.call_count, len([self.site]))
        self.assertEqual(mock_upload.call_count, 1)

        expected_query = consts.VALIDATION_RESULTS_VALUES.format(
            project=self.project,
            dataset=self.dataset,
            table=self.site + consts.VALIDATION_TABLE_SUFFIX,
        )
        self.assertEqual(
            mock_query.assert_called_with(expected_query, batch=True),
            None
        )

        self.assertEqual(
            mock_upload.assert_called_with(
                bucket, filename, ANY
            ),
            None
        )

        expected_report_calls = [
            call(),
            call().write('person_id,first_name,last_name,birth_date,sex,address,phone_number,email,algorithm\n'),
            call().write('1,match,match,match,match,match,match,match,match\n'),
            call().write('2,match,match,no_match,missing,no_match,match,match,match\n'),
            call().seek(0),
            call().close()
        ]
        self.assertEqual(mock_report_file.mock_calls, expected_report_calls)

    @patch('validation.participants.writers.gcs_utils.upload_object')
    @patch('validation.participants.writers.bq_utils.query')
    @patch('validation.participants.writers.StringIO.StringIO')
    def test_create_site_validation_report_with_errors(
            self,
            mock_report_file,
            mock_query,
            mock_upload
    ):
        # preconditions
        mock_query.side_effect = oauth2client.client.HttpAccessTokenRefreshError()

        bucket = 'abc'
        filename = 'output.csv'

        # test
        writer.create_site_validation_report(
            self.project, self.dataset, [self.site], bucket, filename
        )

        # post conditions
        self.assertEqual(mock_report_file.call_count, 1)
        self.assertEqual(mock_query.call_count, len([self.site]))
        self.assertEqual(mock_upload.call_count, 1)

        expected_query = consts.VALIDATION_RESULTS_VALUES.format(
            project=self.project,
            dataset=self.dataset,
            table=self.site + consts.VALIDATION_TABLE_SUFFIX,
        )
        self.assertEqual(
            mock_query.assert_called_with(expected_query, batch=True),
            None
        )

        self.assertEqual(
            mock_upload.assert_called_with(
                bucket, filename, ANY
            ),
            None
        )

        expected_report_calls = [
            call(),
            call().write('person_id,first_name,last_name,birth_date,sex,address,phone_number,email,algorithm\n'),
            call().write("Unable to report id validation match records for site:\t%s.\n", self.site),
            call().seek(0),
            call().close()
        ]
        self.assertEqual(mock_report_file.mock_calls, expected_report_calls)
