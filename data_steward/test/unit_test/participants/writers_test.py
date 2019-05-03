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

    @patch('validation.participants.writers.bq_utils.query')
    def test_append_to_result_table(self, mock_query):
        # pre-conditions
        matches = {1: consts.MATCH, 2: consts.MISMATCH, 3: consts.MISSING}
        field = 'alpha'

        # test
        writer.append_to_result_table(
            self.site, matches, self.project, self.dataset, field
        )

        # post conditions
        expected_insert_values = (
            "(1, '" + consts.MATCH + "', '"+ consts.YES + "'), "
            "(2, '" + consts.MISMATCH + "', '"+ consts.YES + "'), "
            "(3, '" + consts.MISSING + "', '"+ consts.YES + "')"
        )

        self.assertEqual(mock_query.call_count, 1)
        self.assertEqual(
            mock_query.assert_called_with(
                consts.INSERT_MATCH_VALUES.format(
                    project=self.project,
                    dataset=self.dataset,
                    table=self.site + consts.VALIDATION_TABLE_SUFFIX,
                    field=field,
                    values=expected_insert_values,
                    id_field=consts.PERSON_ID_FIELD,
                    algorithm_field=consts.ALGORITHM_FIELD
                ),
                batch=True
            ),
            None
        )

    @patch('validation.participants.writers.bq_utils.query')
    def test_append_to_result_table_error(self, mock_query):
        # pre-conditions
        mock_query.side_effect = oauth2client.client.HttpAccessTokenRefreshError()
        matches = {1: consts.MATCH, 2: consts.MISMATCH, 3: consts.MISSING}
        field = 'alpha'

        # test
        self.assertRaises(
            oauth2client.client.HttpAccessTokenRefreshError,
            writer.append_to_result_table,
            self.site,
            matches,
            self.project,
            self.dataset,
            field
        )

        # post conditions
        expected_insert_values = (
            "(1, '" + consts.MATCH + "', '"+ consts.YES + "'), "
            "(2, '" + consts.MISMATCH + "', '"+ consts.YES + "'), "
            "(3, '" + consts.MISSING + "', '"+ consts.YES + "')"
        )

        self.assertEqual(mock_query.call_count, 1)
        self.assertEqual(
            mock_query.assert_called_with(
                consts.INSERT_MATCH_VALUES.format(
                    project=self.project,
                    dataset=self.dataset,
                    table=self.site + consts.VALIDATION_TABLE_SUFFIX,
                    field=field,
                    values=expected_insert_values,
                    id_field=consts.PERSON_ID_FIELD,
                    algorithm_field=consts.ALGORITHM_FIELD
                ),
                batch=True
            ),
            None
        )

    @patch('validation.participants.writers.bq_utils.query')
    def test_remove_sparse_records(self, mock_query):
        # preconditions

        # test
        writer.remove_sparse_records(self.project, self.dataset, self.site)

        # post conditions
        expected_merge = consts.MERGE_DELETE_SPARSE_RECORDS.format(
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
            mock_query.assert_called_with(expected_merge, batch=True),
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
        expected_merge = consts.MERGE_DELETE_SPARSE_RECORDS.format(
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
            mock_query.assert_called_with(expected_merge, batch=True),
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
        expected_merge = consts.MERGE_SET_MISSING_FIELDS.format(
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
            value=consts.MISSING
        )

        self.assertEqual(mock_query.call_count, 1)
        self.assertEqual(
            mock_query.assert_called_with(expected_merge, batch=True),
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
        expected_merge = consts.MERGE_SET_MISSING_FIELDS.format(
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
            value=consts.MISSING
        )

        self.assertEqual(mock_query.call_count, 1)
        self.assertEqual(
            mock_query.assert_called_with(expected_merge, batch=True),
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
        result = writer.create_site_validation_report(self.project, self.dataset, [self.site], bucket, filename)

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
            call().write('1,Match,Match,Match,Match,Match,Match,Match,Match\n'),
            call().write('2,Match,Match,NoMatch,Missing,NoMatch,Match,Match,Match\n'),
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
        result = writer.create_site_validation_report(self.project, self.dataset, [self.site], bucket, filename)

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
