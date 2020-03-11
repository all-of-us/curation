# Python imports
import unittest

# Third party imports
from mock import patch

# Project imports
from constants.validation.participants import readers as consts
from validation.participants import readers as reader


class ReadersTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    @patch('bq_utils.get_hpo_info')
    def test_get_hpo_site_names(self, mock_csv_list):
        # pre-conditions
        mock_csv_list.return_value = [
            {
                'name': 'Tatooine',
                'hpo_id': 'tatoo'
            },
        ]

        # test
        actual = reader.get_hpo_site_names()

        # post conditions
        expected = ['tatoo']
        self.assertEqual(actual, expected)

    @patch('validation.participants.readers.bq_utils.wait_on_jobs')
    @patch('validation.participants.readers.bq_utils.query')
    def test_create_match_values_table(self, mock_query, mock_wait):
        #pre conditions
        mock_query.return_value = {
            'jobReference': {
                'jobId': 'alpha',
            },
        }
        mock_wait.return_value = []

        # test
        actual = reader.create_match_values_table('project-foo', 'rdr-foo',
                                                  'destination-bar')

        # post conditions
        expected = consts.ID_MATCH_TABLE
        self.assertEqual(actual, expected)
        self.assertEqual(mock_query.call_count, 1)
        self.assertEqual(mock_wait.call_count, 1)

        self.assertEqual(
            mock_query.assert_called_with(
                consts.ALL_PPI_OBSERVATION_VALUES.format(
                    project='project-foo',
                    dataset='rdr-foo',
                    table=consts.OBSERVATION_TABLE,
                    pii_list=','.join(consts.PII_CODES_LIST)),
                destination_dataset_id='destination-bar',
                destination_table_id=consts.ID_MATCH_TABLE,
                write_disposition='WRITE_TRUNCATE',
                batch=True), None)

    @patch('validation.participants.readers.rc.fields_for')
    @patch('validation.participants.readers.bq_utils.large_response_to_rowlist')
    @patch('validation.participants.readers.bq_utils.query')
    def test_get_ehr_person_values_with_duplicate_keys(self, mock_query,
                                                       mock_response,
                                                       mock_fields):
        # pre conditions
        mock_query.return_value = {}
        column_name = 'birth_datetime'
        mock_response.return_value = [
            {
                consts.PERSON_ID_FIELD: 1,
                column_name: 'saLLy',
            },
            {
                consts.PERSON_ID_FIELD: 2,
                column_name: 'Rudy'
            },
            {
                consts.PERSON_ID_FIELD: 3,
                column_name: 'MaTiLdA'
            },
            {
                consts.PERSON_ID_FIELD: 2,
                column_name: 'Rudy'
            },
            {
                consts.PERSON_ID_FIELD: 3,
                column_name: 'mattie'
            },
        ]

        mock_fields.return_value = [{
            'name': column_name,
            'type': consts.DATE_TYPE
        }]

        # test
        actual = reader.get_ehr_person_values('project-foo', 'ehr-bar',
                                              'table-doh', column_name)

        # post-conditions
        expected = {1: 'saLLy', 2: 'Rudy', 3: 'MaTiLdA'}
        self.assertEqual(actual, expected)

        self.assertEqual(mock_query.call_count, 1)
        self.assertEqual(mock_response.call_count, 1)
        self.assertEqual(
            mock_query.assert_called_with(
                consts.EHR_PERSON_VALUES.format(project='project-foo',
                                                dataset='ehr-bar',
                                                table='table-doh',
                                                field=column_name)), None)

    @patch('validation.participants.readers.rc.fields_for')
    @patch('validation.participants.readers.bq_utils.large_response_to_rowlist')
    @patch('validation.participants.readers.bq_utils.query')
    def test_get_ehr_person_values(self, mock_query, mock_response,
                                   mock_fields):
        # pre conditions
        mock_query.return_value = {}
        column_name = 'gender_concept_id'
        mock_response.return_value = [
            {
                consts.PERSON_ID_FIELD: 1,
                column_name: u'saLLy',
            },
            {
                consts.PERSON_ID_FIELD: 2,
                column_name: u'Rudy'
            },
            {
                consts.PERSON_ID_FIELD: 3,
                column_name: u'MaTiLdA'
            },
        ]

        mock_fields.return_value = [{
            'name': column_name,
            'type': consts.DATE_TYPE
        }]

        # test
        actual = reader.get_ehr_person_values('project-foo', 'ehr-bar',
                                              'table-doh', column_name)

        # post-conditions
        expected = {1: 'saLLy', 2: 'Rudy', 3: 'MaTiLdA'}
        self.assertEqual(actual, expected)

        self.assertEqual(mock_query.call_count, 1)
        self.assertEqual(mock_response.call_count, 1)
        self.assertEqual(
            mock_query.assert_called_with(
                consts.EHR_PERSON_VALUES.format(project='project-foo',
                                                dataset='ehr-bar',
                                                table='table-doh',
                                                field=column_name)), None)

    @patch('validation.participants.readers.rc.fields_for')
    @patch('validation.participants.readers.bq_utils.large_response_to_rowlist')
    @patch('validation.participants.readers.bq_utils.query')
    def test_get_rdr_match_values(self, mock_query, mock_response, mock_fields):
        # pre conditions
        mock_query.return_value = {}
        mock_response.return_value = [
            {
                consts.PERSON_ID_FIELD: 1,
                consts.STRING_VALUE_FIELD: 'saLLy',
            },
            {
                consts.PERSON_ID_FIELD: 2,
                consts.STRING_VALUE_FIELD: 'Rudy'
            },
            {
                consts.PERSON_ID_FIELD: 3,
                consts.STRING_VALUE_FIELD: 'MaTiLdA'
            },
        ]

        mock_fields.return_value = [{
            'name': consts.STRING_VALUE_FIELD,
            'type': consts.STRING_TYPE
        }]

        # test
        actual = reader.get_rdr_match_values('project-foo', 'rdr-bar',
                                             'table-oye', 12345)

        # postconditions
        expected = {1: 'saLLy', 2: 'Rudy', 3: 'MaTiLdA'}
        self.assertEqual(actual, expected)
        self.assertEqual(mock_query.call_count, 1)
        self.assertEqual(mock_response.call_count, 1)
        self.assertEqual(
            mock_query.assert_called_with(
                consts.PPI_OBSERVATION_VALUES.format(project='project-foo',
                                                     dataset='rdr-bar',
                                                     table='table-oye',
                                                     field_value=12345)), None)

    @patch('validation.participants.readers.rc.fields_for')
    @patch('validation.participants.readers.bq_utils.large_response_to_rowlist')
    @patch('validation.participants.readers.bq_utils.query')
    def test_get_rdr_match_values_with_duplicates(self, mock_query,
                                                  mock_response, mock_fields):
        # pre conditions
        mock_query.return_value = {}
        mock_response.return_value = [
            {
                consts.PERSON_ID_FIELD: 1,
                consts.STRING_VALUE_FIELD: 'saLLy',
            },
            {
                consts.PERSON_ID_FIELD: 2,
                consts.STRING_VALUE_FIELD: 'Rudy'
            },
            {
                consts.PERSON_ID_FIELD: 3,
                consts.STRING_VALUE_FIELD: 'MaTiLdA'
            },
            {
                consts.PERSON_ID_FIELD: 2,
                consts.STRING_VALUE_FIELD: 'Rudy'
            },
            {
                consts.PERSON_ID_FIELD: 3,
                consts.STRING_VALUE_FIELD: 'mattie'
            },
        ]

        mock_fields.return_value = [{
            'name': consts.STRING_VALUE_FIELD,
            'type': consts.STRING_TYPE
        }]

        # test
        actual = reader.get_rdr_match_values('project-foo', 'rdr-bar',
                                             'table-oye', 12345)

        # postconditions
        expected = {1: 'saLLy', 2: 'Rudy', 3: 'MaTiLdA'}
        self.assertEqual(actual, expected)
        self.assertEqual(mock_query.call_count, 1)
        self.assertEqual(mock_response.call_count, 1)
        self.assertEqual(
            mock_query.assert_called_with(
                consts.PPI_OBSERVATION_VALUES.format(project='project-foo',
                                                     dataset='rdr-bar',
                                                     table='table-oye',
                                                     field_value=12345)), None)

    @patch('validation.participants.readers.rc.fields_for')
    @patch('validation.participants.readers.bq_utils.large_response_to_rowlist')
    @patch('validation.participants.readers.bq_utils.query')
    def test_get_pii_values(self, mock_query, mock_response, mock_fields):
        # pre conditions
        mock_query.return_value = {}
        mock_response.return_value = [
            {
                consts.PERSON_ID_FIELD: 1,
                12345: 'saLLy',
            },
            {
                consts.PERSON_ID_FIELD: 2,
                12345: 'Rudy'
            },
            {
                consts.PERSON_ID_FIELD: 3,
                12345: 'MaTiLdA'
            },
        ]

        mock_fields.return_value = [{
            'name': consts.PERSON_ID_FIELD,
            'type': consts.INTEGER_TYPE
        }]

        # test
        actual = reader.get_pii_values('project-foo', 'pii-bar', 'zeta', '_sea',
                                       12345)

        # postconditions
        expected = [(1, 'saLLy'), (2, 'Rudy'), (3, 'MaTiLdA')]
        self.assertEqual(actual, expected)
        self.assertEqual(mock_query.call_count, 1)
        self.assertEqual(mock_response.call_count, 1)
        self.assertEqual(
            mock_query.assert_called_with(
                consts.PII_VALUES.format(project='project-foo',
                                         dataset='pii-bar',
                                         hpo_site_str='zeta',
                                         table_suffix='_sea',
                                         field=12345)), None)

    @patch('validation.participants.readers.rc.fields_for')
    @patch('validation.participants.readers.bq_utils.large_response_to_rowlist')
    @patch('validation.participants.readers.bq_utils.query')
    def test_get_pii_values_with_duplicates(self, mock_query, mock_response,
                                            mock_fields):
        # pre conditions
        mock_query.return_value = {}
        mock_response.return_value = [
            {
                consts.PERSON_ID_FIELD: 1,
                12345: 'saLLy',
            },
            {
                consts.PERSON_ID_FIELD: 2,
                12345: 'Rudy'
            },
            {
                consts.PERSON_ID_FIELD: 3,
                12345: 'MaTiLdA'
            },
            {
                consts.PERSON_ID_FIELD: 2,
                12345: 'Rudy'
            },
            {
                consts.PERSON_ID_FIELD: 3,
                12345: 89
            },
        ]

        mock_fields.return_value = [{
            'name': consts.PERSON_ID_FIELD,
            'type': consts.INTEGER_TYPE
        }]

        # test
        actual = reader.get_pii_values('project-foo', 'pii-bar', 'zeta', '_sea',
                                       12345)

        # postconditions
        expected = [(1, 'saLLy'), (2, 'Rudy'), (3, 'MaTiLdA'), (2, 'Rudy'),
                    (3, '89')]
        self.assertEqual(actual, expected)
        self.assertEqual(mock_query.call_count, 1)
        self.assertEqual(mock_response.call_count, 1)
        self.assertEqual(
            mock_query.assert_called_with(
                consts.PII_VALUES.format(project='project-foo',
                                         dataset='pii-bar',
                                         hpo_site_str='zeta',
                                         table_suffix='_sea',
                                         field=12345)), None)

    @patch('validation.participants.readers.rc.fields_for')
    @patch('validation.participants.readers.bq_utils.large_response_to_rowlist')
    @patch('validation.participants.readers.bq_utils.query')
    def test_get_location_pii(self, mock_query, mock_response, mock_fields):
        # pre conditions
        mock_query.return_value = {}
        mock_response.side_effect = [[
            {
                consts.PERSON_ID_FIELD: 1,
                consts.LOCATION_ID_FIELD: 85,
            },
            {
                consts.PERSON_ID_FIELD: 2,
                consts.LOCATION_ID_FIELD: 90,
            },
            {
                consts.PERSON_ID_FIELD: 3,
                consts.LOCATION_ID_FIELD: 115,
            },
        ],
                                     [
                                         {
                                             consts.LOCATION_ID_FIELD: 85,
                                             12345: 'Elm Str.'
                                         },
                                         {
                                             consts.LOCATION_ID_FIELD: 90,
                                             12345: '11 Ocean Ave.'
                                         },
                                         {
                                             consts.LOCATION_ID_FIELD: 115,
                                             12345: '1822 RR 25'
                                         },
                                     ]]

        mock_fields.return_value = [{
            'name': consts.LOCATION_ID_FIELD,
            'type': consts.INTEGER_TYPE
        }]

        # test
        actual = reader.get_location_pii('project-foo', 'rdr-bar', 'pii-baz',
                                         'chi', '_sky', 12345)

        # post-conditions
        expected = [(1, 'Elm Str.'), (2, '11 Ocean Ave.'), (3, '1822 RR 25')]
        self.assertEqual(actual, expected)
        self.assertEqual(mock_query.call_count, 2)
        self.assertEqual(mock_response.call_count, 2)
        self.assertEqual(
            mock_query.assert_called_with(
                consts.PII_LOCATION_VALUES.format(project='project-foo',
                                                  dataset='rdr-bar',
                                                  field=12345,
                                                  id_list='85, 90, 115')), None)

    @patch('validation.participants.readers.rc.fields_for')
    @patch('validation.participants.readers.bq_utils.large_response_to_rowlist')
    @patch('validation.participants.readers.bq_utils.query')
    def test_get_ehr_person_values_birthdates(self, mock_query, mock_response,
                                              mock_fields):
        # pre conditions
        mock_query.return_value = {}
        column_name = 'birth_datetime'
        mock_response.return_value = [
            {
                consts.PERSON_ID_FIELD: 1,
                column_name: 16520400.0,
            },
            {
                consts.PERSON_ID_FIELD: 2,
                column_name: -662670000.0,
            },
            {
                consts.PERSON_ID_FIELD: 3,
                column_name: 12459600.0,
            },
        ]

        mock_fields.return_value = [{
            'name': column_name,
            'type': consts.TIMESTAMP_TYPE
        }]

        # test
        actual = reader.get_ehr_person_values('project-foo', 'ehr-bar',
                                              'table-doh', column_name)

        # post-conditions
        expected = {1: '1970-07-11', 2: '1949-01-01', 3: '1970-05-25'}
        self.assertEqual(actual, expected)

        self.assertEqual(mock_query.call_count, 1)
        self.assertEqual(mock_response.call_count, 1)
        self.assertEqual(
            mock_query.assert_called_with(
                consts.EHR_PERSON_VALUES.format(project='project-foo',
                                                dataset='ehr-bar',
                                                table='table-doh',
                                                field=column_name)), None)

    @patch('validation.participants.readers.rc.fields_for')
    @patch('validation.participants.readers.bq_utils.large_response_to_rowlist')
    @patch('validation.participants.readers.bq_utils.query')
    def test_get_ehr_person_values_bytes(self, mock_query, mock_response,
                                         mock_fields):
        # pre conditions
        mock_query.return_value = {}
        column_name = 'foo_field'
        column_value = b'hello'
        mock_response.return_value = [
            {
                consts.PERSON_ID_FIELD: 1,
                column_name: column_value,
            },
        ]

        mock_fields.return_value = [{
            'name': column_name,
            'type': consts.STRING_TYPE
        }]

        # test
        actual = reader.get_ehr_person_values('project-foo', 'ehr-bar',
                                              'table-doh', column_name)

        # post-conditions
        expected = {1: 'hello'}
        self.assertEqual(actual, expected)

        self.assertEqual(mock_query.call_count, 1)
        self.assertEqual(mock_response.call_count, 1)
        self.assertEqual(
            mock_query.assert_called_with(
                consts.EHR_PERSON_VALUES.format(project='project-foo',
                                                dataset='ehr-bar',
                                                table='table-doh',
                                                field=column_name)), None)
