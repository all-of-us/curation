import unittest
import datetime
from mock import mock
from admin import key_rotation
from googleapiclient.errors import HttpError


class KeyRotationTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.project_id = 'mock-project-id'
        self.email = 'test-email@test.com'

    @mock.patch('googleapiclient.discovery.build')
    @mock.patch('oauth2client.client.GoogleCredentials.get_application_default')
    def test_get_iam_service(self, mock_google_credentials, mock_discovery):
        mock_google_credentials.return_value = 'test_credentials'
        key_rotation.get_iam_service()
        mock_discovery.assert_called_once_with('iam', 'v1', credentials='test_credentials')

    @mock.patch('admin.key_rotation.get_iam_service')
    def test_list_service_accounts(self, mock_iam_service):
        expected_accounts = {'accounts': [{'email': 'test-email@test.com'}]}

        mock_service_account_list = mock_iam_service.return_value.projects.return_value. \
            serviceAccounts.return_value.list
        mock_service_account_list.return_value.execute.return_value = expected_accounts

        actual_accounts = key_rotation.list_service_accounts(self.project_id)

        mock_service_account_list.assert_called_once_with(name='projects/' + self.project_id)

        self.assertItemsEqual(expected_accounts['accounts'], actual_accounts)

    @mock.patch('admin.key_rotation.get_iam_service')
    def test_list_keys_for_service_account(self, mock_iam_service):
        keys = {
            'keys': [{'name': 'key-1', 'validBeforeTime': 'beforeTime-1', 'validAfterTime': 'afterTime-1'},
                     {'name': 'key-2', 'validBeforeTime': 'beforeTime-2', 'validAfterTime': 'afterTime-2'}]}

        mock_service_account_key_list = mock_iam_service.return_value.projects.return_value. \
            serviceAccounts.return_value.keys.return_value.list

        mock_service_account_key_list.return_value.execute.return_value = keys

        actual_values = key_rotation.list_keys_for_service_account(self.email)

        mock_service_account_key_list.assert_called_once_with(name='projects/-/serviceAccounts/' + self.email)

        self.assertItemsEqual(keys['keys'], actual_values)

    @mock.patch('datetime.date')
    def test_is_key_expired(self, mock_date):
        mock_date.return_value.today.return_value = datetime.date(2019, 8, 1)
        expired_key = {'validAfterTime': '2019-01-1T22:11:10Z'}
        valid_key = {'validAfterTime': '2019-06-1T22:11:10Z'}
        self.assertTrue(key_rotation.is_key_expired(expired_key))
        self.assertFalse(key_rotation.is_key_expired(valid_key))

    @mock.patch('admin.key_rotation.LOGGER')
    @mock.patch('admin.key_rotation.get_iam_service')
    def test_delete_key(self, mock_iam_service, mock_logger):
        key = {'name': 'key-1'}
        mock_service_account_key_delete = mock_iam_service.return_value.projects.return_value. \
            serviceAccounts.return_value.keys.return_value.delete
        mock_service_account_key_delete.return_value.execute.side_effect = [dict(), HttpError(mock.Mock(status=404),
                                                                                              'not found')]

        key_rotation.delete_key(key)
        mock_service_account_key_delete.assert_called_once_with(name='key-1')

        key_rotation.delete_key(key)
        mock_logger.info.aasert_called_with('{full_key_name} is deleted'.format(full_key_name='key-1'))

    @mock.patch('admin.key_rotation.delete_key')
    @mock.patch('admin.key_rotation.is_key_expired')
    @mock.patch('admin.key_rotation.list_keys_for_service_account')
    @mock.patch('admin.key_rotation.list_service_accounts')
    def test_delete_keys_for_project(self, mock_list_service_accounts,
                                     mock_list_keys_for_service_account,
                                     mock_is_key_expired,
                                     mock_delete_key):
        mock_list_service_accounts.return_value = [{'email': 'test-email@test.com'},
                                                   {'email': 'test-2-email@test.com'}]
        mock_list_keys_for_service_account.side_effect = [[{'id': 'key-1'},
                                                          {'id': 'key-2'}],
                                                         [{'id': 'key-3'},
                                                          {'id': 'key-4'}]]

        mock_is_key_expired.side_effect = [True, False, True, False]

        key_rotation.delete_expired_keys(self.project_id)

        mock_list_service_accounts.assert_called_once_with(self.project_id)

        mock_list_keys_for_service_account.assert_any_call('test-email@test.com')

        mock_list_keys_for_service_account.assert_any_call('test-2-email@test.com')

        mock_is_key_expired.assert_any_call({'id': 'key-1'})
        mock_is_key_expired.assert_any_call({'id': 'key-2'})
        mock_is_key_expired.assert_any_call({'id': 'key-3'})
        mock_is_key_expired.assert_any_call({'id': 'key-4'})

        mock_delete_key.assert_any_call({'id': 'key-1'})
        mock_delete_key.assert_any_call({'id': 'key-3'})
