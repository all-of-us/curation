import unittest
import datetime
from mock import patch, mock
from admin import key_rotation


class KeyRotationTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.project_id = 'mock-project-id'
        self.email = 'test-email@test.com'
        self.mock_iam_service_patcher = patch('admin.key_rotation.get_iam_service')
        self.mock_iam_service = self.mock_iam_service_patcher.start()

    def tearDown(self):
        self.mock_iam_service_patcher.stop()

    def test_list_service_accounts(self):
        expected_accounts = {'accounts': [{'email': 'test-email@test.com'}]}

        mock_service_account_list = self.mock_iam_service.return_value.projects.return_value. \
            serviceAccounts.return_value.list
        mock_service_account_list.return_value.execute.return_value = expected_accounts

        actual_accounts = key_rotation.list_service_accounts(self.project_id)

        mock_service_account_list.assert_called_once_with(name='projects/' + self.project_id)

        self.assertItemsEqual(expected_accounts['accounts'], actual_accounts)

    def test_list_key_for_service_account(self):
        keys = {
            'keys': [{'name': 'key-1', 'validBeforeTime': 'beforeTime-1', 'validAfterTime': 'afterTime-1'},
                     {'name': 'key-2', 'validBeforeTime': 'beforeTime-2', 'validAfterTime': 'afterTime-2'}]}

        expected_values = [{'id': 'key-1', 'validBeforeTime': 'beforeTime-1', 'validAfterTime': 'afterTime-1',
                            'email': 'test-email@test.com'},
                           {'id': 'key-2', 'validBeforeTime': 'beforeTime-2', 'validAfterTime': 'afterTime-2',
                            'email': 'test-email@test.com'}]

        mock_service_account_key_list = self.mock_iam_service.return_value.projects.return_value. \
            serviceAccounts.return_value.keys.return_value.list

        mock_service_account_key_list.return_value.execute.return_value = keys

        actual_values = key_rotation.list_key_for_service_account(self.email)

        mock_service_account_key_list.assert_called_once_with(name='projects/-/serviceAccounts/' + self.email)

        self.assertItemsEqual(expected_values, actual_values)

    @mock.patch('datetime.date')
    def test_is_key_expired(self, mock_date):
        mock_date.return_value.today.return_value = datetime.date(2019, 8, 1)
        expired_key = {'validAfterTime': '2019-01-1T22:11:10Z'}
        valid_key = {'validAfterTime': '2019-06-1T22:11:10Z'}
        self.assertTrue(key_rotation.is_key_expired(expired_key))
        self.assertFalse(key_rotation.is_key_expired(valid_key))
