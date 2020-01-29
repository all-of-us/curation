import unittest
import datetime
import mock
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
        mock_discovery.assert_called_once_with('iam',
                                               'v1',
                                               credentials='test_credentials')

    @mock.patch('admin.key_rotation.get_iam_service')
    def test_list_service_accounts(self, mock_iam_service):
        expected_accounts = {'accounts': [{'email': 'test-email@test.com'}]}

        mock_service_account_list = mock_iam_service.return_value.projects.return_value. \
            serviceAccounts.return_value.list
        mock_service_account_list.return_value.execute.return_value = expected_accounts

        actual_accounts = key_rotation.list_service_accounts(self.project_id)

        mock_service_account_list.assert_called_once_with(name='projects/' +
                                                          self.project_id)

        self.assertCountEqual(expected_accounts['accounts'], actual_accounts)

    @mock.patch('admin.key_rotation.get_iam_service')
    def test_list_keys_for_service_account(self, mock_iam_service):
        keys = {
            'keys': [{
                'name': 'key-1',
                'validBeforeTime': 'beforeTime-1',
                'validAfterTime': 'afterTime-1'
            }, {
                'name': 'key-2',
                'validBeforeTime': 'beforeTime-2',
                'validAfterTime': 'afterTime-2'
            }]
        }

        mock_service_account_key_list = mock_iam_service.return_value.projects.return_value. \
            serviceAccounts.return_value.keys.return_value.list

        mock_service_account_key_list.return_value.execute.return_value = keys

        actual_values = key_rotation.list_keys_for_service_account(self.email)

        mock_service_account_key_list.assert_called_once_with(
            name='projects/-/serviceAccounts/' + self.email)

        self.assertCountEqual(keys['keys'], actual_values)

    def test_is_key_expired(self):
        today = datetime.datetime.now()

        yesterday = today - datetime.timedelta(days=1)
        ok_key = {
            'validAfterTime': yesterday.strftime(key_rotation.GCP_DTM_FMT)
        }
        self.assertFalse(key_rotation.is_key_expired(ok_key))

        beyond_expire = today - datetime.timedelta(
            days=key_rotation.KEY_EXPIRE_DAYS + 14)
        expired_key = {
            'validAfterTime': beyond_expire.strftime(key_rotation.GCP_DTM_FMT)
        }
        self.assertTrue(key_rotation.is_key_expired(expired_key))

        at_expire = today - datetime.timedelta(
            days=key_rotation.KEY_EXPIRE_DAYS)
        expired_key = {
            'validAfterTime': at_expire.strftime(key_rotation.GCP_DTM_FMT)
        }
        self.assertTrue(key_rotation.is_key_expired(expired_key))

    def test_is_key_expiring(self):
        today = datetime.datetime.now()

        yesterday = today - datetime.timedelta(days=1)
        ok_key = {
            'validAfterTime': yesterday.strftime(key_rotation.GCP_DTM_FMT)
        }
        self.assertFalse(key_rotation.is_key_expired_after_period(ok_key))

        in_alert = today - datetime.timedelta(
            days=key_rotation.KEY_EXPIRE_DAYS -
            key_rotation.KEY_EXPIRE_ALERT_DAYS + 1)
        expiring_key = {
            'validAfterTime': in_alert.strftime(key_rotation.GCP_DTM_FMT)
        }
        self.assertTrue(key_rotation.is_key_expired_after_period(expiring_key))

        at_alert = today - datetime.timedelta(
            days=key_rotation.KEY_EXPIRE_DAYS -
            key_rotation.KEY_EXPIRE_ALERT_DAYS)
        expiring_key = {
            'validAfterTime': at_alert.strftime(key_rotation.GCP_DTM_FMT)
        }
        self.assertTrue(key_rotation.is_key_expired_after_period(expiring_key))

        beyond_expire = today - datetime.timedelta(
            days=key_rotation.KEY_EXPIRE_DAYS)
        expired_key = {
            'validAfterTime': beyond_expire.strftime(key_rotation.GCP_DTM_FMT)
        }
        self.assertTrue(key_rotation.is_key_expired_after_period(expired_key))

    @mock.patch('admin.key_rotation.LOGGER')
    @mock.patch('admin.key_rotation.get_iam_service')
    def test_delete_key(self, mock_iam_service, mock_logger):
        key = {'name': 'key-1'}
        mock_service_account_key_delete = mock_iam_service.return_value.projects.return_value. \
            serviceAccounts.return_value.keys.return_value.delete
        mock_service_account_key_delete.return_value.execute.side_effect = [
            dict(), HttpError(mock.Mock(status=404), b'not found')
        ]

        key_rotation.delete_key(key)
        mock_service_account_key_delete.assert_called_once_with(name='key-1')

        key_rotation.delete_key(key)
        mock_logger.info.aasert_called_with(
            '{full_key_name} is deleted'.format(full_key_name='key-1'))

    @mock.patch('admin.key_rotation.delete_key')
    @mock.patch('admin.key_rotation.is_key_expired')
    @mock.patch('admin.key_rotation.list_keys_for_service_account')
    @mock.patch('admin.key_rotation.list_service_accounts')
    def test_delete_keys_for_project(self, mock_list_service_accounts,
                                     mock_list_keys_for_service_account,
                                     mock_is_key_expired, mock_delete_key):
        mock_list_service_accounts.return_value = [{
            'email': 'test-email@test.com'
        }, {
            'email': 'test-2-email@test.com'
        }]
        mock_list_keys_for_service_account.side_effect = [
            [{
                'name': 'key-1',
                'validAfterTime': 'expired-date-1'
            }, {
                'name': 'key-2',
                'validAfterTime': 'valid-date-1'
            }],
            [{
                'name': 'key-3',
                'validAfterTime': 'expired-date-2'
            }, {
                'name': 'key-4',
                'validAfterTime': 'valid-date-2'
            }]
        ]

        mock_is_key_expired.side_effect = [True, False, True, False]

        actual_deleted_keys = key_rotation.delete_expired_keys(self.project_id)

        mock_list_service_accounts.assert_called_once_with(self.project_id)

        mock_list_keys_for_service_account.assert_any_call(
            'test-email@test.com')

        mock_list_keys_for_service_account.assert_any_call(
            'test-2-email@test.com')

        mock_is_key_expired.assert_any_call({
            'name': 'key-1',
            'validAfterTime': 'expired-date-1'
        })
        mock_is_key_expired.assert_any_call({
            'name': 'key-2',
            'validAfterTime': 'valid-date-1'
        })
        mock_is_key_expired.assert_any_call({
            'name': 'key-3',
            'validAfterTime': 'expired-date-2'
        })
        mock_is_key_expired.assert_any_call({
            'name': 'key-4',
            'validAfterTime': 'valid-date-2'
        })

        mock_delete_key.assert_any_call({
            'name': 'key-1',
            'validAfterTime': 'expired-date-1'
        })
        mock_delete_key.assert_any_call({
            'name': 'key-3',
            'validAfterTime': 'expired-date-2'
        })

        expected_deleted_keys = [{
            'service_account_email': 'test-email@test.com',
            'key_name': 'key-1',
            'created_at': 'expired-date-1'
        }, {
            'service_account_email': 'test-2-email@test.com',
            'key_name': 'key-3',
            'created_at': 'expired-date-2'
        }]

        self.assertCountEqual(expected_deleted_keys, actual_deleted_keys)

    @mock.patch('admin.key_rotation.is_key_expired_after_period')
    @mock.patch('admin.key_rotation.list_keys_for_service_account')
    @mock.patch('admin.key_rotation.list_service_accounts')
    def test_get_expiring_keys_for_project(self, mock_list_service_accounts,
                                           mock_list_keys_for_service_account,
                                           mock_is_key_expired_after_period):
        mock_list_service_accounts.return_value = [{
            'email': 'test-email@test.com'
        }, {
            'email': 'test-2-email@test.com'
        }]
        mock_list_keys_for_service_account.side_effect = [
            [{
                'name': 'key-1',
                'validAfterTime': 'expiring-date-1'
            }, {
                'name': 'key-2',
                'validAfterTime': 'valid-date-1'
            }],
            [{
                'name': 'key-3',
                'validAfterTime': 'expiring-date-2'
            }, {
                'name': 'key-4',
                'validAfterTime': 'valid-date-2'
            }]
        ]

        mock_is_key_expired_after_period.side_effect = [
            True, False, True, False
        ]

        actual_expiring_keys = key_rotation.get_expiring_keys(self.project_id)

        mock_list_service_accounts.assert_called_once_with(self.project_id)

        mock_list_keys_for_service_account.assert_any_call(
            'test-email@test.com')

        mock_list_keys_for_service_account.assert_any_call(
            'test-2-email@test.com')

        mock_is_key_expired_after_period.assert_any_call({
            'name': 'key-1',
            'validAfterTime': 'expiring-date-1'
        })
        mock_is_key_expired_after_period.assert_any_call({
            'name': 'key-2',
            'validAfterTime': 'valid-date-1'
        })
        mock_is_key_expired_after_period.assert_any_call({
            'name': 'key-3',
            'validAfterTime': 'expiring-date-2'
        })
        mock_is_key_expired_after_period.assert_any_call({
            'name': 'key-4',
            'validAfterTime': 'valid-date-2'
        })

        expected_expiring_keys = [{
            'service_account_email': 'test-email@test.com',
            'key_name': 'key-1',
            'created_at': 'expiring-date-1'
        }, {
            'service_account_email': 'test-2-email@test.com',
            'key_name': 'key-3',
            'created_at': 'expiring-date-2'
        }]

        self.assertCountEqual(expected_expiring_keys, actual_expiring_keys)
