import mock
import unittest

from admin import admin_api
from googleapiclient.errors import HttpError


class AdminApiTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        super(AdminApiTest, self).setUp()

    def test_email_body(self):
        expired_keys = [{'service_account_email': 'expired-email-address', 'key_name': 'expired-key-name',
                         'created_at': 'expired-key-created-at'}]
        expiring_keys = [{'service_account_email': 'expiring-email-address', 'key_name': 'expiring-key-name',
                          'created_at': 'expiring-key-created-at'}]

        actual_email_body = admin_api.email_body(expired_keys, expiring_keys)

        expected_email_body = ('# Expired keys deleted\n'
                               'service_account_email=expired-email-address\n'
                               'key_name=expired-key-name\n'
                               'created_at=expired-key-created-at\n'
                               '\n'
                               '# Keys expiring soon\n'
                               'service_account_email=expiring-email-address\n'
                               'key_name=expiring-key-name\n'
                               'created_at=expiring-key-created-at\n')

        self.assertEqual(expected_email_body, actual_email_body)

    @mock.patch('admin.admin_api.LOGGER')
    @mock.patch('admin.admin_api.SUBJECT')
    @mock.patch('admin.admin_api.NOTIFICATION_ADDRESS')
    @mock.patch('admin.admin_api.SENDER_ADDRESS')
    @mock.patch('admin.admin_api.email_body')
    @mock.patch('admin.admin_api.mail.send_mail')
    @mock.patch('admin.key_rotation.get_expiring_keys')
    @mock.patch('admin.key_rotation.delete_expired_keys')
    @mock.patch('api_util.check_cron')
    def test_rm_expired_keys_endpoint_callable(self,
                                               mock_check_cron,
                                               mock_delete_expired_keys,
                                               mock_get_expiring_keys,
                                               mock_send_mail,
                                               mock_email_body,
                                               mock_sender_address,
                                               mock_notification_address,
                                               mock_subject,
                                               mock_logger):
        admin_api.app.testing = True
        with admin_api.app.test_client() as c:
            mock_delete_expired_keys.return_value = [{'key_name': 'expired-key'}]
            mock_get_expiring_keys.return_value = [{'key_name': 'expiring-key'}]
            mock_email_body.return_value = 'Test email body'

            c.get(admin_api.REMOVE_EXPIRED_KEYS_RULE)
            self.assertTrue(mock_delete_expired_keys.called)
            self.assertTrue(mock_get_expiring_keys.called)

            mock_send_mail.assert_called_once_with(sender=mock_sender_address,
                                                   to=mock_notification_address,
                                                   subject=mock_subject,
                                                   body='Test email body')

            mock_send_mail.side_effect = [HttpError(mock.Mock(status=404), 'not found')]
            c.get(admin_api.REMOVE_EXPIRED_KEYS_RULE)

            mock_logger.info.aasert_called_with(
                'Failed to send to {notification_address}'.format(notification_address=mock_notification_address))
