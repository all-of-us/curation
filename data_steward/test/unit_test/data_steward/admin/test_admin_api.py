import os
import mock
import unittest
from mock import patch

from admin import admin_api


class AdminApiTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        super(AdminApiTest, self).setUp()
        self.maxDiff = None
        self.expired_key = {'service_account_email': 'expired-email-address',
                            'key_name': 'expired-key-name',
                            'created_at': 'expired-key-created-at'}
        self.expiring_key = {'service_account_email': 'expiring-email-address',
                             'key_name': 'expiring-key-name',
                             'created_at': 'expiring-key-created-at'}
        self.expired_keys = [self.expired_key]
        self.expiring_keys = [self.expiring_key]

        self.mock_slack_token = patch.dict(os.environ, {'SLACK_TOKEN': 'fake token id'})

    def test_text_body(self):
        expired_section = '{header}{details}'.format(header=admin_api.BODY_HEADER_EXPIRED_KEY_TEMPLATE,
                                                     details=admin_api.BODY_TEMPLATE.format(**self.expired_key))
        expiring_section = '{header}{details}'.format(header=admin_api.BODY_HEADER_EXPIRING_KEY_TEMPLATE,
                                                      details=admin_api.BODY_TEMPLATE.format(**self.expiring_key))
        actual = admin_api.text_body(self.expired_keys, [])
        self.assertEqual(actual, expired_section)
        actual = admin_api.text_body([], self.expiring_keys)
        self.assertEqual(actual, expiring_section)
        actual = admin_api.text_body(self.expired_keys, self.expiring_keys)
        self.assertEqual(actual, expired_section + expiring_section)

    @mock.patch('admin.admin_api.slack.WebClient')
    def test_channel_exists(self, mock_slack_client):
        mock_slack_client.channels_list.return_value = {
            'channels': [{'name': 'channel_1'}, {'name': 'channel_2'}]}
        self.assertTrue(admin_api.channel_exists('channel_1'))
        self.assertTrue(admin_api.channel_exists('channel_2'))
        self.assertFalse(admin_api.channel_exists('channel_3'))

    @mock.patch('admin.key_rotation.get_expiring_keys')
    @mock.patch('admin.key_rotation.delete_expired_keys')
    @mock.patch('api_util.check_cron')
    def test_rm_expired_keys_endpoint_callable(self,
                                               mock_check_cron,
                                               mock_delete_expired_keys,
                                               mock_get_expiring_keys, ):
        admin_api.app.testing = True
        with admin_api.app.test_client() as c:
            c.get(admin_api.REMOVE_EXPIRED_KEYS_RULE)
            self.assertTrue(mock_delete_expired_keys.called)
            self.assertTrue(mock_get_expiring_keys.called)

    @mock.patch('admin.admin_api.NOTIFICATION_ADDRESS')
    @mock.patch('admin.admin_api.mail.send_mail')
    @mock.patch('admin.key_rotation.get_expiring_keys')
    @mock.patch('admin.key_rotation.delete_expired_keys')
    @mock.patch('api_util.check_cron')
    def test_rm_expired_keys_notification(self,
                                          mock_check_cron,
                                          mock_delete_expired_keys,
                                          mock_get_expiring_keys,
                                          mock_send_mail,
                                          mock_notification_address):
        """

        """
        mock_delete_expired_keys.side_effect = [self.expired_keys, self.expired_keys, []]
        mock_get_expiring_keys.side_effect = [self.expiring_keys, [], self.expiring_keys]
        full_body = admin_api.email_body(self.expired_keys, self.expiring_keys)
        expired_section = admin_api.email_body(self.expired_keys, [])
        expiring_section = admin_api.email_body([], self.expiring_keys)

        admin_api.app.testing = True
        with admin_api.app.test_client() as c:
            c.get(admin_api.REMOVE_EXPIRED_KEYS_RULE)
            c.get(admin_api.REMOVE_EXPIRED_KEYS_RULE)
            c.get(admin_api.REMOVE_EXPIRED_KEYS_RULE)
            send_mail_args = dict(sender=admin_api.SENDER_ADDRESS,
                                  to=mock_notification_address,
                                  subject=admin_api.SUBJECT)
            self.assertCountEqual(mock_send_mail.call_args_list,
                                  [mock.call(body=full_body, **send_mail_args),
                                   mock.call(body=expired_section, **send_mail_args),
                                   mock.call(body=expiring_section, **send_mail_args)])

    @mock.patch('admin.admin_api.NOTIFICATION_ADDRESS')
    @mock.patch('admin.admin_api.mail.send_mail')
    @mock.patch('admin.key_rotation.get_expiring_keys')
    @mock.patch('admin.key_rotation.delete_expired_keys')
    @mock.patch('api_util.check_cron')
    def test_mail_errors_raised(self,
                                mock_check_cron,
                                mock_delete_expired_keys,
                                mock_get_expiring_keys,
                                mock_send_mail,
                                mock_notification_address):
        """
        Exceptions while sending mail should be raised
        """
        mock_delete_expired_keys.return_value = self.expired_keys
        mock_get_expiring_keys.return_value = self.expiring_keys
        mock_send_mail.side_effect = admin_api.mail.InvalidEmailError()
        admin_api.app.testing = True
        with admin_api.app.test_client() as c:
            with self.assertRaises(admin_api.mail.InvalidEmailError):
                c.get(admin_api.REMOVE_EXPIRED_KEYS_RULE)
