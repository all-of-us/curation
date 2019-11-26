import mock
import unittest
from mock import patch
from admin import admin_api
from slack.errors import SlackClientError


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
        self.get_application_id_patcher = patch('app_identity.get_application_id')
        self.mock_get_application_id = self.get_application_id_patcher.start()

    def tearDown(self):
        self.get_application_id_patcher.stop()

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

    @mock.patch('admin.admin_api.get_slack_client')
    @mock.patch('admin.key_rotation.get_expiring_keys')
    @mock.patch('admin.key_rotation.delete_expired_keys')
    @mock.patch('api_util.check_cron')
    def test_rm_expired_keys_endpoint_callable(self,
                                               mock_check_cron,
                                               mock_delete_expired_keys,
                                               mock_get_expiring_keys,
                                               mock_slack_client):
        admin_api.app.testing = True
        with admin_api.app.test_client() as c:
            c.get(admin_api.REMOVE_EXPIRED_KEYS_RULE)
            self.assertTrue(mock_delete_expired_keys.called)
            self.assertTrue(mock_get_expiring_keys.called)
            self.assertFalse(mock_slack_client.called)

    @mock.patch('admin.admin_api.get_slack_client')
    @mock.patch('admin.key_rotation.get_expiring_keys')
    @mock.patch('admin.key_rotation.delete_expired_keys')
    @mock.patch('api_util.check_cron')
    def test_rm_expired_keys_notification(self,
                                          mock_check_cron,
                                          mock_delete_expired_keys,
                                          mock_get_expiring_keys,
                                          mock_slack_client):
        """

        """
        mock_delete_expired_keys.side_effect = [self.expired_keys, self.expired_keys, []]
        mock_get_expiring_keys.side_effect = [self.expiring_keys, [], self.expiring_keys]
        full_body = admin_api.text_body(self.expired_keys, self.expiring_keys)
        expired_section = admin_api.text_body(self.expired_keys, [])
        expiring_section = admin_api.text_body([], self.expiring_keys)
        mock_post_message = mock_slack_client.return_value.chat_postMessage

        admin_api.app.testing = True
        with admin_api.app.test_client() as c:
            c.get(admin_api.REMOVE_EXPIRED_KEYS_RULE)
            c.get(admin_api.REMOVE_EXPIRED_KEYS_RULE)
            c.get(admin_api.REMOVE_EXPIRED_KEYS_RULE)

            slack_message_args = dict(channel=admin_api.channel_name,
                                      verify=False)

            self.assertCountEqual(mock_post_message.call_args_list,
                                  [mock.call(text=full_body, **slack_message_args),
                                   mock.call(text=expired_section, **slack_message_args),
                                   mock.call(text=expiring_section, **slack_message_args)])

    @mock.patch('admin.admin_api.get_slack_client')
    @mock.patch('admin.key_rotation.get_expiring_keys')
    @mock.patch('admin.key_rotation.delete_expired_keys')
    @mock.patch('api_util.check_cron')
    def test_mail_errors_raised(self,
                                mock_check_cron,
                                mock_delete_expired_keys,
                                mock_get_expiring_keys,
                                mock_slack_client):
        """
        Exceptions while sending mail should be raised
        """
        mock_delete_expired_keys.return_value = self.expired_keys
        mock_get_expiring_keys.return_value = self.expiring_keys
        mock_slack_client.return_value.chat_postMessage.side_effect = SlackClientError()
        admin_api.app.testing = True
        with admin_api.app.test_client() as c:
            with self.assertRaises(SlackClientError):
                c.get(admin_api.REMOVE_EXPIRED_KEYS_RULE)
