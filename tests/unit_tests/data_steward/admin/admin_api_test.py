import unittest
import mock

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
        self.expired_key = {
            'service_account_email': 'expired-email-address',
            'key_name': 'expired-key-name',
            'created_at': 'expired-key-created-at'
        }
        self.expiring_key = {
            'service_account_email': 'expiring-email-address',
            'key_name': 'expiring-key-name',
            'created_at': 'expiring-key-created-at'
        }
        self.expired_keys = [self.expired_key]
        self.expiring_keys = [self.expiring_key]
        self.get_application_id_patcher = mock.patch(
            'app_identity.get_application_id')
        self.mock_get_application_id = self.get_application_id_patcher.start()
        self.addCleanup(self.get_application_id_patcher.stop)

    def test_text_body(self):
        expired_section = '{header}{details}'.format(
            header=admin_api.BODY_HEADER_EXPIRED_KEY_TEMPLATE,
            details=admin_api.BODY_TEMPLATE.format(**self.expired_key))
        expiring_section = '{header}{details}'.format(
            header=admin_api.BODY_HEADER_EXPIRING_KEY_TEMPLATE,
            details=admin_api.BODY_TEMPLATE.format(**self.expiring_key))
        actual = admin_api.text_body(self.expired_keys, [])
        self.assertEqual(actual, expired_section)
        actual = admin_api.text_body([], self.expiring_keys)
        self.assertEqual(actual, expiring_section)
        actual = admin_api.text_body(self.expired_keys, self.expiring_keys)
        self.assertEqual(actual, expired_section + expiring_section)

    @mock.patch('admin.key_rotation.get_expiring_keys')
    @mock.patch('admin.key_rotation.delete_expired_keys')
    @mock.patch('api_util.check_cron')
    def test_rm_expired_keys_endpoint_callable(self, mock_check_cron,
                                               mock_delete_expired_keys,
                                               mock_get_expiring_keys):
        admin_api.app.testing = True
        with admin_api.app.test_client() as c:
            c.get(admin_api.REMOVE_EXPIRED_KEYS_RULE)
            self.assertTrue(mock_delete_expired_keys.called)
            self.assertTrue(mock_get_expiring_keys.called)

    @mock.patch('admin.key_rotation.get_expiring_keys')
    @mock.patch('admin.key_rotation.delete_expired_keys')
    @mock.patch('api_util.check_cron')
    def test_rm_expired_keys_notification(self, mock_check_cron,
                                          mock_delete_expired_keys,
                                          mock_get_expiring_keys):
        mock_delete_expired_keys.side_effect = [
            self.expired_keys, self.expired_keys, []
        ]
        mock_get_expiring_keys.side_effect = [
            self.expiring_keys, [], self.expiring_keys
        ]
        full_body = admin_api.text_body(self.expired_keys, self.expiring_keys)
        expired_section = admin_api.text_body(self.expired_keys, [])
        expiring_section = admin_api.text_body([], self.expiring_keys)

        admin_api.app.testing = True
        with admin_api.app.test_client() as c:
            # test
            with self.assertLogs(level='INFO') as cm:
                c.get(admin_api.REMOVE_EXPIRED_KEYS_RULE)
                c.get(admin_api.REMOVE_EXPIRED_KEYS_RULE)
                c.get(admin_api.REMOVE_EXPIRED_KEYS_RULE)

                # post condition.  assert logs are sent with expected text
                self.assertIn(f'INFO:root:{full_body}', cm.output)
                self.assertIn(f'INFO:root:{expired_section}', cm.output)
                self.assertIn(f'INFO:root:{expiring_section}', cm.output)

    @mock.patch('admin.prod_pid_detection.check_violation')
    @mock.patch('api_util.check_cron')
    def test_detect_pid_violation_endpoint_callable(self, mock_check_cron,
                                                    mock_check_violation):
        admin_api.app.testing = True
        with admin_api.app.test_client() as c:
            c.get(admin_api.DETECT_PID_VIOLATION_RULE)
            self.assertTrue(mock_check_violation.called)
