import mock
import unittest

from admin import admin_api


class AdminApiTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        super(AdminApiTest, self).setUp()

    @mock.patch('api_util.check_cron')
    def test_rm_expired_keys_endpoint_callable(self, mock_check_cron):
        admin_api.app.testing = True
        with admin_api.app.test_client() as c:
            with mock.patch('admin.admin_api.key_rotation.delete_expired_keys') as mock_delete_expired_keys:
                c.get(admin_api.REMOVE_EXPIRED_KEYS_RULE)
                self.assertTrue(mock_delete_expired_keys.called)
