# Python imports
from unittest import TestCase

# Third party imports

# Project imports
import app_identity
from validation import email_notification as en


class EmailNotificationTest(TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.project_id = app_identity.get_application_id()

    def test_hpo_contact_list(self):
        self.assertIn('test', self.project_id)
        contact_dict = en.get_hpo_contact_info(self.project_id)
        self.assertEqual(len(contact_dict), 4)
