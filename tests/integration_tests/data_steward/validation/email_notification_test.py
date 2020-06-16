# Python imports
from unittest import mock, TestCase

# Third party imports

# Project imports
import app_identity
from validation import email_notification as en
from constants.validation import email_notification as consts


class EmailNotificationTest(TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.project_id = app_identity.get_application_id()
        self.assertIn('test', self.project_id)

    def test_hpo_contact_list(self):
        fake_dict = {
            'fake_1': {
                'site_name':
                    'Fake Site Name 1',
                'site_point_of_contact':
                    'fake.email.1@site_1.fakedomain; fake.email.2@site_1.fakedomain'
            },
            'fake_2': {
                'site_name': 'Fake Site Name 2',
                'site_point_of_contact': 'no data steward'
            },
            'fake_3': {
                'site_name':
                    'Fake Site Name 3',
                'site_point_of_contact':
                    'Fake.Email.1@site_3.fake_domain; Fake.Email.2@site_3.fake_domain'
            },
            'fake_4': {
                'site_name':
                    'Fake Site Name 4',
                'site_point_of_contact':
                    'FAKE.EMAIL.1@site4.fakedomain; FAKE.EMAIL.2@site4.fakedomain'
            }
        }
        contact_dict = en.get_hpo_contact_info(self.project_id)
        self.assertEqual(len(contact_dict), 4)
        self.assertDictEqual(contact_dict, fake_dict)
