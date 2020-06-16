# Python imports
import datetime
from unittest import mock, TestCase
import os
import base64
from io import BytesIO

# Third party imports
import dateutil
from matplotlib import image as mpimg

# Project imports
import app_identity
from validation import email_notification as en
from validation.main import get_eastern_time
from constants.validation import email_notification as consts
from tests.test_util import FIVE_PERSON_RESULTS_FILE


class EmailNotificationUnitTest(TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.project_id = app_identity.get_application_id()
        self.hpo_id_1 = 'fake_1'
        self.site_name_1 = 'Fake Site Name 1'
        self.email_1 = [
            'fake.email.1@site_1.fakedomain', 'fake.email.2@site_1.fakedomain'
        ]
        self.hpo_id_2 = 'fake_2'
        self.hpo_id_3 = 'fake_3'
        self.bucket = 'fake'
        self.folder = 'fake_folder'
        self.fake_html_path = f"gs://{self.bucket}/{self.folder}/results.html"
        self.report_data = {
            'folder': self.folder,
            'timestamp': get_eastern_time(),
            'submission_error': False
        }

    @mock.patch('validation.email_notification.get_hpo_contact_info')
    def test_create_recipients_list(self, mock_contact_info):
        mock_fake_1 = {
            'fake_1': {
                'site_name':
                    'Fake Site Name 1',
                'site_point_of_contact':
                    'fake.email.1@site_1.fakedomain; fake.email.2@site_1.fakedomain'
            }
        }
        mock_fake_2 = {
            'fake_2': {
                'site_name': 'Fake Site Name 2',
                'site_point_of_contact': 'no data steward'
            }
        }
        mock_fake_3 = {
            'fake_3': {
                'site_name':
                    'Fake Site Name 3',
                'site_point_of_contact':
                    'Fake.Email.1@site_3.fake_domain; Fake.Email.2@site_3.fake_domain'
            }
        }
        mock_contact_info.side_effect = [mock_fake_1, mock_fake_2, mock_fake_3]
        self.assertIn('test', self.project_id)
        hpo_dict = en.create_recipients_list(self.hpo_id_1)
        self.assertEqual(hpo_dict[consts.SITE_NAME], self.site_name_1)
        for email_dict in hpo_dict[consts.MAIL_TO]:
            self.assertIn(email_dict['email'], self.email_1)
            self.assertEqual(email_dict['type'], 'to')

        hpo_dict = en.create_recipients_list(self.hpo_id_2)
        self.assertEqual(len(hpo_dict[consts.MAIL_TO]), 0)

        hpo_dict = en.create_recipients_list(self.hpo_id_3)
        for email_dict in hpo_dict[consts.MAIL_TO]:
            self.assertTrue(email_dict['email'].islower())

    def test_generate_html_body(self):
        html_body = en.generate_html_body(self.site_name_1, self.fake_html_path,
                                          self.report_data)
        print(html_body)

    def test_aou_logo(self):
        b64_logo = en.get_aou_logo_b64()
        thumbnail_obj = BytesIO()
        thumbnail_obj.write(base64.decodebytes(b64_logo.encode()))
        thumbnail_obj.seek(0)
        thumbnail = mpimg.imread(thumbnail_obj)
        self.assertEqual(thumbnail.shape, (50, 160, 4))

    @mock.patch('validation.email_notification.get_hpo_contact_info')
    def test_generate_email_message(self, mock_fake_info):
        mock_fake_info.return_value = {
            'fake_1': {
                'site_name':
                    'Fake Site Name 1',
                'site_point_of_contact':
                    'fake.email.1@site_1.fakedomain; fake.email.2@site_1.fakedomain'
            }
        }
        with open(FIVE_PERSON_RESULTS_FILE, 'r') as f:
            results_html_str = f.read()
        email_msg = en.generate_email_message(self.hpo_id_1, results_html_str,
                                              self.fake_html_path,
                                              self.report_data)
        print(email_msg.keys())
