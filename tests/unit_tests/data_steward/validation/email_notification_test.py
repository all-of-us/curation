# Python imports
from unittest import mock, TestCase
import base64
from io import BytesIO

# Third party imports
from matplotlib import image as mpimg

# Project imports
import app_identity
from validation import email_notification as en
from validation.main import get_eastern_time
from constants.validation import email_notification as consts


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
        self.hpo_id_4 = 'fake_4'
        self.hpo_id_4 = 'fake_5'
        self.bucket = 'fake'
        self.folder = 'fake_folder'
        self.fake_html_path = f"gs://{self.bucket}/{self.folder}/results.html"
        self.report_data = {
            'folder': self.folder,
            'timestamp': get_eastern_time(),
            'submission_error': False
        }
        self.expected_mail_to_1 = [{
            'email': self.email_1[0],
            'type': 'to'
        }, {
            'email': self.email_1[1],
            'type': 'to'
        }, {
            'email': consts.DATA_CURATION_LISTSERV,
            'type': 'cc'
        }]

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
        mock_fake_4 = {
            'fake_4': {
                'site_name': '',
                'site_point_of_contact': 'No data steward'
            }
        }
        mock_contact_info.side_effect = [
            mock_fake_1, mock_fake_2, mock_fake_3, mock_fake_4
        ]
        self.assertIn('test', self.project_id)
        hpo_dict = en.create_recipients_list(self.hpo_id_1)
        self.assertEqual(hpo_dict[consts.SITE_NAME], self.site_name_1)
        self.assertCountEqual(hpo_dict[consts.MAIL_TO], self.expected_mail_to_1)

        hpo_dict = en.create_recipients_list(self.hpo_id_2)
        self.assertEqual(len(hpo_dict[consts.MAIL_TO]), 0)

        hpo_dict = en.create_recipients_list(self.hpo_id_3)
        for email_dict in hpo_dict[consts.MAIL_TO]:
            self.assertTrue(email_dict['email'].islower())

        hpo_dict = en.create_recipients_list(self.hpo_id_4)
        self.assertEqual(len(hpo_dict[consts.SITE_NAME]), 0)

    def test_generate_html_body(self):
        html_body = en.generate_html_body(self.site_name_1, self.fake_html_path,
                                          self.report_data)
        self.assertIn(
            f"https://console.cloud.google.com/storage/browser/{self.bucket}/{self.folder}",
            html_body)
        self.assertIn('was successfully loaded on', html_body)

    def test_aou_logo(self):
        b64_logo = en.get_aou_logo_b64()
        thumbnail_obj = BytesIO()
        thumbnail_obj.write(base64.decodebytes(b64_logo.encode()))
        thumbnail_obj.seek(0)
        thumbnail = mpimg.imread(thumbnail_obj)
        self.assertEqual(thumbnail.shape, (50, 160, 4))

    @mock.patch('validation.email_notification.get_hpo_contact_info')
    def test_generate_email_message(self, mock_fake_info):
        mock_fake_info_1 = {
            self.hpo_id_1: {
                consts.SITE_NAME: self.site_name_1,
                consts.SITE_POINT_OF_CONTACT: ';'.join(self.email_1)
            }
        }
        mock_fake_info_2 = {
            self.hpo_id_2: {
                consts.SITE_NAME: 'Fake Site Name 2',
                consts.SITE_POINT_OF_CONTACT: ''
            }
        }
        mock_fake_info_3 = {
            self.hpo_id_3: {
                consts.SITE_NAME: 'Fake Site Name 3',
                consts.SITE_POINT_OF_CONTACT: 'no data steward'
            }
        }
        mock_fake_info_4 = {
            self.hpo_id_4: {
                consts.SITE_NAME: '',
                consts.SITE_POINT_OF_CONTACT: '; '.join(self.email_1)
            }
        }
        mock_fake_info.side_effect = [
            mock_fake_info_1, mock_fake_info_2, mock_fake_info_3,
            mock_fake_info_4
        ]
        results_html_str = ''
        email_msg = en.generate_email_message(self.hpo_id_1, results_html_str,
                                              self.fake_html_path,
                                              self.report_data)
        expected_attachment = {'name': 'results.html', 'type': 'text/html'}
        expected_image = {'name': 'aou_logo', 'type': 'image/png'}
        email_msg['attachments'][0].pop('content')
        email_msg['images'][0].pop('content')
        self.assertDictEqual(email_msg['attachments'][0], expected_attachment)
        self.assertDictEqual(email_msg['images'][0], expected_image)
        self.assertTrue(email_msg['auto_html'])
        self.assertEqual(email_msg['from_email'], consts.NO_REPLY_ADDRESS)
        self.assertEqual(email_msg['from_name'], consts.EHR_OPERATIONS)
        self.assertEqual(email_msg['headers']['Reply-To'],
                         consts.EHR_OPS_ZENDESK)
        self.assertFalse(email_msg['important'])
        self.assertTrue(email_msg['preserve_recipients'])
        self.assertEqual(email_msg['subject'],
                         f"EHR Data Submission Report for {self.site_name_1}")
        self.assertListEqual(email_msg['tags'], [self.hpo_id_1])
        self.assertCountEqual(email_msg['to'], self.expected_mail_to_1)

        email_msg = en.generate_email_message(self.hpo_id_2, results_html_str,
                                              self.fake_html_path,
                                              self.report_data)
        self.assertIsNone(email_msg)
        email_msg = en.generate_email_message(self.hpo_id_3, results_html_str,
                                              self.fake_html_path,
                                              self.report_data)
        self.assertIsNone(email_msg)
        email_msg = en.generate_email_message(self.hpo_id_4, results_html_str,
                                              self.fake_html_path,
                                              self.report_data)
        self.assertIsNone(email_msg)
