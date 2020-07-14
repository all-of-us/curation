# Python imports
from unittest import mock, TestCase

# Third party imports

# Project imports
import app_identity
from utils import bq
from constants.utils import bq as bq_consts
from validation import email_notification as en
from validation.main import get_eastern_time


class EmailNotificationTest(TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.project_id = app_identity.get_application_id()
        self.assertIn('test', self.project_id)
        self.hpo_id = 'fake'
        self.site_name = 'Fake Site Name'
        self.bucket = 'fake'
        self.folder = 'fake_folder'
        self.fake_uri_path = f"https://console.cloud.google.com/storage/{self.bucket}/{self.folder}"
        self.report_data = {
            'folder': self.folder,
            'timestamp': get_eastern_time(),
            'submission_error': False
        }

    @mock.patch('utils.bq.query_sheet_linked_bq_table_app_engine')
    def test_hpo_contact_list(self, query_rest_api):
        scopes = [
            "https://www.googleapis.com/auth/drive",
            "https://www.googleapis.com/auth/cloud-platform"
        ]

        # Grant viewing access to the test sheet to BigQuery test accounts
        sheet_url = (
            "https://docs.google.com/spreadsheets"
            "/d/1JI-KyigmwZU9I2J6TZqVTPNoEAWVqiFeF8Y549-dvzM/edit#gid=0")

        query = (f"SELECT * FROM {bq_consts.LOOKUP_TABLES_DATASET_ID}."
                 f"{bq_consts.HPO_ID_CONTACT_LIST_TABLE_ID}")
        query_rest_api.return_value = bq.query_sheet_linked_bq_table_compute_engine(
            self.project_id, query, scopes)
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

    @mock.patch('validation.email_notification.get_hpo_contact_info')
    def _test_send_email(self, mock_fake_info):
        # This test is disabled since it sends an email.
        # Add email in 'site_point_of_contact' separated by ';' to send test emails to
        # Remove the line cc'ing Data Curation in validation.email_notification.create_recipients_list
        mock_fake_info.return_value = {
            'fake': {
                'site_name': self.site_name,
                'site_point_of_contact': '; '
            }
        }
        # The five_person_results.html file referenced below is removed. To generate it, please run
        # integration_tests.data_steward.validation.main_test.test_html_report_five_person
        results_html_str = ''
        # with open(FIVE_PERSON_RESULTS_FILE, 'r') as f:
        #     results_html_str = f.read()
        email_msg = en.generate_email_message(self.hpo_id, results_html_str,
                                              self.fake_uri_path,
                                              self.report_data)
        self.assertIsNotNone(email_msg)
        send_result = en.send_email(email_msg)
        self.assertTrue(send_result)
