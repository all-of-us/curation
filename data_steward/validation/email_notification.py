# Python imports

# Third party imports
import os
import logging

from google.cloud import bigquery
import google.auth
import mandrill
from jinja2 import Template

# Project imports
from constants.utils import bq as bq_consts

LOGGER = logging.getLogger(__name__)

MANDRILL_API_KEY = 'MANDRILL_API_KEY'
UNSET_MANDRILL_API_KEY_MSG = f"Mandrill API key not set in environment variable {MANDRILL_API_KEY}"

CONTACT_LIST_QUERY = """
SELECT *
FROM `{{project}}.{{dataset}}.{{contact_table}}`
"""

CONTACT_QUERY_TMPL = Template(CONTACT_LIST_QUERY)


class MandrillConfigurationError(RuntimeError):
    """
    Raised when the required mandrill api key is not properly configured
    """

    def __init__(self, msg):
        super(MandrillConfigurationError, self).__init__()
        self.msg = msg


def _get_mandrill_api_key():
    """
    Get the token used to interact with the Mandrill API

    :raises:
      SlackConfigurationError: token is not configured
    :return: configured Slack API token as str
    """
    if MANDRILL_API_KEY not in os.environ.keys():
        raise MandrillConfigurationError(UNSET_MANDRILL_API_KEY_MSG)
    return os.environ[MANDRILL_API_KEY]


def get_hpo_contact_info(project_id):
    """
    Fetch email of points of contact for hpo sites
    :param project_id: identifies the project containing the contact lookup table
    :return: dataframe containing site_name, hpo_id and site_point_of_contact
    """
    # add Google Drive scope
    credentials, project = google.auth.default(scopes=[
        "https://www.googleapis.com/auth/drive.readonly",
        "https://www.googleapis.com/auth/cloud-platform",
        "https://www.googleapis.com/auth/bigquery",
    ])
    client = bigquery.Client(credentials=credentials, project=project)

    contact_list_query = CONTACT_QUERY_TMPL.render(
        project=project_id,
        dataset=bq_consts.LOOKUP_TABLES_DATASET_ID,
        contact_table=bq_consts.HPO_ID_CONTACT_LIST_TABLE_ID)
    query_job_config = bigquery.job.QueryJobConfig(use_query_cache=False)
    contact_df = client.query(contact_list_query,
                              job_config=query_job_config).to_dataframe()
    contact_df = contact_df[contact_df.hpo_id.notnull()]
    contact_df = contact_df.set_index('hpo_id')
    contact_dict = contact_df.to_dict('index')
    return contact_dict


def generate_email_message(content, attachment):
    email_message = {
        'attachments': [{
            'content': 'ZXhhbXBsZSBmaWxl',
            'name': 'myfile.txt',
            'type': 'text/plain'
        }],
        'auto_html': None,
        'auto_text': None,
        'bcc_address': 'message.bcc_address@example.com',
        'from_email': 'message.from_email@example.com',
        'from_name': 'Example Name',
        'global_merge_vars': [{
            'content': 'merge1 content',
            'name': 'merge1'
        }],
        'google_analytics_campaign': 'message.from_email@example.com',
        'google_analytics_domains': ['example.com'],
        'headers': {
            'Reply-To': 'message.reply@example.com'
        },
        'html': '<p>Example HTML content</p>',
        'images': [{
            'content': 'ZXhhbXBsZSBmaWxl',
            'name': 'IMAGECID',
            'type': 'image/png'
        }],
        'important': False,
        'inline_css': None,
        'merge': True,
        'merge_language': 'mailchimp',
        'merge_vars': [{
            'rcpt': 'recipient.email@example.com',
            'vars': [{
                'content': 'merge2 content',
                'name': 'merge2'
            }]
        }],
        'metadata': {
            'website': 'www.example.com'
        },
        'preserve_recipients': None,
        'recipient_metadata': [{
            'rcpt': 'recipient.email@example.com',
            'values': {
                'user_id': 123456
            }
        }],
        'return_path_domain': None,
        'signing_domain': None,
        'subaccount': 'customer-123',
        'subject': 'example subject',
        'tags': ['password-resets'],
        'text': 'Example text content',
        'to': [{
            'email': 'recipient.email@example.com',
            'name': 'Recipient Name',
            'type': 'to'
        }],
        'track_clicks': None,
        'track_opens': None,
        'tracking_domain': None,
        'url_strip_qs': None,
        'view_content_link': None
    }
    return email_message


def send_email(mail_to, mail_from, mail_cc, email_message):
    try:
        mandrill_client = mandrill.Mandrill('YOUR_API_KEY')
        result = mandrill_client.messages.send(message=email_message,
                                               send_async=False,
                                               ip_pool='Main Pool',
                                               send_at='example send_at')
        '''
        [{'_id': 'abc123abc123abc123abc123abc123',
          'email': 'recipient.email@example.com',
          'reject_reason': 'hard-bounce',
          'status': 'sent'}]
        '''
    except mandrill.Error as e:
        # Mandrill errors are thrown as exceptions
        LOGGER.exception(f"A mandrill error occurred: {e.__class__} - {e}")
        # A mandrill error occurred: <class 'mandrill.UnknownSubaccountError'> - No subaccount exists with the id 'customer-123'
        raise
