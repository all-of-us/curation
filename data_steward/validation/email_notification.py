# Python imports

# Third party imports
import os
import logging
import base64
from io import BytesIO

from google.cloud import bigquery
import google.auth
import mandrill
from jinja2 import Template
from matplotlib import image as mpimg

# Project imports
import app_identity
from constants.utils import bq as bq_consts
from constants.validation import email_notification as consts
from resources import achilles_images_path

LOGGER = logging.getLogger(__name__)

CONTACT_QUERY_TMPL = Template(consts.CONTACT_LIST_QUERY)


class MandrillConfigurationError(RuntimeError):
    """
    Raised when the required Mandrill API key is not properly configured
    """

    def __init__(self, msg):
        super(MandrillConfigurationError, self).__init__()
        self.msg = msg


def _get_mandrill_api_key():
    """
    Get the token used to interact with the Mandrill API

    :raises:
      MandrillConfigurationError: API key is not configured
    :return: configured Mandrill API key as str
    """
    if consts.MANDRILL_API_KEY not in os.environ.keys():
        raise MandrillConfigurationError(consts.UNSET_MANDRILL_API_KEY_MSG)
    return os.environ[consts.MANDRILL_API_KEY]


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
    if project != project_id:
        raise ValueError(
            f"{project} does not match {project_id}. "
            f"Please verify that the project_id is set correctly in env vars")
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


def create_recipients_list(hpo_id):
    """
    Generates list of recipients for a hpo site

    :param hpo_id: identifies the hpo site
    :return: list of dicts with keys hpo_id, site_name and dict mail_to, with keys email and type
    """
    hpo_recipients = {'hpo_id': hpo_id}
    mail_to = []
    project_id = app_identity.get_application_id()
    hpo_contact_dict = get_hpo_contact_info(project_id).get(hpo_id)
    hpo_recipients[consts.SITE_NAME] = hpo_contact_dict.get(consts.SITE_NAME)
    hpo_emails_str = hpo_contact_dict.get(consts.SITE_POINT_OF_CONTACT)
    hpo_emails = [
        hpo_email.strip().lower() for hpo_email in hpo_emails_str.split(';')
    ]
    for hpo_email_address in hpo_emails:
        if hpo_email_address != consts.NO_DATA_STEWARD:
            recipient_email_dict = {'email': hpo_email_address, 'type': 'to'}
            mail_to.append(recipient_email_dict)
    hpo_recipients[consts.MAIL_TO] = mail_to
    return hpo_recipients


def generate_html_body(site_name, results_html_path, report_data):
    """
    Generates html body of the email content

    :param site_name: name of the hpo_site
    :param results_html_path: path to results.html in bucket
    :param report_data: dict containing report info for submission
    :return: html formatted string
    """
    html_email_body = Template(consts.EMAIL_BODY).render(
        site_name=site_name,
        ehr_ops_site_url=consts.EHR_OPS_SITE_URL,
        results_html_path=results_html_path,
        dc_listserv=consts.DATA_CURATION_LISTSERV,
        aou_logo=consts.AOU_LOGO,
        **report_data)
    return html_email_body


def get_aou_logo_b64():
    logo_path = os.path.join(achilles_images_path, consts.AOU_LOGO_PNG)
    thumbnail_obj = BytesIO()
    logo = mpimg.thumbnail(logo_path, thumbnail_obj, scale=0.3)
    logo_b64 = base64.b64encode(thumbnail_obj).decode()
    return logo_b64


def generate_email_message(hpo_id, results_html, results_html_path,
                           report_data):
    """
    Generates Mandrill API message dict

    :param hpo_id: identifies the hpo site
    :param results_html: hpo report html file in string format
    :param results_html_path: path to results.html in bucket
    :param report_data: dict containing report info for submission
    :return: Message dict formatted for Mandrill API
    """
    hpo_recipients = create_recipients_list(hpo_id)
    site_name = hpo_recipients.get(consts.SITE_NAME)
    mail_to = hpo_recipients.get(consts.MAIL_TO)
    results_html_b64 = base64.b64encode(results_html.encode())
    html_body = generate_html_body(site_name, results_html_path, report_data)
    aou_logo_b64 = get_aou_logo_b64()
    email_subject = f"EHR Data Submission Report for {site_name}"
    email_message = {
        'attachments': [{
            'content': results_html_b64,
            'name': 'results.html',
            'type': 'text/html'
        }],
        'auto_html': True,
        'from_email': consts.NO_REPLY_ADDRESS,
        'from_name': 'Data Curation',
        'headers': {
            'Reply-To': consts.DATA_CURATION_LISTSERV
        },
        'html': html_body,
        'images': [{
            'content': aou_logo_b64,
            'name': consts.AOU_LOGO,
            'type': 'image/png'
        }],
        'important': False,
        'preserve_recipients': False,
        'subject': email_subject,
        'tags': [hpo_id],
        'to': mail_to
    }
    return email_message


def send_email(email_message):
    """
    Send email using Mandrill API

    :param email_message: Mandrill API message dict to send
    :return: result from Mandrill API
    """
    try:
        api_key = _get_mandrill_api_key()
        mandrill_client = mandrill.Mandrill(api_key)
        result = mandrill_client.messages.send(message=email_message)
    except mandrill.Error as e:
        # Mandrill errors are thrown as exceptions
        LOGGER.exception(f"A mandrill error occurred: {e.__class__} - {e}")
        raise
    return result
