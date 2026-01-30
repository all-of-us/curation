# Python imports
import os
import logging
import base64
from io import BytesIO

from PIL import Image

# Third party imports
from common import CDR_SCOPES
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import (Mail, Email, Attachment, FileContent,
                                   FileName, Content, FileType, FileContent,
                                   FileName, Disposition, ContentId, To, Cc)
from jinja2 import Template
from matplotlib import image as mpimg
from google.cloud import bigquery
from python_http_client import HTTPError

# Project imports
import app_identity
from gcloud.bq import BigQueryClient
from gcloud.gsm import SecretManager
from constants.utils import bq as bq_consts
from constants.validation import email_notification as consts
from resources import achilles_images_path

LOGGER = logging.getLogger(__name__)

CONTACT_QUERY_TMPL = Template(consts.CONTACT_LIST_QUERY)


def get_hpo_contact_info(project_id):
    """
    Fetch email of points of contact for hpo sites

    :param project_id: identifies the project containing the contact lookup table
    :return: dictionary with key hpo_id and value as
             dictionary with keys site_name, hpo_id and site_point_of_contact
    """
    # Add scopes for Spreadsheet-sourced-BigQuery table
    scopes = [
        "https://www.googleapis.com/auth/drive",
        "https://www.googleapis.com/auth/cloud-platform"
    ]

    contact_list_query = CONTACT_QUERY_TMPL.render(
        project=project_id,
        dataset=bq_consts.LOOKUP_TABLES_DATASET_ID,
        contact_table=bq_consts.HPO_ID_CONTACT_LIST_TABLE_ID)

    bq_client = BigQueryClient(project_id=project_id, scopes=scopes)
    query_job_config = bigquery.job.QueryJobConfig(use_query_cache=False)
    contact_df = bq_client.query(contact_list_query,
                                 job_config=query_job_config).to_dataframe()
    bq_client.close()

    contact_df = contact_df[contact_df.hpo_id.notnull()]
    contact_df = contact_df.set_index('hpo_id')
    contact_dict = contact_df.to_dict('index')
    LOGGER.info(f"Retrieved contact list using {contact_list_query}")
    return contact_dict


def create_recipients_list(hpo_id):
    """
    Generates list of recipients for a hpo site

    :param hpo_id: identifies the hpo site
    :return: dict with keys hpo_id, site_name, to_emails, and cc_emails
    """
    hpo_recipients = {
        'hpo_id': hpo_id,
        consts.SITE_NAME: '',
        consts.TO_EMAILS: [],
        consts.CC_EMAILS: []
    }

    project_id = app_identity.get_application_id()
    hpo_contact_dict = get_hpo_contact_info(project_id).get(hpo_id, None)
    if hpo_contact_dict is None:
        LOGGER.info(f"No entry for {hpo_id} in contact list")
        return hpo_recipients
    site_name = hpo_contact_dict.get(consts.SITE_NAME, '')
    if site_name.strip() == '':
        LOGGER.info(f"No {consts.SITE_NAME} field for {hpo_id} in contact list")
        return hpo_recipients
    hpo_recipients[consts.SITE_NAME] = site_name
    hpo_emails_str = hpo_contact_dict.get(consts.SITE_POINT_OF_CONTACT, '')
    if hpo_emails_str.strip() == '':
        LOGGER.info(
            f"No {consts.SITE_POINT_OF_CONTACT} field for {hpo_id} in contact list"
        )
        return hpo_recipients
    hpo_emails = [
        hpo_email.strip().lower() for hpo_email in hpo_emails_str.split(';')
    ]
    for hpo_email_address in hpo_emails:
        if '@' in hpo_email_address:
            hpo_recipients[consts.TO_EMAILS].append(hpo_email_address)

    if len(hpo_recipients[consts.TO_EMAILS]) == 0:
        LOGGER.info(f"No valid email addresses for {hpo_id} in contact list")
        return hpo_recipients

    hpo_recipients[consts.CC_EMAILS] = [consts.DATA_CURATION_LISTSERV]
    LOGGER.info(f"Successfully fetched emails for {hpo_id}")
    return hpo_recipients


def generate_html_body(site_name, folder_uri, report_data):
    """
    Generates html body of the email content

    :param site_name: name of the hpo_site
    :param folder_uri: gcs path to submission folder in bucket
    :param report_data: dict containing report info for submission
    :return: html formatted string
    """
    submission_folder_url = folder_uri.replace(
        'gs://', 'https://console.cloud.google.com/storage/browser/')
    html_email_body = Template(consts.EMAIL_BODY).render(
        site_name=site_name,
        transfer_data_drc_url=consts.TRANSFER_DATA_DRC_URL,
        results_html_zendesk_url=consts.RESULTS_HTML_ZENDESK_URL,
        submission_folder_url=submission_folder_url,
        eo_zendesk=consts.EHR_OPS_ZENDESK,
        aou_logo=consts.AOU_LOGO,
        **report_data)
    LOGGER.info(f"Generated html email body")
    return html_email_body


def get_aou_logo_b64():
    logo_path = os.path.join(achilles_images_path, consts.AOU_LOGO_PNG)
    with Image.open(logo_path) as img:
        img.thumbnail((300, 300), Image.LANCZOS)
        rgb_img = img.convert('RGB')
        buffer = BytesIO()
        rgb_img.save(buffer, format='JPEG', quality=85)
        return base64.b64encode(buffer.getvalue()).decode()


def generate_email_message(hpo_id, results_html, folder_uri, report_data):
    """
    Generates SendGrid email message

    :param hpo_id: identifies the hpo site
    :param results_html: hpo report html file in string format
    :param folder_uri: gcs path to submission folder in bucket
    :param report_data: dict containing report info for submission
    :return: SendGrid Mail object ready to be sent
    """
    LOGGER.info(f"Retrieving email ids for {hpo_id}")
    hpo_recipients = create_recipients_list(hpo_id)
    site_name = hpo_recipients.get(consts.SITE_NAME, '')
    to_emails = hpo_recipients.get(consts.TO_EMAILS, [])
    cc_emails = hpo_recipients.get(consts.CC_EMAILS, [])

    if len(site_name) == 0 or len(to_emails) == 0:
        LOGGER.info(
            f"No email ids found for {hpo_id}. Please update contact list.")
        return None

    email_subject = f"EHR Data Submission Report for {site_name}"
    html_body = generate_html_body(site_name, folder_uri, report_data)

    # From and recipients
    from_email = Email(consts.NO_REPLY_ADDRESS, consts.EHR_OPERATIONS)
    to_emails = [To(to_email) for to_email in to_emails]
    cc_emails = [Cc(cc_email) for cc_email in cc_emails]

    # Email body
    content = Content("text/html", html_body)

    # Create SendGrid message
    message = Mail(from_email=from_email,
                   to_emails=to_emails,
                   subject=email_subject,
                   html_content=content)

    # Add CC recipients
    for cc in cc_emails:
        message.add_cc(cc)

    # Add HTML attachment
    encoded_file = base64.b64encode(results_html.encode()).decode()
    attachment = Attachment()
    attachment.file_content = FileContent(encoded_file)
    attachment.file_name = FileName('results.html')
    attachment.file_type = FileType('text/html')
    attachment.disposition = Disposition('attachment')
    message.attachment = attachment

    # Add AOU logo as inline image
    aou_logo_b64 = get_aou_logo_b64()
    logo_inline_image = Attachment()
    logo_inline_image.file_content = FileContent(aou_logo_b64)
    logo_inline_image.file_name = FileName(consts.AOU_LOGO)
    logo_inline_image.file_type = FileType('image/jpeg')
    logo_inline_image.disposition = Disposition('inline')
    logo_inline_image.content_id = ContentId(consts.AOU_LOGO)
    message.add_attachment(logo_inline_image)

    return message


def send_email(email_message):
    """
    Send email using SendGrid API

    :param email_message: SendGrid Mail object to send
    :return: SendGrid API response
    """
    result = None
    try:
        smc = SecretManager()
        api_key = smc.get_secret_from_secret_manager(
            consts.SENDGRID_TOKEN_SECRET_ID
        )  # Update constant name to reflect SendGrid

        sg = SendGridAPIClient(api_key)
        response = sg.send(email_message)
        result = {
            "status_code": response.status_code,
            "body": response.body,
            "headers": dict(response.headers)
        }
        LOGGER.info(
            f"Email sent successfully with status code: {response.status_code}")
    except HTTPError as e:
        # SendGrid errors are thrown as exceptions
        msg = f"A SendGrid error occurred: {e.to_dict}\n{e.__class__}\n{e}"
        LOGGER.exception(msg, exc_info=True)
    except Exception as e:
        msg = f"A SendGrid/GCP error occurred: {e.__class__}\n{e}"
        LOGGER.exception(msg, exc_info=True)

    return result
