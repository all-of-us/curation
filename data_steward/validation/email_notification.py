# Python imports
import os
import logging
import base64
from io import BytesIO

# Third party imports
import mandrill
from jinja2 import Template
from matplotlib import image as mpimg
from google.cloud import bigquery

# Project imports
import app_identity
from gcloud.bq import BigQueryClient
from gcloud.gsm import SecretManager
from utils import bq
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

    # contact_df = bq.query_sheet_linked_bq_table(project_id,
    #                                             contact_list_query,
    #                                             external_data_scopes=scopes)
    bq_client = BigQueryClient(project_id=project_id, scopes=scopes)
    query_job_config = bigquery.job.QueryJobConfig(use_query_cache=False)
    contact_df = bq_client.query(contact_list_query,
                                 job_config=query_job_config).to_dataframe()

    contact_df = contact_df[contact_df.hpo_id.notnull()]
    contact_df = contact_df.set_index('hpo_id')
    contact_dict = contact_df.to_dict('index')
    LOGGER.info(f"Retrieved contact list using {contact_list_query}")
    return contact_dict


def create_recipients_list(hpo_id):
    """
    Generates list of recipients for a hpo site

    :param hpo_id: identifies the hpo site
    :return: list of dicts with keys hpo_id, site_name and dict mail_to, with keys email and type
    """
    hpo_recipients = {
        'hpo_id': hpo_id,
        consts.SITE_NAME: '',
        consts.MAIL_TO: []
    }
    mail_to = []
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
            recipient_email_dict = {'email': hpo_email_address, 'type': 'to'}
            mail_to.append(recipient_email_dict)
    if len(mail_to) == 0:
        LOGGER.info(f"No valid email addresses for {hpo_id} in contact list")
        return hpo_recipients
    mail_to.append({'email': consts.DATA_CURATION_LISTSERV, 'type': 'cc'})
    hpo_recipients[consts.MAIL_TO] = mail_to
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
        ehr_ops_site_url=consts.EHR_OPS_SITE_URL,
        submission_folder_url=submission_folder_url,
        eo_zendesk=consts.EHR_OPS_ZENDESK,
        aou_logo=consts.AOU_LOGO,
        **report_data)
    LOGGER.info(f"Generated html email body")
    return html_email_body


def get_aou_logo_b64():
    logo_path = os.path.join(achilles_images_path, consts.AOU_LOGO_PNG)
    thumbnail_obj = BytesIO()
    mpimg.thumbnail(logo_path, thumbnail_obj, scale=0.15)
    logo_b64 = base64.b64encode(thumbnail_obj.getvalue()).decode()
    return logo_b64


def generate_email_message(hpo_id, results_html, folder_uri, report_data):
    """
    Generates Mandrill API message dict

    :param hpo_id: identifies the hpo site
    :param results_html: hpo report html file in string format
    :param folder_uri: gcs path to submission folder in bucket
    :param report_data: dict containing report info for submission
    :return: Message dict formatted for Mandrill API
    """
    LOGGER.info(f"Retrieving email ids for {hpo_id}")
    hpo_recipients = create_recipients_list(hpo_id)
    site_name = hpo_recipients.get(consts.SITE_NAME, '')
    mail_to = hpo_recipients.get(consts.MAIL_TO, [])
    if len(site_name) == 0 or len(mail_to) == 0:
        LOGGER.info(
            f"No email ids found for {hpo_id}. Please update contact list.")
        return None
    results_html_b64 = base64.b64encode(results_html.encode())
    html_body = generate_html_body(site_name, folder_uri, report_data)
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
        'from_name': consts.EHR_OPERATIONS,
        'headers': {},
        'html': html_body,
        'images': [{
            'content': aou_logo_b64,
            'name': consts.AOU_LOGO,
            'type': 'image/png'
        }],
        'important': False,
        'preserve_recipients': True,
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
    result = None
    try:
        smc = SecretManager()
        api_key = smc.get_secret_from_secret_manager(
            consts.MANDRILL_TOKEN_SECRET_ID)
        mandrill_client = mandrill.Mandrill(api_key)
        result = mandrill_client.messages.send(message=email_message)
    except mandrill.Error as e:
        # Mandrill errors are thrown as exceptions
        msg = f"A mandrill error occurred: {e.__class__} - {e}"
        LOGGER.exception(msg, exec_info=True)
    return result
