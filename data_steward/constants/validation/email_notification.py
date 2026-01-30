SENDGRID_TOKEN_SECRET_ID = 'warehouseSendAPIKey'
UNSET_SENDGRID_SECRET_MSG = f"sendgrid_token secret: `{SENDGRID_TOKEN_SECRET_ID}` is not set in secret manager"

CONTACT_LIST_QUERY = """
SELECT *
FROM `{{project}}.{{dataset}}.{{contact_table}}`
"""

EHR_OPERATIONS = 'EHR Ops'
EHR_OPS_ZENDESK = 'support@aou-ehr-ops.zendesk.com'
DATA_CURATION_LISTSERV = 'curation@aou-ehr-ops.zendesk.com'
NO_REPLY_ADDRESS = 'noreply@pmi-ops.org'
NO_DATA_STEWARD = 'no data steward'

# HPO contact list table columns
SITE_NAME = 'site_name'
HPO_ID = 'hpo_id'
SITE_POINT_OF_CONTACT = 'site_point_of_contact'

# SendGrid API constants
TO_EMAILS = 'to_emails'
CC_EMAILS = 'cc_emails'

TRANSFER_DATA_DRC_URL = 'https://aou-ehr-ops.zendesk.com/hc/en-us/articles/1500012461721-Transferring-Data-to-the-DRC'
RESULTS_HTML_ZENDESK_URL = 'https://aou-ehr-ops.zendesk.com/hc/en-us/articles/1500012365782-Results-html'

# Email content
EMAIL_BODY = """
<p style="font-size:115%;">Hi {{ site_name }},</p>

<p style="font-size:115%;">Your submission <b>{{ folder }}</b> 
{% if submission_error %}was NOT successfully loaded on {{ timestamp }}.<br>
{% else %}was successfully loaded on {{ timestamp }}.<br>
{% endif %}
Please review the <code>results.html</code> submission report attached to this email{% if submission_error %}<br>
and resolve the errors before making a new submission{% endif %}.<br>
If any of your files have not been successfully uploaded, please run the
 <a href="https://github.com/all-of-us/aou-ehr-file-check">local file check</a> before making your submission.<br>  
To view the full set of curation reports, please visit the submission folder in your
 GCS bucket <a href="{{ submission_folder_url }}">here</a>.<br>
<hr>
 <b>✨ New Feature: Participant Summary Stats</b><br>
A new section titled <b>Participant Summary Stats</b> has been added to the <code>results.html</code> file.<br>
Detailed participant-level data are available in your bucket in the file: <code>participant_stats.csv</code><br>
Please refer to this Zendesk page for more information on this new feature: 
 <a href="{{ results_html_zendesk_url }}">Results.html Data Quality Report</a>.<br>
 <hr>
For more information on the reports and how to download them, please refer to
 <a href="{{ transfer_data_drc_url }}">Transferring data to the DRC</a>.</p>

<p style="font-size:115%;">You are receiving this email because you are listed as a point of contact
 for HPO Site <em>{{ site_name }}</em>.<br>
If you have additional questions or wish to no longer receive these emails, please reply/send an
 email to <a href="mailto:{{ eo_zendesk }}">{{ eo_zendesk }}</a>.</p>

<p style="font-size:115%;">EHR Ops team, DRC<br>
<em>All of Us</em> Research Program<br>
<img src="cid:{{ aou_logo }}" width="150" alt="All of Us Logo"/>
"""

AOU_LOGO = 'aou_logo'

AOU_LOGO_PNG = 'all-of-us-logo.png'
