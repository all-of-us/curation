MANDRILL_API_KEY = 'MANDRILL_API_KEY'
UNSET_MANDRILL_API_KEY_MSG = f"Mandrill API key not set in environment variable {MANDRILL_API_KEY}"

CONTACT_LIST_QUERY = """
SELECT *
FROM `{{project}}.{{dataset}}.{{contact_table}}`
"""

EHR_OPERATIONS = 'EHR Ops'
EHR_OPS_ZENDESK = 'support@aou-ehr-ops.zendesk.com'
DATA_CURATION_LISTSERV = 'datacuration@researchallofus.org'
NO_REPLY_ADDRESS = 'noreply@researchallofus.org'
NO_DATA_STEWARD = 'no data steward'

# HPO contact list table columns
SITE_NAME = 'site_name'
HPO_ID = 'hpo_id'
SITE_POINT_OF_CONTACT = 'site_point_of_contact'

# Mandrill API constants
MAIL_TO = 'mail_to'

EHR_OPS_SITE_URL = 'https://sites.google.com/view/ehrupload'

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
For more information on the reports and how to download them, please refer to our
 <a href="{{ ehr_ops_site_url }}">EHR Ops website</a>.</p>

<p style="font-size:115%;">You are receiving this email because you are listed as a point of contact
 for HPO Site <em>{{ site_name }}</em>.<br>
If you have additional questions or wish to no longer receive these emails, please reply/send an
 email to <a href="mailto:{{ eo_zendesk }}">{{ eo_zendesk }}</a>.</p>

<p style="font-size:115%;">EHR Ops team, DRC<br>
<em>All of Us</em> Research Program<br>
<img src="cid:{{ aou_logo }}"/></p>
"""

AOU_LOGO = 'aou_logo'

AOU_LOGO_PNG = 'all-of-us-logo.png'
