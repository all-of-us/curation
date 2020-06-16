MANDRILL_API_KEY = 'MANDRILL_API_KEY'
UNSET_MANDRILL_API_KEY_MSG = f"Mandrill API key not set in environment variable {MANDRILL_API_KEY}"

CONTACT_LIST_QUERY = """
SELECT *
FROM `{{project}}.{{dataset}}.{{contact_table}}`
"""

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
Hi {{ site_name }},

Your submission <b>{{ folder }}</b> was{% if submission_error %} not successfully loaded.
{% else %} successfully loaded on {{ timestamp }}.{% endif %}
Please review the <em>results.html</em> submission report attached to this email{% if submission_error %} 
and resolve the errors before making a new submission{% endif %}.
The report is also placed in the submission folder in your GCS bucket <a href="{{ results_html_path }}">here</a>.
For more information on the report, please refer to our <a href="{{ ehr_ops_site_url }}">EHR Ops website</a>.

You are receiving this email because you are listed as a point of contact for HPO Site <em>{{ site_name }}</em>.
If you have additional questions or wish to no longer receive these emails, 
please send an email to <a href="mailto:{{ dc_listserv }}">{{ dc_listserv }}</a>.

Curation team, DRC
<em>All of Us</em> Research Program
<img src="cid:{{ aou_logo }}"/>
"""

AOU_LOGO = 'aou_logo'

AOU_LOGO_PNG = 'all-of-us-logo.png'
