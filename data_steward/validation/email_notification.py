# Python imports

# Third party imports
from google.cloud import bigquery
import google.auth
import mandrill
from jinja2 import Template

# Project imports
from constants.utils import bq as bq_consts

CONTACT_LIST_QUERY = """
SELECT *
FROM `{{project}}.{{dataset}}.{{contact_table}}`
"""

CONTACT_QUERY_TMPL = Template(CONTACT_LIST_QUERY)


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
