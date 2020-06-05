# Python imports

# Third party imports
import mandrill
from jinja2 import Template

# Project imports
from utils import bq
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
    contact_list_query = CONTACT_QUERY_TMPL.render(
        project=project_id,
        dataset=bq_consts.LOOKUP_TABLES_DATASET_ID,
        contact_table=bq_consts.HPO_ID_CONTACT_LIST_TABLE_ID)
    contact_list = bq.query(contact_list_query)
    return contact_list
