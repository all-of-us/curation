import argparse
import logging
import sys

from utils import bq

from constants.utils import bq as bq_consts
from common import JINJA_ENV

LOGGER = logging.getLogger(__name__)

# Set logger, handler to print query to stdout
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
LOGGER.addHandler(handler)
LOGGER.setLevel(logging.DEBUG)

EHR_UPLOAD_PIDS_BQ_SCRIPT = JINJA_ENV.from_string('''
SELECT ARRAY_TO_STRING(ARRAY_AGG(FORMAT
("""SELECT 
  person_id,
  current_datetime() AS report_run_time,
  Org_ID as org_id,
  HPO_ID as hpo_id,
  Site_Name as site_name, 
  TIMESTAMP_MICROS(t.last_modified_time * 1000) AS latest_upload_time
FROM 
  `{{project_id}}.{{ehr_dataset_id}}.%s_person` p,
  `{{project_id}}.{{lookup_dataset_id}}.{{hpo_mappings}}` m,
  `{{project_id}}.{{ehr_dataset_id}}.__TABLES__` t
  WHERE t.table_id = '%s_person'
  AND m.HPO_ID = '%s'
  AND person_id IN (
    SELECT person_id FROM `{{project_id}}.{{ehr_dataset_id}}.%s_person` UNION DISTINCT
    SELECT person_id FROM `{{project_id}}.{{ehr_dataset_id}}.%s_condition_occurrence` UNION DISTINCT
    SELECT person_id FROM `{{project_id}}.{{ehr_dataset_id}}.%s_procedure_occurrence` UNION DISTINCT
    SELECT person_id FROM `{{project_id}}.{{ehr_dataset_id}}.%s_drug_exposure` UNION DISTINCT
    SELECT person_id FROM `{{project_id}}.{{ehr_dataset_id}}.%s_observation` UNION DISTINCT
    SELECT person_id FROM `{{project_id}}.{{ehr_dataset_id}}.%s_visit_occurrence`)""", 
    LOWER(HPO_ID), LOWER(HPO_ID), HPO_ID, LOWER(HPO_ID), LOWER(HPO_ID), LOWER(HPO_ID), 
    LOWER(HPO_ID), LOWER(HPO_ID), LOWER(HPO_ID))), "\\nUNION ALL \\n") as q
FROM `{{project_id}}.{{lookup_dataset_id}}.{{hpo_mappings}}`
WHERE HPO_ID NOT IN ({{excluded_sites_str}})
''')


def get_excluded_hpo_ids_str(excluded_hpo_ids):
    """
    Formats list of hpo_ids or None to add to bq script, adds empty hpo_id

    :param excluded_hpo_ids: List output by args parser or None
    :return: String of hpo_ids enclosed in single quotes along with empty hpo_ids
    """
    if excluded_hpo_ids is None:
        excluded_hpo_ids = []
    # use uppercase for all hpo_ids as is in the table
    excluded_hpo_ids = [hpo_id.upper() for hpo_id in excluded_hpo_ids]
    # exclude empty site since lookup table contains it
    excluded_hpo_ids.append('')
    excluded_hpo_ids_str = ', '.join(
        [f"'{hpo_id}'" for hpo_id in excluded_hpo_ids])
    return excluded_hpo_ids_str


def generate_ehr_upload_pids_query(project_id,
                                   ehr_dataset_id,
                                   excluded_hpo_ids=None):
    """
    Generate query for all hpo_ids except specified

    :param project_id: Identifies the project
    :param ehr_dataset_id: Identifies the ehr dataset
    :param excluded_hpo_ids: List of sites
    :return: Query string to use in ehr_upload_pids view
    """
    client = bq.get_client(project_id)
    excluded_hpo_ids_str = get_excluded_hpo_ids_str(excluded_hpo_ids)
    query = EHR_UPLOAD_PIDS_BQ_SCRIPT.render(
        project_id=project_id,
        ehr_dataset_id=ehr_dataset_id,
        lookup_dataset_id=bq_consts.LOOKUP_TABLES_DATASET_ID,
        hpo_mappings=bq_consts.HPO_SITE_ID_MAPPINGS_TABLE_ID,
        excluded_sites_str=excluded_hpo_ids_str)
    query_job = client.query(query)
    res = query_job.result().to_dataframe()
    full_query = res["q"].to_list()[0]
    return full_query


def get_args_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-p',
        '--project_id',
        dest='project_id',
        action='store',
        help=
        'Identifies the project containing the ehr dataset and lookup dataset',
        required=True)
    parser.add_argument('-d',
                        '--ehr_dataset_id',
                        dest='ehr_dataset_id',
                        action='store',
                        help='Identifies the ehr dataset',
                        required=True)
    parser.add_argument('-i',
                        '--excluded_hpo_ids',
                        action='store',
                        nargs='+',
                        dest='excluded_hpo_ids',
                        help='Identifies sites with no tables in ehr dataset',
                        required=False)
    return parser


if __name__ == '__main__':
    args_parser = get_args_parser()
    args = args_parser.parse_args()
    ehr_upload_pids_query = generate_ehr_upload_pids_query(
        args.project_id, args.ehr_dataset_id, args.excluded_hpo_ids)
    LOGGER.info(ehr_upload_pids_query)
