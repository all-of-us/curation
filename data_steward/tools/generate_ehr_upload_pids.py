import argparse
import logging
import sys

from gcloud.bq import BigQueryClient

from constants.utils.bq import LOOKUP_TABLES_DATASET_ID, HPO_SITE_ID_MAPPINGS_TABLE_ID
from common import (CONDITION_OCCURRENCE, DRUG_EXPOSURE, EHR_UPLOAD_PIDS,
                    JINJA_ENV, OBSERVATION, OPERATIONS_ANALYTICS, PERSON,
                    PROCEDURE_OCCURRENCE, VISIT_OCCURRENCE)

LOGGER = logging.getLogger(__name__)

# Set logger, handler to print query to stdout
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
LOGGER.addHandler(handler)
LOGGER.setLevel(logging.DEBUG)

pid_tables = [
    PERSON, CONDITION_OCCURRENCE, PROCEDURE_OCCURRENCE, DRUG_EXPOSURE,
    OBSERVATION, VISIT_OCCURRENCE
]

HPO_IDS_QUERY = JINJA_ENV.from_string("""
SELECT LOWER(hpo_id) AS hpo_id FROM `{{project_id}}.{{dataset_id}}.{{table_id}}`
""")

EHR_UPLOAD_PIDS_BQ_QUERY = JINJA_ENV.from_string("""
CREATE OR REPLACE VIEW `{{project_id}}.{{operations_dataset_id}}.{{ehr_pids_view}}` 
OPTIONS(description='A participant-level view of when ehr data was sent. NOTE: the RDR calls this view to support HealthPro (1/27/21)')
AS {% for hpo_site in hpo_sites %}
SELECT 
    person_id,
    current_datetime() AS report_run_time,
    Org_ID as org_id,
    HPO_ID as hpo_id,
    Site_Name as site_name, 
    TIMESTAMP_MICROS(t.last_modified_time * 1000) AS latest_upload_time
FROM 
    `{{project_id}}.{{ehr_dataset_id}}.{{hpo_site}}_person` p,
    `{{project_id}}.{{lookup_dataset_id}}.{{hpo_mappings}}` m,
    `{{project_id}}.{{ehr_dataset_id}}.__TABLES__` t
WHERE t.table_id = '{{hpo_site}}_person'
AND LOWER(m.HPO_ID) = '{{hpo_site}}'
AND person_id IN (
    {% for pid_table in pid_tables %}
    SELECT person_id FROM `{{project_id}}.{{ehr_dataset_id}}.{{hpo_site}}_{{pid_table}}`
    {% if not loop.last %} UNION DISTINCT {% endif %}
    {% endfor %}
){% if not loop.last %} UNION ALL {% endif %}
{% endfor %}
""")


def update_ehr_upload_pids_view(project_id, ehr_dataset_id, bq_client=None):
    """
    Update (=create or replace) ehr_upload_pids view.

    :param project_id: Identifies the project
    :param ehr_dataset_id: Identifies the ehr dataset
    :param bq_client: BigQuery client
    :return: 
    """
    if not bq_client:
        bq_client = BigQueryClient(project_id)

    hpo_query = HPO_IDS_QUERY.render(project_id=project_id,
                                     dataset_id=LOOKUP_TABLES_DATASET_ID,
                                     table_id=HPO_SITE_ID_MAPPINGS_TABLE_ID)

    response = bq_client.query(hpo_query)
    result = list(response.result())
    hpo_sites = [row[0] for row in result]

    hpo_sites_with_submission = [
        hpo_id for hpo_id in hpo_sites if all(
            bq_client.table_exists(f'{hpo_id}_{table}', ehr_dataset_id)
            for table in pid_tables)
    ]
    LOGGER.info(
        f'The following HPO sites will be included in the view `{project_id}.{OPERATIONS_ANALYTICS}.{EHR_UPLOAD_PIDS}`. '
        'These sites are listed in the site mapping table and they have submitted files: '
    )
    LOGGER.info(', '.join(hpo_sites_with_submission))

    query = EHR_UPLOAD_PIDS_BQ_QUERY.render(
        project_id=project_id,
        operations_dataset_id=OPERATIONS_ANALYTICS,
        ehr_pids_view=EHR_UPLOAD_PIDS,
        ehr_dataset_id=ehr_dataset_id,
        lookup_dataset_id=LOOKUP_TABLES_DATASET_ID,
        hpo_mappings=HPO_SITE_ID_MAPPINGS_TABLE_ID,
        hpo_sites=hpo_sites_with_submission,
        pid_tables=pid_tables)

    _ = bq_client.query(query).result()

    LOGGER.info(
        "The view is updated. Ensure the view is accessible without errors: "
        f"`{project_id}.{OPERATIONS_ANALYTICS}.{EHR_UPLOAD_PIDS}`")


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
    return parser


if __name__ == '__main__':
    args_parser = get_args_parser()
    args = args_parser.parse_args()

    bq_client = BigQueryClient(args.project_id)

    update_ehr_upload_pids_view(args.project_id,
                                args.ehr_dataset_id,
                                bq_client=bq_client)
