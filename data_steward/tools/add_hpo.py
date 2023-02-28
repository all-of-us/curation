"""
Add a new HPO site to config file and BigQuery lookup tables and updates the `pipeline_table.site_maskings`
table with any missing hpo_sites in `lookup_tables.hpo_site_id_mappings`.
Check out All of Us CDR Operations Playbook for when and how to use this script.

Note: GAE environment must still be set manually
"""
# Python imports
import logging
import csv

# Third party imports
import pandas as pd

# Project imports
import bq_utils
import constants.bq_utils as bq_consts
from gcloud.gcs import StorageClient
from gcloud.bq import BigQueryClient
from utils import pipeline_logging
from common import JINJA_ENV, PIPELINE_TABLES, SITE_MASKING_TABLE_ID, CDR_SCOPES
from utils.auth import get_impersonation_credentials

LOGGER = logging.getLogger(__name__)

DEFAULT_DISPLAY_ORDER = JINJA_ENV.from_string("""
SELECT MAX(Display_Order) + 1 AS display_order 
FROM `{{project_id}}.{{lookup_tables_dataset}}.{{hpo_site_id_mappings_table}}`
""")

SHIFT_HPO_SITE_DISPLAY_ORDER = JINJA_ENV.from_string("""
UPDATE `{{project_id}}.{{lookup_tables_dataset}}.{{hpo_site_id_mappings_table}}`
SET Display_Order = Display_Order + 1
WHERE Display_Order >= {{display_order}}
""")

ADD_HPO_SITE_ID_MAPPING = JINJA_ENV.from_string("""
INSERT INTO `{{project_id}}.{{lookup_tables_dataset}}.{{hpo_site_id_mappings_table}}`
(Org_ID, HPO_ID, Site_Name, Display_Order)
VALUES ("{{org_id}}", "{{hpo_id}}", "{{hpo_name}}", {{display_order}})
""")

ADD_HPO_ID_BUCKET_NAME = JINJA_ENV.from_string("""
INSERT INTO `{{project_id}}.{{lookup_tables_dataset}}.{{hpo_id_bucket_name_table}}`
(hpo_id, bucket_name, service)
VALUES ("{{hpo_id}}", "{{bucket_name}}", "{{service}}")
""")

UPDATE_SITE_MASKING_QUERY = JINJA_ENV.from_string("""
INSERT INTO `{{project_id}}.{{pipeline_tables_dataset}}.{{site_maskings_table}}` (hpo_id, src_id, state, value_source_concept_id)
WITH available_new_src_ids AS (
   SELECT 
     "{{hpo_id}}" AS hpo_id,
     CONCAT('EHR site ', new_id) AS src_id,
     "{{us_state}}" AS state,
     {{value_source_concept_id}} AS value_source_concept_id
   FROM UNNEST(GENERATE_ARRAY(100, 999)) AS new_id
   WHERE new_id NOT IN (
     SELECT CAST(SUBSTR(src_id, -3) AS INT64) 
    FROM `{{project_id}}.{{pipeline_tables_dataset}}.{{site_maskings_table}}`
    WHERE hpo_id != 'rdr'
  )
)
SELECT LOWER(hpo_id), src_id, state, value_source_concept_id
FROM available_new_src_ids
ORDER BY RAND() LIMIT 1
""")


def verify_hpo_mappings_up_to_date(hpo_file_df, hpo_table_df):
    """
    Verifies that the hpo_mappings.csv file is up to date with lookup_tables.hpo_site_id_mappings

    :param hpo_file_df: df loaded from config/hpo_site_mappings.csv
    :param hpo_table_df: df loaded from lookup_tables.hpo_site_id_mappings
    :raises ValueError: If config/hpo_site_mappings.csv is out of sync
        with lookup_tables.hpo_site_id_mappings
    """
    hpo_ids_df = hpo_file_df['HPO_ID'].dropna()
    if set(hpo_table_df['hpo_id'].to_list()) != set(
            hpo_ids_df.str.lower().to_list()):
        raise ValueError(
            f'Please update the config/hpo_site_mappings.csv file '
            f'to the latest version from curation-devops repository.')


def add_hpo_site_mappings_file_df(hpo_id, hpo_name, org_id,
                                  hpo_site_mappings_path, display_order):
    """
    Creates dataframe with hpo_id, hpo_name, org_id, display_order

    :param hpo_id: hpo_ identifier
    :param hpo_name: name of the hpo
    :param org_id: hpo organization identifier
    :param hpo_site_mappings_path: path to csv file containing hpo site information
    :param display_order: index number in which hpo should be added in table
    :raises ValueError if hpo_id already exists in the lookup table
    """
    hpo_table = bq_utils.get_hpo_info()
    hpo_table_df = pd.DataFrame(hpo_table)
    if hpo_id in hpo_table_df['hpo_id'] or hpo_name in hpo_table_df['name']:
        raise ValueError(
            f"{hpo_id}/{hpo_name} already exists in site lookup table")

    hpo_file_df = pd.read_csv(hpo_site_mappings_path)
    verify_hpo_mappings_up_to_date(hpo_file_df, hpo_table_df)

    if display_order is None:
        display_order = hpo_file_df['Display_Order'].max() + 1

    hpo_file_df.loc[hpo_file_df['Display_Order'] >= display_order,
                    'Display_Order'] += 1
    hpo_file_df.loc['-1'] = [org_id, hpo_id, hpo_name, display_order]
    LOGGER.info(f'Added new entry for hpo_id {hpo_id} to '
                f'config/hpo_site_mappings.csv at position {display_order}. '
                f'Please upload to curation-devops repo.')
    return hpo_file_df.sort_values(by='Display_Order')


def add_hpo_site_mappings_csv(hpo_id,
                              hpo_name,
                              org_id,
                              hpo_site_mappings_path,
                              display_order=None):
    """
    Writes df with hpo_id, hpo_name, org_id, display_order to the hpo_site_id_mappings config file

    :param hpo_id: hpo_ identifier
    :param hpo_name: name of the hpo
    :param org_id: hpo organization identifier
    :param hpo_site_mappings_path: path to csv file containing hpo site information
    :param display_order: index number in which hpo should be added in table
    :return:
    """
    hpo_file_df = add_hpo_site_mappings_file_df(hpo_id, hpo_name, org_id,
                                                hpo_site_mappings_path,
                                                display_order)
    hpo_file_df.to_csv(hpo_site_mappings_path,
                       quoting=csv.QUOTE_ALL,
                       index=False)


def get_last_display_order(bq_client):
    """
    gets the display order from hpo_site_id_mappings table
    :param bq_client: BigQuery Client
    :return:
    """
    q = DEFAULT_DISPLAY_ORDER.render(
        project_id=bq_client.project,
        lookup_tables_dataset=bq_consts.LOOKUP_TABLES_DATASET_ID,
        hpo_site_id_mappings_table=bq_consts.HPO_SITE_ID_MAPPINGS_TABLE_ID)

    query_response = bq_client.query(q)
    rows = bq_utils.response2rows(query_response)
    row = rows[0]
    result = row['display_order']
    return result


def shift_display_orders(bq_client, at_display_order):
    """
    shift the display order in hpo_site_id_mappings_table when a new HPO is to be added.
    :param bq_client: BigQuery Client
    :param at_display_order: index where the display order
    :return:
    """
    q = SHIFT_HPO_SITE_DISPLAY_ORDER.render(
        project_id=bq_client.project,
        lookup_tables_dataset=bq_consts.LOOKUP_TABLES_DATASET_ID,
        hpo_site_id_mappings_table=bq_consts.HPO_SITE_ID_MAPPINGS_TABLE_ID,
        display_order=at_display_order)

    LOGGER.info(f'Shifting lookup with the following query:\n {q}\n')
    query_response = bq_client.query(q)
    return query_response


def add_hpo_mapping(bq_client, hpo_id, hpo_name, org_id, display_order):
    """
    adds hpo_id, hpo_name, org_id, display_order to the hpo_site_id_mappings table
    :param bq_client: BigQuery Client
    :param hpo_id: hpo_ identifier
    :param hpo_name: name of the hpo
    :param org_id: hpo organization identifier
    :param display_order: index number in which hpo should be added in table
    :return:
    """
    q = ADD_HPO_SITE_ID_MAPPING.render(
        project_id=bq_client.project,
        lookup_tables_dataset=bq_consts.LOOKUP_TABLES_DATASET_ID,
        hpo_site_id_mappings_table=bq_consts.HPO_SITE_ID_MAPPINGS_TABLE_ID,
        hpo_id=hpo_id,
        hpo_name=hpo_name,
        org_id=org_id,
        display_order=display_order)
    LOGGER.info(f'Adding mapping lookup with the following query:\n {q}\n')
    query_response = bq_client.query(q)
    return query_response


def add_hpo_bucket(bq_client, hpo_id, bucket_name, service='default'):
    """
    adds hpo bucket name in hpo_bucket_name table.
    :param bq_client: BigQuery Client
    :param hpo_id: hpo identifier
    :param bucket_name: bucket name assigned to hpo
    :return:
    """
    q = ADD_HPO_ID_BUCKET_NAME.render(
        project_id=bq_client.project,
        lookup_tables_dataset=bq_consts.LOOKUP_TABLES_DATASET_ID,
        hpo_id_bucket_name_table=bq_consts.HPO_ID_BUCKET_NAME_TABLE_ID,
        hpo_id=hpo_id,
        bucket_name=bucket_name,
        service=service)
    LOGGER.info(f'Adding bucket lookup with the following query:\n {q}\n')
    query_response = bq_client.query(q)
    return query_response


def add_lookups(bq_client,
                hpo_id,
                hpo_name,
                org_id,
                bucket_name,
                display_order=None,
                service='default'):
    """
    Add hpo to hpo_site_id_mappings and hpo_id_bucket_name

    :param bq_client: BigQuery Client
    :param hpo_id: identifies the hpo
    :param hpo_name: name of the hpo
    :param org_id: identifies the associated organization
    :param bucket_name: identifies the bucket
    :param display_order: site's display order in dashboard; if unset, site appears last
    :return:
    """
    if display_order is None:
        display_order = get_last_display_order(bq_client)
    else:
        shift_display_orders(bq_client, display_order)
    add_hpo_mapping(bq_client, hpo_id, hpo_name, org_id, display_order)
    add_hpo_bucket(bq_client, hpo_id, bucket_name, service)


def bucket_access_configured(gcs_client, bucket_name: str) -> bool:
    """
    Determine if the service account has appropriate permissions on the bucket

    :param gcs_client: Google Cloud Service Client
    :param bucket_name: identifies the GCS bucket
    :return: True if the service account has appropriate permissions, False otherwise
    """
    bucket = gcs_client.bucket(bucket_name)
    permissions: list = bucket.test_iam_permissions("storage.objects.create")
    return len(permissions) >= 1


def update_site_masking_table(bq_client, us_state, value_source_concept_id):
    """
    Creates a unique `site_maskings` sandbox table and updates the `site_maskings` table with the
        new site maskings

    :param bq_client: BigQuery Client
    :param us_state: PIIState ID for the New Site
    :param value_source_concept_id: Value Source Concept ID of Site's State
    :return:
    """
    update_site_maskings_query = UPDATE_SITE_MASKING_QUERY.render(
        project_id=bq_client.project,
        pipeline_tables_dataset=PIPELINE_TABLES,
        site_maskings_table=SITE_MASKING_TABLE_ID,
        lookup_tables_dataset=bq_consts.LOOKUP_TABLES_DATASET_ID,
        hpo_site_id_mappings_table=bq_consts.HPO_SITE_ID_MAPPINGS_TABLE_ID,
        us_state=us_state,
        value_source_concept_id=value_source_concept_id)

    LOGGER.info(
        f'Updating site_masking table with new hpo_id and src_id with the following '
        f'query:\n {update_site_maskings_query}\n ')

    query_job = bq_client.query(update_site_maskings_query)
    if query_job.errors:
        raise RuntimeError(
            f"Failed to update site_masking table. Error message: {query_job.errors}"
        )

    return query_job


def check_state_code_format(us_state):
    """
    Check if the us-state code format is acceptable.
    :param us_state: State code of the Site mentioned in the command
    :return:
    """
    if not us_state.startswith("PIIState_"):
        raise ValueError()


def main(project_id, hpo_id, org_id, hpo_name, bucket_name, display_order,
         addition_type, hpo_site_mappings_path, run_as, us_state,
         value_source_concept_id):
    """
    adds HPO name and details in to hpo_csv and adds HPO to the lookup tables in bigquery
    adds new site masking to pipeline_tables.site_maskings
    :param project_id: Project ID
    :param hpo_id: HPO identifier
    :param org_id: HPO organisation identifier
    :param hpo_name: name of the HPO
    :param bucket_name: bucket name assigned to HPO
    :param display_order: index where new HPO should be added
    :param addition_type: indicates if hpo is added to config file or to lookup tables
        This is necessary because a config update will need to be verified in the curation_devops repo
        before updating the lookup tables. Can take values "update_config" or "update_lookup_tables"
    :param hpo_site_mappings_path: path to csv file containing hpo site information
    :param run_as: Service Account for impersonation
    :param us_state: Site's PIIState ID in PIIState_XY format
    :param value_source_concept_id: Value Source Concept ID for the State.
    :return:
    """

    impersonation_creds = get_impersonation_credentials(
        run_as, target_scopes=CDR_SCOPES)
    bq_client = BigQueryClient(project_id=project_id,
                               credentials=impersonation_creds)
    gcs_client = StorageClient(project_id=project_id,
                               credentials=impersonation_creds)

    if addition_type == "update_config":
        add_hpo_site_mappings_csv(hpo_id, hpo_name, org_id,
                                  hpo_site_mappings_path, display_order)
    elif addition_type == "update_lookup_tables":
        if bucket_access_configured(gcs_client, bucket_name):
            LOGGER.info(f'Accessing bucket {bucket_name} successful. '
                        f'Proceeding to add site.')
            add_lookups(bq_client, hpo_id, hpo_name, org_id, bucket_name,
                        display_order)

            LOGGER.info(
                f'hpo_site_id_mappings table successfully updated. Updating `{bq_consts.HPO_SITE_ID_MAPPINGS_TABLE_ID}` '
                f'table')
            update_site_masking_table(bq_client, us_state,
                                      value_source_concept_id)

        else:
            raise RuntimeError(
                f'{addition_type} was skipped because the bucket {bucket_name} is inaccessible.'
            )


if __name__ == '__main__':
    import argparse

    pipeline_logging.configure(level=logging.DEBUG, add_console_handler=True)

    parser = argparse.ArgumentParser(
        description='Add a new HPO site to hpo config file and lookup tables',
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('-p', '--project_id', required=True, help='Project ID')
    parser.add_argument('-c',
                        '--run_as',
                        required=True,
                        help='Service Account to impersonate')
    parser.add_argument('-i',
                        '--hpo_id',
                        required=True,
                        help='Identifies the HPO site')
    parser.add_argument('-n',
                        '--hpo_name',
                        required=True,
                        help='Name of the HPO site')
    parser.add_argument('-o',
                        '--org_id',
                        required=True,
                        help='Identifies the associated organization')
    parser.add_argument('-b',
                        '--bucket_name',
                        required=True,
                        help='Name of the GCS bucket')
    parser.add_argument('-s',
                        '--us_state',
                        required=True,
                        type=check_state_code_format,
                        help="Site's State as PIIState_XY.")
    parser.add_argument('-v',
                        '--value_source_concept_id',
                        required=True,
                        help="Value Source Concept ID of the site's state.")
    parser.add_argument(
        '-t',
        '--addition_type',
        required=True,
        help='indicates if hpo is added to config file or to lookup tables. '
        'This is necessary because a config update will need to be verified '
        'in the curation_devops repo before updating the lookup tables. '
        'Can take values "update_config" or "update_lookup_tables"')

    parser.add_argument('-f',
                        '--hpo_site_mappings_path',
                        required=True,
                        help='File containing HPO site information')

    parser.add_argument(
        '-d',
        '--display_order',
        type=int,
        required=False,
        default=None,
        help='Display order in dashboard; increments display order by default')

    args = parser.parse_args()
    main(args.project_id, args.hpo_id, args.org_id, args.hpo_name,
         args.bucket_name, args.display_order, args.addition_type,
         args.hpo_site_mappings_path, args.run_as, args.us_state,
         args.value_source_concept_id)
