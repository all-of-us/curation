"""
Add a new HPO site to config file and BigQuery lookup tables

Note: GAE environment must still be set manually
"""
import logging
import csv

from googleapiclient.errors import HttpError
import pandas as pd

import bq_utils
import resources
import constants.bq_utils as bq_consts
from tools import cli_util
import gcs_utils

LOGGER = logging.getLogger(__name__)

DEFAULT_DISPLAY_ORDER = """
SELECT MAX(Display_Order) + 1 AS display_order FROM {hpo_site_id_mappings_table_id}
"""

SHIFT_HPO_SITE_DISPLAY_ORDER = """
UPDATE {hpo_site_id_mappings_table_id}
SET Display_Order = Display_Order + 1
WHERE Display_Order >= {display_order}
"""

ADD_HPO_SITE_ID_MAPPING = """
SELECT '{org_id}' AS Org_ID, '{hpo_id}' AS HPO_ID, '{hpo_name}' AS Site_Name, {display_order} AS Display_Order
"""

ADD_HPO_ID_BUCKET_NAME = """
SELECT '{hpo_id}' AS hpo_id, '{bucket_name}' AS bucket_name
"""


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


def add_hpo_site_mappings_file_df(hpo_id, hpo_name, org_id, display_order):
    """
    Creates dataframe with hpo_id, hpo_name, org_id, display_order

    :param hpo_id: hpo_ identifier
    :param hpo_name: name of the hpo
    :param org_id: hpo organization identifier
    :param display_order: index number in which hpo should be added in table
    :raises ValueError if hpo_id already exists in the lookup table
    """
    hpo_table = bq_utils.get_hpo_info()
    hpo_table_df = pd.DataFrame(hpo_table)
    if hpo_id in hpo_table_df['hpo_id'] or hpo_name in hpo_table_df['name']:
        raise ValueError(
            f"{hpo_id}/{hpo_name} already exists in site lookup table")

    hpo_file_df = pd.read_csv(resources.hpo_site_mappings_path)
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


def add_hpo_site_mappings_csv(hpo_id, hpo_name, org_id, display_order=None):
    """
    Writes df with hpo_id, hpo_name, org_id, display_order to the hpo_site_id_mappings config file

    :param hpo_id: hpo_ identifier
    :param hpo_name: name of the hpo
    :param org_id: hpo organization identifier
    :param display_order: index number in which hpo should be added in table
    :return:
    """
    hpo_file_df = add_hpo_site_mappings_file_df(hpo_id, hpo_name, org_id,
                                                display_order)
    hpo_file_df.to_csv(resources.hpo_site_mappings_path,
                       quoting=csv.QUOTE_ALL,
                       index=False)


def get_last_display_order():
    """
    gets the display order from hpo_site_id_mappings table
    :return:
    """
    q = DEFAULT_DISPLAY_ORDER.format(
        hpo_site_id_mappings_table_id=bq_consts.HPO_SITE_ID_MAPPINGS_TABLE_ID)
    query_response = bq_utils.query(q)
    rows = bq_utils.response2rows(query_response)
    row = rows[0]
    result = row['display_order']
    return result


def shift_display_orders(at_display_order):
    """
    shift the display order in hpo_site_id_mappings_table when a new HPO is to be added.
    :param at_display_order: index where the display order
    :return:
    """
    q = SHIFT_HPO_SITE_DISPLAY_ORDER.format(
        display_order=at_display_order,
        hpo_site_id_mappings_table_id=bq_consts.HPO_SITE_ID_MAPPINGS_TABLE_ID)
    LOGGER.info(f'Shifting lookup with the following query:\n {q}\n')
    query_response = bq_utils.query(q)
    return query_response


def add_hpo_mapping(hpo_id, hpo_name, org_id, display_order):
    """
    adds hpo_id, hpo_name, org_id, display_order to the hpo_site_id_mappings table
    :param hpo_id: hpo_ identifier
    :param hpo_name: name of the hpo
    :param org_id: hpo organization identifier
    :param display_order: index number in which hpo should be added in table
    :return:
    """
    q = ADD_HPO_SITE_ID_MAPPING.format(hpo_id=hpo_id,
                                       hpo_name=hpo_name,
                                       org_id=org_id,
                                       display_order=display_order)
    LOGGER.info(f'Adding mapping lookup with the following query:\n {q}\n')
    query_response = bq_utils.query(
        q,
        destination_table_id=bq_consts.HPO_SITE_ID_MAPPINGS_TABLE_ID,
        write_disposition='WRITE_APPEND')
    return query_response


def add_hpo_bucket(hpo_id, bucket_name):
    """
    adds hpo bucket name in hpo_bucket_name table.
    :param hpo_id: hpo identifier
    :param bucket_name: bucket name assigned to hpo
    :return:
    """
    q = ADD_HPO_ID_BUCKET_NAME.format(hpo_id=hpo_id, bucket_name=bucket_name)
    LOGGER.info(f'Adding bucket lookup with the following query:\n {q}\n')
    query_response = bq_utils.query(
        q,
        destination_table_id=bq_consts.HPO_ID_BUCKET_NAME_TABLE_ID,
        write_disposition='WRITE_APPEND')
    return query_response


def add_lookups(hpo_id, hpo_name, org_id, bucket_name, display_order=None):
    """
    Add hpo to hpo_site_id_mappings and hpo_id_bucket_name

    :param hpo_id: identifies the hpo
    :param hpo_name: name of the hpo
    :param org_id: identifies the associated organization
    :param bucket_name: identifies the bucket
    :param display_order: site's display order in dashboard; if unset, site appears last
    :return:
    """
    if display_order is None:
        display_order = get_last_display_order()
    else:
        shift_display_orders(display_order)
    add_hpo_mapping(hpo_id, hpo_name, org_id, display_order)
    add_hpo_bucket(hpo_id, bucket_name)


def bucket_access_configured(bucket_name):
    """
    Determine if the service account has appropriate permissions on the bucket

    :param bucket_name: identifies the GCS bucket
    :return: True if the service account has appropriate permissions, False otherwise
    :raises HttpError if accessing bucket fails
    """
    try:
        gcs_utils.list_bucket(bucket_name)
        return True
    except HttpError:
        raise


def main(hpo_id, org_id, hpo_name, bucket_name, display_order, addition_type):
    """
    adds HPO name and details in to hpo_csv and adds HPO to the lookup tables in bigquery
    :param hpo_id: HPO identifier
    :param org_id: HPO organisation identifier
    :param hpo_name: name of the HPO
    :param bucket_name: bucket name assigned to HPO
    :param display_order: index where new HPO should be added
    :param addition_type: indicates if hpo is added to config file or to lookup tables
        This is necessary because a config update will need to be verified in the curation_devops repo
        before updating the lookup tables. Can take values "update_config" or "update_lookup_tables"
    :return:
    """
    if addition_type == "update_config":
        add_hpo_site_mappings_csv(hpo_id, hpo_name, org_id, display_order)
    elif addition_type == "update_lookup_tables":
        if bucket_access_configured(bucket_name):
            LOGGER.info(f'Accessing bucket {bucket_name} successful. '
                        f'Proceeding to add site.')
            add_lookups(hpo_id, hpo_name, org_id, bucket_name, display_order)


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(
        description='Add a new HPO site to hpo config file and lookup tables',
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('-c',
                        '--credentials',
                        required=True,
                        help='Path to GCP credentials file')
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
    parser.add_argument(
        '-t',
        '--addition_type',
        required=True,
        help='indicates if hpo is added to config file or to lookup tables. '
        'This is necessary because a config update will need to be verified '
        'in the curation_devops repo before updating the lookup tables. '
        'Can take values "update_config" or "update_lookup_tables"')
    parser.add_argument(
        '-d',
        '--display_order',
        type=int,
        required=False,
        default=None,
        help='Display order in dashboard; increments display order by default')

    args = parser.parse_args()
    creds_path = args.credentials
    cli_util.activate_creds(creds_path)
    cli_util.set_default_dataset_id(bq_consts.LOOKUP_TABLES_DATASET_ID)
    main(args.hpo_id, args.org_id, args.hpo_name, args.bucket_name,
         args.display_order, args.addition_type)
