# Python imports
import argparse
import logging
import re

# Third party imports

# Project imports
from utils import pipeline_logging
from utils import bq

LOGGER = logging.getLogger(__name__)

TIER_LIST = ['controlled', 'registered']
DEID_STAGE_LIST = ['deid', 'base', 'clean']


def validate_tier_param(tier):
    """
    helper function to validate the tier parameter passed is either 'controlled' or 'registered'

    :param tier: tier parameter passed through from either a list or command line argument
    :return: nothing, breaks if not valid
    """
    if tier.lower() not in TIER_LIST:
        msg = f"Parameter ERROR: {tier} is an incorrect input for the tier parameter, accepted: controlled or " \
              f"registered"
        LOGGER.error(msg)
        raise argparse.ArgumentTypeError(msg)


def validate_deid_stage_param(deid_stage):
    """
    helper function to validate the deid_stage parameter passed is correct, must be 'deid', 'base' or 'clean'

    :param deid_stage: deid_stage parameter passed through from either a list or command line argument
    :return: nothing, breaks if not valid
    """
    if deid_stage not in DEID_STAGE_LIST:
        msg = f"Parameter ERROR: {deid_stage} is an incorrect input for the deid_stage parameter, accepted: deid, " \
              f"base, clean"
        LOGGER.error(msg)
        raise argparse.ArgumentTypeError(msg)


def validate_release_tag_param(arg_value):
    """
    User defined helper function to validate that the release_tag parameter follows the correct naming convention

    :param arg_value: release tag parameter passed through either the command line arguments
    :return: arg_value
    """

    release_tag_regex = re.compile(r'[0-9]{4}q[0-9]r[0-9]')
    if not re.match(release_tag_regex, arg_value):
        msg = f"Parameter ERROR {arg_value} is in an incorrect format, accepted: YYYYq#r#"
        LOGGER.error(msg)
        raise argparse.ArgumentTypeError(msg)
    return arg_value


def get_dataset_name(tier, release_tag, deid_stage):
    """
    Helper function to create the dataset name based on the given criteria
    This function should return a name for the final dataset only (not all steps along the way)
    The function returns a string in the form: [C|R]{release_tag}_deid[_base|_clean]

    :param tier: controlled or registered tier intended for the output dataset
    :param release_tag: release tag for dataset in the format of YYYYq#r#
    :param deid_stage: deid stage (deid, base or clean)
    :return: a string for the dataset name
    """
    # validate parameters
    validate_tier_param(tier)
    validate_release_tag_param(release_tag)
    validate_deid_stage_param(deid_stage)

    tier = tier[0].upper()

    dataset_name = f"{tier}{release_tag}_{deid_stage}"

    return dataset_name


def create_datasets(client, name, input_dataset, tier, release_tag, deid_stage):
    """
    Creates backup, staging, sandbox, and final datasets with the proper descriptions
    and tag/labels applied

    :param client: an instantiated bigquery client object
    :param name: the base name of the datasets to be created
    :param input_dataset: name of the input dataset
    :param tier: tier parameter passed through from either a list or command line argument
    :param release_tag: release tag parameter passed through either the command line arguments
    :param deid_stage: deid_stage parameter passed through from either a list or command line argument
    :return: tuple of created dataset names
    """

    if not client:
        raise RuntimeError("Please specify BigQuery client object")
    if not name:
        raise RuntimeError(
            "Please specify the base name of the datasets to be created")
    if not input_dataset:
        raise RuntimeError("Please specify the name of the input dataset")
    if not tier:
        raise RuntimeError(
            "Please specify the tier intended for the output datasets")
    if not release_tag:
        raise RuntimeError(
            "Please specify the release tag for the dataset in the format of YYYY#q#r"
        )
    if not deid_stage:
        raise RuntimeError(
            "Please specify the deid stage (deid, base, or clean)")

    # Construct names of datasets need as part of the deid process
    final_dataset_id = name
    backup_dataset_id = f'backup_{name}'
    staging_dataset_id = f'staging_{name}'
    sandbox_dataset_id = f'sandbox_{name}'

    dataset_ids = [
        final_dataset_id, backup_dataset_id, staging_dataset_id,
        sandbox_dataset_id
    ]

    # base labels and tags for the datasets
    base_labels_and_tags = {
        'release_tag': release_tag,
        'phase': deid_stage,
        'data_tier': tier
    }

    description = f'Dataset created for {release_tag} {tier} CDR run'

    # Creation of dataset objects
    dataset_objects = []
    for dataset in dataset_ids:
        dataset_object = bq.define_dataset(client.project, dataset, description,
                                           base_labels_and_tags)
        dataset_objects.append(dataset_object)

    # Creation of datasets
    for dataset_object in dataset_objects:
        client.create_dataset(dataset_object, exists_ok=False)

    # Update the labels and tags
    bq.update_labels_and_tags(backup_dataset_id, base_labels_and_tags,
                              {'de-identified': 'false'})
    bq.update_labels_and_tags(staging_dataset_id, base_labels_and_tags,
                              {'de-identified': 'true'})
    bq.update_labels_and_tags(final_dataset_id, base_labels_and_tags,
                              {'de-identified': 'true'})

    # Copy input dataset tables to backup and staging datasets
    tables = client.list_tables(input_dataset)
    for table in tables:
        input_tables = f'{input_dataset}.{table.table_id}'
        backup_tables = f'{backup_dataset_id}.{table.table_id}'
        client.copy_table(input_tables, backup_tables)

    for table in tables:
        input_tables = f'{input_dataset}.{table.table_id}'
        staging_tables = f'{staging_dataset_id}.{table.table_id}'
        client.copy_table(input_tables, staging_tables)

    return final_dataset_id, backup_dataset_id, staging_dataset_id, sandbox_dataset_id


def create_tier(credentials_filepath, project_id, tier, input_dataset,
                release_tag, deid_stage):
    """
    This function is the main entry point for the deid process.
    It passes the required parameters to the implementing functions.

    :param credentials_filepath: filepath to credentials to access GCP
    :param project_id: project_id associated with the input dataset
    :param tier: controlled or registered tier intended for the output dataset
    :param input_dataset: name of the input dataset
    :param release_tag: release tag for dataset in the format of YYYYq#r#
    :param deid_stage: deid stage (deid, base or clean)
    :return: name of created controlled or registered dataset
    """

    # validation of params
    validate_tier_param(tier)
    validate_deid_stage_param(deid_stage)
    validate_release_tag_param(release_tag)


def parse_deid_args(args=None):
    parser = argparse.ArgumentParser(
        description='Parse deid command line arguments')
    parser.add_argument('-c',
                        '--credentials_filepath',
                        dest='credentials_filepath',
                        action='store',
                        help='file path to credentials for GCP to access BQ',
                        required=True)
    parser.add_argument('-p',
                        '--project_id',
                        action='store',
                        dest='project_id',
                        help=('Project associated with the '
                              'input dataset.'),
                        required=True)
    parser.add_argument('-t',
                        '--tier',
                        action='store',
                        dest='tier',
                        help='controlled or registered tier',
                        required=True,
                        choices=TIER_LIST)
    parser.add_argument('-i',
                        '--idataset',
                        action='store',
                        dest='idataset',
                        help='Name of the input dataset',
                        required=True)
    parser.add_argument(
        '-r',
        '--release_tag',
        action='store',
        dest='release_tag',
        help='release tag for dataset in the format of YYYYq#r#',
        required=True,
        type=validate_release_tag_param)
    parser.add_argument('-d',
                        '--deid_stage',
                        action='store',
                        dest='deid_stage',
                        help='deid stage (deid, base or clean)',
                        required=True,
                        choices=DEID_STAGE_LIST)
    parser.add_argument('-l',
                        '--console_log',
                        dest='console_log',
                        action='store_true',
                        required=False,
                        help='Log to the console as well as to a file.')
    return parser.parse_args(args)


def main(raw_args=None):
    args = parse_deid_args(raw_args)
    pipeline_logging.configure(level=logging.DEBUG,
                               add_console_handler=args.console_log)
    create_tier(args.credentials_filepath, args.project_id, args.tier,
                args.idataset, args.release_tag, args.deid_stage)


if __name__ == '__main__':
    main()
