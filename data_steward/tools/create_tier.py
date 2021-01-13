# Python imports
import argparse
import logging
import re

# Third party imports

# Project imports
from utils import pipeline_logging

LOGGER = logging.getLogger(__name__)


def validate_tier_param(tier):
    """
    helper function to validate the tier parameter passed is either 'controlled' or 'registered'

    :param tier: tier parameter passed through from either a list or command line argument
    :return: nothing, breaks if not valid
    """
    if tier.lower() not in ['controlled', 'registered']:
        LOGGER.error(
            f"Parameter ERROR: {tier} is an incorrect input for the tier parameter, accepted: controlled or "
            f"registered")


def validate_deid_stage_param(deid_stage):
    """
    helper function to validate the deid_stage parameter passed is correct, must be 'deid', 'base' or 'clean'

    :param deid_stage: deid_stage parameter passed through from either a list or command line argument
    :return: nothing, breaks if not valid
    """
    if deid_stage not in ['deid', 'base', 'clean']:
        LOGGER.error(
            f"Parameter ERROR: {deid_stage} is an incorrect input for the deid_stage parameter, accepted: "
            f"deid, base, clean")


def valid_release_tag(arg_value):
    """
    User defined helper function to validate that the release_tag parameter follows the correct naming convention

    :param arg_value: release tag parameter passed through either the command line arguments
    :return: arg_value
    """

    release_tag_regex = re.compile(r'[0-9]{4}q[0-9]r[0-9]')
    if not re.match(release_tag_regex, arg_value):
        LOGGER.error(
            f"Parameter ERROR {arg_value} is in an incorrect format, accepted: YYYYq#r#"
        )
        raise argparse.ArgumentTypeError
    return arg_value


def create_tier(credentials_filepath, project_id, tier, input_dataset,
                release_tag, deid_stage):
    """
    This function is hte main entry point for the deid process.
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
                        choices=['controlled', 'registered'])
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
        type=valid_release_tag)
    parser.add_argument('-d',
                        '--deid_stage',
                        action='store',
                        dest='deid_stage',
                        help='deid stage (deid, base or clean)',
                        required=True,
                        choices=['deid', 'base', 'clean'])
    return parser.parse_args(args)


def main(raw_args=None):
    args = parse_deid_args(raw_args)
    pipeline_logging.configure(level=logging.DEBUG,
                               add_console_handler=True)
    create_tier(args.credentials_filepath, args.project_id, args.tier,
                args.idataset, args.release_tag, args.deid_stage)


if __name__ == '__main__':
    main()
