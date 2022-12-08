import argparse
import re
import logging

LOGGER = logging.getLogger(__name__)


def validate_release_tag_param(arg_value: str) -> str:
    """
    User defined helper function to validate that the release_tag parameter follows the correct naming convention

    :param arg_value: release tag parameter passed through either the command line arguments
    :return: arg_value
    :raises: ArgumentTypeError if the string is not formatted correctly
    """

    release_tag_regex = re.compile(r'[0-9]{4}q[0-9]r[0-9]')
    if not re.match(release_tag_regex, arg_value):
        msg = (f"Parameter ERROR {arg_value} is in an "
               "incorrect format, accepted: YYYYq#r#")
        LOGGER.error(msg)
        raise argparse.ArgumentTypeError(msg)
    return arg_value


def validate_output_release_tag_param(arg_value: str) -> str:
    """
    User defined helper function to validate that the release_tag parameter follows the correct naming convention

    :param arg_value: release tag parameter passed through either the command line arguments
    :return: arg_value
    :raises: ArgumentTypeError if the string is not formatted correctly
    """

    release_tag_regex = re.compile(r'[0-9]{4}Q[0-9]R[0-9]')
    if not re.match(release_tag_regex, arg_value):
        msg = (f"Parameter ERROR {arg_value} is in an "
               "incorrect format, accepted: YYYYQ#R#")
        LOGGER.error(msg)
        raise argparse.ArgumentTypeError(msg)
    return arg_value


def validate_qualified_bq_tablename(arg_value: str) -> str:
    """
    Given a string, verify it is a BigQuery fully qualified table name

    :param arg_value: string to verify adheres to BigQuery naming
    conventions seen here,
    https://cloud.google.com/bigquery/docs/datasets#dataset-naming
    https://cloud.google.com/bigquery/docs/tables#table_naming
    :return: arg_value
    :raises: ArgumentTypeError if the string is not formatted correctly
    """
    pattern = r"^([a-z0-9A-Z_'!\-]{4,30}).([a-z0-9A-Z_]{1,1024}).([a-z0-9A-Z_\-]{1,1024})$"
    qualified_tablename_regex = re.compile(pattern)
    if not re.match(qualified_tablename_regex, arg_value):
        msg = (f"Parameter ERROR, `{arg_value}` is in an incorrect "
               f"format.  Acceppted format:  `{pattern}`")
        LOGGER.error(msg)
        raise argparse.ArgumentTypeError(msg)
    return arg_value


def validate_bq_project_name(arg_value: str) -> str:
    """
    Given a string, verify it is a BigQuery fully qualified table name

    :param arg_value: string to verify adheres to BigQuery naming
    conventions seen here,
    https://cloud.google.com/bigquery/docs/datasets#dataset-naming
    https://cloud.google.com/bigquery/docs/tables#table_naming
    :return: arg_value
    :raises: ArgumentTypeError if the string is not formatted correctly
    """
    pattern = r"^([a-z0-9A-Z_'!\-]{4,30})$"
    qualified_tablename_regex = re.compile(pattern)
    if not re.match(qualified_tablename_regex, arg_value):
        msg = (f"Parameter ERROR, `{arg_value}` is in an incorrect "
               f"format.  Acceppted format:  `{pattern}`")
        LOGGER.error(msg)
        raise argparse.ArgumentTypeError(msg)
    return arg_value