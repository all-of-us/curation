import argparse
import logging
import os
import re

LOGGER = logging.getLogger(__name__)


def __validate_regular_expression(arg_value: str,
                                  pattern: str,
                                  human_readable: str = None,
                                  match_pattern: bool = True) -> str:
    """
    Private function to standardize regular expression validations.

    :param arg_value: The string value that is being validated.
    :param pattern:  The regular expression that arg_value must match or not match
    :param human_readable:  An optional example of what the pattern represents.
        This makes error messages easier to read.
    :param match_pattern: An optional boolean to indicate if the arg_value must match
        or must not match the given pattern.  The default True, arg_value must match the
        pattern or an Error is raised.

    :return: On successful validation, the string arg_value is returned
    :raises argparse.ArgumentTypeError if the string value cannot be
        validated
    """
    human_readable = pattern if not human_readable else human_readable
    regex = re.compile(pattern)
    msg = ''
    if match_pattern:
        if not re.match(regex, arg_value):
            msg = (f"Parameter ERROR, `{arg_value}` is in an incorrect "
                   f"format.\n\tAccepted format:  `{human_readable}`.\n\t"
                   f"Technical format:  `{pattern}`")
    else:
        if re.match(regex, arg_value):
            msg = (f"Parameter ERROR, `{arg_value}` matches a known "
                   f"bad pattern:  `{human_readable}`")

    if msg:
        LOGGER.error(msg)
        raise argparse.ArgumentTypeError(msg)

    return arg_value


def validate_release_tag_param(arg_value: str) -> str:
    """
    User defined helper function to validate that the release_tag parameter follows the correct naming convention

    :param arg_value: release tag parameter passed through either the command line arguments
    :return: arg_value
    :raises: ArgumentTypeError if the string is not formatted correctly
    """
    pattern = r'[0-9]{4}q[1-4]r[0-9]{1,2}'
    human_readable = 'YYYYq#r#'
    return __validate_regular_expression(arg_value, pattern, human_readable)


def validate_output_release_tag_param(arg_value: str) -> str:
    """
    User defined helper function to validate that the release_tag parameter follows the correct naming convention

    :param arg_value: release tag parameter passed through either the command line arguments
    :return: arg_value
    :raises: ArgumentTypeError if the string is not formatted correctly
    """
    pattern = r'[0-9]{4}Q[1-4]R[0-9]{1,2}'
    human_readable = 'YYYYQ#R#'
    return __validate_regular_expression(arg_value, pattern, human_readable)


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
    human_readable = "project-id.dataset_id.table-id_"
    return __validate_regular_expression(arg_value, pattern, human_readable)


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
    return __validate_regular_expression(arg_value, pattern)


def validate_file_exists(arg_value: str) -> str:
    """
    Given a string, verify it is an existing file

    :param arg_value: string to validate represents a real file
    :return: arg_value
    :raises: ArgumentTypeError if the string is not formatted correctly
    """
    if not os.path.isfile(arg_value):
        msg = (f"Parameter ERROR, `{arg_value}` does not exist.")
        LOGGER.error(msg)
        raise argparse.ArgumentTypeError(msg)
    return arg_value


def validate_email_address(arg_value: str) -> str:
    """
    Given a string, verify it is a BigQuery fully qualified table name

    :param arg_value: string to validate represents a real file
    :return: arg_value
    :raises: ArgumentTypeError if the string is not formatted correctly
    """
    pattern = r"^([a-z0-9A-Z_\-\.]+)@([a-z0-9A-Z_\-]+)(\.([a-z0-9A-Z_\-]+))+$"
    return __validate_regular_expression(arg_value,
                                         pattern,
                                         human_readable='user_name@domain')


def validate_bucket_filepath(arg_value: str) -> str:
    """
    Given a string, verify it is a BigQuery bucket filepath

    This will expect the filepath to begin with 'gs://'.

    Attempts to follow conventions defined here,
    https://cloud.google.com/storage/docs/objects#naming

    :param arg_value: string to validate represents a real file
    :return: arg_value
    :raises: ArgumentTypeError if the string is not formatted correctly
    """
    pattern = r"^(gs:\/\/\.well\-known\/acme\-challenge(\/)?)"
    __validate_regular_expression(
        arg_value,
        pattern,
        human_readable='gs://.well-known/acme-challenge/',
        match_pattern=False)

    pattern = r"^(gs:\/\/\.{1,2})$"
    __validate_regular_expression(arg_value,
                                  pattern,
                                  human_readable='gs://. or gs://..',
                                  match_pattern=False)

    pattern = r"^(gs:\/\/)([a-zA-Z0-9\/\-_\.]+)$"
    return __validate_regular_expression(arg_value, pattern)