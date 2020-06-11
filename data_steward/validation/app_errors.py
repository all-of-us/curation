#!/usr/bin/env python
"""
This module is responsible handling errors raised when validating EHR submissions.

This module focuses on alerting application developers when an error occurs and
returning generic error data to views to ensure program integrity.
"""
# Python imports
import logging

# Third party imports
import flask
from googleapiclient.errors import HttpError

# Project imports
from utils.slack_alerts import post_message, SlackConfigurationError

errors_blueprint = flask.Blueprint('app_errors', __name__)

LOGGER = logging.getLogger(__name__)

DEFAULT_VIEW_MESSAGE = {
    "status": "error",
    "message": "view the logs or alert channel for more information"
}

DEFAULT_ERROR_STATUS = 500


class InternalValidationError(RuntimeError):
    """Raised when an internal error occurs during validation"""

    def __init__(self, msg):
        super(InternalValidationError, self).__init__(msg)


class BucketDoesNotExistError(RuntimeError):
    """Raised when a configured bucket does not exist"""

    def __init__(self, msg, bucket):
        super(BucketDoesNotExistError, self).__init__(msg)
        self.bucket = bucket


def _handle_error(alert_message, view_message=None, response_code=None):
    """
    Helper function to send slack alerts and return generic error views.

    The alert message may contain sensitive information and should NEVER be used
    as part of the view message or view return value.

    :param alert_message:  the message intended to be sent to the alerting
        mechanism
    :param view_message:  the message that will be provided to the end user in
        the event of an error.  this should remain generic and NEVER provide
        insight to site info, participant info, or code info.  an appropriate
        default message is provided if no view_message parameter is specified.
        This can accept either a string or a dictionary.
    :param response_code: The numeric status code to return

    :return: A message string for the view in json format and an error code.
        Any error raised by the application will return a 500 code.
    """
    try:
        post_message(alert_message)
    except SlackConfigurationError:
        LOGGER.exception('Slack is not configured for posting messages.  '
                         'Read the playbook for deployment and updated as '
                         'as needed.')

    view_message = view_message if isinstance(view_message,
                                              (dict,
                                               str)) else DEFAULT_VIEW_MESSAGE
    status_code = response_code if isinstance(response_code,
                                              int) else DEFAULT_ERROR_STATUS
    return view_message, status_code


def format_alert_message(raised_error, message):
    """
    Formats error in the desired format

    :param raised_error: Error that was raised
    :param message: Message accompanying the error
    :return: Formatted f-string
    """
    return f"Error raised: {raised_error}\nMessage: {message}"


@errors_blueprint.app_errorhandler(BucketDoesNotExistError)
def handle_bad_bucket_request(error):
    """
    Error handler to use when a BucketDoesNotExistError is raised.

    Alert message can be modified here as needed.

    :param error:  The error that is handled.

    :return:  an error view
    """
    alert_message = format_alert_message(error.__class__.__name__, str(error))
    return _handle_error(alert_message)


@errors_blueprint.app_errorhandler(InternalValidationError)
def handle_internal_validation_error(error):
    """
    Error handler to use when a InternalValidationError is raised.

    Alert message can be modified here as needed.

    :param error:  The error that is handled.

    :return:  an error view
    """
    alert_message = format_alert_message(error.__class__.__name__, str(error))
    return _handle_error(alert_message)


@errors_blueprint.app_errorhandler(HttpError)
def handle_api_client_errors(error):
    """
    Error handler to use when a HttpError is raised.

    Alert message can be modified here as needed.

    :param error:  The error that is handled.

    :return:  an error view
    """
    alert_message = format_alert_message(error.__class__.__name__, str(error))
    return _handle_error(alert_message)


@errors_blueprint.app_errorhandler(AttributeError)
def handle_attribute_errors(error):
    """
    Error handler to use when a AttributeError is raised.

    Alert message can be modified here as needed.

    :param error:  The error that is handled.

    :return:  an error view
    """
    alert_message = format_alert_message(error.__class__.__name__, str(error))
    return _handle_error(alert_message)


@errors_blueprint.app_errorhandler(OSError)
def handle_os_errors(error):
    """
    Error handler to use when a OSError is raised.

    Alert message can be modified here as needed.

    :param error:  The error that is handled.

    :return:  an error view
    """
    alert_message = format_alert_message(error.__class__.__name__, str(error))
    return _handle_error(alert_message)
