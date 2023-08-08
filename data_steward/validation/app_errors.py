#!/usr/bin/env python
"""
This module is responsible handling errors raised when validating EHR submissions.

This module focuses on alerting application developers when an error occurs and
returning generic error data to views to ensure program integrity.
"""
# Python imports
import logging
import traceback
from time import sleep

# Third party imports
import flask
from googleapiclient.errors import HttpError
from google.api_core.exceptions import ServiceUnavailable

# Project imports

errors_blueprint = flask.Blueprint('app_errors', __name__)

LOGGER = logging.getLogger(__name__)

DEFAULT_VIEW_MESSAGE = {
    "status": "error",
    "message": "view the logs or alert channel for more information"
}

DEFAULT_ERROR_STATUS = 500


def log_traceback(func):
    """
    Wrapper that prints exception tracebacks to stdout

    This is only a temporary fix until we add capability to handle
    all errors encountered by the app within this module/errors_blueprint
    """

    def wrapper(*args, retries=0, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            alert_message = format_alert_message(e.__class__.__name__, str(e))
            logging.exception(alert_message, exc_info=True, stack_info=True)
            logging.info(f'{traceback.print_exc()}')

            if isinstance(e, ServiceUnavailable):
                if retries == 0:
                    logging.error(
                        'The above exception is due to a 503 error. Job will be retried in 3 minutes.'
                    )
                    sleep(60 * 3)
                    logging.info('Retrying job.')
                    wrapper(func(*args, **kwargs), retries=retries + 1)
                else:
                    logging.error(
                        'The above exception is due to a 503. Retries exceeded. Check the logs to troubleshoot.'
                    )
            else:
                raise e

    return wrapper


class InternalValidationError(RuntimeError):
    """Raised when an internal error occurs during validation"""

    def __init__(self, msg):
        super(InternalValidationError, self).__init__(msg)


class BucketNotSet(RuntimeError):

    def __init__(self, msg):
        super(BucketNotSet, self).__init__(self, msg)
        self.message = msg


#TODO: refactor code that has BDNE error to NotFound -- all of it w/ticket
class BucketDoesNotExistError(RuntimeError):
    """Raised when a configured bucket does not exist"""

    def __init__(self, msg, bucket):
        super(BucketDoesNotExistError, self).__init__(msg)
        self.bucket = bucket
        self.message = msg


def _handle_error(alert_message, view_message=None, response_code=None):
    """
    Helper function to return generic error views.

    The alert message may contain sensitive information and should NEVER be used
    as part of the view message or view return value.

    TODO reuse this handler in the future after deprecating log_traceback
    :param alert_message:  the message intended to be sent to the alerting
        mechanism
    :param view_message:  the message that will be provided to the end user in
        the event of an error.  this should remain generic and NEVER provide
        insight to site info, participant info, or code info.  An appropriate
        default message is provided if no view_message parameter is specified.
        This can accept either a string or a dictionary.
    :param response_code: The numeric status code to return

    :return: A message string for the view in json format and an error code.
        Any error raised by the application will return a 500 code.
    """
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
    return f"{raised_error}: {message}"


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
