"""
Module for a logging handle to send messages to a curation slack channel.

Original Issue: DC-1159

This module sets up slack logging by ensuring that the SLACK_TOKEN and CHANNEL_NAME are established as environment variables
and also checking that the channel name is valid. The `is_channel_available` function is imported from the `slack_alerts` module.
This helper functions checks to make sure the both the token and channel names are valid. This was important to add since the
`initialize_slack_logging` is called in `validation.main.py` and if the channel names and token are not valid, this will cause
all unit tests that import `validation.main.py` to fail the CircleCI unit test check.
"""

# Python imports
import logging

# Project imports
from utils.slack_alerts import post_message, is_channel_available, SlackConfigurationError


class SlackLoggingHandler(logging.Handler):
    """
     Logging handler to send messages to a Slack Channel.
    """

    def __init__(self):
        super().__init__(level=logging.WARNING)

    def emit(self, record):

        try:
            # this is added for preventing the infinite loop from happening
            if not self._is_raised_from_itself(record):
                post_message(record.msg % record.args)
        except SlackConfigurationError:
            logging.exception(
                'Slack is not configured for posting messages, refer to playbook.'
            )
            raise

    def _is_raised_from_itself(self, record):
        return record.module in self.__module__


def initialize_slack_logging():
    """
    Setup Slack logging
    """
    if is_channel_available():
        # Configure root logger
        root_logger = logging.getLogger()
        # Configure slack logging handler
        log_handler = SlackLoggingHandler()
        log_handler.setLevel(logging.WARNING)
        # Add slack logging handler to root logger.
        root_logger.addHandler(log_handler)
