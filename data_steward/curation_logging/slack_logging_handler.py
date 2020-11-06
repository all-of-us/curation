# Python imports
import os
import logging

# Project imports
from utils.slack_alerts import post_message, SlackConfigurationError, SLACK_CHANNEL, SLACK_TOKEN


class SlackLoggingHandler(logging.Handler):
    """
     Logging handler to send messages to a Slack Channel.
    """

    def __init__(self):
        super().__init__(level=logging.WARNING)

    def emit(self, record):

        try:
            post_message(record.msg % record.args)
        except SlackConfigurationError:
            logging.exception(
                'Slack is not configured for posting messages, refer to playbook.'
            )
            raise


def initialize_slack_logging():
    """
    Setup Slack logging
    """
    if SLACK_TOKEN in os.environ and SLACK_CHANNEL in os.environ:
        # Configure root logger
        root_logger = logging.getLogger()
        # Configure slack logging handler
        log_handler = SlackLoggingHandler()
        log_handler.setLevel(logging.WARNING)
        # Add slack logging handler to root logger.
        root_logger.addHandler(log_handler)
