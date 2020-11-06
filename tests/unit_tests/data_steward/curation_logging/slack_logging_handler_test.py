"""
Notes: -- if dev is using macOS and gets error:
            [SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed: unable to get local issuer certificate (_ssl.c:1108)
          will need to go to Macintosh HD > Applications > Python3.6 folder (or whatever version of python you're using) >
          double click on "Install Certificates.command" file.
          found: (https://stackoverflow.com/questions/50236117/scraping-ssl-certificate-verify-failed-error-for-http-en-wikipedia-org)
      -- dev will also need to add SLACK_TOKEN and SLACK_CHANNEL as environment variables
"""

# Python imports
import logging

# Project imports
from curation_logging.slack_logging_handler import initialize_slack_logging

if __name__ == "__main__":
    initialize_slack_logging()
    logging.info('Do not send this')
    logging.debug('Do not send this')
    logging.warning('logging.warning slack message sent by logging handler!')
    logging.critical('logging.critical slack message sent by logging handler!')
    logging.error('logging.error slack message sent by logging handler!')
