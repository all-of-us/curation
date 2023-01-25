"""
Checks id_violations_in_lower_envs and generate error messages if any potential
privacy violation is found or if the monitoring process is not working.
Generated messages are used for GCP monitoring and alerting, so the engineers in
Curation can get notified when some action is needed.
"""
from argparse import ArgumentParser
import logging
import os

from common import JINJA_ENV
from gcloud.bq import BigQueryClient
from utils import pipeline_logging

LOGGER = logging.getLogger(__name__)

PROD_PID_DETECTION = """
SELECT DISTINCT * 
FROM `{{project}}.{{admin_dataset}}.id_violations_in_lower_envs`
WHERE monitor_date = CURRENT_DATE()
"""

# These three messages are used for GCP monitoring and alerting.
# If you update the messages here, you also need to update the corresponding
# metrics in logging from GCP console.
HEADER_SCHEDULED_QUERY_FAILED = 'Daily ID violation check failed to run today.'
HEADER_ID_VIOLATION_FOUND = 'ID violation is found.'
HEADER_CHECK_COMPLETED = 'Daily ID violation check completed.'

# These messages are not used for GCP monitoring or alerting.
HEADER_NO_ID_VIOLATION_FOUND = 'No ID violation is found today.'
BODY_SCHEDULED_QUERY_FAILED = (
    'id_violations_in_lower_envs does not have a record of the check result for '
    'today. This can be because the scheduled SQL could not start or got some '
    'errors during execution. Investigate the cause of the error.')
BODY_ID_VIOLATION_FOUND = (
    'Look at id_violations_in_lower_envs and find which tables have the person_ids '
    'or research_ids that also exist in PROD environment. Check if this needs '
    'to be reported as a privacy incident.')


def check_violation(project_id, admin_dataset_id=None):
    """
    Checks if any PID/RID violation is found or not, and if the scheduled query
    is running as designed or not. Generates log messages accordingly. The logs
    are used for GCP monitoring and alerting.
    :param project_id: Project ID.
    :param admin_dataset_id: Dataset ID that has id_violations_in_lower_envs table.
    :return:
    """
    client = BigQueryClient(project_id)

    if not admin_dataset_id:
        admin_dataset_id = os.environ.get('ADMIN_DATASET_ID')

    query = JINJA_ENV.from_string(PROD_PID_DETECTION).render(
        project=project_id, admin_dataset=admin_dataset_id)

    LOGGER.info(f'Running the query:\n{query}')
    response_list = list(client.query(query).result())

    if len(response_list) == 0:
        LOGGER.error(HEADER_SCHEDULED_QUERY_FAILED)
        LOGGER.error(BODY_SCHEDULED_QUERY_FAILED)
    elif response_list[0][0] != 'No violation found':
        LOGGER.error(HEADER_ID_VIOLATION_FOUND)
        LOGGER.error(BODY_ID_VIOLATION_FOUND)
    else:
        LOGGER.info(HEADER_NO_ID_VIOLATION_FOUND)

    LOGGER.info(HEADER_CHECK_COMPLETED)


def parse_args():
    parser = ArgumentParser(description='TODO add description here')
    parser.add_argument('-p',
                        '--project_id',
                        help='Prod BigQuery project ID',
                        dest='project_id',
                        required=True)
    parser.add_argument(
        '-d',
        '--admin_dataset_id',
        help='Admin dataset that has id_violations_in_lower_envs table',
        dest='admin_dataset_id',
        required=False)
    parser.add_argument('-l',
                        '--console_log',
                        dest='console_log',
                        action='store_true',
                        required=False,
                        help='Log to the console as well as to a file.')

    return parser.parse_args()


def main():
    args = parse_args()

    pipeline_logging.configure(level=logging.INFO,
                               add_console_handler=args.console_log)

    project_id = args.project_id
    admin_dataset_id = args.admin_dataset_id

    check_violation(project_id, admin_dataset_id)


if __name__ == '__main__':
    main()
