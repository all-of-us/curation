"""
Ensures there is no EHR data past the deactivation date for deactivated participants.

Original Issue: DC-686

The intent is to sandbox and drop records dated after the date of deactivation for participants
who have deactivated from the Program.
"""

# Python imports
import logging

# Third party imports
import bq_utils
# Project imports
import retraction.retract_deactivated_pids as rdp
import utils.participant_summary_requests as psr

LOGGER = logging.getLogger(__name__)

TICKET_NUMBER = 'dc686'

DEACTIVATED_PARTICIPANTS_COLUMNS = [
    'participantId', 'suspensionStatus', 'suspensionTime'
]


def remove_ehr_data_queries(project_id, ticket_number, pids_project_id,
                            pids_dataset_id, tablename):
    """
    Creates sandboxes and drops all EHR data found for deactivated participants after
    their deactivation date

    :param project_id: BQ name of the project
    :param ticket_number: Jira ticket number to identify and title sandbox tables
    :param pids_project_id: deactivated participants PIDs table in BQ's project_id
    :param pids_dataset_id: deactivated participants PIDs table in BQ's dataset_id
    :param tablename: The name of the table to house the deactivated participant data
    """

    ehr_union_dataset = bq_utils.get_unioned_dataset_id()

    # gets the deactivated participant dataset to ensure it's up-to-date
    psr.get_deactivated_participants(pids_project_id, pids_dataset_id,
                                     tablename,
                                     DEACTIVATED_PARTICIPANTS_COLUMNS)

    # creates sandbox and truncate queries to run for deactivated participant data drops
    queries = rdp.create_queries(project_id, ticket_number, pids_project_id,
                                 pids_dataset_id, tablename, ehr_union_dataset)

    return queries


if __name__ == '__main__':
    ARGS = rdp.parse_args()
    rdp.add_console_logging(ARGS.console_log)

    remove_ehr_data_queries = remove_ehr_data_queries(ARGS.project_id,
                                                      ARGS.ticket_number,
                                                      ARGS.pids_project_id,
                                                      ARGS.pids_dataset_id,
                                                      ARGS.pids_table)

    client = rdp.get_client(ARGS.project_id)
    rdp.run_queries(remove_ehr_data_queries, client)
    LOGGER.info("Removal of ehr data from deactivated participants complete")
