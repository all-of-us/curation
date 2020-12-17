"""
Ensures there is no EHR data past the deactivation date for deactivated participants.

Original Issue: DC-686

The intent is to sandbox and drop records dated after the date of deactivation for participants
who have deactivated from the Program.
"""

# Python imports
import logging

# Third party imports
import google.cloud.bigquery as gbq

# Project imports
from utils import bq, pipeline_logging
import utils.participant_summary_requests as psr
import retraction.retract_deactivated_pids as rdp
import retraction.retract_utils as ru
import sandbox as sb

LOGGER = logging.getLogger(__name__)

DEACTIVATED_PARTICIPANTS_COLUMNS = [
    'participantId', 'suspensionStatus', 'suspensionTime'
]


def remove_ehr_data_queries(project_id, dataset_id, sandbox_dataset_id,
                            fq_deact_table):
    """
    Creates sandboxes and drops all EHR data found for deactivated participants after
    their deactivation date

    :param project_id: BQ name of the project
    :param dataset_id: Identifies the dataset to retract deactivated participants from
    :param sandbox_dataset_id: Identifies the sandbox dataset to store records for dataset_id
    :param fq_deact_table: fully qualified deactivated participants PIDs table in 'project.dataset.table' format
    :returns queries: List of query dictionaries
    """
    # gets the deactivated participant dataset to ensure it's up-to-date
    pids_project_id, pids_dataset_id, table_name = fq_deact_table.split('.')
    df = psr.get_deactivated_participants(pids_project_id, pids_dataset_id,
                                          table_name,
                                          DEACTIVATED_PARTICIPANTS_COLUMNS)
    # To store dataframe in a BQ dataset table
    destination_table = pids_dataset_id + '.' + table_name
    psr.store_participant_data(df, project_id, destination_table)

    deact_table_ref = gbq.TableReference.from_string(f"{fq_deact_table}")
    LOGGER.info(f"Retracting deactivated participants from '{dataset_id}'")
    LOGGER.info(
        f"Using sandbox dataset '{sandbox_dataset_id}' for '{dataset_id}'")
    # creates sandbox and truncate queries to run for deactivated participant data drops
    queries = rdp.generate_queries(client, project_id, dataset_id,
                                   sandbox_dataset_id, deact_table_ref)
    return queries


if __name__ == '__main__':
    pipeline_logging.configure(level=logging.DEBUG, add_console_handler=True)

    parser = rdp.get_parser()
    args = parser.parse_args()
    client = bq.get_client(args.project_id)

    dataset_ids = ru.get_datasets_list(args.project_id, args.dataset_ids)
    # dataset_ids should contain only one dataset (unioned_ehr)
    dataset_id = dataset_ids[0]
    LOGGER.info(
        f"Datasets to retract deactivated participants from: {dataset_id}")
    sandbox_dataset_id = sb.check_and_create_sandbox_dataset(
        args.project_id, dataset_id)
    LOGGER.info(f"Using sandbox dataset: {sandbox_dataset_id}")

    deactivation_queries = remove_ehr_data_queries(args.project_id, dataset_id,
                                                   sandbox_dataset_id,
                                                   args.fq_deact_table)

    job_ids = []
    for query in deactivation_queries:
        job_id = rdp.query_runner(client, query)
        job_ids.append(job_id)
    LOGGER.info(
        f"Retraction of deactivated participants from {dataset_id} complete")
