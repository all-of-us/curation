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
from constants.retraction.retract_deactivated_pids import DEACTIVATED_PARTICIPANTS

LOGGER = logging.getLogger(__name__)

DEACTIVATED_PARTICIPANTS_COLUMNS = [
    'participantId', 'suspensionStatus', 'suspensionTime'
]


def remove_ehr_data_queries(client, api_project_id, project_id, dataset_id,
                            sandbox_dataset_id):
    """
    Sandboxes and drops all EHR data found for deactivated participants after their deactivation date

    :param client: BQ client
    :param api_project_id: Project containing the RDR Participant Summary API
    :param project_id: Identifies the project containing the target dataset
    :param dataset_id: Identifies the dataset to retract deactivated participants from
    :param sandbox_dataset_id: Identifies the sandbox dataset to store records for dataset_id
    :returns queries: List of query dictionaries
    """
    # gets the deactivated participant dataset to ensure it's up-to-date
    df = psr.get_deactivated_participants(api_project_id,
                                          DEACTIVATED_PARTICIPANTS_COLUMNS)

    # To store dataframe in a BQ dataset table named _deactivated_participants
    destination_table = f'{sandbox_dataset_id}.{DEACTIVATED_PARTICIPANTS}'
    psr.store_participant_data(df, project_id, destination_table)

    fq_deact_table = f'{project_id}.{destination_table}'
    deact_table_ref = gbq.TableReference.from_string(f"{fq_deact_table}")
    LOGGER.info(f"Retracting deactivated participants from '{dataset_id}'")
    LOGGER.info(
        f"Using sandbox dataset '{sandbox_dataset_id}' for '{dataset_id}'")
    # creates sandbox and truncate queries to run for deactivated participant data drops
    queries = rdp.generate_queries(client, project_id, dataset_id,
                                   sandbox_dataset_id, deact_table_ref)
    return queries


if __name__ == '__main__':
    parser = rdp.get_base_parser()
    parser.add_argument(
        '-d',
        '--dataset_id',
        action='store',
        dest='dataset_id',
        help=
        'Identifies the target dataset to retract deactivated participant data',
        required=True)
    parser.add_argument(
        '-q',
        '--api_project_id',
        action='store',
        dest='api_project_id',
        help='Identifies the RDR project for participant summary API',
        required=True)
    parser.add_argument('-b',
                        '--sandbox_dataset_id',
                        action='store',
                        dest='sandbox_dataset_id',
                        help='Identifies sandbox dataset to store records',
                        required=True)
    args = parser.parse_args()

    pipeline_logging.configure(level=logging.DEBUG,
                               add_console_handler=args.console_log)

    client = bq.get_client(args.project_id)

    # keep only datasets existing in project
    dataset_ids = ru.get_datasets_list(args.project_id, [args.dataset_id])

    # dataset_ids should contain only one dataset (unioned_ehr)
    if len(dataset_ids) == 1:
        dataset_id = dataset_ids[0]
    else:
        raise RuntimeError(f'More than one dataset specified: {dataset_ids}')

    LOGGER.info(
        f"Dataset to retract deactivated participants from: {dataset_id}. "
        f"Using sandbox dataset: {args.sandbox_dataset_id}")

    deactivation_queries = remove_ehr_data_queries(client, args.api_project_id,
                                                   args.project_id, dataset_id,
                                                   args.sandbox_dataset_id)

    job_ids = []
    for query in deactivation_queries:
        job_id = rdp.query_runner(client, query)
        job_ids.append(job_id)

    LOGGER.info(
        f"Retraction of deactivated participants from {dataset_id} complete")
