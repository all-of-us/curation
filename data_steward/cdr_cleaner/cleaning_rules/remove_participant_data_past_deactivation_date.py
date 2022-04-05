"""
Ensures there is no data past the deactivation date for deactivated participants.

Original Issue: DC-686

The intent is to sandbox and drop records dated after the date of deactivation for participants
who have deactivated from the Program.
"""

# Python imports
import logging

# Project imports
import common
from utils import bq
import constants.cdr_cleaner.clean_cdr as cdr_consts
import utils.participant_summary_requests as psr
import retraction.retract_deactivated_pids as rdp
from constants.retraction.retract_deactivated_pids import DEACTIVATED_PARTICIPANTS
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule

# Third-Party imports
import google.cloud.bigquery as gbq

LOGGER = logging.getLogger(__name__)

DEACTIVATED_PARTICIPANTS_COLUMNS = [
    'participantId', 'suspensionStatus', 'suspensionTime'
]


class RemoveParticipantDataPastDeactivationDate(BaseCleaningRule):
    """
    Ensures there is no data past the deactivation date for deactivated participants.
    """

    def __init__(self,
                 project_id,
                 dataset_id,
                 sandbox_dataset_id,
                 table_namer=None,
                 api_project_id=None):
        """
        Initialize the class with proper information.

        Set the issue numbers, description and affected datasets. As other tickets
        may affect this SQL, append them to the list of Jira Issues.

        DO NOT REMOVE ORIGINAL JIRA ISSUE NUMBERS!
        """
        desc = (
            'Sandbox and drop records dated after the date of deactivation for participants'
            'who have deactivated from the Program.')

        if not api_project_id:
            raise TypeError("`api_project_id` cannot be empty")

        super().__init__(issue_numbers=['DC-1791', 'DC-1896'],
                         description=desc,
                         affected_datasets=[cdr_consts.COMBINED],
                         project_id=project_id,
                         dataset_id=dataset_id,
                         sandbox_dataset_id=sandbox_dataset_id,
                         affected_tables=common.CDM_TABLES +
                         common.FITBIT_TABLES,
                         table_namer=table_namer)
        self.api_project_id = api_project_id
        self.destination_table = (f'{self.project_id}.{self.sandbox_dataset_id}'
                                  f'.{DEACTIVATED_PARTICIPANTS}')

        # initialized to None so that if setup_rule is skipped, it will not
        # query live datasets for table information
        self.client = None

    def get_query_specs(self):
        """
        This function generates a list of query dicts.

        These queries should sandbox and remove all data past the
        participant's deactivation date.

        :return: a list of query dicts
        """
        deact_table_ref = gbq.TableReference.from_string(self.destination_table)
        # creates sandbox and truncate queries to run for deactivated participant data drops
        # setup_rule must be run before this to ensure the client is properly
        # configured.
        queries = rdp.generate_queries(self.client,
                                       self.project_id,
                                       self.dataset_id,
                                       self.sandbox_dataset_id,
                                       deact_table_ref,
                                       data_stage_id=self.table_namer)
        return queries

    def setup_rule(self, client):
        """
        Responsible for grabbing and storing deactivated participant data.

        :param client: client object passed to store the data
        """
        LOGGER.info("Querying RDR API for deactivated participant data")
        # gets the deactivated participant dataset to ensure it's up-to-date
        df = psr.get_deactivated_participants(self.api_project_id,
                                              DEACTIVATED_PARTICIPANTS_COLUMNS)

        LOGGER.info(f"Found '{len(df)}' deactivated participants via RDR API")

        # To store dataframe in a BQ dataset table named _deactivated_participants
        psr.store_participant_data(df, client, self.destination_table)

        LOGGER.info(f"Finished storing participant records in: "
                    f"`{self.destination_table}`")

        LOGGER.debug("instantiating class client object")
        self.client = client

        # reinitializing self.affected_tables
        LOGGER.debug(
            "reinitializing self.affected_tables to actual tables available")
        tables_list = self.client.list_tables(self.dataset_id)
        self.affected_tables = [
            table_item.table_id for table_item in tables_list
        ]

    def get_sandbox_tablenames(self):
        """
        Return a list table names created to backup deleted data.
        """
        LOGGER.info("Generating list of possible sandbox table names "
                    "from self.affected_tables")

        return [
            rdp.get_deactivated_sandbox_table_name(table, self.table_namer)
            for table in self.affected_tables
        ]

    def setup_validation(self, client):
        """
        Run required steps for validation setup

        This abstract method was added to the base class after this rule was authored.
        This rule needs to implement logic to setup validation on cleaning rules that
        will be updating or deleting the values.
        Until done no issue exists for this yet.
        """
        raise NotImplementedError("Please fix me.")

    def validate_rule(self, client):
        """
        Validates the cleaning rule which deletes or updates the data from the tables

        This abstract method was added to the base class after this rule was authored.
        This rule needs to implement logic to run validation on cleaning rules that will
        be updating or deleting the values.
        Until done no issue exists for this yet.
        """
        raise NotImplementedError("Please fix me.")


if __name__ == '__main__':
    import cdr_cleaner.clean_cdr_engine as clean_engine
    import cdr_cleaner.args_parser as parser

    ext_parser = parser.get_argument_parser()
    ext_parser.add_argument(
        '-q',
        '--api_project_id',
        action='store',
        dest='api_project_id',
        help='Identifies the RDR project for participant summary API',
        required=True)
    ARGS = ext_parser.parse_args()

    if ARGS.list_queries:
        clean_engine.add_console_logging()
        query_list = clean_engine.get_query_list(
            ARGS.project_id,
            ARGS.dataset_id,
            ARGS.sandbox_dataset_id,
            [(RemoveParticipantDataPastDeactivationDate,)],
            api_project_id=ARGS.api_project_id,
            table_namer='manual')
        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(
            ARGS.project_id,
            ARGS.dataset_id,
            ARGS.sandbox_dataset_id,
            [(RemoveParticipantDataPastDeactivationDate,)],
            api_project_id=ARGS.api_project_id,
            table_namer='manual')
