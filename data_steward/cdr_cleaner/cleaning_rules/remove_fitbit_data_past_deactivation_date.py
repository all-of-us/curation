"""
Ensures there is no fitbit data past the deactivation date for deactivated participants.

Original Issue: DC-1791

The intent is to sandbox and drop records dated after the date of deactivation for participants
who have deactivated from the Program.
"""

# Python imports
import logging

# Project imports
import common
import constants.cdr_cleaner.clean_cdr as cdr_consts
from constants import bq_utils as bq_consts
import utils.participant_summary_requests as psr
from constants.retraction.retract_deactivated_pids import DEACTIVATED_PARTICIPANTS
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule

LOGGER = logging.getLogger(__name__)

DEACTIVATED_PARTICIPANTS_COLUMNS = [
    'participantId', 'suspensionStatus', 'suspensionTime'
]

SANDBOX_QUERY = common.JINJA_ENV.from_string("""
CREATE OR REPLACE TABLE `{{project}}.{{sandbox_dataset}}.{{sandbox_table_name}}` AS (
SELECT t.*
FROM `{{project}}.{{dataset}}.{{table}}` t
JOIN `{{project}}.{{sandbox_dataset}}.{{deact_pids_table}}` d
USING (person_id)

{% if table in ['activity_summary', 'heart_rate_summary'] %}
WHERE date >= d.deactivated_date
{% else %}
WHERE datetime >= PARSE_DATETIME('%F', CAST(d.deactivated_date as STRING))
{% endif %}
)
""")

CLEAN_QUERY = common.JINJA_ENV.from_string("""
SELECT *
FROM `{{project}}.{{dataset}}.{{table}}`
EXCEPT DISTINCT
SELECT *
FROM `{{project}}.{{sandbox_dataset}}.{{sandbox_table_name}}`
""")


def get_deactivated_participants(api_project_id, project_id,
                                 sandbox_dataset_id):
    """
    Sandboxes and drops all EHR data found for deactivated participants after their deactivation date

    :param api_project_id: Project containing the RDR Participant Summary API
    :param project_id: Identifies the project containing the target dataset
    :param sandbox_dataset_id: Identifies the sandbox dataset to store records for dataset_id
    :returns queries: List of query dictionaries
    """
    # gets the deactivated participant table to ensure it's up-to-date
    df = psr.get_deactivated_participants(api_project_id,
                                          DEACTIVATED_PARTICIPANTS_COLUMNS)

    # To store dataframe in a BQ dataset table named _deactivated_participants
    destination_table = f'{sandbox_dataset_id}.{DEACTIVATED_PARTICIPANTS}'
    psr.store_participant_data(df, project_id, destination_table)


class RemoveFitbitDataPastDeactivationDate(BaseCleaningRule):
    """
    Ensures there is no data past the deactivation date for deactivated participants.
    """

    def __init__(self, project_id, dataset_id, sandbox_dataset_id,
                 api_project_id):
        """
        Initialize the class with proper information.

        Set the issue numbers, description and affected datasets. As other tickets may affect this SQL,
        append them to the list of Jira Issues.
        DO NOT REMOVE ORIGINAL JIRA ISSUE NUMBERS!
        """
        desc = (
            'Sandbox and drop records dated after the date of deactivation for participants'
            'who have deactivated from the Program.')
        super().__init__(issue_numbers=['DC-1791'],
                         description=desc,
                         affected_datasets=[cdr_consts.FITBIT],
                         project_id=project_id,
                         dataset_id=dataset_id,
                         sandbox_dataset_id=sandbox_dataset_id,
                         affected_tables=common.FITBIT_TABLES)
        self.api_project_id = api_project_id

    def get_query_specs(self):
        """
        This function generates a list of query dicts for ensuring the dates and datetimes are consistent

        :return: a list of query dicts for ensuring the dates and datetimes are consistent
        """

        get_deactivated_participants(self.api_project_id, self.project_id,
                                     self.sandbox_dataset_id)
        sandbox_queries = []
        clean_queries = []
        counter = 0
        for table in self.affected_tables:
            sandbox_queries.append({
                cdr_consts.QUERY:
                    SANDBOX_QUERY.render(
                        project=self.project_id,
                        dataset=self.dataset_id,
                        sandbox_dataset=self.sandbox_dataset_id,
                        table=table,
                        sandbox_table_name=self.get_sandbox_tablenames()
                        [counter],
                        deact_pids_table=DEACTIVATED_PARTICIPANTS)
            })
            clean_queries.append({
                cdr_consts.QUERY:
                    CLEAN_QUERY.render(
                        project=self.project_id,
                        dataset=self.dataset_id,
                        sandbox_dataset=self.sandbox_dataset_id,
                        table=table,
                        sandbox_table_name=self.get_sandbox_tablenames()
                        [counter]),
                cdr_consts.DESTINATION_TABLE:
                    table,
                cdr_consts.DESTINATION_DATASET:
                    self.dataset_id,
                cdr_consts.DISPOSITION:
                    bq_consts.WRITE_TRUNCATE
            })
            counter += 1

        return sandbox_queries + clean_queries

    def setup_rule(self, client):
        """
        Function to run any data upload options before executing a query.
        """
        pass

    def get_sandbox_tablenames(self):
        """
        Returns an empty list because this rule does not use sandbox tables.
        """
        sandbox_tables = [
            f'{self._issue_numbers[0].lower()}_{table}'
            for table in self.affected_tables
        ]
        return sandbox_tables

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
            ARGS.sandbox_dataset_id, [(RemoveFitbitDataPastDeactivationDate,)],
            api_project_id=ARGS.api_project_id)
        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(ARGS.project_id,
                                   ARGS.dataset_id,
                                   ARGS.sandbox_dataset_id,
                                   [(RemoveFitbitDataPastDeactivationDate,)],
                                   api_project_id=ARGS.api_project_id)
