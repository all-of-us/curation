"""
This cleaning rule uses a list of participants to remove all participant data from combined. 
"""

# Python imports
import logging

# Project imports
from cdr_cleaner.cleaning_rules.sandbox_and_remove_pids import SandboxAndRemovePids, JINJA_ENV, PERSON_TABLE_QUERY, AOU_DEATH, CDM_TABLES
from constants.cdr_cleaner import clean_cdr as cdr_consts
from gcloud.bq import BigQueryClient

LOGGER = logging.getLogger(__name__)

ISSUE_NUMBERS = ['DC3442']

# Query template to copy withdrawn_dups_table from rdr dataset to combined sandbox dataset
COPY_WITHDRAWN_DUPS_TABLE_TEMPLATE = JINJA_ENV.from_string("""
CREATE OR REPLACE TABLE 
    `{{project_id}}.{{sandbox_dataset_id}}.{{withdrawn_dups_table}}` AS 
    (
        SELECT
            person_id,
            hpo_id
            src_id,
            consent_for_study_enrollment_authored,
            withdrawal_status
        FROM
            `{{project_id}}.{{dataset_id}}.{{withdrawn_dups_table}}`                                       
    )
""")


class SandboxAndRemoveWithdrawnPids(SandboxAndRemovePids):
    """
    Removes all participant data using a list of participants.
    """

    def __init__(self, project_id, dataset_id, sandbox_dataset_id,
                 withdrawn_dups_table):
        """
        Initialize the class with proper information.

        Set the issue numbers, description and affected datasets. As other tickets may affect
        this SQL, append them to the list of Jira Issues.
        DO NOT REMOVE ORIGINAL JIRA ISSUE NUMBERS!
        """

        self.withdrawn_dups_table = withdrawn_dups_table

        desc = 'Sandbox and remove participant data from a list of participants.'

        super().__init__(issue_numbers=ISSUE_NUMBERS,
                         description=desc,
                         affected_datasets=[cdr_consts.RDR],
                         project_id=project_id,
                         dataset_id=dataset_id,
                         sandbox_dataset_id=sandbox_dataset_id,
                         affected_tables=[])

    def setup_rule(self, client: BigQueryClient, ehr_only: bool = False):
        """
        Get list of tables that have a person_id column, excluding mapping tables
        :param ehr_only: For Combined dataset, True if removing only EHR records. False if removing both RDR and EHR records.
        """

        person_table_query = PERSON_TABLE_QUERY.render(project=self.project_id,
                                                       dataset=self.dataset_id,
                                                       ehr_only=ehr_only)
        person_tables = client.query(person_table_query).result()

        self.affected_tables = [
            table.get('table_name')
            for table in person_tables
            if table.get('table_name') in CDM_TABLES + [AOU_DEATH]
        ]

        # Copy withdrawn_dups_table from rdr dataset to combined sandbox dataset
        copy_withdrawn_dups_table_query = COPY_WITHDRAWN_DUPS_TABLE_TEMPLATE.render(
            project_id=self.project_id,
            sandbox_dataset_id=self.sandbox_dataset_id,
            dataset_id=self.dataset_id,
            withdrawn_dups_table=self.withdrawn_dups_table)

        client.query(copy_withdrawn_dups_table_query).result()

    def get_query_specs(self) -> list:
        sandbox_records_queries = self.get_sandbox_queries(
            lookup_table=self.withdrawn_dups_table)
        remove_pids_queries = self.get_remove_pids_queries(
            lookup_table=self.withdrawn_dups_table)

        return sandbox_records_queries + remove_pids_queries

    def get_sandbox_tablenames(self):
        """
        generates sandbox table names
        """
        return [self.sandbox_table_for(table) for table in self.affected_tables]

    def setup_validation(self, client):
        """
        Run required steps for validation setup.
        """
        raise NotImplementedError("Please fix me.")

    def validate_rule(self, client):
        """
        Validates the cleaning rule which deletes or updates the data from the tables.
        """
        raise NotImplementedError("Please fix me.")


def parse_args():
    """
    This function expands the default argument list defined in cdr_cleaner.args_parser
    :return: an expanded argument list object
    """

    import cdr_cleaner.args_parser as parser

    additional_arguments = [{
        parser.SHORT_ARGUMENT: '-l',
        parser.LONG_ARGUMENT: '--withdrawn_dups_table',
        parser.ACTION: 'store',
        parser.DEST: 'withdrawn_dups_table',
        parser.HELP: 'withdrawn_dups_table',
        parser.REQUIRED: True
    }]
    args = parser.default_parse_args(additional_arguments)
    return args


if __name__ == '__main__':
    import cdr_cleaner.args_parser as parser
    import cdr_cleaner.clean_cdr_engine as clean_engine

    ARGS = parser.parse_args()

    if ARGS.list_queries:
        clean_engine.add_console_logging()
        query_list = clean_engine.get_query_list(
            ARGS.project_id,
            ARGS.dataset_id,
            ARGS.sandbox_dataset_id, [(SandboxAndRemoveWithdrawnPids,)],
            withdrawn_dups_table=ARGS.withdrawn_dups_table)
        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(
            ARGS.project_id,
            ARGS.dataset_id,
            ARGS.sandbox_dataset_id, [(SandboxAndRemoveWithdrawnPids,)],
            withdrawn_dups_table=ARGS.withdrawn_dups_table)
