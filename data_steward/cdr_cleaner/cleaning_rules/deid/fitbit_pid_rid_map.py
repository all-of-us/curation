"""
DEID rule to change PIDs to RIDs for specific tables
"""
# Python Imports
import logging

# Project imports
from cdr_cleaner.cleaning_rules.deid.pid_rid_map import PIDtoRID
from common import FITBIT_TABLES

LOGGER = logging.getLogger(__name__)

ISSUE_NUMBERS = ['DC-1000']


class FitbitPIDtoRID(PIDtoRID):
    """
    Use RID instead of PID for specific tables
    """

    def __init__(self, project_id, dataset_id, sandbox_dataset_id,
                 mapping_dataset_id, mapping_table_id):
        """
        Initialize the class with proper info.

        Set the issue numbers, description and affected datasets.  As other
        tickets may affect this SQL, append them to the list of Jira Issues.
        DO NOT REMOVE ORIGINAL JIRA ISSUE NUMBERS!
        """
        super().__init__(project_id=project_id,
                         dataset_id=dataset_id,
                         sandbox_dataset_id=sandbox_dataset_id,
                         mapping_dataset_id=mapping_dataset_id,
                         mapping_table_id=mapping_table_id,
                         affected_tables=FITBIT_TABLES,
                         issue_numbers=ISSUE_NUMBERS)

    def get_query_specs(self):
        """
        Return a list of dictionary query specifications.

        :return:  A list of dictionaries.  Each dictionary contains a
            single query and a specification for how to execute that query.
            The specifications are optional but the query is required.
        """
        pass

    def get_sandbox_tablenames(self):
        return []

    def setup_rule(self, client):
        """
        Function to run any data upload options before executing a query.
        """
        pass

    def setup_validation(self, client):
        """
        Run required steps for validation setup
        """
        raise NotImplementedError("Please fix me.")

    def validate_rule(self, client):
        """
        Validates the cleaning rule which deletes or updates the data from the tables
        """
        pass


if __name__ == '__main__':
    import cdr_cleaner.args_parser as parser
    import cdr_cleaner.clean_cdr_engine as clean_engine

    mapping_dataset_arg = {
        parser.SHORT_ARGUMENT: '-m',
        parser.LONG_ARGUMENT: '--mapping_dataset_id',
        parser.ACTION: 'store',
        parser.DEST: 'mapping_dataset_id',
        parser.HELP: 'Identifies the dataset containing pid-rid map table',
        parser.REQUIRED: True
    }

    mapping_table_arg = {
        parser.SHORT_ARGUMENT: '-t',
        parser.LONG_ARGUMENT: '--mapping_table_id',
        parser.ACTION: 'store',
        parser.DEST: 'mapping_table_id',
        parser.HELP: 'Identifies the pid-rid map table, typically _deid_map',
        parser.REQUIRED: True
    }

    ARGS = parser.default_parse_args([mapping_dataset_arg, mapping_table_arg])

    if ARGS.list_queries:
        clean_engine.add_console_logging()
        query_list = clean_engine.get_query_list(
            ARGS.project_id,
            ARGS.dataset_id,
            ARGS.sandbox_dataset_id, [(PIDtoRID,)],
            mapping_dataset_id=ARGS.mapping_dataset_id,
            mapping_table_id=ARGS.mapping_table_id)
        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(ARGS.project_id,
                                   ARGS.dataset_id,
                                   ARGS.sandbox_dataset_id, [(PIDtoRID,)],
                                   mapping_dataset_id=ARGS.mapping_dataset_id,
                                   mapping_table_id=ARGS.mapping_table_id)
