"""
Date shifting fitbit tables.

Extends the basic date shifting rule by providing table names
and schemas.

Original Issue:  DC-1005
"""
# Python Imports
import logging

# Project imports
from cdr_cleaner.cleaning_rules.deid.dateshift import DateShiftRule
from cdr_cleaner.cleaning_rules.deid.pid_rid_map import PIDtoRID
from common import FITBIT_TABLES
from constants.cdr_cleaner import clean_cdr as cdr_consts
from resources import fields_for

LOGGER = logging.getLogger(__name__)

ISSUE_NUMBERS = ['DC-1005']


class FitbitDateShiftRule(DateShiftRule):
    """
    Date shift any date, datetime, or timestamp fields in the fitbit tables.
    """

    def __init__(self, project_id, dataset_id, sandbox_dataset_id,
                 combined_dataset_id):
        """
        Initialize the class.

        Set the issue numbers, description and affected datasets.  As other
        tickets may affect this SQL, append them to the list of Jira Issues.
        DO NOT REMOVE ORIGINAL JIRA ISSUE NUMBERS!
        """
        desc = (f'Date shift date and timestamp fields by the date shift '
                f'calculated in the static mapping table.  This specifically '
                f'applies to fitbit tables.')

        self.tables = FITBIT_TABLES

        super().__init__(issue_numbers=ISSUE_NUMBERS,
                         description=desc,
                         affected_datasets=[cdr_consts.FITBIT],
                         project_id=project_id,
                         dataset_id=dataset_id,
                         sandbox_dataset_id=sandbox_dataset_id,
                         affected_tables=self.tables,
                         combined_dataset_id=combined_dataset_id,
                         depends_on=[PIDtoRID])

    def get_tables_and_schemas(self):
        tables_and_schemas = dict()
        for table in self.tables:
            try:
                schema = fields_for(table, 'wearables/fitbit')
                # update schema definition if it's available
                tables_and_schemas[table] = schema
            except RuntimeError:
                LOGGER.exception(f"Can't find schema file for {table}.  "
                                 f"Using default schema definition.")

        return tables_and_schemas

    def setup_rule(self, client):
        """
        Function to run any data upload options before executing a query.
        """
        pass

    def setup_validation(self, client):
        """
        Run required steps for validation setup

        This abstract method was added to the base class after this rule was authored.
        This rule needs to implement logic to setup validation on cleaning rules that
        will be updating or deleting the values.
        Until done no issue exists for this yet.
        """
        raise NotImplementedError("Please fix me.")

    def validate_rule(self):
        """
        Validates the cleaning rule which deletes or updates the data from the tables

        This abstract method was added to the base class after this rule was authored.
        This rule needs to implement logic to run validation on cleaning rules that will
        be updating or deleting the values.
        Until done no issue exists for this yet.
        """
        raise NotImplementedError("Please fix me.")

    def get_sandbox_tablenames(self):
        return []


if __name__ == '__main__':
    import cdr_cleaner.args_parser as parser
    import cdr_cleaner.clean_cdr_engine as clean_engine

    combined_dataset_arg = {
        parser.SHORT_ARGUMENT: '-c',
        parser.LONG_ARGUMENT: '--combined_dataset_id',
        parser.ACTION: 'store',
        parser.DEST: 'combined_dataset_id',
        parser.HELP: 'Identifies the combined dataset',
        parser.REQUIRED: True
    }

    ARGS = parser.default_parse_args([combined_dataset_arg])

    if ARGS.list_queries:
        clean_engine.add_console_logging()
        query_list = clean_engine.get_query_list(ARGS.project_id,
                                                 ARGS.dataset_id,
                                                 [(FitbitDateShiftRule,)],
                                                 ARGS.combined_dataset_id)
        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(ARGS.project_id, ARGS.dataset_id,
                                   [(FitbitDateShiftRule,)],
                                   ARGS.combined_dataset_id)
