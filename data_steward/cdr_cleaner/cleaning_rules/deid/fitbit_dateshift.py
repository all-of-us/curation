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
from constants.cdr_cleaner import clean_cdr as cdr_consts
from resources import fields_for

LOGGER = logging.getLogger(__name__)

ISSUE_NUMBERS = ['DC-1005']


class FitbitDateShiftRule(DateShiftRule):
    """
    Date shift any date, datetime, or timestamp fields in the fitbit tables.
    """

    def __init__(self, project_id, dataset_id, sandbox_dataset_id,
                 map_dataset_id, map_tablename):
        """
        Initialize the class.

        Set the issue numbers, description and affected datasets.  As other
        tickets may affect this SQL, append them to the list of Jira Issues.
        DO NOT REMOVE ORIGINAL JIRA ISSUE NUMBERS!
        """
        desc = (f'Date shift date and timestamp fields by the date shift '
                f'calculated in the static mapping table.  This specifically '
                f'applies to fitbit tables.')

        self.tables = [
            'activity_summary', 'heart_rate_minute_level', 'heart_rate_summary',
            'steps_intraday'
        ]
        super().__init__(issue_numbers=ISSUE_NUMBERS,
                         description=desc,
                         affected_datasets=[cdr_consts.FITBIT],
                         project_id=project_id,
                         dataset_id=dataset_id,
                         sandbox_dataset_id=sandbox_dataset_id,
                         affected_tables=self.tables,
                         map_dataset=map_dataset_id,
                         map_table=map_tablename,
                         depends_on=[PIDtoRID])

    def get_tables_and_schemas(self):
        for table in self.tables:
            try:
                schema = fields_for(table, 'wearables/fitbit')
                # update schema defintion if it's available
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

    parser = parser.get_argument_parser()
    parser.add_argument('--mapping-dataset',
                        required=True,
                        dest='map_dataset',
                        help=('Location of the mapping dataset to use'))
    parser.add_argument('--mapping-table',
                        required=True,
                        dest='map_table',
                        help=('Name of the mapping table to use'))
    ARGS = parser.parse_args()
    clean_engine.add_console_logging(ARGS.console_log)
    date_shifter = FitbitDateShiftRule(ARGS.project_id, ARGS.dataset_id,
                                       ARGS.sandbox_dataset_id,
                                       ARGS.map_dataset, ARGS.map_table)
    query_list = date_shifter.get_query_specs()

    if ARGS.list_queries:
        date_shifter.log_queries()
    else:
        clean_engine.clean_dataset(ARGS.project_id, query_list)
