"""
DEID rule to change PIDs to RIDs for Fitbit tables
Maps the PIDs used in fitbit to the RIDs
"""
# Python Imports
import logging

# Project imports
from cdr_cleaner.cleaning_rules.deid.pid_rid_map import PIDtoRID
from cdr_cleaner.cleaning_rules.deid.fitbit_device_id import DeidFitbitDeviceId
from common import FITBIT_TABLES

LOGGER = logging.getLogger(__name__)

ISSUE_NUMBERS = ['DC-1000', 'DC-2136']


class FitbitPIDtoRID(PIDtoRID):
    """
    Use RID instead of PID for Fitbit tables
    """

    def __init__(self,
                 project_id,
                 dataset_id,
                 sandbox_dataset_id,
                 mapping_dataset_id,
                 mapping_table_id,
                 depends_on=[DeidFitbitDeviceId],
                 table_namer=None):
        """
        Initialize the class with proper info.

        Set the issue numbers, description and affected datasets. As other
        tickets may affect this SQL, append them to the list of Jira Issues.
        DO NOT REMOVE ORIGINAL JIRA ISSUE NUMBERS!
        """
        super().__init__(project_id=project_id,
                         dataset_id=dataset_id,
                         sandbox_dataset_id=sandbox_dataset_id,
                         mapping_dataset_id=mapping_dataset_id,
                         mapping_table_id=mapping_table_id,
                         affected_tables=FITBIT_TABLES,
                         issue_numbers=ISSUE_NUMBERS,
                         table_namer=table_namer)


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
            ARGS.sandbox_dataset_id, [(FitbitPIDtoRID,)],
            mapping_dataset_id=ARGS.mapping_dataset_id,
            mapping_table_id=ARGS.mapping_table_id)
        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(ARGS.project_id,
                                   ARGS.dataset_id,
                                   ARGS.sandbox_dataset_id, [(FitbitPIDtoRID,)],
                                   mapping_dataset_id=ARGS.mapping_dataset_id,
                                   mapping_table_id=ARGS.mapping_table_id)
