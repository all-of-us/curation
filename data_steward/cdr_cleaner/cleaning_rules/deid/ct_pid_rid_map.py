"""
DEID rule to change PIDs to RIDs for Fitbit tables
"""
# Python Imports
import logging

# Project imports
from cdr_cleaner.cleaning_rules.deid.pid_rid_map import PIDtoRID
from common import JINJA_ENV
from resources import CDM_TABLES

LOGGER = logging.getLogger(__name__)

ISSUE_NUMBERS = ['DC-1336']

GET_PID_TABLES = JINJA_ENV.from_string("""
SELECT table_name
FROM `{{project_id}}.{{dataset_id}}.INFORMATION_SCHEMA.COLUMNS`
WHERE column_name = "person_id"
AND NOT STARTS_WITH(table_name, '_')
""")


class CtPIDtoRID(PIDtoRID):
    """
    Use RID instead of PID for Fitbit tables
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
                         affected_tables=CDM_TABLES,
                         issue_numbers=ISSUE_NUMBERS)

    @staticmethod
    def get_pid_tables(client):
        pid_tables_query = GET_PID_TABLES.render()
        query_job = client.query(pid_tables_query)
        result_df = query_job.result().to_dataframe()
        return result_df.get('table_name').to_list()

    def setup_rule(self, client):
        super().pid_tables = [
            f'{self.project_id}.{self.dataset_id}.{table_id}'
            for table_id in self.get_pid_tables(client)
        ]
        super(CtPIDtoRID, self).setup_rule(client)


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
            ARGS.sandbox_dataset_id, [(CtPIDtoRID,)],
            mapping_dataset_id=ARGS.mapping_dataset_id,
            mapping_table_id=ARGS.mapping_table_id)
        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(ARGS.project_id,
                                   ARGS.dataset_id,
                                   ARGS.sandbox_dataset_id, [(CtPIDtoRID,)],
                                   mapping_dataset_id=ARGS.mapping_dataset_id,
                                   mapping_table_id=ARGS.mapping_table_id)
