"""
DEID rule to change PIDs to RIDs for OMOP tables and aou_death
"""
# Python Imports
import logging

# Third party imports
import google.cloud.bigquery as gbq

# Project imports
from cdr_cleaner.cleaning_rules.deid.pid_rid_map import PIDtoRID
from common import AOU_DEATH, JINJA_ENV, DEID_MAP
from resources import CDM_TABLES

LOGGER = logging.getLogger(__name__)

ISSUE_NUMBERS = ['DC-1336', 'DC-2639']

GET_PID_TABLES = JINJA_ENV.from_string("""
SELECT table_name
FROM `{{project_id}}.{{dataset_id}}.INFORMATION_SCHEMA.COLUMNS`
WHERE column_name = "person_id"
AND NOT STARTS_WITH(table_name, '_')
""")


class RtCtPIDtoRID(PIDtoRID):
    """
    Use RID instead of PID for OMOP tables in the registered tier
    and the controlled tier
    """

    def __init__(self,
                 project_id,
                 dataset_id,
                 sandbox_dataset_id,
                 table_namer=None):
        """
        Initialize the class with proper info.

        Set the issue numbers, description and affected datasets.  As other
        tickets may affect this SQL, append them to the list of Jira Issues.
        DO NOT REMOVE ORIGINAL JIRA ISSUE NUMBERS!
        """
        super().__init__(project_id=project_id,
                         dataset_id=dataset_id,
                         sandbox_dataset_id=sandbox_dataset_id,
                         mapping_dataset_id=sandbox_dataset_id,
                         mapping_table_id=DEID_MAP,
                         affected_tables=CDM_TABLES + [AOU_DEATH],
                         issue_numbers=ISSUE_NUMBERS,
                         table_namer=table_namer)

    def get_pid_tables(self, client):
        pid_tables_query = GET_PID_TABLES.render(project_id=self.project_id,
                                                 dataset_id=self.dataset_id)
        query_job = client.query(pid_tables_query)
        result_df = query_job.result().to_dataframe()
        return result_df.get('table_name').to_list()

    def setup_rule(self, client):
        self.pid_tables = [
            gbq.TableReference.from_string(
                f'{self.project_id}.{self.dataset_id}.{table_id}')
            for table_id in self.get_pid_tables(client)
        ]
        super().setup_rule(client)


if __name__ == '__main__':
    import cdr_cleaner.args_parser as parser
    import cdr_cleaner.clean_cdr_engine as clean_engine

    ARGS = parser.default_parse_args()

    if ARGS.list_queries:
        clean_engine.add_console_logging()
        query_list = clean_engine.get_query_list(ARGS.project_id,
                                                 ARGS.dataset_id,
                                                 ARGS.sandbox_dataset_id,
                                                 [(RtCtPIDtoRID,)])
        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(ARGS.project_id, ARGS.dataset_id,
                                   ARGS.sandbox_dataset_id, [(RtCtPIDtoRID,)])
