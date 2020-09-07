"""
DEID rule to change PIDs to RIDs for specific tables
"""
# Python Imports
import logging

# Third party imports
import google.cloud.bigquery as gbq

# Project imports
from utils import bq
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule
from constants.bq_utils import WRITE_TRUNCATE
from constants.cdr_cleaner import clean_cdr as cdr_consts
from common import FITBIT_TABLES

LOGGER = logging.getLogger(__name__)

ISSUE_NUMBERS = ['DC-1000']

PID_RID_QUERY = """
SELECT
    t.* EXCEPT (person_id),
    d.research_id as person_id
FROM `{{fitbit_table.project}}.{{fitbit_table.dataset_id}}.{{fitbit_table.table_id}}` t
LEFT JOIN `{{deid_map.project}}.{{deid_map.dataset_id}}.{{deid_map.table_id}}` d
ON t.person_id = d.person_id
"""

PID_RID_QUERY_TMPL = bq.JINJA_ENV.from_string(PID_RID_QUERY)

VALIDATE_QUERY = """
SELECT person_id
FROM `{{fitbit_table.project}}.{{fitbit_table.dataset_id}}.{{fitbit_table.table_id}}`
WHERE person_id NOT IN 
(SELECT research_id
FROM `{{deid_map.project}}.{{deid_map.dataset_id}}.{{deid_map.table_id}}`)
"""

VALIDATE_QUERY_TMPL = bq.JINJA_ENV.from_string(VALIDATE_QUERY)


class PIDtoRID(BaseCleaningRule):
    """
    Use RID instead of PID for specific tables
    """

    def __init__(self, project_id, dataset_id, sandbox_dataset_id,
                 combined_dataset_id):
        """
        Initialize the class with proper info.

        Set the issue numbers, description and affected datasets.  As other
        tickets may affect this SQL, append them to the list of Jira Issues.
        DO NOT REMOVE ORIGINAL JIRA ISSUE NUMBERS!
        """
        desc = f'Change PIDs to RIDs in specified tables'
        super().__init__(issue_numbers=ISSUE_NUMBERS,
                         description=desc,
                         affected_datasets=[dataset_id],
                         project_id=project_id,
                         dataset_id=dataset_id,
                         sandbox_dataset_id=sandbox_dataset_id,
                         affected_tables=FITBIT_TABLES)

        self.pid_tables = [
            gbq.TableReference.from_string(
                f'{self.project_id}.{self.dataset_id}.{table_id}')
            for table_id in FITBIT_TABLES
        ]
        fq_deid_map_table = f'{self.project_id}.{combined_dataset_id}._deid_map'
        self.deid_map = gbq.TableReference.from_string(fq_deid_map_table)
        self.person = gbq.TableReference.from_string(
            f'{self.project_id}.{combined_dataset_id}.person')

    def get_query_specs(self):
        """
        Return a list of dictionary query specifications.

        :return:  A list of dictionaries.  Each dictionary contains a
            single query and a specification for how to execute that query.
            The specifications are optional but the query is required.
        """
        queries = []

        for table in self.pid_tables:
            table_query = {
                cdr_consts.QUERY:
                    PID_RID_QUERY_TMPL.render(fitbit_table=table,
                                              deid_map=self.deid_map,
                                              combined_person=self.person),
                cdr_consts.DESTINATION_TABLE:
                    table.table_id,
                cdr_consts.DESTINATION_DATASET:
                    self.dataset_id,
                cdr_consts.DISPOSITION:
                    WRITE_TRUNCATE
            }
            queries.append(table_query)

        return queries

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
        for table in self.pid_tables:
            query = VALIDATE_QUERY_TMPL.render(
                fitbit_table=table,
                deid_map=self.deid_map,
            )
            result = client.query(query).result()
            if result.total_rows > 0:
                raise RuntimeError(
                    f'PIDs {result.total_rows} not converted to research_ids')


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
    clean_engine.add_console_logging(ARGS.console_log)

    pid_rid_rule = PIDtoRID(ARGS.project_id, ARGS.dataset_id,
                            ARGS.sandbox_dataset_id, ARGS.combined_dataset_id)
    query_list = pid_rid_rule.get_query_specs()

    if ARGS.list_queries:
        pid_rid_rule.log_queries()
    else:
        clean_engine.clean_dataset(ARGS.project_id, query_list)
