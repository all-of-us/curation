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

LOGGER = logging.getLogger(__name__)

ISSUE_NUMBERS = ['DC-1000']

pipeline_tables = 'pipeline_tables'

MAX_AGE = 89

ACTIVITY_SUMMARY = 'activity_summary'
HEART_RATE_MINUTE_LEVEL = 'heart_rate_minute_level'
HEART_RATE_SUMMARY = 'heart_rate_summary'
STEPS_INTRADAY = 'steps_intraday'

FITBIT_TABLES = [
    ACTIVITY_SUMMARY, HEART_RATE_MINUTE_LEVEL, HEART_RATE_SUMMARY,
    STEPS_INTRADAY
]

PID_RID_QUERY = """
SELECT
    {{cols}}
FROM `{{fitbit_table.project}}.{{fitbit_table.dataset_id}}.{{fitbit_table.table_id}}` t
LEFT JOIN `{{deid_map.project}}.{{deid_map.dataset_id}}.{{deid_map.table_id}}` d
ON t.person_id = d.person_id
WHERE t.person_id IN 
(SELECT person_id
FROM `{{combined_person.project}}.{{combined_person.dataset_id}}.{{combined_person.table_id}}`
WHERE (EXTRACT(YEAR FROM CURRENT_DATE()) - year_of_birth) < {{max_age}})
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

    def __init__(self,
                 project_id,
                 dataset_id,
                 sandbox_dataset_id,
                 combined_dataset_id,
                 max_age=MAX_AGE):
        """
        Initialize the class with proper info.

        Set the issue numbers, description and affected datasets.  As other
        tickets may affect this SQL, append them to the list of Jira Issues.
        DO NOT REMOVE ORIGINAL JIRA ISSUE NUMBERS!
        """
        desc = f'Change PIDs to RIDs in specified tables'
        super().__init__(issue_numbers=ISSUE_NUMBERS,
                         description=desc,
                         affected_datasets=['fitbit'],
                         project_id=project_id,
                         dataset_id=dataset_id,
                         sandbox_dataset_id=sandbox_dataset_id,
                         affected_tables=FITBIT_TABLES)

        self.dataset_ref = gbq.DatasetReference(self.project_id,
                                                self.dataset_id)
        self.pid_tables = []
        self.table_refs_cols = []
        self.pipeline_tables_ref = gbq.DatasetReference(self.project_id,
                                                        pipeline_tables)
        self.deid_map = gbq.TableReference(self.pipeline_tables_ref,
                                           'deid_mapping')
        self.combined_dataset_ref = gbq.DatasetReference(
            self.project_id, combined_dataset_id)
        self.max_age = max_age
        self.person = gbq.TableReference(self.combined_dataset_ref, 'person')

    @staticmethod
    def get_cols_str(cols):
        cols_list = []
        for col in cols:
            join_col = f'd.research_id AS {col}' if col == 'person_id' else f't.{col}'
            cols_list.append(join_col)
        return ','.join(cols_list)

    def get_query_specs(self):
        """
        Return a list of dictionary query specifications.

        :return:  A list of dictionaries.  Each dictionary contains a
            single query and a specification for how to execute that query.
            The specifications are optional but the query is required.
        """
        queries = []

        for table_ref_cols in self.table_refs_cols:
            table = table_ref_cols['ref']
            cols = table_ref_cols['cols']
            join_cols = self.get_cols_str(cols)
            table_query = {
                cdr_consts.QUERY:
                    PID_RID_QUERY_TMPL.render(cols=join_cols,
                                              fitbit_table=table,
                                              deid_map=self.deid_map,
                                              combined_person=self.person,
                                              max_age=self.max_age),
                cdr_consts.DESTINATION_TABLE:
                    table.table_id,
                cdr_consts.DESTINATION_DATASET:
                    self.dataset_id,
                cdr_consts.DISPOSITION:
                    WRITE_TRUNCATE
            }
            queries.append(table_query)

        return queries

    def setup_rule(self, client):
        """
        Function to run any data upload options before executing a query.
        """
        cols_query = bq.dataset_columns_query(self.project_id, self.dataset_id)
        table_df = client.query(cols_query).to_dataframe(index='table_name')
        fitbit_df = table_df.filter(items=FITBIT_TABLES, axis=0)
        pid_tables_df = fitbit_df.loc[fitbit_df['column_name'] == 'person_id']
        self.pid_tables = pid_tables_df['table_name'].to_list()
        self.table_refs_cols = [{
            'ref':
                gbq.TableReference(self.dataset_ref, table),
            'cols':
                fitbit_df.loc[fitbit_df['table_name'] == table]
                ['column_name'].to_list()
        } for table in self.pid_tables]
        LOGGER.info(
            f'Identified the following fitbit tables with pids: {self.pid_tables}'
        )

    def setup_validation(self, client):
        """
        Run required steps for validation setup
        """
        raise NotImplementedError("Please fix me.")

    def validate_rule(self):
        """
        Validates the cleaning rule which deletes or updates the data from the tables
        """
        for table_ref_cols in self.table_refs_cols:
            table = table_ref_cols['ref']
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

    max_age_arg = {
        parser.SHORT_ARGUMENT: '-a',
        parser.LONG_ARGUMENT: '--max_age',
        parser.ACTION: 'store',
        parser.DEST: 'max_age',
        parser.DEFAULT: MAX_AGE,
        parser.TYPE: int,
        parser.HELP: f'Set the max age, default={MAX_AGE}',
        parser.REQUIRED: False
    }

    ARGS = parser.default_parse_args([combined_dataset_arg, max_age_arg])
    clean_engine.add_console_logging(ARGS.console_log)
    pid_rid_rule = PIDtoRID(ARGS.project_id, ARGS.dataset_id,
                            ARGS.sandbox_dataset_id, ARGS.combined_dataset_id,
                            ARGS.max_age)
    query_list = pid_rid_rule.get_query_specs()

    if ARGS.list_queries:
        pid_rid_rule.log_queries()
    else:
        client = bq.get_client(ARGS.project_id)
        pid_rid_rule.setup_rule(client)
        clean_engine.clean_dataset(ARGS.project_id, query_list)
