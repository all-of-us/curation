"""
Remove all FitBit data after the cutoff date for participants

Original Issue: DC-1046

The intent is to ensure there is no data after the cutoff date for participants in
Activity Summary, Heart Rate Minute Level, Heart Rate Summary, and Steps Intraday tables
by sandboxing the applicable records and then dropping them.
"""

# Python Imports
import logging

# Third Party Imports
from jinja2 import Environment

# Project Imports
import common
import constants.cdr_cleaner.clean_cdr as cdr_consts
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule
from constants.bq_utils import WRITE_TRUNCATE
from utils.bq import JINJA_ENV

LOGGER = logging.getLogger(__name__)

CUTOFF_DATE = '2019-11-26'
CUTOFF_DATETIME = '2019-11-26T00:00:00'

FITBIT_DATE_TABLES = [common.ACTIVITY_SUMMARY, common.HEART_RATE_SUMMARY]
FITBIT_DATETIME_TABLES = [common.HEART_RATE_MINUTE_LEVEL, common.STEPS_INTRADAY]

FITBIT_TABLES_DATE_FIELDS = {
    common.ACTIVITY_SUMMARY: 'date',
    common.HEART_RATE_SUMMARY: 'date'
}
FITBIT_TABLES_DATETIME_FIELDS = {
    common.HEART_RATE_MINUTE_LEVEL: 'datetime',
    common.STEPS_INTRADAY: 'datetime'
}

# Save rows that will be dropped to a sandboxed dataset
SANDBOX_QUERY = JINJA_ENV.from_string("""
CREATE OR REPLACE TABLE `{{project}}.{{sandbox}}.{{intermediary_table}}` AS (
SELECT * 
FROM `{{project}}.{{dataset}}.{{table_name}}`
WHERE {{date_field}} > '{{cutoff_date}}')""")

# Drop any FitBit data that is newer than the cutoff date
TRUNCATE_FITBIT_DATA_QUERY = JINJA_ENV.from_string("""
SELECT * FROM `{{project}}.{{dataset}}.{{table_name}}` t
EXCEPT DISTINCT
SELECT * FROM `{{project}}.{{sandbox}}.{{intermediary_table}}`""")


class TruncateFitbitData(BaseCleaningRule):
    """
    All rows of FitBit data with dates after 11/26/2019 should be moved from the Activity Summary,
    Heart Rate Minute Level, Heart Rate Summary, and Steps Intraday tables to a sandboxed FitBit dataset
    """

    def __init__(self, project_id, dataset_id, sandbox_dataset_id):
        """
        Initialize the class with proper information.

        Set the issue numbers, description and affected datasets. As other tickets may affect
        this SQL, append them to the list of Jira Issues.
        DO NOT REMOVE ORIGINAL JIRA ISSUE NUMBERS!
        """
        desc = 'All rows of data in the FitBit tables with dates after 11/26/2019 will be truncated.'
        super().__init__(issue_numbers=['DC1046'],
                         description=desc,
                         affected_datasets=[cdr_consts.FITBIT],
                         affected_tables=common.FITBIT_TABLES,
                         project_id=project_id,
                         dataset_id=dataset_id,
                         sandbox_dataset_id=sandbox_dataset_id)

    def get_query_specs(self):
        """
        Return a list of dictionary query specifications.

        :return:  A list of dictionaries. Each dictionary contains a single query
            and a specification for how to execute that query. The specifications
            are optional but the query is required.
        """
        sandbox_queries = []
        truncate_queries = []
        date_sandbox, datetime_sandbox = self.get_sandbox_tablenames()

        # Sandboxes and truncates data from FitBit tables with date
        for i, table in enumerate(FITBIT_DATE_TABLES):
            save_dropped_date_rows = {
                cdr_consts.QUERY:
                    SANDBOX_QUERY.render(
                        project=self.project_id,
                        sandbox=self.sandbox_dataset_id,
                        intermediary_table=date_sandbox[i],
                        dataset=self.dataset_id,
                        table_name=table,
                        date_field=FITBIT_TABLES_DATE_FIELDS[table],
                        cutoff_date=CUTOFF_DATE)
            }
            sandbox_queries.append(save_dropped_date_rows)

            truncate_date_query = {
                cdr_consts.QUERY:
                    TRUNCATE_FITBIT_DATA_QUERY.render(
                        project=self.project_id,
                        dataset=self.dataset_id,
                        table_name=table,
                        sandbox=self.sandbox_dataset_id,
                        intermediary_table=date_sandbox[i]),
                cdr_consts.DESTINATION_TABLE:
                    table,
                cdr_consts.DESTINATION_DATASET:
                    self.dataset_id,
                cdr_consts.DISPOSITION:
                    WRITE_TRUNCATE
            }
            truncate_queries.append(truncate_date_query)

        # Sandboxes and truncates data from FitBit tables with datetime
        for i, table in enumerate(FITBIT_DATETIME_TABLES):
            save_dropped_datetime_rows = {
                cdr_consts.QUERY:
                    SANDBOX_QUERY.render(
                        project=self.project_id,
                        sandbox=self.sandbox_dataset_id,
                        intermediary_table=datetime_sandbox[i],
                        dataset=self.dataset_id,
                        table_name=table,
                        date_field=FITBIT_TABLES_DATETIME_FIELDS[table],
                        cutoff_date=CUTOFF_DATETIME)
            }
            sandbox_queries.append(save_dropped_datetime_rows)

            truncate_date_query = {
                cdr_consts.QUERY:
                    TRUNCATE_FITBIT_DATA_QUERY.render(
                        project=self.project_id,
                        dataset=self.dataset_id,
                        table_name=table,
                        sandbox=self.sandbox_dataset_id,
                        intermediary_table=datetime_sandbox[i]),
                cdr_consts.DESTINATION_TABLE:
                    table,
                cdr_consts.DESTINATION_DATASET:
                    self.dataset_id,
                cdr_consts.DISPOSITION:
                    WRITE_TRUNCATE
            }
            truncate_queries.append(truncate_date_query)

        return sandbox_queries + truncate_queries

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
        raise NotImplementedError("Please fix me.")

    def get_sandbox_tablenames(self):
        sandbox_date_tables = []
        sandbox_datetime_tables = []
        for table in FITBIT_DATE_TABLES:
            sandbox_date_tables.append(
                f'{self._issue_numbers[0].lower()}_{table}')
        for table in FITBIT_DATETIME_TABLES:
            sandbox_datetime_tables.append(
                f'{self._issue_numbers[0].lower()}_{table}')
        return sandbox_date_tables, sandbox_datetime_tables


if __name__ == '__main__':
    import cdr_cleaner.args_parser as parser
    import cdr_cleaner.clean_cdr_engine as clean_engine

    ARGS = parser.parse_args()

    if ARGS.list_queries:
        clean_engine.add_console_logging()
        query_list = clean_engine.get_query_list(ARGS.project_id,
                                                 ARGS.dataset_id,
                                                 ARGS.sandbox_dataset_id,
                                                 [(TruncateFitbitData,)])
        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(ARGS.project_id, ARGS.dataset_id,
                                   ARGS.sandbox_dataset_id,
                                   [(TruncateFitbitData,)])
