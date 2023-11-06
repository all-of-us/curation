"""
Remove all FitBit data after the cutoff date for participants

Original Issue: DC-1046

The intent is to ensure there is no data after the cutoff date for participants in
the fitbit tables by sandboxing the applicable records and then dropping them.
"""

# Python Imports
import logging
from datetime import datetime

# Project Imports
from common import JINJA_ENV, FITBIT_TABLES
from resources import validate_date_string, fields_for
from constants.cdr_cleaner.clean_cdr import FITBIT, DESTINATION_TABLE, DESTINATION_DATASET, QUERY, DISPOSITION
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule
from constants.bq_utils import WRITE_TRUNCATE

LOGGER = logging.getLogger(__name__)

# Save rows that will be dropped to a sandboxed dataset
SANDBOX_QUERY = JINJA_ENV.from_string("""
CREATE OR REPLACE TABLE `{{project_id}}.{{sandbox_id}}.{{intermediary_table}}` AS (
SELECT * 
FROM `{{project_id}}.{{dataset_id}}.{{fitbit_table}}`
WHERE (GREATEST({{date_fields}}) > DATE("{{truncation_date}}"))
)
""")

# Drop any FitBit data that is newer than the cutoff date
TRUNCATE_FITBIT_DATA_QUERY = JINJA_ENV.from_string("""
SELECT * FROM `{{project_id}}.{{dataset_id}}.{{fitbit_table}}` t
EXCEPT DISTINCT
SELECT * FROM `{{project_id}}.{{sandbox_id}}.{{intermediary_table}}`
""")


class TruncateFitbitData(BaseCleaningRule):
    """
    All rows of FitBit data with dates after the truncation date should be moved from the Activity Summary,
    Heart Rate Minute Level, Heart Rate Summary, and Steps Intraday tables to a sandboxed FitBit dataset
    """

    def __init__(self,
                 project_id,
                 dataset_id,
                 sandbox_dataset_id,
                 truncation_date=None):
        """
        Initialize the class with proper information.

        Set the issue numbers, description and affected datasets. As other tickets may affect
        this SQL, append them to the list of Jira Issues.
        DO NOT REMOVE ORIGINAL JIRA ISSUE NUMBERS!
        """
        try:
            # set to provided date string if the date string is valid
            self.truncation_date = validate_date_string(truncation_date)
        except (TypeError, ValueError):
            # otherwise, default to using today's date as the date string
            self.truncation_date = str(datetime.now().date())

        desc = (
            f'All rows of data in the Fitbit dataset {dataset_id} with dates after '
            f'{self.truncation_date} will be sandboxed and dropped.')
        super().__init__(issue_numbers=['DC1046', 'DC3163'],
                         description=desc,
                         affected_datasets=[FITBIT],
                         affected_tables=FITBIT_TABLES,
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

        for table in self.get_affected_tables():
            # gets all fields from the affected tables
            fields = fields_for(table)

            date_fields = []

            for field in fields:
                # appends date and datetime columns to the date_fields list
                if field['type'].lower() == 'date':
                    date_fields.append(
                        f'COALESCE({field["name"]}, DATE("1900-01-01"))')
                elif field['type'].lower() == 'datetime':
                    date_fields.append(
                        f'COALESCE(DATE({field["name"]}), DATE("1900-01-01"))')

            # will render the queries only if a table contains a date or datetime field
            # will ignore the tables that do not have a date or datetime field
            if date_fields:
                sandbox_query = {
                    QUERY:
                        SANDBOX_QUERY.render(
                            project_id=self.project_id,
                            sandbox_id=self.sandbox_dataset_id,
                            intermediary_table=self.sandbox_table_for(table),
                            dataset_id=self.dataset_id,
                            fitbit_table=table,
                            date_fields=(", ".join(date_fields)),
                            truncation_date=self.truncation_date),
                }

                sandbox_queries.append(sandbox_query)

                truncate_fitbit_data_query = {
                    QUERY:
                        TRUNCATE_FITBIT_DATA_QUERY.render(
                            project_id=self.project_id,
                            dataset_id=self.dataset_id,
                            fitbit_table=table,
                            sandbox_id=self.sandbox_dataset_id,
                            intermediary_table=self.sandbox_table_for(table)),
                    DESTINATION_TABLE:
                        table,
                    DESTINATION_DATASET:
                        self.dataset_id,
                    DISPOSITION:
                        WRITE_TRUNCATE
                }

                truncate_queries.append(truncate_fitbit_data_query)

        return sandbox_queries + truncate_queries

    def setup_rule(self, client):
        """
        Function to run any data upload options before executing a query.
        """
        pass

    def get_affected_tables(self):
        """
        This method gets all the tables that are affected by this cleaning rule.

        :return: list of affected tables
        """
        return self.affected_tables

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
        sandbox_tables = []
        for table in self.affected_tables:
            sandbox_tables.append(self.sandbox_table_for(table))
        return sandbox_tables


if __name__ == '__main__':
    import cdr_cleaner.args_parser as parser
    import cdr_cleaner.clean_cdr_engine as clean_engine

    ext_parser = parser.get_argument_parser()
    ext_parser.add_argument(
        '-t',
        '--truncation_date',
        action='store',
        dest='truncation_date',
        help=('Cutoff date for data based on fitbit date and datetime fields.  '
              'Should be in the form YYYY-MM-DD.'),
        required=True)

    ARGS = ext_parser.parse_args()

    if ARGS.list_queries:
        clean_engine.add_console_logging()
        query_list = clean_engine.get_query_list(
            ARGS.project_id,
            ARGS.dataset_id,
            ARGS.sandbox_dataset_id, [(TruncateFitbitData,)],
            truncation_date=ARGS.truncation_date)
        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(ARGS.project_id,
                                   ARGS.dataset_id,
                                   ARGS.sandbox_dataset_id,
                                   [(TruncateFitbitData,)],
                                   truncation_date=ARGS.truncation_date)
