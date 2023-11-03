"""
Remove all FitBit data for participants exceeding the maximum age of 89

Original Issue: DC-1001, DC-1037, DC-2429, DC-2135, DC-3165

The intent is to ensure there is no data for participants over the age of 89 in
Activity Summary, Heart Rate Minute Level, Heart Rate Summary, Steps Intraday,
Sleep Daily Summary, Sleep Level and Device tables by sandboxing the applicable records
and then dropping them.
"""

# Python Imports
import logging

# Third Party Imports
import google.cloud.bigquery as gbq

# Project Imports
import constants.cdr_cleaner.clean_cdr as cdr_consts
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule
from common import FITBIT_TABLES, JINJA_ENV, PIPELINE_TABLES, DEVICE
from constants.bq_utils import WRITE_TRUNCATE

LOGGER = logging.getLogger(__name__)

# Save rows that will be dropped to a sandboxed dataset
SAVE_ROWS_TO_BE_DROPPED_QUERY = JINJA_ENV.from_string("""
CREATE OR REPLACE TABLE `{{project}}.{{sandbox_dataset}}.{{sandbox_table}}` AS
SELECT * FROM `{{project}}.{{dataset}}.{{table}}`
WHERE person_id IN (
  SELECT person_id
    FROM (
    SELECT DISTINCT person_id,
        {{PIPELINE_TABLES}}.calculate_age(CURRENT_DATE, EXTRACT(DATE FROM birth_datetime)) AS age
            FROM `{{combined_dataset.project}}.{{combined_dataset.dataset_id}}.{{combined_dataset.table_id}}` ORDER BY 2)
        WHERE age >= 89)
""")

# Drop rows where age is greater than 89
DROP_MAX_AGE_EXCEEDED_ROWS_QUERY = JINJA_ENV.from_string("""
SELECT *
FROM `{{project}}.{{dataset}}.{{table}}` t
WHERE person_id NOT IN (
    SELECT DISTINCT person_id FROM `{{project}}.{{sandbox_dataset}}.{{sandbox_table}}`)
""")


class RemoveFitbitDataIfMaxAgeExceeded(BaseCleaningRule):
    """
    Ensures that there is no FitBit data for participants over the age of 89
    in the Activity Summary, Heart Rate Minute Level, Heart Rate Summary,
    Steps Intraday, Sleep Daily Summary, and Sleep Level FitBit tables.
    """

    def __init__(self,
                 project_id,
                 dataset_id,
                 sandbox_dataset_id,
                 table_namer=None,
                 combined_dataset_id=None):
        """
        Initialize the class with proper information.

        Set the issue numbers, description and affected datasets. As other tickets may affect
        this SQL, append them to the list of Jira Issues.
        DO NOT REMOVE ORIGINAL JIRA ISSUE NUMBERS!
        """
        desc = (
            'Drops all FitBit data from participants whose max age exceeds 89')
        super().__init__(
            issue_numbers=['DC1001', 'DC1037', 'DC2429', 'DC2135', 'DC3165'],
            description=desc,
            affected_datasets=[cdr_consts.FITBIT],
            # affected_tables=FITBIT_TABLES,
            affected_tables=[DEVICE],
            project_id=project_id,
            dataset_id=dataset_id,
            sandbox_dataset_id=sandbox_dataset_id,
            table_namer=table_namer)

        self.person = gbq.TableReference.from_string(
            f'{project_id}.{combined_dataset_id}.person')

    def get_query_specs(self):
        """
        Return a list of dictionary query specifications.

        :return:  A list of dictionaries. Each dictionary contains a single query
            and a specification for how to execute that query. The specifications
            are optional but the query is required.
        """

        sandbox_queries_list = []
        drop_queries_list = []
        # for table in FITBIT_TABLES:
        for table in self.affected_tables:
            sandbox_queries_list.append({
                cdr_consts.QUERY:
                    SAVE_ROWS_TO_BE_DROPPED_QUERY.render(
                        project=self.project_id,
                        sandbox_dataset=self.sandbox_dataset_id,
                        sandbox_table=self.sandbox_table_for(table),
                        dataset=self.dataset_id,
                        table=table,
                        PIPELINE_TABLES=PIPELINE_TABLES,
                        combined_dataset=self.person,
                    )
            })

            drop_queries_list.append({
                cdr_consts.QUERY:
                    DROP_MAX_AGE_EXCEEDED_ROWS_QUERY.render(
                        project=self.project_id,
                        dataset=self.dataset_id,
                        table=table,
                        sandbox_dataset=self.sandbox_dataset_id,
                        sandbox_table=self.sandbox_table_for(table)),
                cdr_consts.DESTINATION_TABLE:
                    table,
                cdr_consts.DESTINATION_DATASET:
                    self.dataset_id,
                cdr_consts.DISPOSITION:
                    WRITE_TRUNCATE
            })

        # returns the unnested list of list of dictionaries
        return sandbox_queries_list + drop_queries_list

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
        Validates the deid "cleaning rule" which deletes or updates the data from the tables
        """
        raise NotImplementedError("Please fix me.")

    def get_sandbox_tablenames(self):
        return [
            self.sandbox_table_for(affected_table)
            for affected_table in self.affected_tables
        ]


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
        query_list = clean_engine.get_query_list(
            ARGS.project_id,
            ARGS.dataset_id,
            ARGS.sandbox_dataset_id, [(RemoveFitbitDataIfMaxAgeExceeded,)],
            combined_dataset_id=ARGS.combined_dataset_id)
        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(ARGS.project_id,
                                   ARGS.dataset_id,
                                   ARGS.sandbox_dataset_id,
                                   [(RemoveFitbitDataIfMaxAgeExceeded,)],
                                   combined_dataset_id=ARGS.combined_dataset_id)
