"""
Rule to sandbox and drop records in the sleep_level table where level is
not one of the following: awake, light, asleep, deep, restless, wake, rem, unknown.
"""

# Python imports
import logging

# Project imports
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule
from constants.cdr_cleaner import clean_cdr as cdr_consts
from common import JINJA_ENV, FITBIT_TABLES, SLEEP_LEVEL

LOGGER = logging.getLogger(__name__)

ISSUE_NUMBERS = ['DC2605']

SANDBOX_INVALID_LEVEL_RECORDS = JINJA_ENV.from_string("""
CREATE OR REPLACE TABLE
    `{{project_id}}.{{sandbox_dataset}}.{{sandbox_table}}` AS
(
    SELECT
        *
    FROM
        `{{project_id}}.{{dataset_id}}.{{sleep_level_table}}`
    WHERE level NOT IN 
        ('awake','light','asleep','deep','restless','wake','rem','unknown') OR level IS NULL
)
 """)

DELETE_INVALID_LEVEL_RECORDS = JINJA_ENV.from_string("""
DELETE
FROM 
    `{{project_id}}.{{dataset_id}}.{{sleep_level_table}}`
WHERE person_id IN
(
    SELECT 
        person_id
    FROM 
        `{{project_id}}.{{sandbox_dataset}}.{{sandbox_table}}`
)
 """)


class DropInvalidSleepLevelRecords(BaseCleaningRule):

    def __init__(self,
                 project_id,
                 dataset_id,
                 sandbox_dataset_id,
                 table_namer=None):
        """
        Initialize the class with proper information.

        Set the issue numbers, description and affected datasets. As other tickets may affect
        this SQL, append them to the list of Jira Issues.
        DO NOT REMOVE ORIGINAL JIRA ISSUE NUMBERS!
        """

        desc = ('Rule to sandbox and drop records in the sleep_level table')

        super().__init__(issue_numbers=ISSUE_NUMBERS,
                         description=desc,
                         affected_datasets=[cdr_consts.FITBIT],
                         affected_tables=FITBIT_TABLES,
                         project_id=project_id,
                         dataset_id=dataset_id,
                         sandbox_dataset_id=sandbox_dataset_id,
                         table_namer=table_namer)

    def get_query_specs(self, *args, **keyword_args):
        """
        Return a list of dictionary query specifications.

        :return:  A list of dictionaries. Each dictionary contains a single query
            and a specification for how to execute that query. The specifications
            are optional but the query is required.
        """

        sandbox_invalid_records = {
            cdr_consts.QUERY:
                SANDBOX_INVALID_LEVEL_RECORDS.render(
                    project_id=self.project_id,
                    dataset_id=self.dataset_id,
                    sleep_level_table=SLEEP_LEVEL,
                    sandbox_dataset=self.sandbox_dataset_id,
                    sandbox_table=self.get_sandbox_tablenames()[0])
        }

        delete_invalid_records = {
            cdr_consts.QUERY:
                DELETE_INVALID_LEVEL_RECORDS.render(
                    project_id=self.project_id,
                    dataset_id=self.dataset_id,
                    sleep_level_table=SLEEP_LEVEL,
                    sandbox_dataset=self.sandbox_dataset_id,
                    sandbox_table=self.get_sandbox_tablenames()[0])
        }

        return [sandbox_invalid_records, delete_invalid_records]

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
        """
        generates sandbox table names
        """
        sandbox_table = self.sandbox_table_for(SLEEP_LEVEL)
        return [sandbox_table]


if __name__ == '__main__':
    import cdr_cleaner.args_parser as parser
    import cdr_cleaner.clean_cdr_engine as clean_engine

    ARGS = parser.default_parse_args()

    if ARGS.list_queries:
        clean_engine.add_console_logging()
        query_list = clean_engine.get_query_list(
            ARGS.project_id, ARGS.dataset_id, ARGS.sandbox_dataset_id,
            [(DropInvalidSleepLevelRecords,)])
        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(ARGS.project_id, ARGS.dataset_id,
                                   ARGS.sandbox_dataset_id,
                                   [(DropInvalidSleepLevelRecords,)])
