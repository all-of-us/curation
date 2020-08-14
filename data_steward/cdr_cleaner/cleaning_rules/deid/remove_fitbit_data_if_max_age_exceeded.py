"""
Remove all FitBit data for participants exceeding the maximum age of 89

Original Issue: DC-1001

The intent is to ensure there is no data for participants over the age of 89 in
Activity Summary, Heart Rate Minute Level, Heart Rate Summary, and Steps Intraday tables
by sandboxing the applicable records and then dropping them.
"""

# Python Imports
import logging

# Third Party Imports
from jinja2 import Environment

# Project Imports
import constants.cdr_cleaner.clean_cdr as cdr_consts
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule
from constants.bq_utils import WRITE_TRUNCATE

LOGGER = logging.getLogger(__name__)

jinja_env = Environment(
    # help protect against cross-site scripting vulnerabilities
    autoescape=True,
    # block tags on their own lines
    # will not cause extra white space
    trim_blocks=True,
    lstrip_blocks=True,
    # syntax highlighting should be better
    # with these comment delimiters
    comment_start_string='--',
    comment_end_string=' --')

TABLES = [
    'activity_summary', 'heart_rate_minute_level', 'heart_rate_summary',
    'steps_intraday'
]

# Save rows that will be dropped to a sandboxed dataset
SAVE_ROWS_TO_BE_DROPPED_QUERY = jinja_env.from_string("""
CREATE OR REPLACE TABLE `{{project}}.{{sandbox_dataset}}.{{sandbox_table}}` AS
SELECT * FROM `{{project}}.{{dataset}}.{{table}}`
WHERE person_id IN (
  SELECT person_id
    FROM (
    SELECT DISTINCT person_id,
        EXTRACT(YEAR FROM CURRENT_DATE()) - year_of_birth AS age
            FROM `{{project}}.{{dataset}}.person` ORDER BY 2)
        WHERE age > 89)
""")

# Drop rows where age is greater than 89
DROP_MAX_AGE_EXCEEDED_ROWS_QUERY = jinja_env.from_string("""
SELECT *
FROM `{{project}}.{{dataset}}.{{table}}` t
WHERE person_id NOT IN (
    SELECT person_id FROM `{{project}}.{{sandbox_dataset}}.{{sandbox_table}}`)
""")


class RemoveFitbitDataIfMaxAgeExceeded(BaseCleaningRule):
    """
    Ensures that there is no FitBit data for participants over the age of 89
    in the Activity Summary, Heart Rate Minute Level, Heart Rate Summary, and
    Steps Intraday FitBit tables.
    """

    def __init__(self, project_id, dataset_id, sandbox_dataset_id):
        """
        Initialize the class with proper information.

        Set the issue numbers, description and affected datasets. As other tickets may affect
        this SQL, append them to the list of Jira Issues.
        DO NOT REMOVE ORIGINAL JIRA ISSUE NUMBERS!
        """
        desc = (
            'Drops all FitBit data from participants whose max age exceeds 89')
        super().__init__(issue_numbers=['DC1001'],
                         description=desc,
                         affected_datasets=[cdr_consts.RDR],
                         project_id=project_id,
                         dataset_id=dataset_id,
                         sandbox_dataset_id=sandbox_dataset_id,
                         affected_tables=TABLES)

    def get_query_specs(self):
        """
        Return a list of dictionary query specifications.

        :return:  A list of dictionaries. Each dictionary contains a single query
            and a specification for how to execute that query. The specifications
            are optional but the query is required.
        """

        sandbox_queries_list = []
        drop_queries_list = []
        for i, table in enumerate(TABLES):
            sandbox_queries_list.append({
                cdr_consts.QUERY:
                    SAVE_ROWS_TO_BE_DROPPED_QUERY.render(
                        project=self.project_id,
                        sandbox_dataset=self.sandbox_dataset_id,
                        sandbox_table=self.get_sandbox_tablenames()[i],
                        dataset=self.dataset_id,
                        table=table)
            })

        for i, table in enumerate(TABLES):
            drop_queries_list.append({
                cdr_consts.QUERY:
                    DROP_MAX_AGE_EXCEEDED_ROWS_QUERY.render(
                        project=self.project_id,
                        dataset=self.dataset_id,
                        table=table,
                        sandbox_dataset=self.sandbox_dataset_id,
                        sandbox_table=self.get_sandbox_tablenames()[i]),
                cdr_consts.DESTINATION_TABLE:
                    table,
                cdr_consts.DESTINATION_DATASET:
                    self.dataset_id,
                cdr_consts.DISPOSITION:
                    WRITE_TRUNCATE
            })

        return [sandbox_queries_list, drop_queries_list]

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
        sandbox_table_names = list()
        for i in range(0, len(self._affected_tables)):
            sandbox_table_names.append(self._issue_numbers[0].lower() + '_' +
                                       self._affected_tables[i])
        return sandbox_table_names


if __name__ == '__main__':
    import cdr_cleaner.args_parser as parser
    import cdr_cleaner.clean_cdr_engine as clean_engine

    ARGS = parser.parse_args()

    clean_engine.add_console_logging(ARGS.console_log)

    cleaner = RemoveFitbitDataIfMaxAgeExceeded(ARGS.project_id, ARGS.dataset_id,
                                               ARGS.sandbox_dataset_id)
    query_list = cleaner.get_query_specs()

    if ARGS.list_queries:
        cleaner.log_queries()
    else:
        clean_engine.clean_dataset(ARGS.project_id, query_list)
