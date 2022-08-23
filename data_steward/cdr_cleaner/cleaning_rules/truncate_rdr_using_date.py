"""
The data cutoff date for PPI data for the fall 2020 CDR is 8/1. However, due to delays caused by the COPE vocabulary
 update, the RDR ETL was not able to run until several days later, and the ETL cannot truncate exported data by date.

Original Issues: DC-1009

All rows of data in the August RDR export with dates after 08/01/2020 should be moved from the RDR export to a
 sandboxed dataset prior to use of the RDR export to create the CDR
"""

# Python imports
from datetime import datetime
import logging

# Project imports
import common
import constants.cdr_cleaner.clean_cdr as cdr_consts
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule

LOGGER = logging.getLogger(__name__)

TRUNCATE_TABLES = [
    common.VISIT_OCCURRENCE, common.OBSERVATION, common.MEASUREMENT,
    common.PROCEDURE_OCCURRENCE, common.SURVEY_CONDUCT
]

TABLES_DATES_FIELDS = {
    common.VISIT_OCCURRENCE: 'visit_start_date',
    common.OBSERVATION: 'observation_date',
    common.MEASUREMENT: 'measurement_date',
    common.PROCEDURE_OCCURRENCE: 'procedure_date',
    common.SURVEY_CONDUCT: 'survey_start_date'
}

# Query to create tables in sandbox with the rows that will be removed per cleaning rule
SANDBOX_QUERY = common.JINJA_ENV.from_string("""
CREATE OR REPLACE TABLE
    `{{project}}.{{sandbox_dataset}}.{{intermediary_table}}` AS (
SELECT *
FROM
    `{{project}}.{{dataset}}.{{table_name}}`
    where {{field_name}} > '{{cutoff_date}}'
)
""")

TRUNCATE_ROWS = common.JINJA_ENV.from_string("""
DELETE
FROM
  `{{project}}.{{dataset}}.{{table_name}}`
WHERE
 {{field_name}} > '{{cutoff_date}}'
""")


class TruncateRdrData(BaseCleaningRule):
    """
    All rows of data in the August RDR export with dates after 08/01/2020 should be moved from the RDR export to a
    sandboxed dataset prior to use of the RDR export to create the CDR
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

        :params: truncation_date: the last date that should be included in the
            dataset
        """
        try:
            # set to provided date string if the date string is valid
            self.cutoff_date = validate_date_string(truncation_date)
            LOGGER.info(f'Using valid trunction date of: `{truncation_date}`')
        except (TypeError, ValueError):
            # otherwise, default to using todays date as the date string
            self.cutoff_date = str(datetime.now().date())
            LOGGER.warning(
                f'Using current date for truncation.  This may be undesirable.')

        desc = (f'All rows of data in the RDR export with dates after '
                f'{self.cutoff_date} will be truncated.')

        super().__init__(issue_numbers=['DC1009', 'DC2630'],
                         description=desc,
                         affected_datasets=[cdr_consts.RDR],
                         affected_tables=TRUNCATE_TABLES,
                         project_id=project_id,
                         dataset_id=dataset_id,
                         sandbox_dataset_id=sandbox_dataset_id,
                         run_for_synthetic=True)

    def get_query_specs(self):
        """
        Return a list of dictionary query specifications.

        :return:  A list of dictionaries. Each dictionary contains a single query
            and a specification for how to execute that query. The specifications
            are optional but the query is required.
        """
        counter = 0
        sandbox_queries = []
        truncate_queries = []
        for table in self.affected_tables:

            save_changed_rows = {
                cdr_consts.QUERY:
                    SANDBOX_QUERY.render(
                        project=self.project_id,
                        dataset=self.dataset_id,
                        sandbox_dataset=self.sandbox_dataset_id,
                        intermediary_table=self.get_sandbox_tablenames()
                        [counter],
                        table_name=table,
                        field_name=TABLES_DATES_FIELDS[table],
                        cutoff_date=self.cutoff_date)
            }

            sandbox_queries.append(save_changed_rows)

            truncate_query = {
                cdr_consts.QUERY:
                    TRUNCATE_ROWS.render(project=self.project_id,
                                         dataset=self.dataset_id,
                                         table_name=table,
                                         field_name=TABLES_DATES_FIELDS[table],
                                         cutoff_date=self.cutoff_date),
            }

            truncate_queries.append(truncate_query)
            counter += 1

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
        sandbox_tables = []
        for table in self.affected_tables:
            sandbox_tables.append(f'{self._issue_numbers[0].lower()}_{table}')
        return sandbox_tables


def validate_date_string(date_string):
    """
    Validates the date string is a valid date in the YYYY-MM-DD format.

    If the string is valid, it returns the string.  Otherwise, it raises either
    a ValueError or TypeError.

    :param date_string: The string to validate

    :return:  a valid date string
    :raises:  A ValueError if the date string is not a valid date or
        doesn't conform to the specified format.
    """
    datetime.strptime(date_string, '%Y-%m-%d')
    return date_string


if __name__ == '__main__':
    import cdr_cleaner.args_parser as parser
    import cdr_cleaner.clean_cdr_engine as clean_engine

    ext_parser = parser.get_argument_parser()
    ext_parser.add_argument(
        '--truncation_date',
        dest='cutoff_date',
        action='store',
        help=('Cutoff date for data based on <table_name>_date fields.  '
              'Should be in the form YYYY-MM-DD.'),
        required=True,
        type=validate_date_string,
    )
    ARGS = ext_parser.parse_args()

    if ARGS.list_queries:
        clean_engine.add_console_logging()
        query_list = clean_engine.get_query_list(ARGS.project_id,
                                                 ARGS.dataset_id,
                                                 ARGS.sandbox_dataset_id,
                                                 [(TruncateRdrData,)],
                                                 cutoff_date=ARGS.cutoff_date)
        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(ARGS.project_id,
                                   ARGS.dataset_id,
                                   ARGS.sandbox_dataset_id,
                                   ARGS.cutoff_date, [(TruncateRdrData,)],
                                   cutoff_date=ARGS.cutoff_date)
