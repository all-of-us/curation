"""
Enforces a data cutoff date for PPI data.

Original Issue: DC-1445

Intent is to enforce the data cutoff date for PPI data in all CDM tables excluding the person table by sandboxing and
    removing any records that persist after the data cutoff date.
"""

# Python imports
import logging
from datetime import datetime

# Project imports
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule
from common import JINJA_ENV, AOU_REQUIRED
from constants import bq_utils as bq_consts
from utils import pipeline_logging
from resources import fields_for, validate_date_string
import constants.cdr_cleaner.clean_cdr as cdr_consts

LOGGER = logging.getLogger(__name__)

SANDBOX_QUERY = JINJA_ENV.from_string("""
CREATE OR REPLACE TABLE `{{project_id}}.{{sandbox_id}}.{{intermediary_table}}` AS (
SELECT * FROM `{{project_id}}.{{dataset_id}}.{{cdm_table}}`
WHERE
    (GREATEST({{date_fields}}) > DATE("{{cutoff_date}}"))
{% if datetime_fields != '' %}
    AND (GREATEST({{datetime_fields}}) > TIMESTAMP("{{cutoff_date}}"))
{% endif %}
)
""")

DATE_CUTOFF_QUERY = JINJA_ENV.from_string("""
SELECT * FROM `{{project_id}}.{{dataset_id}}.{{cdm_table}}` cdm
EXCEPT DISTINCT
SELECT * FROM `{{project_id}}.{{sandbox_id}}.{{intermediary_table}}`
""")


class EhrSubmissionDataCutoff(BaseCleaningRule):
    """
    All rows of data in the RDR ETL with dates after the cutoff date should be sandboxed and dropped
    """

    def __init__(self,
                 project_id,
                 dataset_id,
                 sandbox_dataset_id,
                 cutoff_date=None):
        """
        Initialize the class with proper information.

        Set the issue numbers, description and affected datasets. As other tickets may affect
        this SQL, append them to the list of Jira Issues.
        DO NOT REMOVE ORIGINAL JIRA ISSUE NUMBERS!

        :params: cutoff_date: the last date that should be included in the
            dataset
        """
        try:
            # set to provided date string if the date string is valid
            self.cutoff_date = validate_date_string(cutoff_date)
        except (TypeError, ValueError):
            # otherwise, default to using today's date as the date string
            self.cutoff_date = str(datetime.now().date())

        desc = (f'All rows of data in the RDR ETL with dates after '
                f'{self.cutoff_date} will be sandboxed and dropped.')

        super().__init__(issue_numbers=['DC1445'],
                         description=desc,
                         affected_datasets=[cdr_consts.UNIONED],
                         project_id=project_id,
                         dataset_id=dataset_id,
                         sandbox_dataset_id=sandbox_dataset_id)

    def get_affected_tables(self):
        """
        This method gets all the tables that are affected by this cleaning rule which are all the CDM tables
            except for the person table. The birth date field in the person table will be cleaned in another
            cleaning rule where all participants under the age of 18 will be dropped. Ignoring this table will
            optimize this cleaning rule's runtime.

        :return: list of affected tables
        """
        tables = []
        for table in AOU_REQUIRED:

            # skips the person table
            if table == 'person':
                continue

            # appends all CDM tables except for the person table
            else:
                tables.append(table)
        return tables

    def get_query_specs(self, *args, **keyword_args):
        """
        Return a list of dictionary query specifications.

        :return:  A list of dictionaries. Each dictionary contains a single query
            and a specification for how to execute that query. The specifications
            are optional but the query is required.
        """

        queries_list = []
        sandbox_queries_list = []

        for table in self.get_affected_tables():
            # gets all fields from the affected table
            fields = fields_for(table)

            date_fields = []
            datetime_fields = []

            for field in fields:
                # appends only the date columns to the date_fields list
                if field['type'] in ['date']:
                    date_fields.append(
                        f'COALESCE({field["name"]}, DATE("1900-01-01"))')

                # appends only the datetime columns to the datetime_fields list
                if field['type'] in ['timestamp']:
                    datetime_fields.append(
                        f'COALESCE({field["name"]}, TIMESTAMP("1900-01-01"))')

            # will render the queries only if a CDM table contains a date or datetime field
            # will ignore the CDM tables that do not have a date or datetime field
            if date_fields or datetime_fields:
                sandbox_query = {
                    cdr_consts.QUERY:
                        SANDBOX_QUERY.render(
                            project_id=self.project_id,
                            sandbox_id=self.sandbox_dataset_id,
                            intermediary_table=self.sandbox_table_for(table),
                            dataset_id=self.dataset_id,
                            cdm_table=table,
                            date_fields=(", ".join(date_fields)),
                            datetime_fields=(", ".join(datetime_fields)),
                            cutoff_date=self.cutoff_date),
                }

                sandbox_queries_list.append(sandbox_query)

                date_cutoff_query = {
                    cdr_consts.QUERY:
                        DATE_CUTOFF_QUERY.render(
                            project_id=self.project_id,
                            dataset_id=self.dataset_id,
                            cdm_table=table,
                            sandbox_id=self.sandbox_dataset_id,
                            intermediary_table=self.sandbox_table_for(table)),
                    cdr_consts.DESTINATION_TABLE:
                        table,
                    cdr_consts.DESTINATION_DATASET:
                        self.dataset_id,
                    cdr_consts.DISPOSITION:
                        bq_consts.WRITE_TRUNCATE
                }

                queries_list.append(date_cutoff_query)

        return sandbox_queries_list + queries_list

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
            sandbox_tables.append(self.sandbox_table_for(table))
        return sandbox_tables


if __name__ == '__main__':
    import cdr_cleaner.args_parser as parser
    import cdr_cleaner.clean_cdr_engine as clean_engine

    ext_parser = parser.get_argument_parser()
    ext_parser.add_argument(
        '-c',
        '--cutoff_date',
        dest='cutoff_date',
        action='store',
        help=
        ('Cutoff date for data based on <table_name>_date and <table_name>_datetime fields.  '
         'Should be in the form YYYY-MM-DD.'),
        required=True,
        type=validate_date_string,
    )

    ARGS = ext_parser.parse_args()
    pipeline_logging.configure(level=logging.DEBUG, add_console_handler=True)

    if ARGS.list_queries:
        clean_engine.add_console_logging()
        query_list = clean_engine.get_query_list(ARGS.project_id,
                                                 ARGS.dataset_id,
                                                 ARGS.sandbox_dataset_id,
                                                 [(EhrSubmissionDataCutoff,)],
                                                 cutoff_date=ARGS.cutoff_date)
        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(ARGS.project_id,
                                   ARGS.dataset_id,
                                   ARGS.sandbox_dataset_id,
                                   ARGS.cutoff_date,
                                   [(EhrSubmissionDataCutoff,)],
                                   cutoff_date=ARGS.cutoff_date)
