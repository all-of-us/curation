"""
All data for any participant less than 18 years old at the time of consent needs to be dropped from
 all the tables. For RDR exports, they may be cleaned with the current date or a truncation date may be set.

Original Issues: DC-1724, DC-2260
"""

# Python imports
import logging
from datetime import datetime

# Third party imports
from google.cloud.exceptions import GoogleCloudError

# Project imports
import common
import constants.cdr_cleaner.clean_cdr as cdr_consts
from resources import get_person_id_tables, validate_date_string
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule
from cdr_cleaner.clean_cdr_utils import get_tables_in_dataset

LOGGER = logging.getLogger(__name__)

UNDER18_PARTICIPANTS_LOOKUP_TABLE = '_under18_participants'

AFFECTED_TABLES = [table for table in get_person_id_tables(common.CATI_TABLES)]

PARTICIPANTS_UNDER_18_AT_CONSENT_QUERY = common.JINJA_ENV.from_string("""
  CREATE OR REPLACE TABLE `{{project}}.{{sandbox_dataset}}.{{under18_participant_lookup_table}}` AS (
  SELECT person_id
  FROM `{{project}}.{{dataset}}.observation`
  JOIN `{{project}}.{{dataset}}.person` 
  USING (person_id)
  WHERE (observation_source_concept_id = 1585482 OR observation_concept_id = 1585482)
  AND FLOOR(CAST(FORMAT_DATE('%Y.%m%d', observation_date) AS FLOAT64) - CAST(FORMAT_DATE('%Y.%m%d', DATE(birth_datetime)) AS FLOAT64)) < 18
  )
""")

SANDBOX_ROWS = common.JINJA_ENV.from_string("""
CREATE OR REPLACE TABLE
  `{{project}}.{{sandbox_dataset}}.{{sandbox_table}}` AS (
  SELECT
    *
  FROM
    `{{project}}.{{dataset}}.{{domain_table}}` d
  WHERE
    person_id IN (
    SELECT
      person_id
    FROM
      `{{project}}.{{sandbox_dataset}}.{{under18_participant_lookup_table}}`)
  )
""")

DROP_ROWS = common.JINJA_ENV.from_string("""
DELETE
FROM
  `{{project}}.{{dataset}}.{{domain_table}}`
WHERE
  person_id IN (
  SELECT
    person_id
  FROM
    `{{project}}.{{sandbox_dataset}}.{{under18_participant_lookup_table}}`)  
""")


class RemoveParticipantsUnder18Years(BaseCleaningRule):
    """
    All EHR data associated with a participant who was younger than 18 years old at consent 
    is to be sandboxed and dropped from the CDR.
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
        desc = (
            'All EHR data associated with a participant who was younger than 18 years old at consent '
            'is to be sandboxed and dropped from the CDR.')

        super().__init__(
            issue_numbers=['DC1724', 'DC2260', 'DC2632'],
            description=desc,
            affected_datasets=[cdr_consts.RDR],
            affected_tables=AFFECTED_TABLES,
            project_id=project_id,
            dataset_id=dataset_id,
            sandbox_dataset_id=sandbox_dataset_id,
        )

    def get_query_specs(self):
        """
        Return a list of dictionary query specifications.

        :return:  A list of dictionaries. Each dictionary contains a single query
            and a specification for how to execute that query. The specifications
            are optional but the query is required.
        """
        lookup_queries = []
        sandbox_queries = []
        drop_queries = []
        unconsented_lookup_query = {
            cdr_consts.QUERY:
                PARTICIPANTS_UNDER_18_AT_CONSENT_QUERY.render(
                    project=self.project_id,
                    dataset=self.dataset_id,
                    sandbox_dataset=self.sandbox_dataset_id,
                    export_date=self.cutoff_date,
                    under18_participant_lookup_table=
                    UNDER18_PARTICIPANTS_LOOKUP_TABLE)
        }
        lookup_queries.append(unconsented_lookup_query)

        for table in self.affected_tables:

            sandbox_query = {
                cdr_consts.QUERY:
                    SANDBOX_ROWS.render(
                        project=self.project_id,
                        dataset=self.dataset_id,
                        sandbox_dataset=self.sandbox_dataset_id,
                        sandbox_table=self.sandbox_table_for(table),
                        domain_table=table,
                        under18_participant_lookup_table=
                        UNDER18_PARTICIPANTS_LOOKUP_TABLE)
            }

            sandbox_queries.append(sandbox_query)

            drop_query = {
                cdr_consts.QUERY:
                    DROP_ROWS.render(project=self.project_id,
                                     dataset=self.dataset_id,
                                     sandbox_dataset=self.sandbox_dataset_id,
                                     domain_table=table,
                                     under18_participant_lookup_table=
                                     UNDER18_PARTICIPANTS_LOOKUP_TABLE)
            }

            drop_queries.append(drop_query)

        return lookup_queries + sandbox_queries + drop_queries

    def setup_rule(self, client):
        """
        Function to run any data upload options before executing a query.
        """
        try:
            self.affected_tables = get_tables_in_dataset(
                client, self.project_id, self.dataset_id, self.affected_tables)
        except GoogleCloudError as error:
            LOGGER.error(error)
            raise

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
        Returns an iterable of sandbox table names
        """
        return [self.sandbox_table_for(table) for table in self.affected_tables]


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

    ext_parser = parser.get_argument_parser()
    ARGS = ext_parser.parse_args()

    if ARGS.list_queries:
        clean_engine.add_console_logging()
        query_list = clean_engine.get_query_list(
            ARGS.project_id, ARGS.dataset_id, ARGS.sandbox_dataset_id,
            [(RemoveParticipantsUnder18Years,)], ARGS.cutoff_date)
        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(ARGS.project_id, ARGS.dataset_id,
                                   ARGS.sandbox_dataset_id,
                                   [(RemoveParticipantsUnder18Years,)],
                                   ARGS.cutoff_date)
