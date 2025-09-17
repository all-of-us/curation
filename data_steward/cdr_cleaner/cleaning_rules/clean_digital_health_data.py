"""
Remove wearables (fitbit) data for participants without consent,
sandbox and delete records for such participants

Original Issue: DC-1910
"""

# Python imports
import logging

# Project imports
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule
from constants.cdr_cleaner import clean_cdr as cdr_consts
from common import JINJA_ENV, FITBIT_TABLES, PIPELINE_TABLES, DIGITAL_HEALTH_SHARING_STATUS, PS_API_VALUES
from utils import pipeline_logging
from google.cloud.bigquery import Table

LOGGER = logging.getLogger(__name__)

CLEAN_QUERY = JINJA_ENV.from_string("""
{{query_type}}
FROM `{{fq_fitbit_table}}`
WHERE person_id NOT IN (
  SELECT person_id
  FROM `{{fq_digital_health_status_table}}`
  WHERE wearable = 'fitbit'
  AND status = 'YES'
)
""")

SANDBOX_QUERY = JINJA_ENV.from_string("""
CREATE TABLE `{{fq_sandbox_table}}` AS
SELECT *
""")

POPULATE_DIGITAL_HEALTH_SHARING_STATUS_TABLE_QUERY = JINJA_ENV.from_string("""
INSERT INTO `{{project}}.{{dataset}}.{{digital_health_sharing_status}}` (
  person_id,
  wearable,
  status,
  authored_time,
  history
)
WITH deduped AS (
  SELECT DISTINCT
    person_id,
    wearable,
    status,
    authored_time,
    suspension_status,
    withdrawal_status
  FROM `{{project}}.{{rdr_dataset}}.{{ps_api_values}}`
),
filtered AS (
  SELECT *
  FROM deduped
  WHERE suspension_status = 'NOT_SUSPENDED'
    AND withdrawal_status = 'NOT_WITHDRAWN'
),
ranked AS (
  SELECT
    f.*,
    ROW_NUMBER() OVER (PARTITION BY person_id ORDER BY authored_time DESC) AS rn
  FROM filtered f
),
latest AS (
  SELECT
    person_id,
    wearable,
    status,
    authored_time
  FROM ranked
  WHERE rn = 1
),
older AS (
  SELECT
    id,
    ARRAY_AGG(STRUCT(status AS status, authored_time AS authored_time) ORDER BY authored_time DESC) AS history
  FROM ranked
  WHERE rn > 1
  GROUP BY id
)
SELECT
  l.person_id,
  l.wearable,
  l.status,
  l.authored_time,
  COALESCE(o.history, []) AS history
FROM latest l
LEFT JOIN older o
USING (person_id)
""")


class CleanDigitalHealthStatus(BaseCleaningRule):

    def __init__(self,
                 project_id,
                 dataset_id,
                 rdr_dataset_id,
                 sandbox_dataset_id,
                 table_namer=None,
                 api_project_id=None):
        """
        Initialize the class with proper information.

        Set the issue numbers, description and affected datasets. As other tickets may affect
        this SQL, append them to the list of Jira Issues.
        DO NOT REMOVE ORIGINAL JIRA ISSUE NUMBERS!
        """
        desc = ('Sandbox and remove wearables data for participants without '
                'consent in digitalHealthSharingStatus')

        if not api_project_id:
            raise TypeError("`api_project_id` cannot be empty")

        super().__init__(issue_numbers=['DC1910'],
                         description=desc,
                         affected_datasets=[cdr_consts.FITBIT],
                         affected_tables=FITBIT_TABLES,
                         project_id=project_id,
                         dataset_id=dataset_id,
                         sandbox_dataset_id=sandbox_dataset_id,
                         table_namer=table_namer)
        self.api_project_id = api_project_id
        self.rdr_dataset_id = rdr_dataset_id

    def setup_rule(self, client, *args, **keyword_args):
        """
        Load required resources prior to executing cleaning rule queries.

        Method to run data upload options before executing the first cleaning
        rule of a class.  For example, if your class requires loading a static
        table, that load operation should be defined here.  It SHOULD NOT BE
        defined as part of get_query_specs().
        :param client: a BigQueryClient
        :return:
        """

        # Insert query
        q = POPULATE_DIGITAL_HEALTH_SHARING_STATUS_TABLE_QUERY.render(
            project=self.project_id,
            dataset=self.dataset_id,
            rdr_dataset=self.rdr_dataset_id,
            digital_health_sharing_status=DIGITAL_HEALTH_SHARING_STATUS,
            ps_api_values=PS_API_VALUES)
        query_job = client.query(q)
        query_job.result()
        if query_job.exception():
            LOGGER.error(
                f"New `{DIGITAL_HEALTH_SHARING_STATUS}` table was not populated"
            )

        LOGGER.info(
            f"New `{DIGITAL_HEALTH_SHARING_STATUS}` table was populated")

        # Snapshot DIGITAL_HEALTH_SHARING_STATUS table for current CDR
        client.copy_table(
            f'{self.project_id}.{PIPELINE_TABLES}.{DIGITAL_HEALTH_SHARING_STATUS}',
            f'{self.project_id}.{self.sandbox_dataset_id}.{DIGITAL_HEALTH_SHARING_STATUS}'
        )

    def get_query_specs(self, *args, **keyword_args):
        """
        Interface to return a list of query dictionaries.

        :returns:  a list of query dictionaries.  Each dictionary specifies
            the query to execute and how to execute.  The dictionaries are
            stored in list order and returned in list order to maintain
            an ordering.
        """
        sandbox_queries = []
        delete_queries = []
        for table in self.affected_tables:
            fq_fitbit_table = f'{self.project_id}.{self.dataset_id}.{table}'
            fq_digital_health_status_table = f'{self.project_id}.{PIPELINE_TABLES}.{DIGITAL_HEALTH_SHARING_STATUS}'
            fq_sandbox_table = f'{self.project_id}.{self.sandbox_dataset_id}.{self.sandbox_table_for(table)}'

            sandbox_query = dict()
            sandbox_template = SANDBOX_QUERY.render(
                fq_sandbox_table=fq_sandbox_table)
            sandbox_query[cdr_consts.QUERY] = CLEAN_QUERY.render(
                query_type=sandbox_template,
                fq_fitbit_table=fq_fitbit_table,
                fq_digital_health_status_table=fq_digital_health_status_table)
            sandbox_queries.append(sandbox_query)

            delete_query = dict()
            delete_query[cdr_consts.QUERY] = CLEAN_QUERY.render(
                query_type='DELETE',
                fq_fitbit_table=fq_fitbit_table,
                fq_digital_health_status_table=fq_digital_health_status_table)
            delete_queries.append(delete_query)

        return sandbox_queries + delete_queries

    def setup_validation(self, client, *args, **keyword_args):
        """
        Run required steps for validation setup

        Method to run to setup validation on cleaning rules that will be updating or deleting the values.
        For example:
        if your class updates all the datetime fields you should be implementing the
        logic to get the initial list of values which adhere to a condition we are looking for.

        if your class deletes a subset of rows in the tables you should be implementing
        the logic to get the row counts of the tables prior to applying cleaning rule

        """
        pass

    def validate_rule(self, client, *args, **keyword_args):
        """
        Validates the cleaning rule which deletes or updates the data from the tables

        Method to run validation on cleaning rules that will be updating the values.
        For example:
        if your class updates all the datetime fields you should be implementing the
        validation that checks if the date time values that needs to be updated no
        longer exists in the table.

        if your class deletes a subset of rows in the tables you should be implementing
        the validation that checks if the count of final final row counts + deleted rows
        should equals to initial row counts of the affected tables.

        Raises RunTimeError if the validation fails.
        """

        pass

    def get_sandbox_tablenames(self):
        return [self.sandbox_table_for(name) for name in self.affected_tables]


if __name__ == '__main__':
    import cdr_cleaner.args_parser as parser
    import cdr_cleaner.clean_cdr_engine as clean_engine

    ext_parser = parser.get_argument_parser()
    ext_parser.add_argument(
        '-q',
        '--api_project_id',
        action='store',
        dest='api_project_id',
        help='Identifies the RDR project for participant summary API',
        required=True)

    ARGS = ext_parser.parse_args()

    pipeline_logging.configure(level=logging.DEBUG, add_console_handler=True)

    if ARGS.list_queries:
        clean_engine.add_console_logging()
        query_list = clean_engine.get_query_list(
            ARGS.project_id,
            ARGS.dataset_id,
            ARGS.rdr_dataset_id,
            ARGS.sandbox_dataset_id,
            [(CleanDigitalHealthStatus,)],
            api_project_id=ARGS.api_project_id,
        )

        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(
            ARGS.project_id,
            ARGS.dataset_id,
            ARGS.rdr_dataset_id,
            ARGS.sandbox_dataset_id,
            [(CleanDigitalHealthStatus,)],
            api_project_id=ARGS.api_project_id,
        )
