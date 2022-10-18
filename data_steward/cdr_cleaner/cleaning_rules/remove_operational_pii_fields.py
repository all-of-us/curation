# coding=utf-8
"""
Removes operational Pii fields from rdr export.
Some new operational fields exists, that were not blacklisted in the RDR export. These rows needs to be dropped in the
RDR load process so they do not make it to CDR. These do not have concept_id maps.
The supplemental operational_pii_fields.csv shows all present PPI codes without a mapped concepts,
indicating which should be dropped in the “drop_value” column
Jira issues = DC-500, DC-831
"""
# Python imports
import logging
import os
from datetime import datetime

# Project imports
from google.cloud import bigquery
from gcloud.bq import BigQueryClient
import resources
import constants.cdr_cleaner.clean_cdr as cdr_consts
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule, query_spec_list
from common import JINJA_ENV, OBSERVATION

LOGGER = logging.getLogger(__name__)

OPERATIONAL_PII_FIELDS_TABLE = '_operational_pii_fields'

JIRA_ISSUE_NUMBERS = ['DC500', 'DC831']

SANDBOX_OPERATIONAL_PII_FIELDS = JINJA_ENV.from_string("""
CREATE OR REPLACE TABLE
    `{{project_id}}.{{sandbox_dataset_id}}.{{sandbox_table}}` AS (
  SELECT
    *
    FROM
    `{{project_id}}.{{dataset_id}}.observation`
  WHERE
    observation_id IN (
    SELECT
      observation_id
    FROM
      `{{project_id}}.{{sandbox_dataset_id}}.{{operational_pii_fields_table}}` as pii
    JOIN
      `{{project_id}}.{{dataset_id}}.observation` as ob
    USING
        (observation_source_value)
    WHERE
      drop_value=TRUE)
      )
""")

DELETE_QUERY = JINJA_ENV.from_string("""
DELETE
FROM
    `{{project_id}}.{{dataset_id}}.observation`
WHERE
observation_id
IN (SELECT
    observation_id
    FROM `{{project_id}}.{{sandbox_dataset_id}}.{{sandbox_table}}` )
""")

### Validation Query ####
COUNTS_QUERY = JINJA_ENV.from_string("""
SELECT
    COUNT(*) AS total_count
FROM
    `{{project_id}}.{{dataset_id}}.observation`
WHERE
observation_id
IN (SELECT
    observation_id
    FROM `{{project}}.{{sandbox_dataset}}.{{sandbox_table}}` )
""")


class RemoveOperationalPiiFields(BaseCleaningRule):

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
        desc = 'Removes rows with the observation_source_values listed in the lookup table.'

        super().__init__(issue_numbers=JIRA_ISSUE_NUMBERS,
                         description=desc,
                         affected_datasets=[cdr_consts.RDR],
                         affected_tables=[OBSERVATION],
                         project_id=project_id,
                         dataset_id=dataset_id,
                         sandbox_dataset_id=sandbox_dataset_id,
                         table_namer=table_namer)

        self.counts_query = COUNTS_QUERY.render(
            project_id=self.project_id,
            dataset_id=self.dataset_id,
            sandbox_dataset_id=self.sandbox_dataset_id,
            sandbox_table=self.get_sandbox_tablenames()[0])

    def get_sandbox_tablenames(self) -> list:
        return [self.sandbox_table_for(table) for table in self.affected_tables]

    def setup_rule(self, client: BigQueryClient, *args, **keyword_args) -> None:
        """
        Load the lookup table values into the sandbox.
        The following queries will use the lookup table as part of the execution.
        Loads the operational pii fields from resource_files/_operational_pii_fields.csv
        into project_id.sandbox_dataset_id.operational_pii_fields in BQ
        """
        table_path = os.path.join(resources.resource_files_path,
                                  f"{OPERATIONAL_PII_FIELDS_TABLE}.csv")
        with open(table_path, 'rb') as csv_file:
            schema_list = client.get_table_schema(OPERATIONAL_PII_FIELDS_TABLE)
            table_id = f'{self.project_id}.{self.sandbox_dataset_id}.{OPERATIONAL_PII_FIELDS_TABLE}'
            job_config = bigquery.LoadJobConfig(
                schema=schema_list,
                skip_leading_rows=1,
                source_format=bigquery.SourceFormat.CSV,
                field_delimiter=',',
                allow_quoted_newlines=True,
                quote_character='"',
                write_disposition=bigquery.job.WriteDisposition.WRITE_TRUNCATE)

            # job_id defined to the second precision
            job_id = f'{self.dataset_id}_{self.__class__.__name__}_{datetime.now().strftime("%Y%m%d_%H%M%S")}'
            LOGGER.info(f'Loading `{table_id}`')
            try:
                load_job = client.load_table_from_file(
                    csv_file, table_id, job_config=job_config,
                    job_id=job_id)  # Make an API request.

                load_job.result()  # Waits for the job to complete.
            except (ValueError, TypeError) as exc:
                LOGGER.info(
                    'Something went wrong and the table did not load correctly')
                raise exc
            else:
                LOGGER.info(f'Loading of `{table_id}` completed.')

    def get_query_specs(self, *args, **keyword_args) -> query_spec_list:
        """
        Return a list of dictionary query specifications.
        :return:  A list of dictionaries. Each dictionary contains a single query
            and a specification for how to execute that query. The specifications
            are optional but the query is required.
        """
        queries_list = []

        sandbox_query = dict()
        sandbox_query[cdr_consts.QUERY] = SANDBOX_OPERATIONAL_PII_FIELDS.render(
            project_id=self.project_id,
            dataset_id=self.dataset_id,
            sandbox_dataset_id=self.sandbox_dataset_id,
            operational_pii_fields_table=OPERATIONAL_PII_FIELDS_TABLE,
            sandbox_table=self.get_sandbox_tablenames()[0])
        queries_list.append(sandbox_query)

        delete_query = dict()
        delete_query[cdr_consts.QUERY] = DELETE_QUERY.render(
            project_id=self.project_id,
            dataset_id=self.dataset_id,
            sandbox_dataset_id=self.sandbox_dataset_id,
            sandbox_table=self.get_sandbox_tablenames()[0])
        queries_list.append(delete_query)

        return queries_list

    def setup_validation(self, client: BigQueryClient) -> None:
        """
        Run required steps for validation setup
        """
        init_counts = self._get_counts(client)

        if init_counts.get('total_count') == 0:
            raise RuntimeError('NO DATA TO REMOVE IN OBSERVATION TABLE ')

    def validate_rule(self, client: BigQueryClient) -> None:
        """
        Validates the cleaning rule which deletes or updates the data from the tables
        """
        clean_counts = self._get_counts(client)

        if clean_counts.get('total_count') != 0:
            raise RuntimeError(
                f'{self.__class__.__name__} did not clean as expected.\n'
                f'Found data in: {clean_counts}')

    def _get_counts(self, client: BigQueryClient) -> dict:
        """
        Counts query, used for job validation.
        """
        job = client.query(self.counts_query)
        response = job.result()

        errors = []
        if job.exception():
            errors.append(job.exception())
            LOGGER.error(f"FAILURE:  {job.exception()}\n"
                         f"Problem executing query:\n{self.counts_query}")
        else:
            for item in response:
                total_count = item.get('total_count', 0)

        return {'total_count': total_count}


if __name__ == '__main__':
    import cdr_cleaner.args_parser as parser
    import cdr_cleaner.clean_cdr_engine as clean_engine

    ARGS = parser.parse_args()

    if ARGS.list_queries:
        clean_engine.add_console_logging()
        query_list = clean_engine.get_query_list(
            ARGS.project_id, ARGS.dataset_id, ARGS.sandbox_dataset_id,
            [(RemoveOperationalPiiFields,)])
        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(ARGS.project_id, ARGS.dataset_id,
                                   ARGS.sandbox_dataset_id,
                                   [(RemoveOperationalPiiFields,)])
