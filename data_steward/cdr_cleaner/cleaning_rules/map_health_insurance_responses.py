"""
Clean insurance data from Spanish Basics and the HCAU follow-up.

For a short time the spanish basics survey did not display the answers to the insurance question (43528428) correctly.
The question was later asked again in the HCAU survey but with the correct branching logic.

For PIDs who took the original spanish basics insurance question (43528428), invalidate their answers.
FOR PIDs who took the both insurance questions, update the answer given with the HCAU survey to use the codes/ids used
in the basics survey)
"""
import logging
import os
from datetime import datetime

# Project imports
from google.cloud import bigquery
from gcloud.bq import BigQueryClient
import resources
from common import JINJA_ENV, OBSERVATION, RDR, PIPELINE_TABLES
from constants.cdr_cleaner import clean_cdr as cdr_consts
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule, query_spec_list

LOGGER = logging.getLogger(__name__)

ORIGINAL_OBSERVATION_SOURCE_CONCEPT_ID = 43528428
HCAU_OBSERVATION_SOURCE_CONCEPT_ID = 1384450

ISSUE_NUMBERS = ['DC826', 'DC2746']

INSURANCE_LOOKUP = 'insurance_lookup'
NEW_INSURANCE_ROWS = 'new_insurance_rows'
SANDBOXED_INSURANCE_ROWS = 'sandboxed_insurance_rows'
HEALTH_INSURANCE_PIDS = 'health_insurance_pids'

# Sandbox basics rows to be invalidated, and the hcau rows to be updated.
SANDBOX_CHANGES_QUERY = JINJA_ENV.from_string("""
CREATE OR REPLACE TABLE `{{project_id}}.{{sandbox_dataset_id}}.{{sandboxed_insurance_rows}}`
AS
SELECT 
    *
FROM 
    `{{project_id}}.{{dataset_id}}.observation` 
WHERE 
    observation_source_concept_id IN ({{ORIGINAL_OBSERVATION_SOURCE_CONCEPT_ID}},{{HCAU_OBSERVATION_SOURCE_CONCEPT_ID}})
    AND person_id IN (SELECT person_id FROM `{{project_id}}.{{pipeline_tables}}.{{health_insurance_pids}}`)
""")

# Invalidate where PIDs took the original insurance question.
UPDATE_INVALID_QUERY = JINJA_ENV.from_string("""
UPDATE 
    `{{project_id}}.{{dataset_id}}.observation` 
SET
    value_as_concept_id = 46237613,
    value_as_string = 'Invalid',
    value_source_concept_id = 46237613,
    value_source_value = 'Invalid'
WHERE 
    observation_source_concept_id IN ({{ORIGINAL_OBSERVATION_SOURCE_CONCEPT_ID}})
AND person_id IN (SELECT person_id FROM
    `{{project_id}}.{{pipeline_tables}}.{{health_insurance_pids}}`)
""")

# Create a temp table of the HCAU answers with the codes/ids replaced by codes/ids used in the basics survey.
CREATE_NEW_INSURANCE_ROWS = JINJA_ENV.from_string("""
CREATE OR REPLACE TABLE `{{project_id}}.{{sandbox_dataset_id}}.{{new_insurance_rows}}`
AS
SELECT
  *
FROM
(SELECT 
  observation_id,
  new_value_source_value AS value_as_string, 
  new_value_as_concept_id AS value_as_concept_id, 
  'HealthInsurance_InsuranceTypeUpdate' AS observation_source_value, 
  {{ORIGINAL_OBSERVATION_SOURCE_CONCEPT_ID}} AS observation_source_concept_id, 
  new_value_source_concept_id AS value_source_concept_id, 
  new_value_source_value AS value_source_value
FROM `{{project_id}}.{{dataset_id}}.observation` ob
JOIN (
  SELECT DISTINCT
    hcau_value_source_concept_id,
    source_c.concept_code AS new_value_source_value, 
    source_c.concept_id AS new_value_source_concept_id, 
    FIRST_VALUE(standard_c.concept_id) OVER (PARTITION BY source_c.concept_id ORDER BY c_r.relationship_id DESC)
    AS new_value_as_concept_id
  FROM 
    `{{project_id}}.{{sandbox_dataset_id}}.{{insurance_lookup}}`
  JOIN `{{project_id}}.{{dataset_id}}.concept` source_c ON (basics_value_source_concept_id=source_c.concept_id)
  JOIN `{{project_id}}.{{dataset_id}}.concept_relationship` c_r ON (source_c.concept_id=c_r.concept_id_1)
  JOIN `{{project_id}}.{{dataset_id}}.concept` standard_c ON (standard_c.concept_id=c_r.concept_id_2)
  WHERE source_c.vocabulary_id='PPI' 
  AND c_r.relationship_id LIKE 'Maps to%'  --prefers the 'maps to value', but will take 'maps to' if necessary --
)
ON ob.value_source_concept_id = hcau_value_source_concept_id 
WHERE observation_source_concept_id IN ({{HCAU_OBSERVATION_SOURCE_CONCEPT_ID}})
AND person_id IN (SELECT person_id FROM
     `{{project_id}}.{{pipeline_tables}}.{{health_insurance_pids}}`)
)
""")

# Where possible, update the basics rows with the data in the temp table(new_insurance_rows).
UPDATE_HCAU_ROWS = JINJA_ENV.from_string("""
UPDATE
    `{{project_id}}.{{dataset_id}}.observation` ob
SET
    value_as_concept_id = nir.value_as_concept_id,
    value_as_string = nir.value_as_string,
    value_source_concept_id = nir.value_source_concept_id,
    value_source_value = nir.value_source_value,
    observation_source_value = nir.observation_source_value,
    observation_source_concept_id = nir.observation_source_concept_id 
FROM  
    `{{project_id}}.{{sandbox_dataset_id}}.{{new_insurance_rows}}` nir 
WHERE 
    ob.observation_id = nir.observation_id    
""")

### Validation Query ####
COUNTS_QUERY = JINJA_ENV.from_string("""
SELECT 
    COUNT(*) as count_valid
FROM 
    `{{project_id}}.{{dataset_id}}.observation` 
WHERE 
    observation_source_concept_id IN ({{ORIGINAL_OBSERVATION_SOURCE_CONCEPT_ID}})
AND person_id IN (SELECT person_id FROM
     `{{project_id}}.{{pipeline_tables}}.{{health_insurance_pids}}`
AND value_source_value = 'Invalid'
""")


class MapHealthInsuranceResponses(BaseCleaningRule):

    def __init__(self,
                 project_id,
                 dataset_id,
                 sandbox_dataset_id,
                 table_namer=None):
        """
        Initialize the class.
        Set the issue numbers, description and affected datasets. As other tickets may affect
        this SQL, append them to the list of Jira Issues.
        DO NOT REMOVE ORIGINAL JIRA ISSUE NUMBERS!
        """
        desc = (
            'Clean insurance data from Spanish Basics and the HCAU follow-up.')

        super().__init__(issue_numbers=ISSUE_NUMBERS,
                         description=desc,
                         affected_tables=[OBSERVATION],
                         affected_datasets=[RDR],
                         project_id=project_id,
                         dataset_id=dataset_id,
                         sandbox_dataset_id=sandbox_dataset_id,
                         table_namer=table_namer)

        self.counts_query = COUNTS_QUERY.render(
            project_id=self.project_id,
            dataset_id=self.dataset_id,
            ORIGINAL_OBSERVATION_SOURCE_CONCEPT_ID=
            ORIGINAL_OBSERVATION_SOURCE_CONCEPT_ID,
            pipeline_tables=PIPELINE_TABLES,
            health_insurance_pids=HEALTH_INSURANCE_PIDS)

    def setup_rule(self, client, *args, **keyword_args):
        """
        Run required steps for validation setup

        Method to run to setup validation on cleaning rules that will be updating or deleting the values.
        For example:
        if your class updates all the datetime fields you should be implementing the
        logic to get the initial list of values which adhere to a condition we are looking for.

        if your class deletes a subset of rows in the tables you should be implementing
        the logic to get the row counts of the tables prior to applying cleaning rule

        """

        table_path = os.path.join(resources.resource_files_path,
                                  f"{INSURANCE_LOOKUP}.csv")
        with open(table_path, 'rb') as csv_file:
            schema_list = client.get_table_schema(INSURANCE_LOOKUP)
            table_id = f'{self.project_id}.{self.sandbox_dataset_id}.{INSURANCE_LOOKUP}'
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
        queries = []

        sandbox_query = dict()
        sandbox_query[cdr_consts.QUERY] = SANDBOX_CHANGES_QUERY.render(
            project_id=self.project_id,
            dataset_id=self.dataset_id,
            sandbox_dataset_id=self.sandbox_dataset_id,
            ORIGINAL_OBSERVATION_SOURCE_CONCEPT_ID=
            ORIGINAL_OBSERVATION_SOURCE_CONCEPT_ID,
            HCAU_OBSERVATION_SOURCE_CONCEPT_ID=
            HCAU_OBSERVATION_SOURCE_CONCEPT_ID,
            sandboxed_insurance_rows=self.sandbox_table_for(
                SANDBOXED_INSURANCE_ROWS),
            pipeline_tables=PIPELINE_TABLES,
            health_insurance_pids=HEALTH_INSURANCE_PIDS)
        queries.append(sandbox_query)

        invalidate_query = dict()
        invalidate_query[cdr_consts.QUERY] = UPDATE_INVALID_QUERY.render(
            project_id=self.project_id,
            dataset_id=self.dataset_id,
            ORIGINAL_OBSERVATION_SOURCE_CONCEPT_ID=
            ORIGINAL_OBSERVATION_SOURCE_CONCEPT_ID,
            pipeline_tables=PIPELINE_TABLES,
            health_insurance_pids=HEALTH_INSURANCE_PIDS)
        queries.append(invalidate_query)

        new_insurance_rows_query = dict()
        new_insurance_rows_query[
            cdr_consts.QUERY] = CREATE_NEW_INSURANCE_ROWS.render(
                project_id=self.project_id,
                dataset_id=self.dataset_id,
                sandbox_dataset_id=self.sandbox_dataset_id,
                new_insurance_rows=self.sandbox_table_for(NEW_INSURANCE_ROWS),
                insurance_lookup=INSURANCE_LOOKUP,
                ORIGINAL_OBSERVATION_SOURCE_CONCEPT_ID=
                ORIGINAL_OBSERVATION_SOURCE_CONCEPT_ID,
                HCAU_OBSERVATION_SOURCE_CONCEPT_ID=
                HCAU_OBSERVATION_SOURCE_CONCEPT_ID,
                pipeline_tables=PIPELINE_TABLES,
                health_insurance_pids=HEALTH_INSURANCE_PIDS)
        queries.append(new_insurance_rows_query)

        update_basics_query = dict()
        update_basics_query[cdr_consts.QUERY] = UPDATE_HCAU_ROWS.render(
            project_id=self.project_id,
            dataset_id=self.dataset_id,
            ORIGINAL_OBSERVATION_SOURCE_CONCEPT_ID=
            ORIGINAL_OBSERVATION_SOURCE_CONCEPT_ID,
            HCAU_OBSERVATION_SOURCE_CONCEPT_ID=
            HCAU_OBSERVATION_SOURCE_CONCEPT_ID,
            new_insurance_rows=self.sandbox_table_for(NEW_INSURANCE_ROWS),
            sandbox_dataset_id=self.sandbox_dataset_id,
            pipeline_tables=PIPELINE_TABLES,
            health_insurance_pids=HEALTH_INSURANCE_PIDS)
        queries.append(update_basics_query)

        return queries

    def get_sandbox_tablenames(self) -> list:
        return [
            self.sandbox_table_for(SANDBOXED_INSURANCE_ROWS),
            self.sandbox_table_for(NEW_INSURANCE_ROWS)
        ]

    def setup_validation(self, client) -> None:
        """
        Run required steps for validation setup
        """
        self.init_counts = self._get_counts(client)

        if self.init_counts.get('count_valid') == 0:
            raise RuntimeError(
                'NO DATA TO UPDATE OR INVALIDATE IN OBSERVATION TABLE ')

    def validate_rule(self, client) -> None:
        """
        Validates the cleaning rule which deletes or updates the data from the tables
        """
        clean_counts = self._get_counts(client)

        if clean_counts.get('count_valid') == 0:
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
                count_valid = item.get('count_valid', 0)

        return {'count_valid': count_valid}


if __name__ == '__main__':
    import cdr_cleaner.args_parser as parser
    import cdr_cleaner.clean_cdr_engine as clean_engine

    ARGS = parser.default_parse_args()

    if ARGS.list_queries:
        clean_engine.add_console_logging()
        query_list = clean_engine.get_query_list(
            ARGS.project_id, ARGS.dataset_id, ARGS.sandbox_dataset_id,
            [(MapHealthInsuranceResponses,)])
        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(ARGS.project_id, ARGS.dataset_id,
                                   ARGS.sandbox_dataset_id,
                                   [(MapHealthInsuranceResponses,)])
