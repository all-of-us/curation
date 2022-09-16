"""
Update the smoking question rows with new source and standard ids.

The mapping of several smoking, question and answer pairs in the lifestyle survey
were written abnormally in REDCap. The irregular branching logic was used by the survey
implementers as well as Odysseus when creating the vocabulary. The smoking rows need
to have their source and standard concept_ids remapped to ensure clear and meaningful data.

This rule should be reviewed after any change in Athena to the concepts in smoking_lookup.csv.
Post-coordination of these terms could impact how this rule is applied.

This rule generates corrected rows and deletes incorrect rows.

Original issues: AC-77  , DC-806
"""
import logging
import os
from datetime import datetime
from typing import Dict

from google.cloud import bigquery
from gcloud.bq import BigQueryClient
import resources
from constants.cdr_cleaner import clean_cdr as cdr_consts
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule, query_spec_list
from cdr_cleaner.cleaning_rules.convert_pre_post_coordinated_concepts import (
    ConvertPrePostCoordinatedConcepts)
from common import JINJA_ENV, OBSERVATION

LOGGER = logging.getLogger(__name__)

SMOKING_LOOKUP_TABLE = 'smoking_lookup'
NEW_SMOKING_ROWS = 'new_smoking_rows'

JIRA_ISSUE_NUMBERS = ['AC77', 'DC806']

SANDBOX_CREATE_QUERY = JINJA_ENV.from_string("""
CREATE TABLE `{{project_id}}.{{sandbox_dataset_id}}.{{new_smoking_rows}}`
AS
SELECT
    observation_id,
    person_id,
    observation_concept_id,
    observation_date,
    observation_datetime,
    observation_type_concept_id,
    value_as_number,
    value_as_string,
    value_as_concept_id,
    qualifier_concept_id,
    unit_concept_id,
    provider_id,
    visit_occurrence_id,
    visit_detail_id,
    observation_source_value,
    observation_source_concept_id,
    unit_source_value,
    qualifier_source_value,
    value_source_concept_id,
    value_source_value,
    questionnaire_response_id
FROM
(SELECT
    observation_id,
    person_id,
    new_observation_concept_id as observation_concept_id,
    observation_date,
    observation_datetime,
    observation_type_concept_id,
    value_as_number,
    value_as_string,
    new_value_as_concept_id as value_as_concept_id,
    qualifier_concept_id,
    unit_concept_id,
    provider_id,
    visit_occurrence_id,
    visit_detail_id,
    observation_source_value,
    new_observation_source_concept_id as observation_source_concept_id,
    unit_source_value,
    qualifier_source_value,
    new_value_source_concept_id as value_source_concept_id,
    value_source_value,
    questionnaire_response_id,
    ROW_NUMBER() OVER(PARTITION BY person_id, new_observation_source_concept_id ORDER BY rank ASC) AS this_row
FROM
    `{{project_id}}.{{sandbox_dataset_id}}.{{smoking_lookup_table}}`
JOIN `{{project_id}}.{{dataset_id}}.observation`
    USING (observation_source_concept_id, value_as_concept_id)
)
WHERE this_row=1
""")

DELETE_INCORRECT_RECORDS = JINJA_ENV.from_string("""
DELETE
FROM `{{project_id}}.{{dataset_id}}.observation`
WHERE observation_source_concept_id IN
(SELECT
  observation_source_concept_id
FROM `{{project_id}}.{{sandbox_dataset_id}}.{{smoking_lookup_table}}`
)
""")

INSERT_CORRECTED_RECORDS = JINJA_ENV.from_string("""
INSERT INTO `{{project_id}}.{{dataset_id}}.observation`
    (observation_id,
    person_id,
    observation_concept_id,
    observation_date,
    observation_datetime,
    observation_type_concept_id,
    value_as_number,
    value_as_string,
    value_as_concept_id,
    qualifier_concept_id,
    unit_concept_id,
    provider_id,
    visit_occurrence_id,
    visit_detail_id,
    observation_source_value,
    observation_source_concept_id,
    unit_source_value,
    qualifier_source_value,
    value_source_concept_id,
    value_source_value,
    questionnaire_response_id)
SELECT
    *
FROM `{{project_id}}.{{sandbox_dataset_id}}.{{new_smoking_rows}}`
""")

### Validation Query ####
COUNTS_QUERY = JINJA_ENV.from_string("""
SELECT
COUNTIF(observation_source_concept_id IN (1585866,903062,1585872,1586158,903063,
    1586161,903064,1586164,1585865,1585871,1586157,1586160,1586163)
    AND value_as_concept_id = 45877994) AS n_old_child,
COUNTIF(observation_source_concept_id IN (1585864,1585870,1585873,1586159,1586162) 
    AND (value_as_concept_id = 0 OR value_as_concept_id = 903096) AS n_old_parent,
COUNT(*) AS total
FROM `{{project_id}}.{{dataset_id}}.{{obs_table}}`
""")


class CleanSmokingPpi(BaseCleaningRule):

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
        desc = 'Cleans the data associated with the smoking concepts which have a bug in the vocabulary.'

        super().__init__(issue_numbers=JIRA_ISSUE_NUMBERS,
                         description=desc,
                         affected_datasets=[cdr_consts.RDR],
                         affected_tables=[OBSERVATION],
                         project_id=project_id,
                         dataset_id=dataset_id,
                         sandbox_dataset_id=sandbox_dataset_id,
                         depends_on=[ConvertPrePostCoordinatedConcepts],
                         table_namer=table_namer)

        self.counts_query = COUNTS_QUERY.render(project_id=self.project_id,
                                                dataset_id=self.dataset_id,
                                                obs_table=OBSERVATION)

    def get_sandbox_tablenames(self) -> list:
        return [self.sandbox_table_for(OBSERVATION)]

    def setup_rule(self, client: BigQueryClient, *args, **keyword_args) -> None:
        """
        Run the lookup table load.

        Load the lookup table values into the sandbox.  The following queries
        will use the lookup table as part of the execution.
        """
        table_path = os.path.join(resources.resource_files_path,
                                  f"{SMOKING_LOOKUP_TABLE}.csv")
        with open(table_path, 'rb') as csv_file:
            schema_list = client.get_table_schema(SMOKING_LOOKUP_TABLE)
            table_id = f'{self.project_id}.{self.sandbox_dataset_id}.{SMOKING_LOOKUP_TABLE}'
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
        sandbox_query[cdr_consts.QUERY] = SANDBOX_CREATE_QUERY.render(
            project_id=self.project_id,
            sandbox_dataset_id=self.sandbox_dataset_id,
            new_smoking_rows=NEW_SMOKING_ROWS,
            smoking_lookup_table=SMOKING_LOOKUP_TABLE,
            dataset_id=self.dataset_id)
        queries_list.append(sandbox_query)

        delete_query = dict()
        delete_query[cdr_consts.QUERY] = DELETE_INCORRECT_RECORDS.render(
            project_id=self.project_id,
            dataset_id=self.dataset_id,
            sandbox_dataset_id=self.sandbox_dataset_id,
            smoking_lookup_table=SMOKING_LOOKUP_TABLE)
        queries_list.append(delete_query)

        insert_query = dict()
        insert_query[cdr_consts.QUERY] = INSERT_CORRECTED_RECORDS.render(
            project_id=self.project_id,
            dataset_id=self.dataset_id,
            sandbox_dataset_id=self.sandbox_dataset_id,
            new_smoking_rows=NEW_SMOKING_ROWS)
        queries_list.append(insert_query)

        return queries_list

    def setup_validation(self, client: BigQueryClient) -> None:
        """
        Run required steps for validation setup
        """
        self.init_counts = self._get_counts(client)

        if self.init_counts.get('total_rows') == 0:
            raise RuntimeError('NO DATA EXISTS IN OBSERVATION TABLE')

    def validate_rule(self, client: BigQueryClient) -> None:
        """
        Validates the cleaning rule which deletes or updates the data from the tables
        """
        clean_counts = self._get_counts(client)

        if clean_counts.get('total_rows') == 0:
            raise RuntimeError('LOST ALL DATA IN OBSERVATION TABLE')

        if self.init_counts.get('total_rows') != clean_counts.get('total_rows'):
            raise RuntimeError(
                f'{self.__class__.__name__} unexpectedly changed table counts.\n'
                f'initial count is: {self.init_counts.get("total_rows")}\n'
                f'clean count is: {clean_counts.get("total_rows")}')

        if clean_counts.get('child_count') or clean_counts.get('parent_count'):
            raise RuntimeError(
                f'{self.__class__.__name__} did not clean as expected.\n'
                f'Found data in: {clean_counts}')

    def _get_counts(self, client: BigQueryClient) -> Dict[str, int]:
        """
        Counts query.

        Used for job validation.
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
                total = item.get('total', 0)
                n_child = item.get('n_old_child', 0)
                n_parent = item.get('n_old_parent', 0)

        return {
            'total_rows': total,
            'child_count': n_child,
            'parent_count': n_parent
        }


if __name__ == '__main__':
    import cdr_cleaner.args_parser as parser
    import cdr_cleaner.clean_cdr_engine as clean_engine

    ARGS = parser.default_parse_args()

    if ARGS.list_queries:
        clean_engine.add_console_logging()
        query_list = clean_engine.get_query_list(ARGS.project_id,
                                                 ARGS.dataset_id,
                                                 ARGS.sandbox_dataset_id,
                                                 [(CleanSmokingPpi,)])
        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(ARGS.project_id, ARGS.dataset_id,
                                   ARGS.sandbox_dataset_id,
                                   [(CleanSmokingPpi,)])
