# coding=utf-8
"""
Update survey Questions and Answers not mapped to OMOP concepts.

There are several survey questions and answers that are not getting properly mapped from the RDR into the CDR.
There are two sources for this error:
    > Odysseus attempted to introduce some “short codes” that were not implemented in the RDR.
    > There were some concepts that were invalidated and changed around.
To address this, we implemented a fix that maps from the source_values present in the data to the “real”
concept_ids in the OMOP vocabulary. This includes leveraging the supplemental old_map_short_codes.CSV where,
Odyseus provided the short codes, as well as a brief review of other outstanding unmapped PPI codes.

1. For “Question” codes, it updates the observation_concept_id and observation_source_concept_id to a non-zero value.

2. For “Answer” codes, it updates the value_as_concept_id and value_source_concept_id to a non-zero value.

3. The original unmapped code strings are left in place in observation_source_value and value_source_value 
   fields because those original codes are used by the PpiBranchingLogic rule that runs next.  The “bad” code strings 
   are removed and replaced by valid concept_codes during the deid and deid_base steps.
"""
from datetime import datetime
import logging
import os

# Third party imports
from google.cloud import bigquery

# Project imports
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule, query_spec_list
from common import OBSERVATION, JINJA_ENV
import constants.cdr_cleaner.clean_cdr as cdr_consts
import resources

LOGGER = logging.getLogger(__name__)

JIRA_ISSUE_NUMBERS = ['DC499', 'DC830']

OLD_MAP_SHORT_CODES_TABLE = 'old_map_short_codes'

SANDBOX_ALTERED_RECORDS = JINJA_ENV.from_string("""
  CREATE OR REPLACE TABLE `{{project}}.{{sandbox_dataset}}.{{sandbox_table}}` AS (
      SELECT
      -- only sandboxing the identifiers, criteria, and updated fields --
      observation_id
      ,person_id
      ,observation_concept_id
      ,observation_source_concept_id
      ,observation_source_value
      ,value_as_concept_id
      ,value_source_concept_id
      ,value_source_value
      FROM `{{project}}.{{dataset}}.observation`
      WHERE observation_source_value in (SELECT pmi_code FROM `{{project}}.{{sandbox_dataset}}.{{lookup_table}}` WHERE type = "Question")
      or value_source_value in (SELECT  pmi_code FROM `{{project}}.{{sandbox_dataset}}.{{lookup_table}}` WHERE type = "Answer")
  )
""")

UPDATE_QUESTIONS_MAP_QUERY = JINJA_ENV.from_string("""
    UPDATE
        `{{project}}.{{dataset}}.observation` obs
    SET
        observation_concept_id=new_observation_concept_id,
        observation_source_concept_id=new_observation_source_concept_id
    FROM
        (SELECT
        DISTINCT pmi_code AS observation_source_value,
        source_c.concept_id AS new_observation_source_concept_id,
        FIRST_VALUE(standard_c.concept_id) OVER (PARTITION BY source_c.concept_id ORDER BY c_r.relationship_id DESC ) AS new_observation_concept_id
        FROM (
            SELECT
            pmi_code,
            short_code
            FROM
                `{{project}}.{{sandbox}}.{{old_map}}`
            WHERE
                type='Question' )
        LEFT JOIN
            `{{project}}.{{dataset}}.concept` source_c
        ON
            (short_code=concept_code)
        JOIN
            `{{project}}.{{dataset}}.concept_relationship` c_r  
        ON
            (source_c.concept_id=c_r.concept_id_1)
        JOIN
            `{{project}}.{{dataset}}.concept` standard_c
        ON
            (standard_c.concept_id=c_r.concept_id_2)
        WHERE
            source_c.vocabulary_id='PPI'
            AND c_r.relationship_id LIKE 'Maps to%'
        ) map
    WHERE
        map.observation_source_value=obs.observation_source_value""")

UPDATE_ANSWERS_MAP_QUERY = JINJA_ENV.from_string("""
    UPDATE
        `{{project}}.{{dataset}}.observation` obs
    SET
        value_as_concept_id=new_value_as_concept_id,
        value_source_concept_id=new_value_source_concept_id
    FROM (
        SELECT
        DISTINCT pmi_code AS value_source_value,
        source_c.concept_id AS new_value_source_concept_id,
        FIRST_VALUE(standard_c.concept_id) OVER (PARTITION BY source_c.concept_id ORDER BY c_r.relationship_id DESC ) AS new_value_as_concept_id
        FROM (
            SELECT
            pmi_code,
            short_code
            FROM
                `{{project}}.{{sandbox}}.{{old_map}}`
            WHERE
                type='Answer' )
        LEFT JOIN
            `{{project}}.{{dataset}}.concept` source_c
        ON
            (short_code=concept_code)
        JOIN
            `{{project}}.{{dataset}}.concept_relationship` c_r
        ON
            (source_c.concept_id=c_r.concept_id_1)
        JOIN
            `{{project}}.{{dataset}}.concept` standard_c
        ON
            (standard_c.concept_id=c_r.concept_id_2)
        WHERE
            source_c.vocabulary_id='PPI'
        AND c_r.relationship_id LIKE 'Maps to%') map
    WHERE
        map.value_source_value=obs.value_source_value""")


class SetConceptIdsForSurveyQuestionsAnswers(BaseCleaningRule):

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
        desc = (
            'Update the survey questions and answers that have an '
            'observation_source_value or value_source_value code that corresponds '
            'to the pmi_code in the lookup table.  This will set observation_concept_id '
            'and observation_source_concept_id for "Question" codes.  It wll set '
            'value_as_concept_id and value_source_concept_id for "Answer" codes.  '
            'The original string value will not be changed here.  Several rules '
            'depend on the original/"bad" concept_code to function properly.')

        super().__init__(issue_numbers=JIRA_ISSUE_NUMBERS,
                         description=desc,
                         affected_datasets=[cdr_consts.RDR],
                         affected_tables=[OBSERVATION],
                         project_id=project_id,
                         dataset_id=dataset_id,
                         sandbox_dataset_id=sandbox_dataset_id,
                         table_namer=table_namer,
                         run_for_synthetic=True)

    def get_sandbox_tablenames(self):
        return [self.sandbox_table_for(OBSERVATION)]

    def setup_rule(self, client, *args, **keyword_args):
        """
        Run the lookup table load.

        Load the lookup table values into the sandbox.  The following queries
        will use the lookup table as part of the execution.
        """
        table_data_path = os.path.join(resources.resource_files_path,
                                       f"{OLD_MAP_SHORT_CODES_TABLE}.csv")
        with open(table_data_path, 'rb') as csv_file:
            schema_list = client.get_table_schema(OLD_MAP_SHORT_CODES_TABLE)
            table_id = f'{self.project_id}.{self.sandbox_dataset_id}.{OLD_MAP_SHORT_CODES_TABLE}'
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
        This function gets the queries required to update the questions and answers that were unmapped to OMOP concept idenfitiers

        This updates zero values in observation_concept_id, observation_source_concept_id,
        value_as_concept_id, and value_source_concept_id to valid concept_ids found in the vocabulary.
        It does not update the concept_codes in observation_source_value or value_source_value fields.

        :return: a list of queries to execute
        """
        queries_list = []

        # Sandbox records that are expected to be impacted
        queries_list.append({
            cdr_consts.QUERY:
                SANDBOX_ALTERED_RECORDS.render(
                    project=self.project_id,
                    dataset=self.dataset_id,
                    sandbox_dataset=self.sandbox_dataset_id,
                    sandbox_table=self.sandbox_table_for(OBSERVATION),
                    lookup_table=OLD_MAP_SHORT_CODES_TABLE)
        })

        # Update concept_ids to questions using OLD_MAP_SHORT_CODES_TABLE.
        queries_list.append({
            cdr_consts.QUERY:
                UPDATE_QUESTIONS_MAP_QUERY.render(
                    dataset=self.dataset_id,
                    project=self.project_id,
                    old_map=OLD_MAP_SHORT_CODES_TABLE,
                    sandbox=self.sandbox_dataset_id)
        })

        # Update concept_ids to answers using OLD_MAP_SHORT_CODES_TABLE.
        queries_list.append({
            cdr_consts.QUERY:
                UPDATE_ANSWERS_MAP_QUERY.render(
                    dataset=self.dataset_id,
                    project=self.project_id,
                    old_map=OLD_MAP_SHORT_CODES_TABLE,
                    sandbox=self.sandbox_dataset_id)
        })

        return queries_list

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


if __name__ == '__main__':
    import cdr_cleaner.args_parser as parser
    import cdr_cleaner.clean_cdr_engine as clean_engine

    ARGS = parser.parse_args()

    if ARGS.list_queries:
        clean_engine.add_console_logging()
        query_list = clean_engine.get_query_list(
            ARGS.project_id, ARGS.dataset_id, ARGS.sandbox_dataset_id,
            [(SetConceptIdsForSurveyQuestionsAnswers,)])
        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(ARGS.project_id, ARGS.dataset_id,
                                   ARGS.sandbox_dataset_id,
                                   [(SetConceptIdsForSurveyQuestionsAnswers,)])
