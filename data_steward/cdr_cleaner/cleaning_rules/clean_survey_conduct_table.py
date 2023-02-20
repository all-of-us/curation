"""
Ensures the survey conduct table is populated as expected.

Survey_conduct table is relatively new at the point of this CR's creation. Because bugs still exist in the creation of
this table.  This CR is needed to ensure all fields are populated as expected.

Expectations:
1. If a valid row the survey_source_concept_id and survey_concept_id will be populated with the same, valid concept_id.
(OMOP concept id or AOU custom id).
2. If not valid(examples in the integration test) the survey_source_concept_id and survey_concept_id fields will be set
to 0

Dependencies:
The surveys requiring a AOU custom id should have had their fields updated in a previously run CR.
Observations that do not have a valid survey will be dropped in a CR that runs after this one.

Original Issues: DC-3013
"""
# Python imports
import logging
from datetime import datetime

# Project imports
from google.cloud import bigquery
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule, query_spec_list
from cdr_cleaner.cleaning_rules.drop_unverified_survey_data import DropUnverifiedSurveyData
from constants.cdr_cleaner import clean_cdr as cdr_consts
from common import JINJA_ENV, SURVEY_CONDUCT
import resources

LOGGER = logging.getLogger(__name__)

JIRA_ISSUE_NUMBERS = ['DC3013']

DOMAIN_TABLES = [SURVEY_CONDUCT]
AOU_CUSTOM_VOCAB = 'aou_custom_vocab'

SANDBOX_SURVEY_CONDUCT = JINJA_ENV.from_string("""
CREATE OR REPLACE TABLE `{{project_id}}.{{sandbox_dataset_id}}.{{sandbox_table_id}}` AS (
  WITH vocab AS ( -- All modules and custom concept_ids --
    SELECT concept_id 
    FROM `{{project_id}}.{{dataset_id}}.concept` 
    WHERE vocabulary_id = 'PPI'
    AND concept_class_id = 'Module'
             
    UNION ALL
             
    SELECT concept_id
    FROM `{{project_id}}.{{sandbox_dataset_id}}.{{aou_custom_vocab}}`
        ) 
  SELECT sc.*
  FROM `{{project_id}}.{{dataset_id}}.survey_conduct` sc
  WHERE survey_concept_id != survey_source_concept_id
  OR survey_concept_id NOT IN (SELECT concept_id FROM vocab)
  OR survey_source_concept_id NOT IN (SELECT concept_id FROM vocab)
)
""")

CLEAN_SURVEY_CONDUCT = JINJA_ENV.from_string("""
CREATE OR REPLACE TABLE `{{project_id}}.{{dataset_id}}.survey_conduct` AS (
  WITH vocab AS ( -- All modules and custom concept_ids --
    SELECT concept_id 
    FROM `{{project_id}}.{{dataset_id}}.concept` 
    WHERE vocabulary_id = 'PPI'
    AND concept_class_id = 'Module'
    
    UNION ALL
             
    SELECT concept_id
    FROM `{{project_id}}.{{sandbox_dataset_id}}.{{aou_custom_vocab}}`
                ) 
SELECT 
survey_conduct_id,
person_id,
    CASE
      WHEN COALESCE(NULLIF(survey_concept_id,0), survey_source_concept_id) NOT IN (SELECT concept_id FROM vocab) THEN 0
      WHEN COALESCE(NULLIF(survey_source_concept_id,0), survey_concept_id) NOT IN (SELECT concept_id FROM vocab) THEN 0
      WHEN COALESCE(NULLIF(survey_concept_id,0), survey_source_concept_id) IN (SELECT concept_id FROM vocab) THEN COALESCE(NULLIF(survey_concept_id,0), survey_source_concept_id)
      ELSE survey_concept_id
    END AS
survey_concept_id,
survey_start_date,
survey_start_datetime,
survey_end_date,
survey_end_datetime,
provider_id,
assisted_concept_id,
respondent_type_concept_id,
timing_concept_id,
collection_method_concept_id,
assisted_source_value,
respondent_type_source_value,
timing_source_value,
collection_method_source_value,
survey_source_value,
    CASE
      WHEN COALESCE(NULLIF(survey_concept_id,0), survey_source_concept_id) NOT IN (SELECT concept_id FROM vocab) THEN 0
      WHEN COALESCE(NULLIF(survey_source_concept_id,0), survey_concept_id) NOT IN (SELECT concept_id FROM vocab) THEN 0
      WHEN COALESCE(NULLIF(survey_concept_id,0), survey_source_concept_id) IN (SELECT concept_id FROM vocab) THEN COALESCE(NULLIF(survey_concept_id,0), survey_source_concept_id)
    ELSE survey_source_concept_id
    END AS
survey_source_concept_id,
survey_source_identifier,
validated_survey_concept_id,
validated_survey_source_value,
survey_version_number,
visit_occurrence_id,
response_visit_occurrence_id

FROM `{{project_id}}.{{dataset_id}}.survey_conduct`   
)
""")


class CleanSurveyConduct(BaseCleaningRule):

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
        desc = ('Updates/Cleans survey_conduct concept_id fields.')

        super().__init__(issue_numbers=JIRA_ISSUE_NUMBERS,
                         description=desc,
                         affected_datasets=[cdr_consts.RDR],
                         affected_tables=DOMAIN_TABLES,
                         project_id=project_id,
                         dataset_id=dataset_id,
                         sandbox_dataset_id=sandbox_dataset_id,
                         depends_on=[DropUnverifiedSurveyData],
                         table_namer=table_namer)

    def get_query_specs(self, *args, **keyword_args) -> query_spec_list:
        """
        Return a list of dictionary query specifications.

        :return:  A list of dictionaries. Each dictionary contains a single query
            and a specification for how to execute that query. The specifications
            are optional but the query is required.
        """
        queries_list = []

        sandbox_sc_query = dict()
        sandbox_sc_query[cdr_consts.QUERY] = SANDBOX_SURVEY_CONDUCT.render(
            project_id=self.project_id,
            dataset_id=self.dataset_id,
            sandbox_dataset_id=self.sandbox_dataset_id,
            aou_custom_vocab=AOU_CUSTOM_VOCAB,
            sandbox_table_id=self.sandbox_table_for(SURVEY_CONDUCT))
        queries_list.append(sandbox_sc_query)

        clean_sc_query = dict()
        clean_sc_query[cdr_consts.QUERY] = CLEAN_SURVEY_CONDUCT.render(
            project_id=self.project_id,
            dataset_id=self.dataset_id,
            sandbox_dataset_id=self.sandbox_dataset_id,
            aou_custom_vocab=AOU_CUSTOM_VOCAB,
            sandbox_table_id=self.sandbox_table_for(SURVEY_CONDUCT))
        queries_list.append(clean_sc_query)

        return queries_list

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

        table_path = resources.AOU_VOCAB_CONCEPT_CSV_PATH

        with open(table_path, 'rb') as csv_file:
            schema_list = client.get_table_schema(AOU_CUSTOM_VOCAB)
            table_id = f'{self.project_id}.{self.sandbox_dataset_id}.{AOU_CUSTOM_VOCAB}'
            job_config = bigquery.LoadJobConfig(
                schema=schema_list,
                skip_leading_rows=1,
                source_format=bigquery.SourceFormat.CSV,
                field_delimiter='\t',
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
        return [self.sandbox_table_for(table) for table in DOMAIN_TABLES]


if __name__ == '__main__':
    import cdr_cleaner.args_parser as parser
    import cdr_cleaner.clean_cdr_engine as clean_engine

    ARGS = parser.default_parse_args()

    if ARGS.list_queries:
        clean_engine.add_console_logging()
        query_list = clean_engine.get_query_list(ARGS.project_id,
                                                 ARGS.dataset_id,
                                                 ARGS.sandbox_dataset_id,
                                                 [(CleanSurveyConduct,)])
        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(ARGS.project_id, ARGS.dataset_id,
                                   ARGS.sandbox_dataset_id,
                                   [(CleanSurveyConduct,)])
