"""
Remove from the observation table PPI responses that are inconsistent with branching logic.

The relevance of some survey questions depend on responses given to previous (parent)
questions. This rule addresses a bug wherein child questions may appear even though a
requisite response to a parent question was NOT provided.

For example, consider the questions below taken from the lifestyle survey module.

  Smoking_100CigsLifetime Have you smoked at least 100 cigarettes in your entire life?
  Smoking_SmokeFrequency Do you now smoke cigarettes [...]?

The child question `Smoking_SmokeFrequency` should pertain only to participants who
answer the parent question `Smoking_100CigsLifetime` affirmatively however due to the bug
`Smoking_SmokeFrequency` may appear for participants who do not respond
affirmatively to the parent question. This rule removes these invalid `Smoking_SmokeFrequency`
questions.

# Setup Rule

Load CSV files into a lookup table which represents PPI branching logic.

The columns of the lookup table are described below.
| column name     | description                            | field in observation table |
|-----------------|----------------------------------------|----------------------------|
| child_question  | child question concept_code            | observation_source_value   |
| parent_question | parent question concept_code           | observation_source_value   |
| parent_value    | concept_code of parent_question answer | value_source_value         |

 * rule_type: the action to perform on child_question if parent_question = parent_value
 * rule_source: name of the CSV file the rule originated from
 * notes: comments from the author of the logical rule

# Rule execution

1. Backup rows of the observation table that are child questions missing requisite
   parent responses and which must be removed.

2. Recreate the observation table such that it excludes the backed up rows.

   Note: A current limitation of the CREATE OR REPLACE TABLE DDL statement is it
   cannot load the table via query when the existent table is ingestion-time
   partitioned (see https://bit.ly/2VeMs7e). The rule will therefore
     * stage the cleaned observation table in the sandbox
     * drop the existent observation table
     * create the observation table and load via query on the staged table

# Limitations

 * Some concept codes are truncated in the RDR export
 **TODO** Reference parent/child questions by concept_id rather than concept_code
 * Rules indicated by author notes are NOT applied (as they are incompatible with approach)
 **TODO** Complete the Basics and Overall Health branching logic CSV files
 * Some rules indicate child questions to keep and others indicate child questions to remove
 **TODO** Standardize branching logic CSV files
 
 DC-1055
  * Support cope survey branching logic errors.
  * Account for branching logic issues where child questions exist due to incorrect branching
    logic from one parent q/a and correct branching logic from a different parent q/a.
    In such cases, the child question is considered correct and must not be dropped.
  * In certain cases, the `value_as_number` needs to be used for the parent answer instead of 
    `value_source_value`. Currently only supports `>` and this needs to be specified via the
    `keep_gt` rule type and `parent_value` must be a float.
"""
# Python imports
import logging
from pathlib import Path
from typing import Union

# Third Party imports
import pandas
from google.cloud import bigquery

# Project imports
import constants.cdr_cleaner.clean_cdr as cdr_consts
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule, query_spec_list
from cdr_cleaner.cleaning_rules.set_unmapped_question_answer_survey_concepts import (
    SetConceptIdsForSurveyQuestionsAnswers)
from common import OBSERVATION, JINJA_ENV
from gcloud.bq import BigQueryClient
from resources import PPI_BRANCHING_RULE_PATHS
from utils import bq

LOGGER = logging.getLogger(__name__)

ISSUE_NUMBERS = ['dc-545', 'dc-1055']
PPI_BRANCHING_TABLE_PREFIX = '_ppi_branching'
RULES_LOOKUP_TABLE_ID = f'{PPI_BRANCHING_TABLE_PREFIX}_rules_lookup'
OBSERVATION_BACKUP_TABLE_ID = f'{PPI_BRANCHING_TABLE_PREFIX}_observation_drop'
OBSERVATION_STAGE_TABLE_ID = f'{PPI_BRANCHING_TABLE_PREFIX}_observation_stage'

BACKUP_ROWS_QUERY = JINJA_ENV.from_string("""
WITH rule AS
(SELECT
  rule_type,
  child_question,
  parent_question,
  -- array of parent answer codes --
  ARRAY_AGG(parent_value) AS parent_values
FROM
  `{{lookup_table.project}}.{{lookup_table.dataset_id}}.{{lookup_table.table_id}}`
WHERE parent_value IS NOT NULL
GROUP BY rule_type, child_question, parent_question),

-- generate master list of parent child combinations --
parent_child_combs AS 
(SELECT
-- rule cols --
    r.rule_type,
    r.child_question,
    r.parent_question,
    r.parent_values,
-- child cols --
    oc.observation_id as oc_observation_id,
    oc.person_id as oc_person_id,
    oc.observation_concept_id as oc_observation_concept_id,
    oc.observation_date as oc_observation_date,
    oc.observation_datetime as oc_observation_datetime,
    oc.observation_type_concept_id as oc_observation_type_concept_id,
    oc.value_as_number as oc_value_as_number,
    oc.value_as_string as oc_value_as_string,
    oc.value_as_concept_id as oc_value_as_concept_id,
    oc.qualifier_concept_id as oc_qualifier_concept_id,
    oc.unit_concept_id as oc_unit_concept_id,
    oc.provider_id as oc_provider_id,
    oc.visit_occurrence_id as oc_visit_occurrence_id,
    oc.visit_detail_id as oc_visit_detail_id,
    oc.observation_source_value as oc_observation_source_value,
    oc.observation_source_concept_id as oc_observation_source_concept_id,
    oc.unit_source_value as oc_unit_source_value,
    oc.qualifier_source_value as oc_qualifier_source_value,
    oc.value_source_concept_id as oc_value_source_concept_id,
    oc.value_source_value as oc_value_source_value,
    oc.questionnaire_response_id as oc_questionnaire_response_id,
-- parent cols --
    op.observation_id as op_observation_id,
    op.person_id as op_person_id,
    op.observation_concept_id as op_observation_concept_id,
    op.observation_date as op_observation_date,
    op.observation_datetime as op_observation_datetime,
    op.observation_type_concept_id as op_observation_type_concept_id,
    op.value_as_number as op_value_as_number,
    op.value_as_string as op_value_as_string,
    op.value_as_concept_id as op_value_as_concept_id,
    op.qualifier_concept_id as op_qualifier_concept_id,
    op.unit_concept_id as op_unit_concept_id,
    op.provider_id as op_provider_id,
    op.visit_occurrence_id as op_visit_occurrence_id,
    op.visit_detail_id as op_visit_detail_id,
    op.observation_source_value as op_observation_source_value,
    op.observation_source_concept_id as op_observation_source_concept_id,
    op.unit_source_value as op_unit_source_value,
    op.qualifier_source_value as op_qualifier_source_value,
    op.value_source_concept_id as op_value_source_concept_id,
    op.value_source_value as op_value_source_value,
    op.questionnaire_response_id as op_questionnaire_response_id
FROM rule r
  JOIN `{{src_table.project}}.{{src_table.dataset_id}}.{{src_table.table_id}}` oc
    ON r.child_question = oc.observation_source_value
  JOIN `{{src_table.project}}.{{src_table.dataset_id}}.{{src_table.table_id}}` op
    ON op.person_id = oc.person_id
    -- account for repeated questions associated with longitudinal surveys (i.e. COPE) --
    AND op.questionnaire_response_id = oc.questionnaire_response_id
    AND op.observation_source_value = r.parent_question),

-- rows to drop via 'drop' rule_type --
drop_rule_obs AS
(SELECT 
  oc_observation_id
FROM parent_child_combs
WHERE
(rule_type = 'drop' AND op_value_source_value IN UNNEST(parent_values))),

-- rows to keep via 'keep' rule_type --
keep_rule_obs AS
(SELECT 
  oc_observation_id
FROM parent_child_combs
WHERE
(rule_type = 'keep' AND op_value_source_value IN UNNEST(parent_values))),

-- rows to keep via 'keep_gt' rule_type --
-- ONLY for rules using gte with one value --
keep_gt_rule_obs AS 
(SELECT 
  oc_observation_id
FROM parent_child_combs
WHERE
(rule_type = 'keep_gt'
    AND SAFE_CAST(parent_values[OFFSET(0)] AS FLOAT64) IS NOT NULL 
    {{ '/* Every keep_gt rule row should be associated with one parent_value */' }}
    AND op_value_as_number > SAFE_CAST(parent_values[OFFSET(0)] AS FLOAT64))),

-- rows to drop with values not in 'keep' rule_type values --
-- note that this may include child rows which are resulting --
-- from correct branching logic via a different parent q/a --
not_keep_rule_obs AS 
(SELECT 
  oc_observation_id
FROM parent_child_combs
WHERE
(rule_type = 'keep' AND op_value_source_value NOT IN UNNEST(parent_values))),

-- rows to drop with values 'null' or less than 'keep_gt' rule_type values --
-- note that this may include child rows which are resulting --
-- from correct branching logic via a different parent q/a --
-- ONLY for rules using gte with one value --
not_keep_gt_rule_obs AS 
(SELECT 
  oc_observation_id
FROM parent_child_combs
WHERE
(rule_type = 'keep_gt' 
    AND SAFE_CAST(parent_values[OFFSET(0)] AS FLOAT64) IS NOT NULL
    AND (op_value_as_number <= SAFE_CAST(parent_values[OFFSET(0)] AS FLOAT64)
        OR op_value_as_number IS NULL)))

-- final list of rows to drop --
SELECT 
  o.*
FROM
  `{{src_table.project}}.{{src_table.dataset_id}}.{{src_table.table_id}}` o
WHERE observation_id IN
(SELECT oc_observation_id 
    FROM drop_rule_obs
    UNION ALL
SELECT oc_observation_id
    FROM not_keep_rule_obs
    UNION ALL
SELECT oc_observation_id 
    FROM not_keep_gt_rule_obs)
AND observation_id NOT IN
-- exclude 'keep' rows so that child rows resulting from correct --
-- branching logic via a different parent q/a are not dropped, --
-- even if they are marked for deletion via some parent q/a --
(SELECT oc_observation_id 
    FROM keep_rule_obs
    UNION ALL
SELECT oc_observation_id 
    FROM keep_gt_rule_obs)
""")

CLEANED_ROWS_QUERY = JINJA_ENV.from_string("""
SELECT {{ scope or 'src.*' }} 
FROM `{{src.project}}.{{src.dataset_id}}.{{src.table_id}}` src
WHERE NOT EXISTS
 (SELECT 1 
  FROM `{{backup.project}}.{{backup.dataset_id}}.{{backup.table_id}}` bak
  WHERE bak.observation_id = src.observation_id)
""")


class PpiBranching(BaseCleaningRule):

    def __init__(self, project_id, dataset_id, sandbox_dataset_id):
        desc = (
            'Load a lookup of PPI branching rules represented in CSV files. '
            'Store observation rows that violate the rules in a sandbox table. '
            'Stage the cleaned rows in a sandbox table. '
            'Drop and create the observation table with rows from stage.')

        super().__init__(issue_numbers=ISSUE_NUMBERS,
                         description=desc,
                         affected_datasets=[cdr_consts.RDR],
                         project_id=project_id,
                         dataset_id=dataset_id,
                         sandbox_dataset_id=sandbox_dataset_id,
                         affected_tables=[OBSERVATION],
                         depends_on=[SetConceptIdsForSurveyQuestionsAnswers],
                         run_for_synthetic=True)

        dataset_ref = bigquery.DatasetReference(project_id, dataset_id)
        sandbox_dataset_ref = bigquery.DatasetReference(project_id,
                                                        sandbox_dataset_id)

        self.rule_paths = PPI_BRANCHING_RULE_PATHS
        self.observation_table = bigquery.Table(
            bigquery.TableReference(dataset_ref, OBSERVATION))
        self.lookup_table = bigquery.TableReference(sandbox_dataset_ref,
                                                    RULES_LOOKUP_TABLE_ID)
        self.backup_table = bigquery.TableReference(
            sandbox_dataset_ref, OBSERVATION_BACKUP_TABLE_ID)
        self.stage_table = bigquery.TableReference(sandbox_dataset_ref,
                                                   OBSERVATION_STAGE_TABLE_ID)
        self.bq_client = BigQueryClient(project_id)

    def create_rules_dataframe(self) -> pandas.DataFrame:
        """
        Create dataframe which contains all the rules in the provided file paths

        :return: dataframe with all the rules
        """
        all_rules_df = pandas.DataFrame()
        for rule_path in self.rule_paths:
            rules_df = pandas.read_csv(rule_path, header=0)
            rules_df['rule_source'] = Path(rule_path).name
            all_rules_df = all_rules_df.append(rules_df)
        return all_rules_df

    def load_rules_lookup(self,
                          client: bigquery.Client) -> bigquery.job.LoadJob:
        """
        Load rule csv files to a BigQuery table

        :param client: active BigQuery Client object
        :return: the completed LoadJob object
        """
        job_config = bigquery.LoadJobConfig()
        rules_dataframe = self.create_rules_dataframe()
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
        job = client.load_table_from_dataframe(rules_dataframe,
                                               destination=self.lookup_table,
                                               job_config=job_config)
        return job.result()

    def backup_rows_to_drop_ddl(self) -> str:
        """
        Get a DDL statement which loads a backup table with rows to be dropped

        :return: the DDL statement
        """
        observation_schema = self.bq_client.get_table_schema(OBSERVATION)
        query = BACKUP_ROWS_QUERY.render(lookup_table=self.lookup_table,
                                         src_table=self.observation_table)
        return self.bq_client.get_create_or_replace_table_ddl(
            dataset_id=self.backup_table.dataset_id,
            table_id=self.backup_table.table_id,
            schema=observation_schema,
            as_query=query)

    def stage_cleaned_table_ddl(self) -> str:
        """
        Get a DDL statement which stages cleaned table

        Note: This avoids potential partitioning mismatch error
              when directly overwriting observation table

        :return: the DDL statement
        """
        observation_schema = self.bq_client.get_table_schema(OBSERVATION)
        query = CLEANED_ROWS_QUERY.render(src=self.observation_table,
                                          backup=self.backup_table)
        return self.bq_client.get_create_or_replace_table_ddl(
            dataset_id=self.stage_table.dataset_id,
            table_id=self.stage_table.table_id,
            schema=observation_schema,
            as_query=query)

    def drop_observation_ddl(
            self, table: Union[bigquery.TableReference, bigquery.Table]) -> str:
        """
        Get a DDL statement which drops a specified table

        :return: the DDL statement
        """
        return f'DROP TABLE `{table.project}.{table.dataset_id}.{table.table_id}`'

    def stage_to_target_ddl(self) -> str:
        """
        Get a DDL statement which drops and creates the observation
        table with rows from stage

        :return: the DDL statement
        """
        observation_schema = self.bq_client.get_table_schema(OBSERVATION)
        stage = self.stage_table
        query = f'''SELECT * FROM `{stage.project}.{stage.dataset_id}.{stage.table_id}`'''
        return self.bq_client.get_create_or_replace_table_ddl(
            dataset_id=self.observation_table.dataset_id,
            schema=observation_schema,
            table_id=self.observation_table.table_id,
            as_query=query)

    def cleaning_script(self) -> str:
        """
        Get script which cleans the observation table

        :return: the SQL script
        """
        script = f"""
        {self.backup_rows_to_drop_ddl()};
        {self.stage_cleaned_table_ddl()};
        {self.drop_observation_ddl(self.observation_table)};
        {self.stage_to_target_ddl()};
        {self.drop_observation_ddl(self.stage_table)};
        """
        return script

    def get_sandbox_tablenames(self):
        return [
            RULES_LOOKUP_TABLE_ID, OBSERVATION_STAGE_TABLE_ID,
            OBSERVATION_BACKUP_TABLE_ID
        ]

    def setup_rule(self, client: bigquery.Client, *args, **keyword_args):
        self.load_rules_lookup(client)

    def get_query_specs(self, *args, **keyword_args) -> query_spec_list:
        return [{cdr_consts.QUERY: self.cleaning_script()}]

    def setup_validation(self, client, *args, **keyword_args):
        pass

    def validate_rule(self, client: bigquery.Client, *args, **keyword_args):
        """
        Raise an error if there are still rows to delete

        :param client: active BigQuery client object
        :param args:
        :param keyword_args:
        :return: None
        """
        backup_table_obj = client.get_table(self.backup_table)
        if not backup_table_obj.created:
            raise RuntimeError(
                f'Backup table {backup_table_obj.table_id} for branching cleaning rule was not '
                f'found on the server')
        query = BACKUP_ROWS_QUERY.render(lookup_table=self.lookup_table,
                                         src_table=self.observation_table)
        result = client.query(query).result()
        if result.total_rows > 0:
            raise RuntimeError(
                f'Branching cleaning rule was run but still identifies {result.total_rows} '
                f'rows from the observation table to drop')


if __name__ == '__main__':
    import cdr_cleaner.args_parser as parser
    import cdr_cleaner.clean_cdr_engine as clean_engine

    ARGS = parser.parse_args()

    if ARGS.list_queries:
        clean_engine.add_console_logging()
        query_list = clean_engine.get_query_list(ARGS.project_id,
                                                 ARGS.dataset_id,
                                                 ARGS.sandbox_dataset_id,
                                                 [(PpiBranching,)])
        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(ARGS.project_id, ARGS.dataset_id,
                                   ARGS.sandbox_dataset_id, [(PpiBranching,)])
