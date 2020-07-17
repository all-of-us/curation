# + language="html"
# <style>
#   .rendered_html table {margin-left: 0}
#   .rendered_html td, .rendered_html th {text-align: left}
# </style>
# -

# ## Problem
# Some survey questions follow branching logic, that is, they may follow naturally
# from responses given to previous (parent) questions. For example, consider the
# questions below taken from the lifestyle survey module.
#
#   Smoking_100CigsLifetime Have you smoked at least 100 cigarettes in your entire life?
#   Smoking_SmokeFrequency Do you now smoke cigarettes [...]?
#
# The child question `Smoking_SmokeFrequency` should pertain only to participants who
# answer the parent question `Smoking_100CigsLifetime` affirmatively.
#
# A bug has been identified which may cause child questions to appear even when a
# requisite parent response has not been provided. In this case, `Smoking_SmokeFrequency`
# might appear for participants who do **not** respond affirmatively to `Smoking_100CigsLifetime`.
#
# ## Solution
# Remove from the observation table child questions where requisite parent responses are missing.
#
# Branching logic is represented by CSV files whose columns are described below.
#
# <table>
# <tr>
#   <th>column name</th>
#   <th>description</th>
#   <th>field in observation table</th>
# </tr>
# <tr>
#     <td>child_question</td>
#     <td>concept_code of the child question</td>
#     <td>observation_source_value</td>
# </tr>
# <tr>
#     <td>parent_question</td>
#     <td>concept_code of the parent question</td>
#     <td>observation_source_value</td>
# </tr>
# <tr>
#     <td>keep_if_parent_value_equals</td>
#     <td>concept_code of answer to parent_question</td>
#     <td>value_source_value</td>
# </tr>
# </table>
#
# 1. Identify rows of the observation table that are child questions missing requisite
#    parent responses and which must be removed. Backup the rows in a sandboxed table.
#
#   For each CSV file, we group columns to yield
#
#   `(child_question, parent_question) => {parent_answer1, parent_answer2, ..}`
#
# ```sql
# -- these are the child rows to REMOVE and save in sandbox
# for (child_question, parent_question), parent_answers in grouped_rules:
#     SELECT oc.*
#     FROM `{{dataset_id}}.observation` oc
#       LEFT JOIN `{{dataset_id}}.observation` op
#        ON op.person_id = oc.person_id
#        AND op.observation_source_value = '{{parent_question}}'
#        AND op.value_source_value IN (
#          for parent_answer in parent_answers:
#            '{{parent_answer}}'
#        )
#     WHERE
#      oc.observation_source_value = '{{child_question}}'
#      AND op.observation_id IS NOT NULL -- the parent
#     UNION ALL
# ```
#
# 2. Reload all rows in the observation table, excluding the sandboxed rows.
#
# ```sql
#     SELECT * FROM `{{dataset_id}}.observation` o
#     WHERE NOT EXISTS (
#       SELECT 1 FROM `{{dataset_id}}.observation` oc
#       WHERE oc.observation_id = o.observation_id
#     )
# ```
#
# # Limitations
#  * Some concept codes are truncated in the RDR export
#
#    **TODO** Reference parent/child questions by concept_id rather than concept_code
#
#  * CSV rules which note any additional logic are currently skipped (as they are incompatible with approach)
#
#    **TODO** Complete the Basics and Overall Health branching logic CSV files
#
#  * Some CSV files indicate child questions to keep and others indicate child questions to remove
#
#    **TODO** Standardize branching logic CSV files

# +
from pathlib import Path

import pandas
from google.cloud import bigquery

import constants.cdr_cleaner.clean_cdr as cdr_consts
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule, query_spec_list
from common import OBSERVATION
from resources import PPI_BRANCHING_RULE_PATHS
from utils import bq

ISSUE_NUMBER = 'dc-545'
PPI_BRANCHING_TABLE_PREFIX = '_ppi_branching'
RULES_LOOKUP_TABLE_ID = f'{PPI_BRANCHING_TABLE_PREFIX}_rules_lookup'
OBSERVATION_BACKUP_TABLE_ID = f'{PPI_BRANCHING_TABLE_PREFIX}_observation_drop'
OBSERVATION_STAGE_TABLE_ID = f'{PPI_BRANCHING_TABLE_PREFIX}_observation_stage'
BACKUP_ROWS_QUERY = bq.JINJA_ENV.from_string("""
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
GROUP BY rule_type, child_question, parent_question)

SELECT 
  -- SELECT * permissible here because it makes this MORE resilient to schema changes --
  oc.*
FROM rule r
  JOIN `{{src_table.project}}.{{src_table.dataset_id}}.{{src_table.table_id}}` oc
    ON r.child_question = oc.observation_source_value
  JOIN `{{src_table.project}}.{{src_table.dataset_id}}.{{src_table.table_id}}` op
    ON op.person_id = oc.person_id
    AND op.observation_source_value = r.parent_question
WHERE
(r.rule_type = 'drop' AND op.value_source_value IN UNNEST(r.parent_values)) OR
(r.rule_type = 'keep' AND op.value_source_value NOT IN UNNEST(r.parent_values))
""")
CLEANED_ROWS_QUERY = bq.JINJA_ENV.from_string("""
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
        super().__init__(issue_numbers=[ISSUE_NUMBER],
                         description=desc,
                         affected_datasets=[cdr_consts.RDR],
                         project_id=project_id,
                         dataset_id=dataset_id,
                         sandbox_dataset_id=sandbox_dataset_id,
                         affected_tables=[OBSERVATION])
        dataset_ref = bigquery.DatasetReference(project_id, dataset_id)
        sandbox_dataset_ref = bigquery.DatasetReference(project_id,
                                                        sandbox_dataset_id)

        self.rule_paths = PPI_BRANCHING_RULE_PATHS
        self.observation_table = bigquery.Table(bigquery.TableReference(
            dataset_ref, OBSERVATION))
        self.lookup_table = bigquery.TableReference(sandbox_dataset_ref,
                                                    RULES_LOOKUP_TABLE_ID)
        self.backup_table = bigquery.TableReference(
            sandbox_dataset_ref, OBSERVATION_BACKUP_TABLE_ID)
        self.stage_table = bigquery.TableReference(
            sandbox_dataset_ref, OBSERVATION_STAGE_TABLE_ID
        )

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
        observation_schema = bq.get_table_schema(OBSERVATION)
        query = BACKUP_ROWS_QUERY.render(lookup_table=self.lookup_table,
                                         src_table=self.observation_table)
        return bq.get_table_ddl(dataset_id=self.backup_table.dataset_id,
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
        observation_schema = bq.get_table_schema(OBSERVATION)
        query = CLEANED_ROWS_QUERY.render(src=self.observation_table, backup=self.backup_table)
        return bq.get_table_ddl(dataset_id=self.stage_table.dataset_id,
                                table_id=self.stage_table.table_id,
                                schema=observation_schema,
                                as_query=query)

    def drop_observation_ddl(self) -> str:
        """
        Get a DDL statement which drops the observation table

        :return: the DDL statement
        """
        table = self.observation_table
        return f'DROP TABLE `{table.project}.{table.dataset_id}.{table.table_id}`'

    def stage_to_target_ddl(self) -> str:
        """
        Get a DDL statement which drops and creates the observation
        table with rows from stage

        :return: the DDL statement
        """
        observation_schema = bq.get_table_schema(OBSERVATION)
        stage = self.stage_table
        query = f'''SELECT * FROM `{stage.project}.{stage.dataset_id}.{stage.table_id}`'''
        return bq.get_table_ddl(self.observation_table.dataset_id,
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
        {self.drop_observation_ddl()};
        {self.stage_to_target_ddl()};
        """
        return script

    def get_sandbox_tablenames(self):
        return [RULES_LOOKUP_TABLE_ID, OBSERVATION_STAGE_TABLE_ID, OBSERVATION_BACKUP_TABLE_ID]

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
    clean_engine.add_console_logging(ARGS.console_log)
    cleaner = PpiBranching(ARGS.project_id, ARGS.dataset_id,
                           ARGS.sandbox_dataset_id)
    query_list = cleaner.get_query_specs()
    if ARGS.list_queries:
        cleaner.log_queries()
    else:
        clean_engine.clean_dataset(ARGS.project_id,
                                   query_list,
                                   data_stage=cdr_consts.RDR)
