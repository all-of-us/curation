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
import argparse
from pathlib import Path
from typing import Iterable

import pandas
from google.cloud import bigquery

import sandbox
from common import OBSERVATION
from resources import PPI_BRANCHING_RULE_PATHS
from utils import bq

ISSUE_KEY = 'dc-545'
PPI_BRANCHING_TABLE_PREFIX = '_ppi_branching'
RULES_LOOKUP_TABLE_ID = f'{PPI_BRANCHING_TABLE_PREFIX}_rules_lookup'
OBSERVATION_BACKUP_TABLE_ID = f'{PPI_BRANCHING_TABLE_PREFIX}_observation'
BACKUP_ROWS_QUERY = bq.JINJA_ENV.from_string("""
WITH rule AS
(SELECT
  rule_type,
  child_question,
  parent_question,
  -- array of parent answer codes --
  ARRAY_AGG(parent_value) AS parent_values
FROM
  {{lookup_table.project}}.{{lookup_table.dataset_id}}.{{lookup_table.table_id}}
WHERE parent_value IS NOT NULL
GROUP BY rule_type, child_question, parent_question)

SELECT 
  -- SELECT * permissible here because it makes this MORE resilient to schema changes --
  oc.*
FROM rule r
  JOIN {{src_table.project}}.{{src_table.dataset_id}}.{{src_table.table_id}} oc
    ON r.child_question = oc.observation_source_value
  JOIN {{src_table.project}}.{{src_table.dataset_id}}.{{src_table.table_id}} op
    ON op.person_id = oc.person_id
    AND op.observation_source_value = r.parent_question
WHERE
(r.rule_type = 'drop' AND op.value_source_value IN UNNEST(r.parent_values)) OR
(r.rule_type = 'keep' AND op.value_source_value NOT IN UNNEST(r.parent_values))
""")


def _load_dataframe(rule_paths: Iterable[str]) -> pandas.DataFrame:
    """
    Create dataframe which contains all the rules in the provided file paths

    :param rule_paths: paths to rule csv files
    :return: dataframe with all the rules
    """
    all_rules_df = pandas.DataFrame()
    for rule_path in rule_paths:
        rules_df = pandas.read_csv(rule_path, header=0)
        rules_df['rule_source'] = Path(rule_path).name
        all_rules_df = all_rules_df.append(rules_df)
    return all_rules_df


def load_rules_lookup(client: bigquery.client.Client,
                      destination_table: bigquery.TableReference,
                      rule_paths: Iterable[str]) -> bigquery.job.LoadJob:
    """
    Load rule csv files to a BigQuery table

    :param client: active BigQuery Client object
    :param destination_table: Identifies the table to use for loading the data
    :param rule_paths: Paths to CSV rule files

    :return: the completed LoadJob object
    """
    rules_df = _load_dataframe(rule_paths)
    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
    job = client.load_table_from_dataframe(rules_df,
                                           destination=destination_table,
                                           job_config=job_config)
    return job.result()


def get_backup_rows_query(src_table: bigquery.TableReference,
                          dst_table: bigquery.TableReference,
                          lookup_table: bigquery.TableReference) -> str:
    observation_schema = bq.get_table_schema(OBSERVATION)
    query = BACKUP_ROWS_QUERY.render(lookup_table=lookup_table, src_table=src_table)
    return bq.get_table_ddl(dataset_id=dst_table.dataset_id,
                            table_id=dst_table.table_id,
                            schema=observation_schema,
                            as_query=query)


def backup_rows(src_table: bigquery.TableReference,
                dst_table: bigquery.TableReference,
                lookup_table: bigquery.TableReference,
                client: bigquery.Client) -> bigquery.QueryJob:
    job_config = bigquery.QueryJobConfig()
    job_config.destination = dst_table
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
    query = get_backup_rows_query(src_table=src_table,
                                  dst_table=dst_table,
                                  lookup_table=lookup_table)
    query_job = client.query(query=query, job_config=job_config)
    return query_job.result()


def get_observation_replace_query(src_table: bigquery.TableReference,
                                  backup_table: bigquery.TableReference) -> str:
    observation_schema = bq.get_table_schema(OBSERVATION)
    query = f"""
    SELECT o.* 
    FROM {src_table.project}.{src_table.dataset_id}.{src_table.table_id} src
    WHERE NOT EXISTS
     (SELECT 1 
      FROM {backup_table.project}.{backup_table.dataset_id}.{backup_table.table_id} bak
      WHERE bak.observation_id = src.observation_id)
    """
    return bq.get_table_ddl(src_table.dataset_id,
                            schema=observation_schema,
                            table_id=src_table.table_id,
                            as_query=query)


def drop_rows(client: bigquery.Client,
              src_table: bigquery.TableReference,
              backup_table: bigquery.TableReference) -> bigquery.QueryJob:
    query = get_observation_replace_query(src_table, backup_table)
    job_config = bigquery.QueryJobConfig()
    job_config.labels['issue_key'] = ISSUE_KEY
    job_config.destination = src_table
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
    query_job = client.query(query, job_config)
    return query_job.result()


def run(project_id: str, dataset_id: str, sandbox_dataset_id: str,
        rule_paths: Iterable[str] = None):
    if not rule_paths:
        rule_paths = PPI_BRANCHING_RULE_PATHS
    client = bigquery.client.Client(project=project_id)

    # target dataset refs
    dataset = bigquery.DatasetReference(project_id, dataset_id)
    src_table = bigquery.TableReference(dataset, OBSERVATION)

    # sandbox dataset refs
    sandbox_dataset = bigquery.DatasetReference(project_id, sandbox_dataset_id)
    rules_lookup_table = bigquery.TableReference(sandbox_dataset, RULES_LOOKUP_TABLE_ID)
    backup_table = bigquery.TableReference(sandbox_dataset, OBSERVATION_BACKUP_TABLE_ID)

    load_rules_job = load_rules_lookup(client,
                                       destination_table=rules_lookup_table,
                                       rule_paths=rule_paths)
    backup_job = backup_rows(src_table, backup_table, rules_lookup_table, client)
    drop_rows_job = drop_rows(client, src_table, backup_table)


def get_arg_parser():
    """
    Get an argument parser

    :return: the parser
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-p',
        '--project_id',
        action='store',
        dest='project_id',
        help='Identifies the project containing the dataset',
        required=True)
    parser.add_argument('-d',
                        '--dataset_id',
                        action='store',
                        dest='dataset_id',
                        help='Identifies dataset to clean',
                        required=True)
    parser.add_argument('-f',
                        '--files',
                        action='store',
                        nargs='+',
                        dest='files',
                        help='Rule files',
                        required=False)
    return parser


def parse_args(args=None):
    """
    Parse command line arguments

    :return: namespace with parsed arguments
    """
    parser = get_arg_parser()
    args = parser.parse_args(args)
    return args


if __name__ == '__main__':
    ARGS = parse_args()
    SANDBOX_DATASET_ID = sandbox.get_sandbox_dataset_id(ARGS.dataset_id)
    run(ARGS.project_id, ARGS.dataset_id, SANDBOX_DATASET_ID, ARGS.files)
