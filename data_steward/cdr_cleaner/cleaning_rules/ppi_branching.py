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
from enum import Enum

import jinja2
import pandas
from google.cloud import bigquery


class RuleType(Enum):
    """
    Represents whether rule is to keep or remove child question

    Note: Member names are the same as column name in associated CSV file
    """
    remove_if_parent_value_equals = 1
    keep_if_parent_value_equals = 2


ISSUE_KEY = 'dc-545'
JINJA_ENV = jinja2.Environment(
    # block tags on their own lines
    # will not cause extra white space
    trim_blocks=True,
    lstrip_blocks=True,
    # syntax highlighting should be better
    # with these comment delimiters
    comment_start_string='--',
    comment_end_string=' --',
    autoescape=True)
RULE_TYPE_COL_SUFFIX = '_if_parent_value_equals'
QUERY_TEMPLATE = JINJA_ENV.from_string("""
{% for (child_question, parent_question), parent_answers in grouped_rules %}
SELECT oc.person_id,
       oc.observation_id           child_observation_id,
       oc.observation_source_value child_observation_source_value,
       oc.value_source_value       child_value_source_value,
       op.observation_id           parent_observation_id,
       op.observation_source_value parent_observation_source_value,
       op.value_source_value       parent_value_source_value
FROM `{{dataset_id}}.observation` oc
  LEFT JOIN `{{dataset_id}}.observation` op
   ON op.person_id = oc.person_id
   AND op.observation_source_value = '{{parent_question}}'
   AND op.value_source_value IN (
     {% for parent_answer in parent_answers %}
     '{{ parent_answer }}'{{ ',' if loop.nextitem is defined }}
     {% endfor %}
   )
WHERE
 oc.observation_source_value = '{{child_question}}'
  -- does not have suitable parent answer --
 AND op.observation_id IS {{ 'NOT' if rule_type == RuleType.remove_if_parent_value_equals }} NULL

{{ 'UNION ALL' if loop.nextitem is defined }}

{% endfor %}
""", globals={'RuleType': RuleType})
# +
# TODO Store CSVs in BQ and generate table with schema below so we can
#  1. avoid having large queries (and have to solve query length limits) and
#  2. perform sandboxing and removal using atomic operations
#  (rule_type: STRING,
#   rule_source: STRING,
#   child_code: STRING,
#   parent_code: STRING,
#   parent_answers: ARRAY[STRING])

# from google.oauth2 import service_account
# credentials = service_account.Credentials.from_service_account_file('/path/to/json')
# lifestyle_df['rule_type'] = 'remove'
# lifestyle_df['source'] = 'lifestyle.csv'
# to_gbq_result = lifestyle_df.to_gbq(destination_table='{SANDBOX_DATASET}.branching_rules',
#                                     credentials=credentials,
#                                     if_exists='replace')
# -


def get_rule_type(df) -> RuleType:
    rule_type_lookup = df.columns[2]
    return RuleType[rule_type_lookup]


def get_sandbox_queries(project_id, dataset_id, rule_paths):
    for rule_path in rule_paths:
        rule_df = pandas.read_csv(rule_path, header=0)
        rule_type = get_rule_type(rule_df)

        rule_gb = rule_df.groupby(['child_question', 'parent_question'])
        rm_by_child_parent_series = rule_gb[rule_type.name].apply(set)
        grouped_rules = list(rm_by_child_parent_series.items())
        query = QUERY_TEMPLATE.render(dataset_id=dataset_id,
                                      grouped_rules=grouped_rules,
                                      rule_type=rule_type)
        yield query


def get_sandbox_records(project_id, dataset_id, query):
    client = bigquery.client.Client(project=project_id)
    query_job_config = bigquery.QueryJobConfig()
    query_job_config.labels['issue_key'] = ISSUE_KEY
    query_job = client.query(query=query, job_config=query_job_config)
    query_results = query_job.result()
    results_df = query_results.to_dataframe()
    return results_df


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
                        required=True)
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
    QUERIES = get_sandbox_queries(ARGS.project_id, ARGS.dataset_id, ARGS.files)
    for QUERY in QUERIES:
        print(QUERY)
