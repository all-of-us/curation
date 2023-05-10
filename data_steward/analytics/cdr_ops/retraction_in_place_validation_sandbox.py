# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.7.1
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# + tags=["parameters"]
project_id: str = ""  # identifies the project where datasets are located
datasets: list = []  # identifies the dataset names after retraction
lookup_table: str = ""  # lookup table name where the pids and rids needs to be retracted are stored
lookup_table_dataset: str = ""  # the sandbox dataset where lookup table is located
run_as: str = ""  # service account email to impersonate
# -
# Third party imports
import pandas as pd
import numpy as np
from IPython.core import display as ICD

from common import JINJA_ENV, CDM_TABLES
from utils import auth
from gcloud.bq import BigQueryClient
from analytics.cdr_ops.notebook_utils import execute, IMPERSONATION_SCOPES, provenance_table_for

impersonation_creds = auth.get_impersonation_credentials(
    run_as, target_scopes=IMPERSONATION_SCOPES)
client = BigQueryClient(project_id, credentials=impersonation_creds)

# ## List of tables with person_id column

all_pid_tables_lists = []
for dataset in datasets:
    person_id_tables_query = JINJA_ENV.from_string('''
  SELECT table_name
  FROM `{{project}}.{{dataset}}.INFORMATION_SCHEMA.COLUMNS`
  WHERE column_name = "person_id"
  ''').render(project=project_id, dataset=dataset)
    pid_table_list = client.query(person_id_tables_query).to_dataframe().get(
        'table_name').to_list()
    all_pid_tables_lists.append(pid_table_list)
for table_list, dataset in zip(all_pid_tables_lists, datasets):
    print(dataset)
    ICD.display(table_list)
    print("\n")
# +
pids_query = JINJA_ENV.from_string('''
WITH
  pids AS ( SELECT person_id
  FROM `{{project}}.{{dataset}}.{{lookup_table}}`)
''').render(project=project_id,
            dataset=lookup_table_dataset,
            lookup_table=lookup_table)

rids_query = JINJA_ENV.from_string('''
WITH
  pids AS ( SELECT research_id as person_id
  FROM `{{project}}.{{dataset}}.{{lookup_table}}`)
''').render(project=project_id,
            dataset=lookup_table_dataset,
            lookup_table=lookup_table)
# -

# ## 1. Verify participants listed to be dropped in the lookup table are dropped from the pid_tables

# +
all_results = []
for dataset, pid_table_list in zip(datasets, all_pid_tables_lists):
    table_check_query = JINJA_ENV.from_string('''
  SELECT
    \'{{table_name}}\' AS table_name,
    Case when count(tb.person_id) = 0 then 'OK'
    ELSE 
    'PROBLEM' end as retraction_status
  FROM
    `{{project}}.{{dataset}}.{{table_name}}` as tb
  right JOIN
    pids as p
  USING(person_id)
  ''')

    queries_list = []
    is_deidentified = str('deid' in dataset).lower()

    for table in pid_table_list:
        queries_list.append(
            table_check_query.render(project=project_id,
                                     dataset=dataset,
                                     table_name=table))

    union_all_query = '\nUNION ALL\n'.join(queries_list)

    retraction_status_query = (f'{rids_query}\n{union_all_query}'
                               if is_deidentified == 'true' else
                               f'{pids_query}\n{union_all_query}')
    result = execute(client, retraction_status_query)
    all_results.append(result)

for result, dataset in zip(all_results, datasets):
    print(dataset)
    ICD.display(result)
    print("\n")
# -

# ## 2. Verify Row counts of source dataset minus the retracted participants  data is equal to the

all_results = []
for dataset, pid_table_list in zip(datasets, all_pid_tables_lists):
    table_row_counts_query = JINJA_ENV.from_string('''
  SELECT 
    '{{table_name}}' as table_id, count(*) as {{count}}
  FROM 
    `{{project}}.{{dataset}}.{{table_name}}`
        FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {{days}} DAY)
  {% if days != '0' %}
  WHERE person_id NOT IN (
      SELECT
          person_id
      FROM
          pids
      WHERE person_id IS NOT NULL
  )
  {% endif %}
  ''')

    old_row_counts_queries_list = []
    new_row_counts_queries_list = []
    is_deidentified = str('deid' in dataset).lower()

    for table in pid_table_list:
        old_row_counts_queries_list.append(
            table_row_counts_query.render(project=project_id,
                                          dataset=dataset,
                                          table_name=table,
                                          count='old_minus_aian_row_count',
                                          days='1'))

        new_row_counts_queries_list.append(
            table_row_counts_query.render(project=project_id,
                                          dataset=dataset,
                                          table_name=table,
                                          count='new_row_count',
                                          days='0'))

    old_row_counts_union_all_query = '\nUNION ALL\n'.join(
        old_row_counts_queries_list)

    new_row_counts_union_all_query = '\nUNION ALL\n'.join(
        new_row_counts_queries_list)

    old_retraction_table_count_query = (
        f'{rids_query}\n{old_row_counts_union_all_query}'
        if is_deidentified == 'true' else
        f'{pids_query}\n{old_row_counts_union_all_query}')

    new_retraction_table_count_query = (
        f'{rids_query}\n{new_row_counts_union_all_query}'
        if is_deidentified == 'true' else
        f'{pids_query}\n{new_row_counts_union_all_query}')

    old_count = execute(client, old_retraction_table_count_query)
    new_count = execute(client, new_retraction_table_count_query)

    results = pd.merge(old_count, new_count, on='table_id', how='outer')

    conditions = [
        (results['old_minus_aian_row_count'] == results['new_row_count']) |
        (results['old_minus_aian_row_count'] is None) &
        (results['new_row_count'] is None),
        (results['old_minus_aian_row_count'] is not None) &
        (results['new_row_count'] is None),
        (results['old_minus_aian_row_count'] is None) &
        (results['new_row_count'] is not None)
    ]
    table_count_status = ['OK', 'POTENTIAL PROBLEM', 'PROBLEM']
    results['table_count_status'] = np.select(conditions,
                                              table_count_status,
                                              default='PROBLEM')
    all_results.append(results)
for result, dataset in zip(all_results, datasets):
    print(dataset)
    ICD.display(result)
    print("\n")
