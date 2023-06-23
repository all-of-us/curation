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

# ---
# This notebook validates in-place retraction for the following datasets.  It assumes time travel is enabled for them:
# - `only_ehr`: EHR datasets
# - `only_ehr`: Unioned EHR datasets
# - `rdr_and_ehr`: RDR datasets
# - `rdr_and_ehr`: combined/ combined release datasets
# - `rdr_and_ehr`: CT deid base/ clean datasets
# - `rdr_and_ehr`: RT deid base/ clean datasets
#
# This notebook does NOT validate the following datasets:
# - `only_ehr`: All datasets except EHR and Unioned EHR datasets
# - For the datasets above, use `ehr_retraction_in_place_validation.py` instead.
#
# This notebook is not tested against the following datasets. Use it carefully:
# - `rdr_and_ehr`: Backup datasets
# - `rdr_and_ehr`: Sandbox datasets
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

pd.options.display.max_rows = 1000
# -

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

    for start in range(0, len(queries_list), 300):

        union_all_query = f"{'  UNION ALL  '.join(queries_list[start:start+300])} ORDER BY retraction_status;"

        retraction_status_query = (f'{rids_query}\n{union_all_query}'
                                   if is_deidentified == 'true' else
                                   f'{pids_query}\n{union_all_query}')
        result = execute(client, retraction_status_query)
        all_results.append((dataset, result))

for i, (dataset, result) in enumerate(all_results):
    print(f'{dataset} - {i+1}')
    ICD.display(result)
    print("\n")
# -

# ## 2. Verify Row counts of source dataset minus the retracted participants  data is equal to the

# +
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

    for start in range(0, len(queries_list), 200):

        old_row_counts_union_all_query = f"{'  UNION ALL  '.join(old_row_counts_queries_list[start:start+200])};"
        new_row_counts_union_all_query = f"{'  UNION ALL  '.join(new_row_counts_queries_list[start:start+200])};"

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

        all_results.append((dataset, results))

for i, (dataset, result) in enumerate(all_results):
    print(f'{dataset} - {i+1}')
    ICD.display(result)
    print("\n")

# -

# ## 3. Verify if retracted participants are dropped from fact_relationship table.
# No need to run this check for EHR dataset.

all_results = []
for dataset in datasets:
    fact_relationship_retraction = JINJA_ENV.from_string('''
  SELECT
    'fact_relationship' AS table_name,
    CASE
      WHEN COUNT(p1.person_id) = 0 AND COUNT(p2.person_id) = 0 THEN 'OK'
    ELSE
    'PROBLEM'
  END
    AS retraction_status
  FROM
    `{{project}}.{{dataset}}.fact_relationship` fr
  RIGHT JOIN
    pids p1
  ON
    fr.fact_id_1 = p1.person_id
  RIGHT JOIN
    pids p2
  ON
    fr.fact_id_2 = p2.person_id
  WHERE
    domain_concept_id_1 = 56
  ''').render(project=project_id, dataset=dataset)

    is_deidentified = str('deid' in dataset).lower()

    retraction_status_query = f'{pids_query}\n{fact_relationship_retraction}'
    # if is_deidentified == 'false':
    result = execute(client, retraction_status_query)
    all_results.append(result)
for result, dataset in zip(all_results, datasets):
    print(dataset)
    ICD.display(result)
    print("\n")

# ## 4. Verify if mapping/ext tables are cleaned after retraction
# No need to run this check for EHR dataset

all_results = []
for dataset, pid_table_list in zip(datasets, all_pid_tables_lists):
    mapping_ext_check_query = JINJA_ENV.from_string('''
  SELECT
    \'{{mapping_table}}\' as table_name,
  CASE
      WHEN COUNT(*) = 0 THEN 'OK'
    ELSE
    'PROBLEM'
  END
    AS clean_mapping_status
  FROM
    `{{project}}.{{dataset}}.{{table_name}}` d
  RIGHT JOIN
    `{{project}}.{{dataset}}.{{mapping_table}}` mp
  USING
    ({{table_name}}_id)
  WHERE
    d.{{table_name}}_id IS null
  ''')

    mapping_queries_list = []
    is_deidentified = str('deid' in dataset).lower()

    for table in pid_table_list:
        if table in CDM_TABLES and table not in ('death', 'person'):
            mapping_queries_list.append(
                mapping_ext_check_query.render(
                    project=project_id,
                    dataset=dataset,
                    table_name=table,
                    mapping_table=provenance_table_for(table, is_deidentified)))
    mapping_check = '\nUNION ALL\n'.join(mapping_queries_list)
    all_results.append(result)
    result = execute(client, mapping_check)
for result, dataset in zip(all_results, datasets):
    print(dataset)
    ICD.display(result)
    print("\n")
