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
# This notebook validates in-place EHR data retraction for the sandbox datasets only, excluding ehr datasets.
# Note:
#   1. Tables that have 'person' in the name were not retracted from because the records come from RDR.
#   2. Tables with 'ehr' in the name, or that don't have one of the domain table names as part of the name, contain unknown records. This means
#      that we can't know for sure whether the records are from rdr or ehr.

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

from utils import auth
from gcloud.bq import BigQueryClient
from analytics.cdr_ops.notebook_utils import execute, IMPERSONATION_SCOPES
from common import (JINJA_ENV, AOU_REQUIRED, CARE_SITE, CONDITION_ERA, DOSE_ERA,
                    DRUG_ERA, FACT_RELATIONSHIP, JINJA_ENV, LOCATION, NOTE_NLP,
                    OBSERVATION_PERIOD, PAYER_PLAN_PERIOD, PROVIDER)

NON_PID_TABLES = [CARE_SITE, LOCATION, FACT_RELATIONSHIP, PROVIDER]
OTHER_PID_TABLES = [
    CONDITION_ERA, DOSE_ERA, DRUG_ERA, NOTE_NLP, OBSERVATION_PERIOD,
    PAYER_PLAN_PERIOD
]
domain_tables = set(AOU_REQUIRED + OTHER_PID_TABLES) - set(NON_PID_TABLES)

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
    {% if domain_id not in ['ehr', 'person', 'death', ''] %}
        Case when count(tb.{{domain_id}}_id) = 0 then 'OK'
        ELSE
        'PROBLEM' end as retraction_status,
        'EHR_domain_id' as source,
        '{{domain_id}}' as domain
    {% elif domain_id == 'death' %}
        Case when count(tb.person_id) = 0 then 'OK'
        ELSE
        'PROBLEM' end as retraction_status,
        'person_id' as source,
        '{{domain_id}}' as domain
    {% elif domain_id == 'person' %}
        Case when count(tb.person_id) = 0 then 'OK'
        ELSE
        'OK' end as retraction_status,
        'person_id' as source,
        '{{domain_id}}' as domain
    {% elif domain_id == 'ehr' or domain_id == '' %}
        Case when count(tb.person_id) = 0 then 'OK'
        ELSE
        'OK' end as retraction_status,
        'person_id' as source,
        'UNKNOWN' as domain
    {% endif %}
  FROM
    `{{project}}.{{dataset}}.{{table_name}}` as tb
    right JOIN
        pids as p
    USING(person_id)
    {% if domain_id not in ['ehr', 'person', 'death', ''] %}
      where {{domain_id}}_id > 2000000000000000
    {% endif %}
  ''')

    queries_list = []
    is_deidentified = str('deid' in dataset).lower()

    for table in pid_table_list:
        domain_id = ''
        if 'ehr' in table:
            domain_id = 'ehr'
        elif 'person' in table:
            domain_id = 'person'
        elif 'death' in table:
            domain_id = 'death'
        else:
            for name in domain_tables:
                if name in table:
                    if 'observation' in name:
                        if 'observation_period' in table:
                            domain_id = 'observation_period'
                        else:
                            domain_id = 'observation'
                        break
                    else:
                        domain_id = name
                        break

        queries_list.append(
            table_check_query.render(project=project_id,
                                     dataset=dataset,
                                     table_name=table,
                                     domain_id=domain_id))

    union_all_query = f"{' UNION ALL '.join(queries_list)} ORDER BY retraction_status"

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
    {% if domain_id not in ['ehr', 'person', 'death', ''] %}
        '{{table_name}}' as table_id, 
         count(*) as {{count}},
        'EHR_domain_id' as source,
         '{{domain_id}}' as domain
    {% elif domain_id == 'death' %}
        '{{table_name}}' as table_id, 
         count(*) as {{count}},
        'person_id'  as source,
         '{{domain_id}}' as domain
    {% elif domain_id == 'person' %}
        '{{table_name}}' as table_id, 
         count(*) as {{count}},
        'person_id'  as source,
         '{{domain_id}}' as domain
    {% elif domain_id == 'ehr' or domain_id == '' %}
        '{{table_name}}' as table_id, 
         count(*) as {{count}},
        'person_id'  as source,
         'UNKNOWN' as domain
    {% endif %}
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
  {% if domain_id not in ['ehr', 'person', 'death', ''] and days != '0' %}
    and {{domain_id}}_id > 2000000000000000
  {% elif domain_id not in ['ehr', 'person', 'death', ''] and days == '0' %}
    where {{domain_id}}_id > 2000000000000000
  {% endif %}
  ''')

    old_row_counts_queries_list = []
    new_row_counts_queries_list = []
    is_deidentified = str('deid' in dataset).lower()

    for table in pid_table_list:
        domain_id = ''
        if 'ehr' in table:
            domain_id = 'ehr'
        elif 'person' in table:
            domain_id = 'person'
        elif 'death' in table:
            domain_id = 'death'
        else:
            for name in domain_tables:
                if name in table:
                    if 'observation' in name:
                        if 'observation_period' in table:
                            domain_id = 'observation_period'
                        else:
                            domain_id = 'observation'
                        break
                    else:
                        domain_id = name
                        break

        old_row_counts_queries_list.append(
            table_row_counts_query.render(project=project_id,
                                          dataset=dataset,
                                          table_name=table,
                                          count='old_minus_aian_row_count',
                                          days='1',
                                          domain_id=domain_id))

        new_row_counts_queries_list.append(
            table_row_counts_query.render(project=project_id,
                                          dataset=dataset,
                                          table_name=table,
                                          count='new_row_count',
                                          days='0',
                                          domain_id=domain_id))

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

    results = pd.merge(old_count,
                       new_count,
                       on=['table_id', 'source', 'domain'],
                       how='inner')

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
    results = results.reindex(columns=[
        'table_id', 'old_minus_aian_row_count', 'new_row_count',
        'table_count_status', 'source', 'domain'
    ])
    all_results.append(results)
for result, dataset in zip(all_results, datasets):
    print(dataset)
    ICD.display(result)
    print("\n")
