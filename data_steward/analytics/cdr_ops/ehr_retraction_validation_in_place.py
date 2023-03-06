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
new_datasets: list = []  # identifies the dataset names after retraction
lookup_table: str = ""  # lookup table name where the pids and rids needs to be retracted are stored
lookup_table_dataset: str = ""  # the sandbox dataset where lookup table is located
is_deidentified: str = "true"  # identifies if a dataset is pre or post deid
run_as: str = ""  # service account email to impersonate
# -
# Third party imports
import pandas as pd
import numpy as np
from IPython.core import display as ICD

# Project Imports
from common import JINJA_ENV, CDM_TABLES
from utils import auth
from gcloud.bq import BigQueryClient
from analytics.cdr_ops.notebook_utils import execute, IMPERSONATION_SCOPES, provenance_table_for

impersonation_creds = auth.get_impersonation_credentials(
    run_as, target_scopes=IMPERSONATION_SCOPES)
client = BigQueryClient(project_id, credentials=impersonation_creds)

# ## List of tables with person_id column

all_pid_table_list = []
for new_dataset in new_datasets:
    person_id_tables_query = JINJA_ENV.from_string('''
  SELECT table_name
  FROM `{{project}}.{{new_dataset}}.INFORMATION_SCHEMA.COLUMNS`
  WHERE column_name = "person_id"
  ''').render(project=project_id, new_dataset=new_dataset)
    pid_tables = client.query(person_id_tables_query).to_dataframe().get(
        'table_name').to_list()
    pid_table_list = [
        table for table in pid_tables
        # Ignoring person and survey_conduct as we copy both the tables as-is from RDR,
        # and we don't have to retract from them
        # Ignoring Death as it does not have mapping table and below validation queries rely on mapping table.
        # A separate check for death table is added to verify retraction.
        if table in CDM_TABLES and table not in ('person', 'death',
                                                 'survey_conduct')
    ]
    all_pid_table_list.append(pid_table_list)
for table_list, new_dataset in zip(all_pid_table_list, new_datasets):
    print(new_dataset)
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
rids_query
# -

# ## 1. Verify ehr data for participants listed to be dropped in the lookup table are dropped from the pid_tables
#
# Here we are checking for no EHR data in a given dataset. We are joining the domain tables onto the provenence tables to identify if a particular record is from EHR submission.

# +
all_results = []
for new_dataset in new_datasets:
    table_check_query = JINJA_ENV.from_string('''
  SELECT
    \'{{table_name}}\' AS table_name,
    CASE
      WHEN COUNT(tb.person_id) = 0 THEN 'OK'
    ELSE
    'PROBLEM'
  END
    AS ehr_retraction_status
  FROM
    `{{project}}.{{dataset}}.{{table_name}}` AS tb
  INNER JOIN
    pids AS p
  USING
    (person_id)
  INNER JOIN
    `{{project}}.{{dataset}}.{{mapping_table}}` mp
  ON
    tb.{{table_name}}_id = mp.{{table_name}}_id
  {% if is_deidentified.lower() == 'false' %}
  WHERE REGEXP_CONTAINS(src_hpo_id, r'(?i)ehr') AND src_hpo_id is not null
  {% else %}
  WHERE REGEXP_CONTAINS(src_id, r'(?i)EHR Site') AND src_id is not null
  {% endif %}
  ''')

    queries_list = []
    is_deidentified = str('deid' in new_dataset).lower()

    for table in pid_table_list:
        queries_list.append(
            table_check_query.render(project=project_id,
                                     dataset=new_dataset,
                                     table_name=table,
                                     is_deidentified=is_deidentified,
                                     mapping_table=provenance_table_for(
                                         table, is_deidentified)))

    union_all_query = '\nUNION ALL\n'.join(queries_list)

    retraction_status_query = (f'{rids_query}\n{union_all_query}'
                               if is_deidentified == 'true' else
                               f'{pids_query}\n{union_all_query}')
    result = execute(client, retraction_status_query)
    all_results.append(result)

for result, new_dataset in zip(all_results, new_datasets):
    print(new_dataset)
    ICD.display(result)
    print("\n")
# -

# ## 2. Row counts of participants in source dataset minus the ehr data is equal to the row counts for the participants in new dataset.
#
# We expect PPI/PM data to exist for the listed participants even after the retraction. So here we are checking the record count of the source data minus the EHR records is equal to the record count post retraction.

all_results = []
for new_dataset in new_datasets:
    table_row_counts_query = JINJA_ENV.from_string('''
  SELECT 
    '{{table_name}}' as table_id, count(*) as {{count}}
  FROM 
    `{{project}}.{{new_dataset}}.{{table_name}}`
        FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {{days}} DAY)
  {% if days == '0' %}
      INNER JOIN
          pids
      USING
          (person_id) 
  {% else %}
    INNER JOIN
        pids AS p
      USING
        (person_id)
      INNER JOIN
        `{{project}}.{{new_dataset}}.{{mapping_table}}` mp
      USING
        ({{table_name}}_id) 
      {% if is_deidentified.lower() == 'true' %}
      WHERE
        src_id = 'PPI/PM' 
      {% else %}
      WHERE
        src_hpo_id = 'rdr' 
      {% endif %} 
  {% endif %}
  ''')

    old_row_counts_queries_list = []
    new_row_counts_queries_list = []
    is_deidentified = str('deid' in new_dataset).lower()

    for table in pid_table_list:
        old_row_counts_queries_list.append(
            table_row_counts_query.render(project=project_id,
                                          new_dataset=new_dataset,
                                          is_deidentified=is_deidentified,
                                          table_name=table,
                                          mapping_table=provenance_table_for(
                                              table, is_deidentified),
                                          count='old_minus_aian_row_count',
                                          days='1'))

        new_row_counts_queries_list.append(
            table_row_counts_query.render(project=project_id,
                                          new_dataset=new_dataset,
                                          is_deidentified=is_deidentified,
                                          table_name=table,
                                          mapping_table=provenance_table_for(
                                              table, is_deidentified),
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
for result, new_dataset in zip(all_results, new_datasets):
    print(new_dataset)
    ICD.display(result)
    print("\n")

# ## 3. Verify Death table retraction.
#
# Death table is copied as-is from EHR dataset as we do not receive death data via the RDR export yett. Death table is missing the provenence table as it doesn't have a domain_id column so we will use person_id column to identify any records.

all_results = []
for new_dataset in new_datasets:
    table_check_query = JINJA_ENV.from_string('''
  SELECT
  'death' AS table_name,
  CASE
  WHEN COUNT(tb.person_id) = 0 THEN 'OK'
  ELSE
  'PROBLEM'
  END
  AS retraction_status
  FROM
  `{{project}}.{{dataset}}.death` AS tb
  RIGHT JOIN
  pids AS p
  USING
  (person_id)
  ''')
    death_query = table_check_query.render(project=project_id,
                                           dataset=new_dataset,
                                           table_name=table)

    is_deidentified = str('deid' in new_dataset).lower()

    retraction_status_query = (f'{rids_query}\n{death_query}'
                               if is_deidentified.lower() == 'true' else
                               f'{pids_query}\n{death_query}')
    result = execute(client, retraction_status_query)
    all_results.append(result)
for result, new_dataset in zip(all_results, new_datasets):
    print(new_dataset)
    ICD.display(result)
    print("\n")

# ## 4. Verify if mapping/ext tables are cleaned after retraction
#

all_results = []
for new_dataset in new_datasets:
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
    is_deidentified = str('deid' in new_dataset).lower()

    for table in pid_table_list:
        if table in CDM_TABLES and table not in ('death', 'person'):
            mapping_queries_list.append(
                mapping_ext_check_query.render(
                    project=project_id,
                    dataset=new_dataset,
                    table_name=table,
                    mapping_table=provenance_table_for(table, is_deidentified)))
    mapping_check = '\nUNION ALL\n'.join(mapping_queries_list)
    result = execute(client, mapping_check)
    all_results.append(result)
for result, new_dataset in zip(all_results, new_datasets):
    print(new_dataset)
    ICD.display(result)
    print("\n")
