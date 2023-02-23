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
old_dataset: str = ""  # identifies the dataset name where participants are to be retracted
new_dataset: str = ""  # identifies the dataset name after retraction
lookup_table: str = ""  # lookup table name where the pids and rids needs to be retracted are stored
lookup_table_dataset: str = ""  # the sandbox dataset where lookup table is located
is_deidentified: str = "true"  # identifies if a dataset is pre or post deid
run_as: str = ""  # service account email to impersonate
# -

from common import JINJA_ENV, PIPELINE_TABLES, FITBIT_TABLES, CDM_TABLES
from utils import auth
from gcloud.bq import BigQueryClient
from analytics.cdr_ops.notebook_utils import execute, IMPERSONATION_SCOPES, provenance_table_for

impersonation_creds = auth.get_impersonation_credentials(
    run_as, target_scopes=IMPERSONATION_SCOPES)
client = BigQueryClient(project_id, credentials=impersonation_creds)

# ## List of tables with person_id column

person_id_tables_query = JINJA_ENV.from_string('''
SELECT table_name
FROM `{{project}}.{{new_dataset}}.INFORMATION_SCHEMA.COLUMNS`
WHERE column_name = "person_id"
''').render(project=project_id, new_dataset=new_dataset)
pid_table_list = client.query(person_id_tables_query).to_dataframe().get(
    'table_name').to_list()
pid_table_list

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
for table in pid_table_list:
    queries_list.append(
        table_check_query.render(project=project_id,
                                 dataset=new_dataset,
                                 table_name=table))

union_all_query = '\nUNION ALL\n'.join(queries_list)

retraction_status_query = (f'{rids_query}\n{union_all_query}'
                           if is_deidentified == 'true' else
                           f'{pids_query}\n{union_all_query}')
execute(client, retraction_status_query)
# -

# ## 2. Verify Row counts of source dataset minus the retracted participants  data is equal to the
# table row counts for all the pid tables in new dataset.

# +
import pandas as pd
days_interval = '0'

table_row_counts_query = JINJA_ENV.from_string('''

SELECT 
  '{{table_name}}' as table_id, count(*) as {{count}}
FROM 
  `{{project}}.{{new_dataset}}.{{table_name}}`
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

for table in pid_table_list:
    old_row_counts_queries_list.append(
        table_row_counts_query.render(project=project_id,
                                      new_dataset=new_dataset,
                                      table_name=table,
                                      count='old_minus_aian_row_count',
                                      days=days_interval))
    new_row_counts_queries_list.append(
        table_row_counts_query.render(project=project_id,
                                      new_dataset=new_dataset,
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
results
# -

# ## 3. Verify if retracted participants are dropped from fact_relationship table.

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
''').render(project=project_id, dataset=new_dataset)
retraction_status_query = f'{pids_query}\n{fact_relationship_retraction}'
if is_deidentified == 'false':
    execute(client, retraction_status_query)

# ## 4. Verify if mapping/ext tables are cleaned after retraction

# +
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
for table in pid_table_list:
    if table in CDM_TABLES and table not in ('death', 'person'):
        mapping_queries_list.append(
            mapping_ext_check_query.render(project=project_id,
                                           dataset=new_dataset,
                                           table_name=table,
                                           mapping_table=provenance_table_for(
                                               table, is_deidentified)))
mapping_check = '\nUNION ALL\n'.join(mapping_queries_list)

execute(client, mapping_check)
