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
project_id = ''
q3_dataset = ''
q4_dataset = ''
lookup_table = ''
lookup_table_dataset = ''
is_deidentified = 'true'
run_as = ''
# -

from common import JINJA_ENV, PIPELINE_TABLES, FITBIT_TABLES, CDM_TABLES
from utils import auth
from gcloud.bq import BigQueryClient
from analytics.cdr_ops.notebook_utils import execute, IMPERSONATION_SCOPES, render_message

impersonation_creds = auth.get_impersonation_credentials(
    run_as, target_scopes=IMPERSONATION_SCOPES)
client = BigQueryClient(project_id, credentials=impersonation_creds)

# ## List of tables with person_id column

person_id_tables_query = JINJA_ENV.from_string('''
SELECT table_name
FROM `{{project}}.{{new_dataset}}.INFORMATION_SCHEMA.COLUMNS`
WHERE column_name = "person_id"
''').render(project=project_id, new_dataset=q4_dataset)
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
                                 dataset=q4_dataset,
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
table_row_counts_query = JINJA_ENV.from_string('''
SELECT
\'{{table_name}}\' as table_name,
q4r3_minus_aian_row_count,
q4r4_row_count,
case when q4r3_minus_aian_row_count = q4r4_row_count then 'OK'
  when q4r3_minus_aian_row_count is null AND q4r4_row_count is null then 'OK'
  when q4r3_minus_aian_row_count is NOT NULL and q4r4_row_count is null then 'PROBLEM'
  when q4r3_minus_aian_row_count is NULL and q4r4_row_count is not NULL then 'PROBLEM'
  ELSE 'PROBLEM'
  end as table_count_status FROM
(SELECT
  count(*) as q4r3_minus_aian_row_count,
  (select row_count
  from `{{project}}.{{new_dataset}}.__TABLES__` 
  WHERE table_id = \'{{table_name}}\') as q4r4_row_count
FROM
  `{{project}}.{{old_dataset}}.{{table_name}}` as tb
left JOIN
  pids as p USING (person_id)
 where p.person_id is null)
''')
row_counts_queries_list = []
for table in pid_table_list:
    row_counts_queries_list.append(
        table_row_counts_query.render(project=project_id,
                                      old_dataset=q3_dataset,
                                      new_dataset=q4_dataset,
                                      table_name=table))

row_counts_union_all_query = '\nUNION ALL\n'.join(row_counts_queries_list)

retraction_table_count_query = (f'{rids_query}\n{row_counts_union_all_query}'
                                if is_deidentified == 'true' else
                                f'{pids_query}\n{row_counts_union_all_query}')
execute(client, retraction_table_count_query)
# -

# ## 3. Verify if retracted participants are dropped from fact_relationship table.

fact_relationship_retraction = JINJA_ENV.from_string('''
SELECT 'fact_relationship' as table_name,
Case when count(p1.person_id) = 0 AND count(p2.person_id) = 0 then 'OK'
  ELSE 'PROBLEM' end as retraction_status
FROM `{{project}}.{{dataset}}.fact_relationship`  fr
RIGHT JOIN pids p1
on fr.fact_id_1 = p1.person_id 
RIGHT JOIN pids p2
on fr.fact_id_2 = p2.person_id
where domain_concept_id_1 = 56
''').render(project=project_id, dataset=q4_dataset)
retraction_status_query = (f'{rids_query}\n{fact_relationship_retraction}'
                           if is_deidentified == 'true' else
                           f'{pids_query}\n{fact_relationship_retraction}')
if is_deidentified == 'false':
    execute(client, retraction_status_query)

# ## 4. Verify if mapping/ext tables are cleaned after retraction

# +
mapping_ext_check_query = JINJA_ENV.from_string('''
select \'{{mapping_table}}\' as table_name,
count(*) as extra_mapping_records
from `{{project}}.{{dataset}}.{{table_name}}` d
right join `{{project}}.{{dataset}}.{{mapping_table}}` mp using({{table_name}}_id)
where d.{{table_name}}_id is null
''')


def mapping_table_for(table, is_deidentified):
    if is_deidentified == 'false':
        return f'_mapping_{table}'
    else:
        return f'{table}_ext'


mapping_queries_list = []
for table in pid_table_list:
    if table in CDM_TABLES and table not in ('death', 'person'):
        mapping_queries_list.append(
            mapping_ext_check_query.render(project=project_id,
                                           dataset=q4_dataset,
                                           table_name=table,
                                           mapping_table=mapping_table_for(
                                               table, is_deidentified)))
mapping_check = '\nUNION ALL\n'.join(mapping_queries_list)

execute(client, mapping_check)
