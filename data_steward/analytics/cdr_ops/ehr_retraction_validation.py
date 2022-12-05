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
old_dataset = ''
new_dataset = ''
lookup_table = ''
lookup_table_dataset = ''
is_deidentified = 'true'
run_as = ''
# -

from common import JINJA_ENV, CDM_TABLES
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
''').render(project=project_id, new_dataset=new_dataset)
pid_tables = client.query(person_id_tables_query).to_dataframe().get(
    'table_name').to_list()
pid_table_list = [
    table for table in pid_tables
    if table in CDM_TABLES and table not in ('person', 'death',
                                             'survey_conduct')
]
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


def mapping_table_for(table, is_deidentified):
    if is_deidentified == 'false':
        return f'_mapping_{table}'
    else:
        return f'{table}_ext'


# ## 1. Verify ehr data for participants listed to be dropped in the lookup table are dropped from the pid_tables

# +
table_check_query = JINJA_ENV.from_string('''
SELECT
  \'{{table_name}}\' AS table_name,
  Case when count(tb.person_id) = 0 then 'OK'
  ELSE 
  'PROBLEM' end as retraction_status
FROM
  `{{project}}.{{dataset}}.{{table_name}}` as tb
inner JOIN
  pids as p
USING(person_id)
inner join `{{project}}.{{dataset}}.{{mapping_table}}` mp
on tb.{{table_name}}_id = mp.{{table_name}}_id
{% if is_deidentified == 'false' %}
where src_hpo_id != 'rdr' AND src_hpo_id is not null
{% else %}
where src_id != 'PPI/PM' AND src_id is not null
{% endif %}
''')
queries_list = []
for table in pid_table_list:
    queries_list.append(
        table_check_query.render(project=project_id,
                                 dataset=new_dataset,
                                 table_name=table,
                                 is_deidentified=is_deidentified,
                                 mapping_table=mapping_table_for(
                                     table, is_deidentified)))

union_all_query = '\nUNION ALL\n'.join(queries_list)

retraction_status_query = (f'{rids_query}\n{union_all_query}'
                           if is_deidentified == 'true' else
                           f'{pids_query}\n{union_all_query}')
execute(client, retraction_status_query)
# -

# ## 2. Verify Row counts of participants in source dataset minus the ehr data is equal to the row counts for the participants in new dataset.

# +
table_row_counts_query = JINJA_ENV.from_string('''
SELECT
\'{{table_name}}\' as table_name,
old_minus_ehr_row_count,
new_row_count,
case when old_minus_ehr_row_count = new_row_count then 'OK'
  when old_minus_ehr_row_count is null AND new_row_count is null then 'OK'
  when old_minus_ehr_row_count is NOT NULL and new_row_count is null then 'PROBLEM'
  when old_minus_ehr_row_count is NULL and new_row_count is not NULL then 'PROBLEM'
  ELSE 'PROBLEM'
  end as table_count_status FROM
(SELECT
  count(*) as old_minus_ehr_row_count,
  (select count(*)
  from `{{project}}.{{new_dataset}}.{{table_name}}` tb1
  inner join pids using (person_id) 
  ) as new_row_count
FROM
  `{{project}}.{{old_dataset}}.{{table_name}}` as tb
INNER JOIN
  pids as p USING (person_id)
INNER join `{{project}}.{{old_dataset}}.{{mapping_table}}` mp
on tb.{{table_name}}_id = mp.{{table_name}}_id
{% if is_deidentified == 'true' %}
where src_id = 'PPI/PM'
{% else %}
where src_hpo_id = 'rdr'
{% endif %}
)
''')
row_counts_queries_list = []
for table in pid_table_list:
    row_counts_queries_list.append(
        table_row_counts_query.render(project=project_id,
                                      old_dataset=old_dataset,
                                      new_dataset=new_dataset,
                                      is_deidentified=is_deidentified,
                                      table_name=table,
                                      mapping_table=mapping_table_for(
                                          table, is_deidentified)))

row_counts_union_all_query = '\nUNION ALL\n'.join(row_counts_queries_list)

retraction_table_count_query = (f'{rids_query}\n{row_counts_union_all_query}'
                                if is_deidentified == 'true' else
                                f'{pids_query}\n{row_counts_union_all_query}')
execute(client, retraction_table_count_query)
# -

# ## 3. Verify Death table retraction.

# +
table_check_query = JINJA_ENV.from_string('''
SELECT
  'Death' AS table_name,
  Case when count(tb.person_id) = 0 then 'OK'
  ELSE 
  'PROBLEM' end as retraction_status
FROM
  `{{project}}.{{dataset}}.death` as tb
right JOIN
  pids as p
USING(person_id)
''')
death_query = table_check_query.render(project=project_id,
                                       dataset=new_dataset,
                                       table_name=table)

retraction_status_query = (f'{rids_query}\n{death_query}'
                           if is_deidentified == 'true' else
                           f'{pids_query}\n{death_query}')
execute(client, retraction_status_query)
# -

# ## 4. Verify if mapping/ext tables are cleaned after retraction

# +
mapping_ext_check_query = JINJA_ENV.from_string('''
select \'{{mapping_table}}\' as table_name,
count(*) as extra_mapping_records
from `{{project}}.{{dataset}}.{{table_name}}` d
right join `{{project}}.{{dataset}}.{{mapping_table}}` mp using({{table_name}}_id)
where d.{{table_name}}_id is null
''')

mapping_queries_list = []
for table in pid_table_list:
    if table in CDM_TABLES and table not in ('death', 'person'):
        mapping_queries_list.append(
            mapping_ext_check_query.render(project=project_id,
                                           dataset=new_dataset,
                                           table_name=table,
                                           mapping_table=mapping_table_for(
                                               table, is_deidentified)))
mapping_check = '\nUNION ALL\n'.join(mapping_queries_list)
execute(client, mapping_check)
# -
