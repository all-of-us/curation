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

from common import JINJA_ENV, CDM_TABLES
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

# ## 1. Verify ehr data for participants listed to be dropped in the lookup table are dropped from the pid_tables
#
# Here we are checking for no EHR data in a given dataset. We are joining the domain tables onto the provenence tables to identify if a particular record is from EHR submission.

# +
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
execute(client, retraction_status_query)
# -

# ## 2. Row counts of participants in source dataset minus the ehr data is equal to the row counts for the participants in new dataset.
#
# We expect PPI/PM data to exist for the listed participants even after the retraction. So here we are checking the record count of the source data minus the EHR records is equal to the record count post retraction.

# +
table_row_counts_query = JINJA_ENV.from_string('''
SELECT
  \'{{table_name}}\' as table_name,
old_minus_ehr_row_count,
  new_row_count,
  CASE
    WHEN old_minus_ehr_row_count = new_row_count THEN 'OK'
    WHEN old_minus_ehr_row_count IS NULL AND new_row_count IS NULL THEN 'OK'
    WHEN old_minus_ehr_row_count IS NOT NULL AND new_row_count IS NULL THEN 'POTENTIAL PROBLEM'
    WHEN old_minus_ehr_row_count IS NULL AND new_row_count IS NOT NULL THEN 'PROBLEM'
  ELSE
  'PROBLEM'
END
  AS table_count_status
FROM (
  SELECT
    COUNT(*) AS old_minus_ehr_row_count,
    (
    SELECT
      COUNT(*)
    FROM
      `{{project}}.{{new_dataset}}.{{table_name}}` tb1
    INNER JOIN
      pids
    USING
      (person_id) ) AS new_row_count
  FROM
    `{{project}}.{{old_dataset}}.{{table_name}}` AS tb
  INNER JOIN
    pids AS p
  USING
    (person_id)
  INNER JOIN
    `{{project}}.{{old_dataset}}.{{mapping_table}}` mp
  USING
    ({{table_name}}_id) 
  {% if is_deidentified.lower() == 'true' %}
  WHERE
    src_id = 'PPI/PM' 
  {% else %}
  WHERE
    src_hpo_id = 'rdr' 
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
                                      mapping_table=provenance_table_for(
                                          table, is_deidentified)))

row_counts_union_all_query = '\nUNION ALL\n'.join(row_counts_queries_list)

retraction_table_count_query = (f'{rids_query}\n{row_counts_union_all_query}'
                                if is_deidentified == 'true' else
                                f'{pids_query}\n{row_counts_union_all_query}')
execute(client, retraction_table_count_query)
# -

# ## 3. Verify Death table retraction.
#
# Death table is copied as-is from EHR dataset as we do not receive death data via the RDR export yett. Death table is missing the provenence table as it doesn't have a domain_id column so we will use person_id column to identify any records.

# +
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

retraction_status_query = (f'{rids_query}\n{death_query}'
                           if is_deidentified.lower() == 'true' else
                           f'{pids_query}\n{death_query}')
execute(client, retraction_status_query)
# -

# ## 4. Verify if mapping/ext tables are cleaned after retraction
#

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
