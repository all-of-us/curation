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

# # QA queries on new CDR_deid fitdata
#

import pandas as pd
from common import JINJA_ENV, FITBIT_TABLES
from utils import auth
from gcloud.bq import BigQueryClient
from analytics.cdr_ops.notebook_utils import execute, IMPERSONATION_SCOPES
pd.options.display.max_rows = 120

# + tags=["parameters"]
project_id = ""  # aou project id
pipeline = ""  # pipeline tables dataset
non_deid_fitbit = ""  # fitbt dataset prior to deidentification
deid_cdr_fitbit = ""  # fitbit dataset post deidentification(either tier)
deid_cdr = ""  # deidentified dataset within the current cdr.
combined_cdr = ""  # fully identified dataset within the current cdr
truncation_date = ""  # Current cdr cutoff date
maximum_age = 89  # Maximum age
run_as = ""  # # using impersonation, run all these queries as this service account"
# -

date_columns = {
    'activity_summary': 'date',
    'heart_rate_summary': 'date',
    'heart_rate_minute_level': 'datetime',
    'steps_intraday': 'datetime',
    'sleep_level': 'sleep_date',
    'sleep_daily_summary': 'sleep_date',
    'device': 'device_date',
}
secondary_date_column = {
    'device': 'last_sync_time',
    'sleep_level': 'start_datetime',
}

# +
impersonation_creds = auth.get_impersonation_credentials(
    run_as, target_scopes=IMPERSONATION_SCOPES)

client = BigQueryClient(project_id, credentials=impersonation_creds)
# -

# df will have a summary in the end
summary = pd.DataFrame(columns=['query', 'result'])

# This notebook was updated per [DC-1786].
#
#

# # Verify that the data newer than truncation_date (i.e.,11/26/2019) is truncated in fitbit tables (suppressed).
#
# DC-1046
#
# by adding m.shift back to deid_table and see if any date is newer than cutoff date.

# +
query = JINJA_ENV.from_string("""
SELECT
  '{{table}}' as table,
  COUNT(1) bad_rows
FROM
  `{{project_id}}.{{dataset_id}}.{{table}}` a
  JOIN `{{project_id}}.{{pipeline}}.pid_rid_mapping` m
ON m.research_id = a.person_id
WHERE DATE_ADD(date({{column_date}}), INTERVAL m.shift DAY) > '{{truncation_date}}'
  {% if secondary_date_column -%}
  OR DATE_ADD(date({{secondary_date_column}}), INTERVAL m.shift DAY) > '{{truncation_date}}'
  {% endif %}
""")

queries_list = []
for table in FITBIT_TABLES:
    queries_list.append(
        query.render(project_id=project_id,
                     dataset_id=non_deid_fitbit,
                     table=table,
                     column_date=date_columns[table],
                     secondary_date_column=secondary_date_column.get(table),
                     truncation_date=truncation_date,
                     pipeline=pipeline))

union_all_query = '\nUNION ALL\n'.join(queries_list)
result = execute(client, union_all_query)

if sum(result['bad_rows']) == 0:
    summary = summary.append(
        {
            'query': 'Data Truncation Query',
            'result': 'PASS'
        }, ignore_index=True)
else:
    summary = summary.append(
        {
            'query': 'Data Truncation Query',
            'result': 'Failure'
        },
        ignore_index=True)
result
# -

# # Verify if that the fitdata data is removed FROM the fitbit tables for participants exceeding allowable age (maximum_age, i.e.,89). ((row counts = 0))
#
#
#
#
# DC-1001

# +
query = JINJA_ENV.from_string("""
SELECT
  '{{table}}' as table,
  COUNT(1) bad_rows
FROM
    `{{project_id}}.{{dataset_id}}.{{table}}` a
JOIN `{{project_id}}.{{pipeline}}.pid_rid_mapping` m
ON m.research_id = a.person_id
JOIN `{{project_id}}.{{combined_cdr}}.person` i
ON m.person_id = i.person_id
WHERE FLOOR(DATE_DIFF(CURRENT_DATE(), DATE(i.birth_datetime), YEAR)) > {{maximum_age}}
""")

queries_list = []
for table in FITBIT_TABLES:
    queries_list.append(
        query.render(project_id=project_id,
                     dataset_id=non_deid_fitbit,
                     table=table,
                     combined_cdr=combined_cdr,
                     maximum_age=maximum_age,
                     pipeline=pipeline))

union_all_query = '\nUNION ALL\n'.join(queries_list)
result = execute(client, union_all_query)

if sum(result['bad_rows']) == 0:
    summary = summary.append({
        'query': 'Date Shift Query',
        'result': 'PASS'
    },
                             ignore_index=True)
else:
    summary = summary.append({
        'query': 'Date Shift Query',
        'result': 'Failure'
    },
                             ignore_index=True)
result
# -

# # Verify that correct date shift is applied to the fitbit data
#
# DC-1005
#
# objective:
#
# find the difference between the non-deid date and the deid date to validate that the dateshift is applied as specified in the map.  the original code uses min(date) to have the difference, but not sure why min(), not max(), or any date.
#
# **Note:  Should a failure occur during this (long) query, it is advisable to replace `FITBIT_TABLES` with the table in question**
#
# [DC-1786] date shifting should be checked against activity_summary, heart_rate_summary, heart_rate_minute_level, and steps_intraday.

# +
query = JINJA_ENV.from_string("""
SELECT
  '{{table}}' as table,
  COUNT(1) bad_rows
FROM (SELECT d.person_id,
      CONCAT(d.person_id, '_', DATE_ADD(d.{{date_type}}, INTERVAL m.shift DAY)) AS d_newc
      FROM `{{project_id}}.{{pipeline}}.pid_rid_mapping` m
      JOIN `{{project_id}}.{{deid_cdr_fitbit}}.{{table}}` d
      ON m.research_id = d.person_id)
WHERE d_newc NOT IN (SELECT
      CONCAT(m.research_id, '_', i.{{date_type}}) as i_newc
      FROM `{{project_id}}.{{pipeline}}.pid_rid_mapping` m
      JOIN `{{project_id}}.{{non_deid_fitbit}}.{{table}}` i
      ON m.person_id = i.person_id)
""")

queries_list = []
for table in FITBIT_TABLES:
    queries_list.append(
        query.render(project_id=project_id,
                     table=table,
                     pipeline=pipeline,
                     date_type=date_columns[table],
                     deid_cdr=deid_cdr,
                     non_deid_fitbit=non_deid_fitbit,
                     deid_cdr_fitbit=deid_cdr_fitbit))

union_all_query = '\nUNION ALL\n'.join(queries_list)
result = execute(client, union_all_query)

if sum(result['bad_rows']) == 0:
    summary = summary.append({
        'query': 'Date Shift Query',
        'result': 'PASS'
    },
                             ignore_index=True)
else:
    summary = summary.append({
        'query': 'Date Shift Query',
        'result': 'Failure'
    },
                             ignore_index=True)
result
# -

# # Verify that the participants are correctly mapped to their Research ID
#
# DC-1000

# +
query = JINJA_ENV.from_string("""
SELECT
  '{{table}}' as table,
  COUNT(1) bad_rows
FROM (SELECT DISTINCT i.person_id  AS non_deid_pid, m.research_id
      FROM `{{project_id}}.{{pipeline}}.pid_rid_mapping` m
      JOIN `{{project_id}}.{{non_deid_fitbit}}.{{table}}` i
      ON m.person_id = i.person_id)
JOIN (SELECT DISTINCT d.person_id  AS deid_pid,m.research_id
     FROM `{{project_id}}.{{pipeline}}.pid_rid_mapping` m
     JOIN `{{project_id}}.{{deid_cdr_fitbit}}.{{table}}` d
     ON d.person_id = m.research_id)
USING (research_id)
WHERE research_id != deid_pid
""")

queries_list = []
for table in FITBIT_TABLES:
    queries_list.append(
        query.render(project_id=project_id,
                     table=table,
                     pipeline=pipeline,
                     non_deid_fitbit=non_deid_fitbit,
                     deid_cdr_fitbit=deid_cdr_fitbit))

union_all_query = '\nUNION ALL\n'.join(queries_list)
result = execute(client, union_all_query)

if sum(result['bad_rows']) == 0:
    summary = summary.append({
        'query': 'Pid Rid Query',
        'result': 'PASS'
    },
                             ignore_index=True)
else:
    summary = summary.append({
        'query': 'Pid Rid Query',
        'result': 'Failure'
    },
                             ignore_index=True)
result
# -

# # Verify all person_ids in fitbit datasets exsit in deid_cdr person table
#
# [DC-1788] Add additional person existence check to Fitbit notebook
#
# This check should fail if a person_id in the activity_summary, heart_rate_summary, heart_rate_minute_level, or steps_intra_day tables does not exist in a corresponding RT de-identified dataset.

# +
query = JINJA_ENV.from_string("""
SELECT
  '{{table}}' as table,
  COUNT(1) bad_rows
FROM `{{project_id}}.{{deid_cdr_fitbit}}.{{table}}`
WHERE person_id NOT IN (SELECT person_id FROM `{{project_id}}.{{deid_cdr}}.person`)
""")
queries_list = []
for table in FITBIT_TABLES:
    queries_list.append(
        query.render(project_id=project_id,
                     table=table,
                     deid_cdr=deid_cdr,
                     deid_cdr_fitbit=deid_cdr_fitbit))

union_all_query = '\nUNION ALL\n'.join(queries_list)
result = execute(client, union_all_query)

if sum(result['bad_rows']) == 0:
    summary = summary.append(
        {
            'query': 'Valid person_ids Query',
            'result': 'PASS'
        },
        ignore_index=True)
else:
    summary = summary.append(
        {
            'query': 'Valid person_ids Query',
            'result': 'Failure'
        },
        ignore_index=True)
result

# -

# # Verify proper deidentification of device_id
#
# DC-3280
# If the query fails use these hints to help investigate.
# * not_research_device_ids  - These 'deidentified' device_ids are not in the masking table. Did the CR run that updates the masking table with new research_device_ids?
# * check_uuid_format - These deidentified device_ids should have the uuid format and not NULL.
# * check_uuid_unique - All deidentified device_ids for a device_id/person_id should be unique. If not, replace the non-unique research_device_ids in the masking table with new UUIDs.
#
# Device_id is being deidentified by a combination of the following cleaning rules.
# 1. generate_research_device_ids - Keeps the masking table(wearables_device_id_masking) updated.
# 2. deid_fitbit_device_id - Replaces the device_id with the deidentified device_id(research_device_id).
#

# +
query = JINJA_ENV.from_string("""
WITH not_research_device_ids AS (
SELECT DISTINCT device_id
FROM `{{project_id}}.{{deid_cdr_fitbit}}.device`
WHERE device_id NOT IN (SELECT research_device_id FROM `{{project_id}}.{{pipeline}}.wearables_device_id_masking`)
),
check_uuid_format AS (
SELECT DISTINCT device_id
FROM `{{project_id}}.{{deid_cdr_fitbit}}.device`
WHERE NOT REGEXP_CONTAINS(device_id, r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$')
OR device_id IS NULL
),
check_uuid_unique AS (
SELECT DISTINCT device_id
FROM `{{project_id}}.{{deid_cdr_fitbit}}.device`
GROUP BY person_id, device_id
HAVING COUNT(device_id) > 1
)
SELECT 'not_research_device_ids' as issue, COUNT(*) as bad_rows
FROM not_research_device_ids
UNION ALL
SELECT 'uuid_incorrect_format' as issue, COUNT(*) as bad_rows
FROM check_uuid_format
UNION ALL
SELECT 'uuid_not_unique' as issue, COUNT(*) as bad_rows
FROM check_uuid_unique
  
""").render(project_id=project_id,
            pipeline=pipeline,
            deid_cdr_fitbit=deid_cdr_fitbit)

result = execute(client, query)

if sum(result['bad_rows']) == 0:
    summary = summary.append(
        {
            'query':
                'Query7 device_id was deidentified properly for all records.',
            'result':
                'PASS'
        },
        ignore_index=True)
else:
    summary = summary.append(
        {
            'query':
                'Query7 device_id was not deidentified properly. See query description for hints.',
            'result':
                'Failure'
        },
        ignore_index=True)
result

# -

# # Check deidentification of src_ids
#
# DC-3376
#

# +
src_check = JINJA_ENV.from_string("""
SELECT
  '{{table_name}}' as table,
  COUNT(1) bad_rows
FROM
  `{{project}}.{{dataset}}.{{table_name}}`
WHERE
    NOT REGEXP_CONTAINS(src_id, r'(?i)Participant Portal')                            
OR 
    src_id IS NULL
""")

queries_list = []
for table in FITBIT_TABLES:
    queries_list.append(
        src_check.render(project=project_id,
                         dataset=deid_cdr_fitbit,
                         table_name=table))
union_all_query = '\nUNION ALL\n'.join(queries_list)

execute(client, union_all_query)

# -

# # Summary_fitdata


# +
def highlight_cells(val):
    color = 'red' if 'Failure' in val else 'white'
    return f'background-color: {color}'


summary.style.applymap(highlight_cells).set_properties(**{'text-align': 'left'})
# -
