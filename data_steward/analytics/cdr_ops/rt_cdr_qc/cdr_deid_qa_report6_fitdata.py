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

import urllib
import pandas as pd
from common import JINJA_ENV, FITBIT_TABLES
from utils import auth
from gcloud.bq import BigQueryClient
from analytics.cdr_ops.notebook_utils import execute, IMPERSONATION_SCOPES
pd.options.display.max_rows = 120

# + tags=["parameters"]
project_id = ""
pipeline=""
non_deid_fitbit=""
deid_cdr_fitbit=""
deid_cdr=""
com_cdr=""
truncation_date=""
maximum_age=""
fitbit_sandbox_dataset = ""
sleep_level_sandbox_table = ""
fitbit_dataset = ""
run_as=""

# +


impersonation_creds = auth.get_impersonation_credentials(
    run_as, target_scopes=IMPERSONATION_SCOPES)

client = BigQueryClient(project_id, credentials=impersonation_creds)
# -

# df will have a summary in the end
df = pd.DataFrame(columns = ['query', 'result'])

# This notebook was updated per [DC-1786].
#
#

# # Verify that the data newer than truncation_date (i.e.,11/26/2019) is truncated in fitbit tables (suppressed).
#
# DC-1046
#
# by adding m.shift back to deid_table and see if any date is newer than cutoff date.

# +
health_sharing_consent_check = JINJA_ENV.from_string('''SELECT
  \'{{table_name}}\' as table,
  COUNT(1) bad_rows
FROM
  `{{project_id}}.{{dataset_id}}.{{table_name}}` a
  JOIN `{{project_id}}.{{pipeline}}.pid_rid_mapping` m
ON m.research_id = a.person_id
WHERE DATE_ADD(date({{column_date}}), INTERVAL m.shift DAY) > '{{truncation_date}}'
  {% if secondary_date_column -%}
  OR DATE_ADD(date({{secondary_date_column}}), INTERVAL m.shift DAY) > '{{truncation_date}}'
  {% endif %}
''')
queries_list = []
for table in FITBIT_TABLES:
    queries_list.append(
        health_sharing_consent_check.render(project_id=project_id,
                                            dataset_id=non_deid_fitbit,
                                            table_name=table,
                                            column_date=date_columns[table],
                                            secondary_date_column=secondary_date_column.get(table),
                                            truncation_date=truncation_date,
                                            pipeline=pipeline))

union_all_query = '\nUNION ALL\n'.join(queries_list)
execute(client, union_all_query)
# -

# # Verify if that the fitdata data is removed FROM the fitbit tables for participants exceeding allowable age (maximum_age, i.e.,89). ((row counts = 0))
#
#
#
#
# DC-1001

# +
query = JINJA_ENV.from_string(
"""
SELECT
  \'{{table_name}}\' as table,
  COUNT(1) bad_rows
FROM
    `{{project_id}}.{{dataset_id}}.{{table_name}}` a
JOIN `{{project_id}}.{{pipeline}}.pid_rid_mapping` m
ON m.research_id = a.person_id
JOIN `{{project_id}}.{{combined_cdr}}.person` i
ON m.person_id = i.person_id
WHERE FLOOR(DATE_DIFF(CURRENT_DATE(), DATE(i.birth_datetime), YEAR)) > {{maximum_age}}
"""
)

queries_list = []
for table in FITBIT_TABLES:
    queries_list.append(
        query.render(project_id=project_id,
                                            dataset_id=non_deid_fitbit,
                                            table_name=table,
                                            # truncation_date=truncation_date,
                                            combined_cdr=combined_cdr,
                                            maximum_age=maximum_age,
                                            pipeline=pipeline))

union_all_query = '\nUNION ALL\n'.join(queries_list)
execute(client, union_all_query)
# -


# # Verify that correct date shift is applied to the fitbit data
#
# DC-1005
#
# objective:
#
# find the difference between the non-deid date and the deid date to validate that the dateshift is applied as specified in the map .
#
# the original code uses min(date) to have the difference, but not sure why min(), not max(), or any date.
#
#
# [DC-1786] date shifting should be checked against activity_summary, heart_rate_summary, heart_rate_minute_level, and steps_intraday.

# +
query = JINJA_ENV.from_string("""
WITH non_shifted_tables AS (
SELECT m.research_id,
CONCAT(m.research_id, '_', i.{{date_type}}) as i_newc
FROM `{{project_id}}.{{pipeline}}.pid_rid_mapping` m
JOIN `{{project_id}}.{{non_deid_fitbit}}.{{table}}` i
ON m.person_id = i.person_id
),

deid_and_reverse_shifted AS (
SELECT d.person_id,
CONCAT(d.person_id, '_', DATE_ADD(d.{{date_type}}, INTERVAL m.shift DAY)) AS d_newc
FROM `{{project_id}}.{{pipeline}}.pid_rid_mapping` m
JOIN `{{project_id}}.{{deid_cdr_fitbit}}.{{table}}` d
ON m.research_id = d.person_id
)

SELECT COUNT (*) n_row_not_pass FROM deid_and_reverse_shifted
WHERE d_newc NOT IN (SELECT i_newc FROM non_shifted_tables)
""")

query_list = []
results = []
for table in FITBIT_TABLES:
    q = (query.render(project_id=project_id,
                      table=table,
                      pipeline=pipeline,
                      date_type=date_columns[table],
                      combined_cdr=combined_cdr,
                      deid_cdr=deid_cdr,
                      non_deid_fitbit=non_deid_fitbit,
                      deid_cdr_fitbit=deid_cdr_fitbit,
                      truncation_date=truncation_date,
                      maximum_age=maximum_age))

    results.append(execute(client, q))

res_list = []
for query in query_list:
    res_list.append(execute(client, query))
    if res.eq(0).any().any():
         df = df.append({'query' : 'Query3.2 Date shifted in heart_rate_summary', 'result' : 'PASS'},
                ignore_index = True)
    else:
         df = df.append({'query' : 'Query3.2 Date shifted in heart_rate_summary', 'result' : 'Failure'},
                ignore_index = True)


display(results)
# -

# # Verify all person_ids in fitbit datasets exsit in deid_cdr person table
#
# [DC-1788] Add additional person existence check to Fitbit notebook
#
# This check should fail if a person_id in the activity_summary, heart_rate_summary, heart_rate_minute_level, or steps_intra_day tables does not exist in a corresponding RT de-identified dataset.

query = JINJA_ENV.from_string("""

WITH df1 AS (
SELECT '1' AS col , COUNT (DISTINCT person_id)  AS n_person_id_not_pass_activity_summary
FROM `{{project_id}}.{{deid_cdr_fitbit}}.activity_summary`
WHERE person_id NOT IN (SELECT person_id FROM `{{project_id}}.{{deid_cdr}}.person`)),

df2 AS (
SELECT '1' AS col, COUNT (DISTINCT person_id)  AS n_person_id_not_pass_heart_rate_summary
FROM `{{project_id}}.{{deid_cdr_fitbit}}.heart_rate_summary`
WHERE person_id NOT IN (SELECT person_id FROM `{{project_id}}.{{deid_cdr}}.person`)),

df3 AS (
SELECT '1' AS col,COUNT (DISTINCT person_id)  AS n_person_id_not_pass_heart_rate_minute_level
FROM `{{project_id}}.{{deid_cdr_fitbit}}.heart_rate_minute_level`
WHERE person_id NOT IN (SELECT person_id FROM `{{project_id}}.{{deid_cdr}}.person`)),

df4 AS (
SELECT '1' AS col,COUNT (DISTINCT person_id) AS n_person_id_not_pass_steps_intraday
FROM `{{project_id}}.{{deid_cdr_fitbit}}.steps_intraday` a
WHERE person_id NOT IN (SELECT person_id FROM `{{project_id}}.{{deid_cdr}}.person`))

SELECT * FROM df1
JOIN df2 USING (col)
JOIN df3 USING (col)
JOIN df4 USING (col)
""")
q =query.render(project_id=project_id,pipeline=pipeline,com_cdr=com_cdr,deid_cdr=deid_cdr,non_deid_fitbit=non_deid_fitbit,deid_cdr_fitbit=deid_cdr_fitbit,truncation_date=truncation_date,maximum_age=maximum_age)
df1=execute(client, q)
df1=df1.iloc[:,1:5]
if df1.loc[0].sum()==0:
 df = df.append({'query' : 'Query5 person_ids in fitbit exist in deid.person table', 'result' : 'PASS'},
                ignore_index = True)
else:
 df = df.append({'query' : 'Query5 person_ids in fitbit exist in deid.person table', 'result' : 'Failure'},
                ignore_index = True)
df1.T

# # Verify that all records with invalid level values are being dropped from sleep_level table
#
# DC-2606
#
# Queries the sandbox table for the corresponding cleaning rule (created in DC-2605) and outputs any records that
# are being dropped due to invalid level values. It also outputs any records in the sleep_level table that should
# have been dropped but were not.

# +
query = JINJA_ENV.from_string("""

WITH df1 AS (
  SELECT
    level, 'dropped' AS dropped_status, count(*) as n_violations
  FROM
    `{{project_id}}.{{fitbit_sandbox_dataset}}.{{sleep_level_sandbox_table}}`
  GROUP BY 1, 2
  ORDER BY n_violations
),

df2 AS (
  SELECT
    level, 'not dropped' AS dropped_status, count(*) as n_violations
  FROM
    `{{project_id}}.{{fitbit_dataset}}.sleep_level`
  WHERE
  (
      LOWER(level) NOT IN
          ('awake','light','asleep','deep','restless','wake','rem','unknown') OR level IS NULL
  )
  GROUP BY 1, 2
)

select *
from df1
UNION ALL
select *
from df2
""")

q =query.render(project_id=project_id,fitbit_sandbox_dataset=fitbit_sandbox_dataset,fitbit_dataset=fitbit_dataset,sleep_level_sandbox_table=sleep_level_sandbox_table)

df2 = execute(client, q)

if 'not dropped' not in set(df2['dropped_status']):
    df = df.append(
        {
            'query': 'Query6 no invalid level records in sleep_level table',
            'result': 'PASS'
        },
        ignore_index=True)
else:
    df = df.append(
        {
            'query': 'Query6 no invalid level records in sleep_level table',
            'result': 'Failure'
        },
        ignore_index=True)
df2


# -

# # Summary_fitdata

# +
def highlight_cells(val):
    color = 'red' if 'Failure' in val else 'white'
    return f'background-color: {color}'

df.style.applymap(highlight_cells).set_properties(**{'text-align': 'left'})
# -


