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
from common import JINJA_ENV
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

query = JINJA_ENV.from_string("""
WITH df1 AS (
SELECT '1' as col ,COUNT (*) AS n_row_not_pass_activity_summary
FROM `{{project_id}}.{{deid_cdr_fitbit}}.activity_summary`  a
JOIN `{{project_id}}.{{pipeline}}.pid_rid_mapping` m
ON m.research_id = a.person_id
WHERE DATE_ADD(date(a.date), INTERVAL m.shift DAY) > '{{truncation_date}}'),

df2 AS (
SELECT '1' as col ,COUNT (*) n_row_not_pass_heart_rate_summary
FROM `{{project_id}}.{{deid_cdr_fitbit}}.heart_rate_summary`  a
JOIN `{{project_id}}.{{pipeline}}.pid_rid_mapping` m
ON m.research_id = a.person_id
WHERE DATE_ADD(date(a.date), INTERVAL m.shift DAY) > '{{truncation_date}}' ),

df3 AS (
SELECT '1' as col ,COUNT (*) n_row_not_pass_heart_rate_minute_level
FROM `{{project_id}}.{{deid_cdr_fitbit}}.heart_rate_minute_level`  a
JOIN `{{project_id}}.{{pipeline}}.pid_rid_mapping` m
ON m.research_id = a.person_id
WHERE DATE_ADD(date(a.datetime), INTERVAL m.shift DAY) > '{{truncation_date}}'),

df4 AS (
SELECT '1' as col ,COUNT (*) n_row_not_pass_steps_intraday
FROM `{{project_id}}.{{deid_cdr_fitbit}}.steps_intraday`  a
JOIN `{{project_id}}.{{pipeline}}.pid_rid_mapping` m
ON m.research_id = a.person_id
WHERE DATE_ADD(date(a.datetime), INTERVAL m.shift DAY) > '{{truncation_date}}' )

SELECT * from df1
JOIN df2 USING (col)
JOIN df3 USING (col)
JOIN df4 USING (col)

""")
q =query.render(project_id=project_id,pipeline=pipeline,com_cdr=com_cdr,deid_cdr=deid_cdr,non_deid_fitbit=non_deid_fitbit,deid_cdr_fitbit=deid_cdr_fitbit,truncation_date=truncation_date,maximum_age=maximum_age)  
df1=execute(client, q)  
df1=df1.iloc[:,1:5]
if df1.loc[0].sum()==0:
 df = df.append({'query' : 'Query1 cutoff date in fitbit datasets', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query1 cutoff date in fitbit datasets', 'result' : 'Failure'},  
                ignore_index = True) 
df1

# # Verify if that the fitdata data is removed FROM the fitbit tables for participants exceeding allowable age (maximum_age, i.e.,89). ((row counts = 0))
#
# DC-1001

query = JINJA_ENV.from_string("""

WITH df1 AS (
SELECT '1' as col ,COUNT (*) n_row_not_pass_activity_summary
FROM `{{project_id}}.{{deid_cdr_fitbit}}.activity_summary` d
JOIN `{{project_id}}.{{pipeline}}.pid_rid_mapping` m
ON m.research_id = d.person_id
JOIN `{{project_id}}.{{com_cdr}}.person` i
ON m.person_id = i.person_id
WHERE  FLOOR(DATE_DIFF(CURRENT_DATE(),DATE(i.birth_datetime), YEAR)) >{{maximum_age}}),

df2 AS (
SELECT '1' as col ,COUNT (*) n_row_not_pass_heart_rate_summary
FROM `{{project_id}}.{{deid_cdr_fitbit}}.heart_rate_summary` d
JOIN `{{project_id}}.{{pipeline}}.pid_rid_mapping` m
ON m.research_id = d.person_id
JOIN `{{project_id}}.{{com_cdr}}.person` i
ON m.person_id = i.person_id
WHERE  FLOOR(DATE_DIFF(CURRENT_DATE(),DATE(i.birth_datetime), YEAR)) >{{maximum_age}}),

df3 AS (
SELECT '1' as col ,COUNT (*) n_row_not_pass_heart_rate_minute_level
FROM `{{project_id}}.{{deid_cdr_fitbit}}.heart_rate_minute_level` d
JOIN `{{project_id}}.{{pipeline}}.pid_rid_mapping` m
ON m.research_id = d.person_id
JOIN `{{project_id}}.{{com_cdr}}.person` i
ON m.person_id = i.person_id
WHERE  FLOOR(DATE_DIFF(CURRENT_DATE(),DATE(i.birth_datetime), YEAR)) >{{maximum_age}}),

df4 AS (
SELECT '1' as col ,COUNT (*) n_row_not_pass_steps_intraday
FROM `{{project_id}}.{{deid_cdr_fitbit}}.steps_intraday` d
JOIN `{{project_id}}.{{pipeline}}.pid_rid_mapping` m
ON m.research_id = d.person_id
JOIN `{{project_id}}.{{com_cdr}}.person` i
ON m.person_id = i.person_id
WHERE  FLOOR(DATE_DIFF(CURRENT_DATE(),DATE(i.birth_datetime), YEAR)) >{{maximum_age}})

SELECT * from df1
JOIN df2 USING (col)
JOIN df3 USING (col)
JOIN df4 USING (col)

""")
q =query.render(project_id=project_id,pipeline=pipeline,com_cdr=com_cdr,deid_cdr=deid_cdr,non_deid_fitbit=non_deid_fitbit,deid_cdr_fitbit=deid_cdr_fitbit,truncation_date=truncation_date,maximum_age=maximum_age)  
df1=execute(client, q)  
df1=df1.iloc[:,1:5]
if df1.loc[0].sum()==0:
 df = df.append({'query' : 'Query2 no maximum_age in fitbit datasets', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query2 no maximum_age in fitbit datasets', 'result' : 'Failure'},  
                ignore_index = True) 
df1               

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

# activity_summary
query = JINJA_ENV.from_string("""

WITH df1 AS (
SELECT m.research_id,
CONCAT(m.research_id, '_', i.date) as i_newc
FROM `{{project_id}}.{{pipeline}}.pid_rid_mapping` m
JOIN `{{project_id}}.{{non_deid_fitbit}}.activity_summary` i
ON m.person_id = i.person_id
),

df2 AS (
SELECT d.person_id,
CONCAT(d.person_id, '_', DATE_ADD(d.date, INTERVAL m.shift DAY))  d_newc
FROM `{{project_id}}.{{pipeline}}.pid_rid_mapping` m
JOIN `{{project_id}}.{{deid_cdr_fitbit}}.activity_summary` d
ON m.research_id = d.person_id 
)

SELECT COUNT (*) n_row_not_pass FROM df2
WHERE d_newc NOT IN (SELECT i_newc FROM df1)
""")
q =query.render(project_id=project_id,pipeline=pipeline,com_cdr=com_cdr,deid_cdr=deid_cdr,non_deid_fitbit=non_deid_fitbit,deid_cdr_fitbit=deid_cdr_fitbit,truncation_date=truncation_date,maximum_age=maximum_age)  
df1=execute(client, q)  
if df1.eq(0).any().any():
 df = df.append({'query' : 'Query3.1 Date shifted in activity_summary', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query3.1 Date shifted in activity_summary', 'result' : 'Failure'},  
                ignore_index = True) 
df1        

# heart_rate_summary
query = JINJA_ENV.from_string("""

WITH df1 AS (
SELECT m.research_id,
CONCAT(m.research_id, '_', i.date) as i_newc
FROM `{{project_id}}.{{pipeline}}.pid_rid_mapping` m
JOIN `{{project_id}}.{{non_deid_fitbit}}.heart_rate_summary` i
ON m.person_id = i.person_id
),

df2 AS (
SELECT d.person_id,
CONCAT(d.person_id, '_', DATE_ADD(d.date, INTERVAL m.shift DAY)) AS d_newc
FROM `{{project_id}}.{{pipeline}}.pid_rid_mapping` m
JOIN `{{project_id}}.{{deid_cdr_fitbit}}.heart_rate_summary` d
ON m.research_id = d.person_id 
)

SELECT COUNT (*) n_row_not_pass FROM df2
WHERE d_newc NOT IN (SELECT i_newc FROM df1)
""")
q =query.render(project_id=project_id,pipeline=pipeline,com_cdr=com_cdr,deid_cdr=deid_cdr,non_deid_fitbit=non_deid_fitbit,deid_cdr_fitbit=deid_cdr_fitbit,truncation_date=truncation_date,maximum_age=maximum_age)  
df1=execute(client, q)  
if df1.eq(0).any().any():
 df = df.append({'query' : 'Query3.2 Date shifted in heart_rate_summary', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query3.2 Date shifted in heart_rate_summary', 'result' : 'Failure'},  
                ignore_index = True) 
df1        

# +
# heart_rate_minute_level

query = JINJA_ENV.from_string("""

WITH df1 AS (
SELECT m.research_id,
CONCAT(m.research_id, '_', i.datetime) as i_newc
FROM `{{project_id}}.{{pipeline}}.pid_rid_mapping` m
JOIN `{{project_id}}.{{non_deid_fitbit}}.heart_rate_minute_level` i
ON m.person_id = i.person_id
),

df2 AS (
SELECT d.person_id,
CONCAT(d.person_id, '_', DATE_ADD(d.datetime, INTERVAL m.shift DAY)) AS d_newc
FROM `{{project_id}}.{{pipeline}}.pid_rid_mapping` m
JOIN `{{project_id}}.{{deid_cdr_fitbit}}.heart_rate_minute_level` d
ON m.research_id = d.person_id 
)

SELECT COUNT (*) n_row_not_pass FROM df2
WHERE d_newc NOT IN (SELECT i_newc FROM df1)
""")
q =query.render(project_id=project_id,pipeline=pipeline,com_cdr=com_cdr,deid_cdr=deid_cdr,non_deid_fitbit=non_deid_fitbit,deid_cdr_fitbit=deid_cdr_fitbit,truncation_date=truncation_date,maximum_age=maximum_age)  
df1=execute(client, q)  
if df1.eq(0).any().any():
 df = df.append({'query' : 'Query3.3 Date shifted in heart_rate_minute_level', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query3.3 Date shifted in heart_rate_minute_level', 'result' : 'Failure'},  
                ignore_index = True) 
df1       

# +
# steps_intraday

query = JINJA_ENV.from_string("""

WITH df1 AS (
SELECT m.research_id,
CONCAT(m.research_id, '_', i.datetime) as i_newc
FROM `{{project_id}}.{{pipeline}}.pid_rid_mapping` m
JOIN `{{project_id}}.{{non_deid_fitbit}}.steps_intraday` i
ON m.person_id = i.person_id
),

df2 AS (
SELECT d.person_id,
CONCAT(d.person_id, '_', DATE_ADD(d.datetime, INTERVAL m.shift DAY)) AS d_newc
FROM `{{project_id}}.{{pipeline}}.pid_rid_mapping` m
JOIN `{{project_id}}.{{deid_cdr_fitbit}}.steps_intraday` d
ON m.research_id = d.person_id 
)

SELECT COUNT (*) n_row_not_pass FROM df2
WHERE d_newc NOT IN (SELECT i_newc FROM df1)
""")
q =query.render(project_id=project_id,pipeline=pipeline,com_cdr=com_cdr,deid_cdr=deid_cdr,non_deid_fitbit=non_deid_fitbit,deid_cdr_fitbit=deid_cdr_fitbit,truncation_date=truncation_date,maximum_age=maximum_age)  
df1=execute(client, q)  
if df1.eq(0).any().any():
 df = df.append({'query' : 'Query3.4 Date shifted in steps_intraday', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query3.4 Date shifted in steps_intraday', 'result' : 'Failure'},  
                ignore_index = True) 
df1   
# -

# # Verify that the participants are correctly mapped to their Research ID 
#
# DC-1000

# activity_summary
query = JINJA_ENV.from_string("""

WITH df1 AS (
SELECT DISTINCT i.person_id  AS non_deid_pid,m.research_id
FROM `{{project_id}}.{{pipeline}}.pid_rid_mapping` m
JOIN `{{project_id}}.{{non_deid_fitbit}}.activity_summary` i
ON m.person_id = i.person_id ),

df2 AS (
SELECT DISTINCT d.person_id  AS deid_pid,m.research_id
FROM `{{project_id}}.{{pipeline}}.pid_rid_mapping` m
JOIN `{{project_id}}.{{deid_cdr_fitbit}}.activity_summary` d
ON d.person_id = m.research_id)

SELECT COUNT (*) n_row_not_pass_activity_summary FROM df1
JOIN df2 USING (research_id)
WHERE research_id != deid_pid
""")
q =query.render(project_id=project_id,pipeline=pipeline,com_cdr=com_cdr,deid_cdr=deid_cdr,non_deid_fitbit=non_deid_fitbit,deid_cdr_fitbit=deid_cdr_fitbit,truncation_date=truncation_date,maximum_age=maximum_age)  
df1=execute(client, q)  
if df1.eq(0).any().any():
 df = df.append({'query' : 'Query4.1 resarch_id=person_id in activity_summary', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query4.1 resarch_id=person_id in activity_summary', 'result' : 'Failure'},  
                ignore_index = True) 
df1        

# heart_rate_summary
query = JINJA_ENV.from_string("""

WITH df1 AS (
SELECT DISTINCT i.person_id  AS non_deid_pid,m.research_id
FROM `{{project_id}}.{{pipeline}}.pid_rid_mapping` m
JOIN `{{project_id}}.{{non_deid_fitbit}}.heart_rate_summary` i
ON m.person_id = i.person_id ),

df2 AS (
SELECT DISTINCT d.person_id  AS deid_pid,m.research_id
FROM `{{project_id}}.{{pipeline}}.pid_rid_mapping` m
JOIN `{{project_id}}.{{deid_cdr_fitbit}}.heart_rate_summary` d
ON d.person_id = m.research_id)

SELECT COUNT (*) n_row_not_pass_heart_rate_summary FROM df1
JOIN df2 USING (research_id)
WHERE research_id != deid_pid
""")
q =query.render(project_id=project_id,pipeline=pipeline,com_cdr=com_cdr,deid_cdr=deid_cdr,non_deid_fitbit=non_deid_fitbit,deid_cdr_fitbit=deid_cdr_fitbit,truncation_date=truncation_date,maximum_age=maximum_age)  
df1=execute(client, q)  
if df1.eq(0).any().any():
 df = df.append({'query' : 'Query4.2 resarch_id=person_id in heart_rate_summary', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query4.2 resarch_id=person_id in heart_rate_summary', 'result' : 'Failure'},  
                ignore_index = True) 
df1     

# heart_rate_minute_level
query = JINJA_ENV.from_string("""

WITH df1 AS (
SELECT DISTINCT i.person_id  AS non_deid_pid,m.research_id
FROM `{{project_id}}.{{pipeline}}.pid_rid_mapping` m
JOIN `{{project_id}}.{{non_deid_fitbit}}.heart_rate_minute_level` i
ON m.person_id = i.person_id ),

df2 AS (
SELECT DISTINCT d.person_id  AS deid_pid,m.research_id
FROM `{{project_id}}.{{pipeline}}.pid_rid_mapping` m
JOIN `{{project_id}}.{{deid_cdr_fitbit}}.heart_rate_minute_level` d
ON d.person_id = m.research_id)

SELECT COUNT (*) n_row_not_pass_heart_rate_minute_level FROM df1
JOIN df2 USING (research_id)
WHERE research_id != deid_pid
""")
q =query.render(project_id=project_id,pipeline=pipeline,com_cdr=com_cdr,deid_bcdr=deid_cdr,non_deid_fitbit=non_deid_fitbit,deid_cdr_fitbit=deid_cdr_fitbit,truncation_date=truncation_date,maximum_age=maximum_age)  
df1=execute(client, q)  
if df1.eq(0).any().any():
 df = df.append({'query' : 'Query4.3 resarch_id=person_id in heart_rate_minute_level', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query4.3 resarch_id=person_id in heart_rate_minute_level', 'result' : 'Failure'},  
                ignore_index = True) 
df1     

# steps_intraday
query = JINJA_ENV.from_string("""

WITH df1 AS (
SELECT DISTINCT i.person_id  AS non_deid_pid,m.research_id
FROM `{{project_id}}.{{pipeline}}.pid_rid_mapping` m
JOIN `{{project_id}}.{{non_deid_fitbit}}.steps_intraday` i
ON m.person_id = i.person_id ),

df2 AS (
SELECT DISTINCT d.person_id  AS deid_pid,m.research_id
FROM `{{project_id}}.{{pipeline}}.pid_rid_mapping` m
JOIN `{{project_id}}.{{deid_cdr_fitbit}}.steps_intraday` d
ON d.person_id = m.research_id)

SELECT COUNT (*) n_row_not_pass_steps_intraday FROM df1
JOIN df2 USING (research_id)
WHERE research_id != deid_pid

""")
q =query.render(project_id=project_id,pipeline=pipeline,com_cdr=com_cdr,deid_cdr=deid_cdr,non_deid_fitbit=non_deid_fitbit,deid_cdr_fitbit=deid_cdr_fitbit,truncation_date=truncation_date,maximum_age=maximum_age)  
df1=execute(client, q)  
if df1.eq(0).any().any():
 df = df.append({'query' : 'Query4.4 resarch_id=person_id in steps_intraday', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query4.4 resarch_id=person_id in steps_intraday', 'result' : 'Failure'},  
                ignore_index = True) 
df1     

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

# # Verify proper deidentification of device_id
#
# DC-3280
# If the query fails use these hints to help investigate.
# * not_research_ids - These 'deidentified' device_ids are not in the masking table. Did the CR run that updates the masking table with new research_device_ids?
# * check_uuid_format - These deidentified device_ids should have the uuid format and not NULL.
# * check_uuid_unique - All deidentified device_ids for a device_id/person_id should be unique. If not, replace the non-unique research_device_ids in the masking table with new UUIDs. 
#
# Device_id is being deidentified by a combination of the following cleaning rules.
# 1. generate_research_device_ids - Keeps the masking table(wearables_device_id_masking) updated.
# 2. deid_fitbit_device_id - Replaces the device_id with the deidentified device_id(research_device_id).
#

# +
query = JINJA_ENV.from_string("""
WITH not_research_ids AS (
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
SELECT 'not_research_ids' as issue, COUNT(*) as bad_rows
FROM not_research_ids
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
    summary = summary.append(        {
            'query': 'Query7 device_id was deidentified properly for all records.',
            'result': 'PASS'
        },
        ignore_index=True)
else:
    summary = summary.append(
        {
            'query': 'Query7 device_id was not deidentified properly. See query description for hints.',
            'result': 'Failure'
        },
        ignore_index=True)
result

# -

# # Summary_fitdata

# +
def highlight_cells(val):
    color = 'red' if 'Failure' in val else 'white'
    return f'background-color: {color}' 

df.style.applymap(highlight_cells).set_properties(**{'text-align': 'left'})
# -


