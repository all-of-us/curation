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

query=f''' 
WITH df1 AS (
SELECT '1' as col ,COUNT (*) AS n_row_not_pass_activity_summary
FROM `{project_id}.{deid_cdr_fitbit}.activity_summary`  a
JOIN `{project_id}.{pipeline}.pid_rid_mapping` m
ON m.research_id = a.person_id
WHERE DATE_ADD(date(a.date), INTERVAL m.shift DAY) > '{truncation_date}'),

df2 AS (
SELECT '1' as col ,COUNT (*) n_row_not_pass_heart_rate_summary
FROM `{project_id}.{deid_cdr_fitbit}.heart_rate_summary`  a
JOIN `{project_id}.{pipeline}.pid_rid_mapping` m
ON m.research_id = a.person_id
WHERE DATE_ADD(date(a.date), INTERVAL m.shift DAY) > '{truncation_date}' ),

df3 AS (
SELECT '1' as col ,COUNT (*) n_row_not_pass_heart_rate_minute_level
FROM `{project_id}.{deid_cdr_fitbit}.heart_rate_minute_level`  a
JOIN `{project_id}.{pipeline}.pid_rid_mapping` m
ON m.research_id = a.person_id
WHERE DATE_ADD(date(a.datetime), INTERVAL m.shift DAY) > '{truncation_date}'),

df4 AS (
SELECT '1' as col ,COUNT (*) n_row_not_pass_steps_intraday
FROM `{project_id}.{deid_cdr_fitbit}.steps_intraday`  a
JOIN `{project_id}.{pipeline}.pid_rid_mapping` m
ON m.research_id = a.person_id
WHERE DATE_ADD(date(a.datetime), INTERVAL m.shift DAY) > '{truncation_date}' )

SELECT * from df1
JOIN df2 USING (col)
JOIN df3 USING (col)
JOIN df4 USING (col)

'''
df1=pd.read_gbq(query, dialect='standard')  
df1=df1.iloc[:,1:5]
if df1.loc[0].sum()==0:
 df = df.append({'query' : 'Query1 cutoff date in fitbit datasets', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query1 cutoff date in fitbit datasets', 'result' : ''},  
                ignore_index = True) 
df1

# # Verify if that the fitdata data is removed FROM the fitbit tables for participants exceeding allowable age (maximum_age, i.e.,89). ((row counts = 0))
#
# DC-1001

query=f''' 

WITH df1 AS (
SELECT '1' as col ,COUNT (*) n_row_not_pass_activity_summary
FROM `{project_id}.{deid_cdr_fitbit}.activity_summary` d
JOIN `{project_id}.{pipeline}.pid_rid_mapping` m
ON m.research_id = d.person_id
JOIN `{project_id}.{com_cdr}.person` i
ON m.person_id = i.person_id
WHERE  FLOOR(DATE_DIFF(CURRENT_DATE(),DATE(i.birth_datetime), YEAR)) >{maximum_age}),

df2 AS (
SELECT '1' as col ,COUNT (*) n_row_not_pass_heart_rate_summary
FROM `{project_id}.{deid_cdr_fitbit}.heart_rate_summary` d
JOIN `{project_id}.{pipeline}.pid_rid_mapping` m
ON m.research_id = d.person_id
JOIN `{project_id}.{com_cdr}.person` i
ON m.person_id = i.person_id
WHERE  FLOOR(DATE_DIFF(CURRENT_DATE(),DATE(i.birth_datetime), YEAR)) >{maximum_age}),

df3 AS (
SELECT '1' as col ,COUNT (*) n_row_not_pass_heart_rate_minute_level
FROM `{project_id}.{deid_cdr_fitbit}.heart_rate_minute_level` d
JOIN `{project_id}.{pipeline}.pid_rid_mapping` m
ON m.research_id = d.person_id
JOIN `{project_id}.{com_cdr}.person` i
ON m.person_id = i.person_id
WHERE  FLOOR(DATE_DIFF(CURRENT_DATE(),DATE(i.birth_datetime), YEAR)) >{maximum_age}),

df4 AS (
SELECT '1' as col ,COUNT (*) n_row_not_pass_steps_intraday
FROM `{project_id}.{deid_cdr_fitbit}.steps_intraday` d
JOIN `{project_id}.{pipeline}.pid_rid_mapping` m
ON m.research_id = d.person_id
JOIN `{project_id}.{com_cdr}.person` i
ON m.person_id = i.person_id
WHERE  FLOOR(DATE_DIFF(CURRENT_DATE(),DATE(i.birth_datetime), YEAR)) >{maximum_age})

SELECT * from df1
JOIN df2 USING (col)
JOIN df3 USING (col)
JOIN df4 USING (col)

'''
df1=pd.read_gbq(query, dialect='standard')  
df1=df1.iloc[:,1:5]
if df1.loc[0].sum()==0:
 df = df.append({'query' : 'Query2 no maximum_age in fitbit datasets', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query2 no maximum_age in fitbit datasets', 'result' : ''},  
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
query=f''' 

WITH df1 AS (
SELECT m.research_id,
CONCAT(m.research_id, '_', i.date) as i_newc
FROM `{project_id}.{pipeline}.pid_rid_mapping` m
JOIN `{project_id}.{non_deid_fitbit}.activity_summary` i
ON m.person_id = i.person_id
),

df2 AS (
SELECT d.person_id,
CONCAT(d.person_id, '_', DATE_ADD(d.date, INTERVAL m.shift DAY))  d_newc
FROM `{project_id}.{pipeline}.pid_rid_mapping` m
JOIN `{project_id}.{deid_cdr_fitbit}.activity_summary` d
ON m.research_id = d.person_id 
)

SELECT COUNT (*) n_row_not_pass FROM df2
WHERE d_newc NOT IN (SELECT i_newc FROM df1)
'''
df1=pd.read_gbq(query, dialect='standard')  
if df1.eq(0).any().any():
 df = df.append({'query' : 'Query3.1 Date shifted in activity_summary', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query3.1 Date shifted in activity_summary', 'result' : ''},  
                ignore_index = True) 
df1        

# heart_rate_summary
query=f''' 

WITH df1 AS (
SELECT m.research_id,
CONCAT(m.research_id, '_', i.date) as i_newc
FROM `{project_id}.{pipeline}.pid_rid_mapping` m
JOIN `{project_id}.{non_deid_fitbit}.heart_rate_summary` i
ON m.person_id = i.person_id
),

df2 AS (
SELECT d.person_id,
CONCAT(d.person_id, '_', DATE_ADD(d.date, INTERVAL m.shift DAY)) AS d_newc
FROM `{project_id}.{pipeline}.pid_rid_mapping` m
JOIN `{project_id}.{deid_cdr_fitbit}.heart_rate_summary` d
ON m.research_id = d.person_id 
)

SELECT COUNT (*) n_row_not_pass FROM df2
WHERE d_newc NOT IN (SELECT i_newc FROM df1)
'''
df1=pd.read_gbq(query, dialect='standard')  
if df1.eq(0).any().any():
 df = df.append({'query' : 'Query3.2 Date shifted in heart_rate_summary', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query3.2 Date shifted in heart_rate_summary', 'result' : ''},  
                ignore_index = True) 
df1        

# +
# heart_rate_minute_level

query=f''' 

WITH df1 AS (
SELECT m.research_id,
CONCAT(m.research_id, '_', i.datetime) as i_newc
FROM `{project_id}.{pipeline}.pid_rid_mapping` m
JOIN `{project_id}.{non_deid_fitbit}.heart_rate_minute_level` i
ON m.person_id = i.person_id
),

df2 AS (
SELECT d.person_id,
CONCAT(d.person_id, '_', DATE_ADD(d.datetime, INTERVAL m.shift DAY)) AS d_newc
FROM `{project_id}.{pipeline}.pid_rid_mapping` m
JOIN `{project_id}.{deid_cdr_fitbit}.heart_rate_minute_level` d
ON m.research_id = d.person_id 
)

SELECT COUNT (*) n_row_not_pass FROM df2
WHERE d_newc NOT IN (SELECT i_newc FROM df1)
'''
df1=pd.read_gbq(query, dialect='standard')  
if df1.eq(0).any().any():
 df = df.append({'query' : 'Query3.3 Date shifted in heart_rate_minute_level', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query3.3 Date shifted in heart_rate_minute_level', 'result' : ''},  
                ignore_index = True) 
df1       

# +
# steps_intraday

query=f''' 

WITH df1 AS (
SELECT m.research_id,
CONCAT(m.research_id, '_', i.datetime) as i_newc
FROM `{project_id}.{pipeline}.pid_rid_mapping` m
JOIN `{project_id}.{non_deid_fitbit}.steps_intraday` i
ON m.person_id = i.person_id
),

df2 AS (
SELECT d.person_id,
CONCAT(d.person_id, '_', DATE_ADD(d.datetime, INTERVAL m.shift DAY)) AS d_newc
FROM `{project_id}.{pipeline}.pid_rid_mapping` m
JOIN `{project_id}.{deid_cdr_fitbit}.steps_intraday` d
ON m.research_id = d.person_id 
)

SELECT COUNT (*) n_row_not_pass FROM df2
WHERE d_newc NOT IN (SELECT i_newc FROM df1)
'''
df1=pd.read_gbq(query, dialect='standard')  
if df1.eq(0).any().any():
 df = df.append({'query' : 'Query3.4 Date shifted in steps_intraday', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query3.4 Date shifted in steps_intraday', 'result' : ''},  
                ignore_index = True) 
df1   
# -

# # Verify that the participants are correctly mapped to their Research ID 
#
# DC-1000

# activity_summary
query=f''' 

WITH df1 AS (
SELECT DISTINCT i.person_id  AS non_deid_pid,m.research_id
FROM `{project_id}.{pipeline}.pid_rid_mapping` m
JOIN `{project_id}.{non_deid_fitbit}.activity_summary` i
ON m.person_id = i.person_id ),

df2 AS (
SELECT DISTINCT d.person_id  AS deid_pid,m.research_id
FROM `{project_id}.{pipeline}.pid_rid_mapping` m
JOIN `{project_id}.{deid_cdr_fitbit}.activity_summary` d
ON d.person_id = m.research_id)

SELECT COUNT (*) n_row_not_pass_activity_summary FROM df1
JOIN df2 USING (research_id)
WHERE research_id != deid_pid
'''
df1=pd.read_gbq(query, dialect='standard')  
if df1.eq(0).any().any():
 df = df.append({'query' : 'Query4.1 resarch_id=person_id in activity_summary', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query4.1 resarch_id=person_id in activity_summary', 'result' : ''},  
                ignore_index = True) 
df1        

# heart_rate_summary
query=f''' 

WITH df1 AS (
SELECT DISTINCT i.person_id  AS non_deid_pid,m.research_id
FROM `{project_id}.{pipeline}.pid_rid_mapping` m
JOIN `{project_id}.{non_deid_fitbit}.heart_rate_summary` i
ON m.person_id = i.person_id ),

df2 AS (
SELECT DISTINCT d.person_id  AS deid_pid,m.research_id
FROM `{project_id}.{pipeline}.pid_rid_mapping` m
JOIN `{project_id}.{deid_cdr_fitbit}.heart_rate_summary` d
ON d.person_id = m.research_id)

SELECT COUNT (*) n_row_not_pass_heart_rate_summary FROM df1
JOIN df2 USING (research_id)
WHERE research_id != deid_pid
'''
df1=pd.read_gbq(query, dialect='standard')  
if df1.eq(0).any().any():
 df = df.append({'query' : 'Query4.2 resarch_id=person_id in heart_rate_summary', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query4.2 resarch_id=person_id in heart_rate_summary', 'result' : ''},  
                ignore_index = True) 
df1     

# heart_rate_minute_level
query=f''' 

WITH df1 AS (
SELECT DISTINCT i.person_id  AS non_deid_pid,m.research_id
FROM `{project_id}.{pipeline}.pid_rid_mapping` m
JOIN `{project_id}.{non_deid_fitbit}.heart_rate_minute_level` i
ON m.person_id = i.person_id ),

df2 AS (
SELECT DISTINCT d.person_id  AS deid_pid,m.research_id
FROM `{project_id}.{pipeline}.pid_rid_mapping` m
JOIN `{project_id}.{deid_cdr_fitbit}.heart_rate_minute_level` d
ON d.person_id = m.research_id)

SELECT COUNT (*) n_row_not_pass_heart_rate_minute_level FROM df1
JOIN df2 USING (research_id)
WHERE research_id != deid_pid
'''
df1=pd.read_gbq(query, dialect='standard')  
if df1.eq(0).any().any():
 df = df.append({'query' : 'Query4.3 resarch_id=person_id in heart_rate_minute_level', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query4.3 resarch_id=person_id in heart_rate_minute_level', 'result' : ''},  
                ignore_index = True) 
df1     

# steps_intraday
query=f''' 

WITH df1 AS (
SELECT DISTINCT i.person_id  AS non_deid_pid,m.research_id
FROM `{project_id}.{pipeline}.pid_rid_mapping` m
JOIN `{project_id}.{non_deid_fitbit}.steps_intraday` i
ON m.person_id = i.person_id ),

df2 AS (
SELECT DISTINCT d.person_id  AS deid_pid,m.research_id
FROM `{project_id}.{pipeline}.pid_rid_mapping` m
JOIN `{project_id}.{deid_cdr_fitbit}.steps_intraday` d
ON d.person_id = m.research_id)

SELECT COUNT (*) n_row_not_pass_steps_intraday FROM df1
JOIN df2 USING (research_id)
WHERE research_id != deid_pid

'''
df1=pd.read_gbq(query, dialect='standard')  
if df1.eq(0).any().any():
 df = df.append({'query' : 'Query4.4 resarch_id=person_id in steps_intraday', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query4.4 resarch_id=person_id in steps_intraday', 'result' : ''},  
                ignore_index = True) 
df1     

# # Verify all person_ids in fitbit datasets exsit in deid_cdr person table
#
# [DC-1788] Add additional person existence check to Fitbit notebook
#
# This check should fail if a person_id in the activity_summary, heart_rate_summary, heart_rate_minute_level, or steps_intra_day tables does not exist in a corresponding RT de-identified dataset.

query=f''' 

WITH df1 AS (
SELECT '1' AS col , COUNT (DISTINCT person_id)  AS n_person_id_not_pass_activity_summary
FROM `{project_id}.{deid_cdr_fitbit}.activity_summary` 
WHERE person_id NOT IN (SELECT person_id FROM `{project_id}.{deid_cdr}.person`)),

df2 AS (
SELECT '1' AS col, COUNT (DISTINCT person_id)  AS n_person_id_not_pass_heart_rate_summary
FROM `{project_id}.{deid_cdr_fitbit}.heart_rate_summary` 
WHERE person_id NOT IN (SELECT person_id FROM `{project_id}.{deid_cdr}.person`)),

df3 AS (
SELECT '1' AS col,COUNT (DISTINCT person_id)  AS n_person_id_not_pass_heart_rate_minute_level
FROM `{project_id}.{deid_cdr_fitbit}.heart_rate_minute_level` 
WHERE person_id NOT IN (SELECT person_id FROM `{project_id}.{deid_cdr}.person`)),

df4 AS (
SELECT '1' AS col,COUNT (DISTINCT person_id) AS n_person_id_not_pass_steps_intraday
FROM `{project_id}.{deid_cdr_fitbit}.steps_intraday` a
WHERE person_id NOT IN (SELECT person_id FROM `{project_id}.{deid_cdr}.person`))

SELECT * FROM df1
JOIN df2 USING (col)
JOIN df3 USING (col)
JOIN df4 USING (col)
'''
df1=pd.read_gbq(query, dialect='standard')  
df1=df1.iloc[:,1:5]
if df1.loc[0].sum()==0:
 df = df.append({'query' : 'Query5 person_ids in fitbit exist in deid.person table', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query5 person_ids in fitbit exist in deid.person table', 'result' : ''},  
                ignore_index = True) 
df1.T             

# # Summary_fitdata

# if not pass, will be highlighted in red
df = df.mask(df.isin(['Null','']))
df.style.highlight_null(null_color='red').set_properties(**{'text-align': 'left'})
