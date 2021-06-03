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
non_deid=""
deid_cdr=""
com_cdr=""
# -

# df will have a summary in the end
df = pd.DataFrame(columns = ['query', 'result']) 

# # 1 Verify that the data newer than 11/26/2019 is truncated in activity_summary table (suppressed).
#
# DC-1046
#
# by adding m.shift back to deid_table and see if any date is newer than cutoff date.

query=f''' 

SELECT COUNT (*) AS n_row_not_pass
FROM `{project_id}.{deid_cdr}.activity_summary`  a
JOIN `{project_id}.{pipeline}.pid_rid_mapping` m
ON m.research_id = a.person_id
WHERE DATE_ADD(date(a.date), INTERVAL m.shift DAY) > '2019-11-26' 

'''
df1=pd.read_gbq(query, dialect='standard')  
if df1.eq(0).any().any():
 df = df.append({'query' : 'Query1 cutoff date in activity_summary', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query1 cutoff date in activity_summary', 'result' : ''},  
                ignore_index = True) 
df1

# # 2 Verify that the data newer than 11/26/2019 is truncated in heart_rate_minute_level table (suppressed).

# +
query=f''' 

SELECT COUNT (*) n_row_not_pass
FROM `{project_id}.{deid_cdr}.heart_rate_minute_level`  a
JOIN `{project_id}.{pipeline}.pid_rid_mapping` m
ON m.research_id = a.person_id
WHERE DATE_ADD(date(a.datetime), INTERVAL m.shift DAY) > '2019-11-26' 

'''
df1=pd.read_gbq(query, dialect='standard')  

if df1.eq(0).any().any():
 df = df.append({'query' : 'Query2 cutoff date in heart_rate_minute_level', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query2 cutoff date in heart_rate_minute_level', 'result' : ''},  
                ignore_index = True) 
df1
# -

# # 3 Verify that the data newer than 11/26/2019 is truncated in heart_rate_summary table (suppressed).

# +
query=f''' 

SELECT COUNT (*) n_row_not_pass
FROM `{project_id}.{deid_cdr}.heart_rate_summary`  a
JOIN `{project_id}.{pipeline}.pid_rid_mapping` m
ON m.research_id = a.person_id
WHERE DATE_ADD(date(a.date), INTERVAL m.shift DAY) > '2019-11-26' 

'''
df1=pd.read_gbq(query, dialect='standard')  

if df1.eq(0).any().any():
 df = df.append({'query' : 'Query3 cutoff date in heart_rate_summary', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query3 cutoff date in heart_rate_summary', 'result' : ''},  
                ignore_index = True) 
df1
# -

# # 4 Verify that the data newer than 11/26/2019 is truncated in  steps_intraday table (suppressed).

# +
query=f''' 

SELECT COUNT (*) n_row_not_pass
FROM `{project_id}.{deid_cdr}.steps_intraday`  a
JOIN `{project_id}.{pipeline}.pid_rid_mapping` m
ON m.research_id = a.person_id
WHERE DATE_ADD(date(a.datetime), INTERVAL m.shift DAY) > '2019-11-26' 

'''
df1=pd.read_gbq(query, dialect='standard')  

if df1.eq(0).any().any():
 df = df.append({'query' : 'Query4 cutoff date in steps_intraday', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query4 cutoff date in steps_intraday', 'result' : ''},  
                ignore_index = True) 
df1
# -

# # 5 Verify if that the fitdata data is removed FROM the fitbit tables for participants exceeding allowable age (89). ((row counts = 0))
#
# DC-1001

query=f''' 
SELECT COUNT (*) n_row_not_pass
FROM `{project_id}.{deid_cdr}.activity_summary` d
JOIN `{project_id}.{pipeline}.pid_rid_mapping` m
ON m.research_id = d.person_id
JOIN `{project_id}.{com_cdr}.person` i
ON m.person_id = i.person_id
WHERE  i.birth_datetime <= '1932-01-01'
'''
df1=pd.read_gbq(query, dialect='standard')  
if df1.eq(0).any().any():
 df = df.append({'query' : 'Query5 no age>=89 in activity_summary', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query5 no age>=89 in activity_summary', 'result' : ''},  
                ignore_index = True) 
df1        

# # 6 Verify that correct date shift is applied to the fitbit data
#
# DC-1005
#
# objective: 
#
# find the difference between the non-deid date and the deid date to validate that the dateshift is applied as specified in the map . 
#
# the original code uses min(date) to have the difference, but not sure why min(), not max(), or any date.
# and the original sql was not complete and <font color='red'> 
#

# final version, check all dates
query=f''' 

with df1 AS (
SELECT m.research_id,
CONCAT(m.research_id, '_', i.date) as i_newc
FROM `{project_id}.{pipeline}.pid_rid_mapping` m
JOIN `{project_id}.{non_deid}.activity_summary` i
ON m.person_id = i.person_id
),

df2 AS (
SELECT d.person_id,
CONCAT(d.person_id, '_', DATE_ADD(d.date, INTERVAL m.shift DAY)) as d_newc
FROM `{project_id}.{pipeline}.pid_rid_mapping` m
JOIN `{project_id}.{deid_cdr}.activity_summary` d
ON m.research_id = d.person_id 
)

SELECT COUNT (*) n_row_not_pass FROM df2
FULL OUTER JOIN df1 
ON df1.i_newc=df2.d_newc
WHERE i_newc !=d_newc

'''
df1=pd.read_gbq(query, dialect='standard')  
if df1.eq(0).any().any():
 df = df.append({'query' : 'Query6 date shifted', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query6 date shifted', 'result' : ''},  
                ignore_index = True) 
df1        

# # 7 Verify that the participants are correctly mapped to their Research ID 
#
# DC-1000

query=f''' 

WITH df1 AS (
SELECT DISTINCT i.person_id  AS non_deid_pid,m.research_id
FROM `{project_id}.{pipeline}.pid_rid_mapping` m
JOIN `{project_id}.{non_deid}.activity_summary` i
ON m.person_id = i.person_id ),

df2 AS (
SELECT DISTINCT d.person_id  AS deid_pid,m.research_id
FROM `{project_id}.{pipeline}.pid_rid_mapping` m
JOIN `{project_id}.{deid_cdr}.activity_summary` d
ON d.person_id = m.research_id)

SELECT COUNT (*) n_row_not_pass FROM df1
JOIN df2 USING (research_id)
WHERE research_id != deid_pid

'''
df1=pd.read_gbq(query, dialect='standard')  
if df1.eq(0).any().any():
 df = df.append({'query' : 'Query7 resarch_id=person_id', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query7 resarch_id=person_id', 'result' : ''},  
                ignore_index = True) 
df1        

# # Summary_fitdata

# if not pass, will be highlighted in red
df = df.mask(df.isin(['Null','']))
df.style.highlight_null(null_color='red').set_properties(**{'text-align': 'left'})
