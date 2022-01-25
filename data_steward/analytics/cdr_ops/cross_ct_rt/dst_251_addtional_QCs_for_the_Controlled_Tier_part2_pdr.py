# -*- coding: utf-8 -*-
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

import urllib
import pandas as pd
pd.options.display.max_rows = 120

# df will have a summary IN the end
df = pd.DataFrame(columns = ['query', 'result']) 

# # This part has to use my own account in order to access PDR dataset
#
# DC-2030 was created to obtain the permission to access PDR using analytic service account
#
# https://precisionmedicineinitiative.atlassian.net/browse/DC-2030

# Parameters
project_id=""
rt_dataset="" # has birth date
ct_dataset=""
project_id2=''
pdr_dataset=''
cut_off_date=''

# # Query 4: No withdrawn participants
#
# withdrawal_authored < the withdrawal date or cu_off_date

# +
query = f"""
    
WITH person_withdrawal AS (
   
SELECT DISTINCT participant_id
FROM `{project_id2}.{pdr_dataset}.pdr_participant`
WHERE withdrawal_authored < '{cut_off_date}'
)

SELECT 
'person' AS table_name,
'person_id' AS column_name,

COUNT(*) AS row_counts_failure,
CASE WHEN 
  COUNT(*) > 0
  THEN 1 ELSE 0
END
 AS Failure_no_witdrawal
 
FROM `{project_id}.{ct_dataset}.person` c
JOIN {project_id}.{rt_dataset}._deid_map map
ON c.person_id=map.research_id
WHERE  map.person_id IN (SELECT participant_id FROM person_withdrawal)
"""
result= pd.read_gbq(query,dialect='standard')

result.shape
result
# -

if result.iloc[:,3].sum()==0:
 df = df.append({'query' : 'Query 4: No withdrawn participants', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query 4: No withdrawn participants', 'result' : ''},  
                ignore_index = True) 

# # Query 5 No data after participant's suspension
# date column> suspension_date

# +
query = f"""
WITH
    table1 AS (
    SELECT
      table_name,
      column_name
    FROM
      `{project_id}.{ct_dataset}.INFORMATION_SCHEMA.COLUMNS`
    WHERE
      column_name='person_id' ),
    table2 AS (
    SELECT
      table_id AS table_name,
      row_count
    FROM
      `{project_id}.{ct_dataset}.__TABLES__`
    WHERE
      row_count>1)
      
  SELECT
    table_name,
    column_name
  FROM
    `{project_id}.{ct_dataset}.INFORMATION_SCHEMA.COLUMNS` c
  WHERE
    table_name IN (
    SELECT
      DISTINCT table_name
    FROM
      table2
    WHERE
      table_name IN (
      SELECT
        DISTINCT table_name
      FROM
        table1))
    AND c.data_type IN ('DATE')
    AND NOT REGEXP_CONTAINS(column_name, r'(?i)(_PAR)') 
"""

target_tables=pd.read_gbq(query, dialect='standard')
target_tables.shape
# -

target_tables

# need to do obs table seperatly
df1=target_tables
df1=df1[df1.table_name.str.contains("obs")]
df1=df1[~df1.table_name.str.contains("period")]
target_tables2=df1
target_tables2


def my_sql(table_name,column_name):
    
    query = f"""
    
   WITH person_suspension AS (
    SELECT DISTINCT participant_id, suspension_status_id, suspension_status
  ,suspension_time,SAFE_CAST (suspension_time AS DATE) AS suspension_date,person_id, research_id
FROM `{project_id2}.{pdr_dataset}.pdr_participant` pdr
JOIN {project_id}.{rt_dataset}._deid_map rt
  ON pdr.participant_id=rt.person_id
   WHERE suspension_time is NOT null
)
    
SELECT 

'{table_name}' AS table_name,
'{column_name}' AS column_name,

COUNT(*) AS row_counts_failure,
CASE WHEN 
  COUNT(*) > 0
  THEN 1 ELSE 0
END
 AS Failure_no_data_after_suspension
 
FROM `{project_id}.{ct_dataset}.{table_name}` c
JOIN person_suspension
ON c.person_id=person_suspension.research_id
WHERE  c.{column_name} > person_suspension.suspension_date
AND c.{table_name}_concept_id NOT IN (4013886, 4135376, 4271761)
"""
    df11= pd.read_gbq(query, dialect='standard')
    return df11


result = [my_sql (table_name, column_name) for table_name, column_name in zip(target_tables2['table_name'], target_tables2['column_name'])]
result

# +
n=len(target_tables2.index)
res2 = pd.DataFrame(result[0])
for x in range(1,n):    
  res2=res2.append(result[x])
    
res2=res2.sort_values(by='row_counts_failure', ascending=False)
res2
# -

# then do the rest of tables
df1=target_tables
df1=df1[~df1.table_name.str.contains("obs")]
target_tables2=df1
target_tables2


def my_sql(table_name,column_name):
    
    query = f"""
    
WITH person_suspension AS (
  SELECT DISTINCT participant_id, suspension_status_id, suspension_status,
  suspension_time,SAFE_CAST (suspension_time AS DATE) AS suspension_date,person_id, research_id
FROM `{project_id2}.{pdr_dataset}.pdr_participant` pdr
JOIN {project_id}.{rt_dataset}._deid_map rt
  ON pdr.participant_id=rt.person_id
  WHERE suspension_time is NOT null
)
    
SELECT 
'{table_name}' AS table_name,
'{column_name}' AS column_name,

COUNT(*) AS row_counts_failure,
CASE WHEN 
  COUNT(*) > 0
  THEN 1 ELSE 0
END
 AS Failure_no_data_after_suspension
 FROM `{project_id}.{ct_dataset}.{table_name}` c
JOIN person_suspension
ON c.person_id=person_suspension.research_id
WHERE  c.{column_name} > person_suspension.suspension_date
"""
    df11= pd.read_gbq(query, dialect='standard')
    return df11


# use a loop to get table name AND column name AND run sql function
result = [my_sql (table_name, column_name) for table_name, column_name in zip(target_tables2['table_name'], target_tables2['column_name'])]
result

# +
n=len(target_tables2.index)
res2 = pd.DataFrame(result[0])

for x in range(1,n):    
  res2=res2.append(result[x])
    
res21=res2.sort_values(by='row_counts_failure', ascending=False)
res21
# -

# final results
res2=res2.append(res21, ignore_index=True)
res2

if res2.iloc[:,3].sum()==0:
 df = df.append({'query' : 'Query 5: No data after participant suspension', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query 5: No data after participant suspension', 'result' : 'Failure'},  
                ignore_index = True) 

# # Query 8: All participants WITH Fitbit have said yes to primary consents
#
# ExtraConsent_TodaysDate IN pdr table  > '{cut_off_date}'
#
# AND ExtraConsent_TodaysDate !='PMI_Skip'

# +
# get target tables
query = f"""
SELECT
    table_name,
    column_name
  FROM
    `{project_id}.{ct_dataset}.INFORMATION_SCHEMA.COLUMNS` c
  WHERE   
    REGEXP_CONTAINS(column_name, r'(?i)(person_id)') 
    AND NOT REGEXP_CONTAINS(column_name, r'(?i)(_PAR)') 
    AND (
       REGEXP_CONTAINS(table_name, r'(?i)(steps)')
    OR REGEXP_CONTAINS(table_name, r'(?i)(heart)')
    OR REGEXP_CONTAINS(table_name, r'(?i)(activity)')
    )
"""

target_tables=pd.read_gbq(query, dialect='standard')

target_tables.shape
# -

target_tables


def my_sql(table_name,column_name):
    
    query = f"""
   WITH person_all_pdr AS (
   
  SELECT DISTINCT participant_id, ExtraConsent_AgreeToConsent,ExtraConsent_TodaysDate,research_id
  FROM `{project_id2}.rdr_ops_data_view.pdr_mod_consentpii` pdr
JOIN {project_id}.{rt_dataset}._deid_map rt
  ON pdr.participant_id=rt.person_id
  WHERE ExtraConsent_TodaysDate > '{cut_off_date}'
  AND ExtraConsent_TodaysDate !='PMI_Skip'
  )
    
SELECT 
'{table_name}' AS table_name,
'person_id' AS column_name,

COUNT(*) AS row_counts_failure,
CASE WHEN 
  COUNT(*) > 0
  THEN 1 ELSE 0
END
 AS Failure_said_yes_to_primary_consents
 FROM `{project_id}.{ct_dataset}.{table_name}` c
WHERE  person_id IN (SELECT research_id FROM person_all_pdr)
"""
    df11= pd.read_gbq(query, dialect='standard')    
    return df11


# +
result = [my_sql (table_name, column_name) for table_name, column_name in zip(target_tables['table_name'], target_tables2['column_name'])]
result

# AND then get the result back FROM loop result list
n=len(target_tables.index)
res2 = pd.DataFrame(result[0])

for x in range(1,n):    
  res2=res2.append(result[x])
    
res2=res2.sort_values(by='row_counts_failure', ascending=False)
res2
# -

if res2.iloc[:,3].sum()==0:
 df = df.append({'query' : 'Query 8: All participants WITH Fitbit have said yes to primary consents', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query 8: All participants WITH Fitbit have said yes to primary consents', 'result' : 'Failure'},  
                ignore_index = True) 


# # final summary result

# +
def highlight_cells(val):
    color = 'red' if 'Failure' in val else 'white'
    return f'background-color: {color}' 

df.style.applymap(highlight_cells).set_properties(**{'text-align': 'left'})