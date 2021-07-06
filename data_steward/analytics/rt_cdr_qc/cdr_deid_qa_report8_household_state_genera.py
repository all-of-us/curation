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

# # QA queries on new CDR_deid household AND state general

import urllib
import pandas as pd
pd.options.display.max_rows = 120

# + tags=["parameters"]
project_id = ""
deid_cdr=""
# deid_clean=""
com_cdr = ""
# -

# df will have a summary in the end
df = pd.DataFrame(columns = ['query', 'result']) 

# # 1 Verify that if the observation_source_concept_id  field in OBSERVATION table populates: 1585890, the value_as_concept_id field in de-id table should populate : 2000000012
#
# DC-1049
#
# Expected result:
#
# Null is the value poplulated in the value_as_number fields 
#
# AND 2000000012, 2000000010 AND 903096 are the values that are populated in value_as_concept_id field  in the deid table.
#
# Per Francis, the other two values are valid. so it is pass. 

# +
query=f''' 

SELECT COUNT (*) AS n_row_not_pass
FROM `{project_id}.{deid_cdr}.observation`
WHERE
  observation_source_concept_id = 1585890
  AND value_as_concept_id NOT IN (2000000012,2000000010,903096)
'''
df1=pd.read_gbq(query, dialect='standard')  

if df1.loc[0].sum()==0:
 df = df.append({'query' : 'Query1 observation_source_concept_id 1585890', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query1 observation_source_concept_id 1585890', 'result' : ''},  
                ignore_index = True) 
df1
# -

# # 2 Verify that if the observation_source_concept_id  field in OBSERVATION table populates: 1333023 , the value_as_concept_id field in de-id table should populate : 2000000012
#
# expected results:
#
# Null is the value poplulated in the value_as_number fields
#
# AND 2000000012, 2000000010 AND 903096 are the values that are populated in value_as_concept_id field in the deid table.
#
# ## one row had error in new cdr

# +
query=f''' 

SELECT COUNT (*) AS n_row_not_pass
FROM `{project_id}.{deid_cdr}.observation`
WHERE
  observation_source_concept_id = 1333023
  AND value_as_concept_id NOT IN (2000000012,2000000010,903096)
'''
df1=pd.read_gbq(query, dialect='standard')  

if df1.loc[0].sum()==0:
 df = df.append({'query' : 'Query2 observation_source_concept_id 1333023', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query2 observation_source_concept_id 1333023', 'result' : ''},  
                ignore_index = True) 
df1
# -

query=f''' 

SELECT *
FROM `{project_id}.{deid_cdr}.observation`
WHERE
  observation_source_concept_id = 1333023
  AND value_as_concept_id NOT IN (2000000012,2000000010,903096)
'''
df1=pd.read_gbq(query, dialect='standard')  
df1

# # 3 Verify that if the observation_source_concept_id  field in OBSERVATION table populates: 1585889,  the value_as_concept_id field in de-id table should populate : 2000000013
#
# DC-1059
#
# expected results:
#
# Null is the value poplulated in the value_as_number fields 
#
# AND 2000000013, 2000000010 AND 903096 are the values that are populated in value_as_concept_id field in the deid table.

# +
query=f''' 

SELECT COUNT (*) AS n_row_not_pass
FROM  `{project_id}.{deid_cdr}.observation`
WHERE
  observation_source_concept_id = 1585889
  AND value_as_concept_id NOT IN (2000000013,2000000010,903096)
'''
df1=pd.read_gbq(query, dialect='standard')  

if df1.loc[0].sum()==0:
 df = df.append({'query' : 'Query3 observation_source_concept_id 1585889', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query3 observation_source_concept_id 1585889', 'result' : ''},  
                ignore_index = True) 
df1
# -

# # 4 Verify that if the observation_source_concept_id  field in OBSERVATION table populates: 1333015,  the value_as_concept_id field in de-id table should populate : 2000000013
#
# Generalization Rules for reference 
#
# Living Situation: COPE survey Generalize household size >10
#
# expected results:
#
# Null is the value poplulated in the value_as_number fields 
#
# AND 2000000013, 2000000010 AND 903096 are the values that are populated in value_as_concept_id field in the deid table.

# +
query=f''' 

SELECT COUNT (*) AS n_row_not_pass
FROM  `{project_id}.{deid_cdr}.observation`
WHERE
  observation_source_concept_id = 1333015
  AND value_as_concept_id NOT IN (2000000013,2000000010,903096)
'''
df1=pd.read_gbq(query, dialect='standard')  

if df1.loc[0].sum()==0:
 df = df.append({'query' : 'Query4 observation_source_concept_id 1333015', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query4 observation_source_concept_id 1333015', 'result' : ''},  
                ignore_index = True) 
df1
# -

# # state generalizaion

# # 5 Verify that in Observation table where observation_source_concept_id = 1585249, value_source_concept_id that populates are none of the ones listed in the J column AND one of the value_source_concept_id popluates as 2000000011.
#
# DC-1045
#
# For rows in the pre_deid_com_cdr OBSERVATION table where observation_source_concept_id = 1585249 (StreetAddress_PIIState)  where the value_source_concept_id is one of the values listed in the query,  (generalize value_source_concept_id to XXX (Unspecified state))
#
# step1:
#
# 1. query with the condition 
# observation_source_concept_id = 1585249
# 2. verify that none value_source_concept_id listed in J show up in the results.  
#
# expected results:
#
# 1. value_source_concept_id of the States that are not generalized show up AND 
#
# 2.only one row displays the generalized value_source_concept_id :  2000000011. 
#  
# step2 
#
# 1. query using the listed value_source_concept_id as condition
#
# 2. these are the states that are generalized. 
#
# expected results: returns no results 

# +
query=f''' 
WITH df1 AS (
SELECT distinct deid.value_source_concept_id
FROM
  `{project_id}.{com_cdr}.observation` com
  JOIN `{project_id}.{deid_cdr}.observation` deid 
  ON com.observation_id=deid.observation_id
  
WHERE
  com.observation_source_value LIKE 'StreetAddress_PIIState'
  AND com.observation_source_concept_id = 1585249
  AND ( com.value_source_concept_id =         1585299
OR com.value_source_concept_id =        1585304
OR com.value_source_concept_id =        1585284
OR com.value_source_concept_id =        1585315
OR com.value_source_concept_id =        1585271
OR com.value_source_concept_id =        1585263
OR com.value_source_concept_id =        1585306
OR com.value_source_concept_id =        1585274
OR com.value_source_concept_id =        1585270
OR com.value_source_concept_id =        1585411
OR com.value_source_concept_id =        1585313
OR com.value_source_concept_id =        1585409
OR com.value_source_concept_id =        1585262
OR com.value_source_concept_id =        1585309
OR com.value_source_concept_id =        1585307
OR com.value_source_concept_id =        1585275)
)

SELECT COUNT (*) AS n_row_not_pass FROM df1
WHERE value_source_concept_id !=2000000011
'''
df1=pd.read_gbq(query, dialect='standard')  

if df1.loc[0].sum()==0:
 df = df.append({'query' : 'Query5_state_generalization', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query5_state_generalization', 'result' : ''},  
                ignore_index = True) 
df1
# -

# # Summary_deid_household AND state generalization

# if not pass, will be highlighted in red
df = df.mask(df.isin(['Null','']))
df.style.highlight_null(null_color='red').set_properties(**{'text-align': 'left'})
