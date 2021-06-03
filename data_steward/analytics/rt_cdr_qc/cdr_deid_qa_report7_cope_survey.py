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

# + [markdown] papermill={"duration": 0.024011, "end_time": "2021-02-02T22:30:31.951734", "exception": false, "start_time": "2021-02-02T22:30:31.927723", "status": "completed"} tags=[]
# #  QA queries on new CDR_deid COPE Survey
#
# Quality checks performed on a new CDR_deid dataset using QA queries

# + papermill={"duration": 0.709639, "end_time": "2021-02-02T22:30:32.661373", "exception": false, "start_time": "2021-02-02T22:30:31.951734", "status": "completed"} tags=[]
import urllib
import pandas as pd
pd.options.display.max_rows = 120

# + papermill={"duration": 0.023643, "end_time": "2021-02-02T22:30:31.880820", "exception": false, "start_time": "2021-02-02T22:30:31.857177", "status": "completed"} tags=["parameters"]
project_id = ""
com_cdr = ""
deid_cdr = ""
# deid_base_cdr=""
# -

# df will have a summary in the end
df = pd.DataFrame(columns = ['query', 'result']) 

# + [markdown] papermill={"duration": 0.02327, "end_time": "2021-02-02T22:30:32.708257", "exception": false, "start_time": "2021-02-02T22:30:32.684987", "status": "completed"} tags=[]
# # 1 Verify that the COPE Survey Data identified to be suppressed as de-identification action in OBSERVATION table have been removed from the de-id dataset.
#
#  [DC-892] 
#
# 715711, 1333327, 1333326, 1333014, 1333118, 1332742,1333324 1333012 1333234

# + papermill={"duration": 4.105203, "end_time": "2021-02-02T22:30:36.813460", "exception": false, "start_time": "2021-02-02T22:30:32.708257", "status": "completed"} tags=[]
query = f'''

SELECT COUNT(*) AS n_row_not_pass FROM 
`{project_id}.{deid_cdr}.observation` 
WHERE observation_source_concept_id IN
(715711, 1333327, 1333326, 1333014, 1333118, 1332742, 1333324, 1333012, 1333234)

'''
df1=pd.read_gbq(query, dialect='standard')
if df1.loc[0].sum()==0:
 df = df.append({'query' : 'Query1 No COPE in deid_observation table', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query1 No COPE in deid_observation table' , 'result' : ''},  
                ignore_index = True) 
df1

# + [markdown] papermill={"duration": 0.023633, "end_time": "2021-02-02T22:30:36.860798", "exception": false, "start_time": "2021-02-02T22:30:36.837165", "status": "completed"} tags=[]
# # 2   Verify if a survey version is provided for the COPE survey.
#
# [DC-1040]
#
# expected results: all the person_id and the questionnaire_response_id has a survey_version_concept_id 
# original sql missed something.
# -

# ## error in new cdr, no sruvey version at all??

# correct version
query = f'''
WITH df1 as (
SELECT distinct survey_version_concept_id
FROM `{project_id}.{deid_cdr}.concept` c1
LEFT JOIN `{project_id}.{deid_cdr}.concept_relationship` cr ON cr.concept_id_2 = c1.concept_id
JOIN `{project_id}.{deid_cdr}.observation` ob on ob.observation_concept_id=c1.concept_id
LEFT JOIN `{project_id}.{deid_cdr}.observation_ext` ext USING(observation_id)

WHERE
 cr.concept_id_1 IN (1333174,1333343,1333207,1333310,1332811,1332812,1332715,1332813,1333101,1332814,1332815,1332816,1332817,1332818)
 AND cr.relationship_id = "PPI parent code of" 
 )
 
SELECT COUNT (*) AS n_row_not_pass FROM df1
WHERE survey_version_concept_id=0 or survey_version_concept_id IS NULL
'''
df1=pd.read_gbq(query, dialect='standard')
if df1.loc[0].sum()==0:
 df = df.append({'query' : 'Query2 survey version provided', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query2 survey version provided', 'result' : ''},  
                ignore_index = True) 
df1

# +
# new cdr
query = f'''
SELECT
 distinct survey_version_concept_id
FROM  `{project_id}.{deid_cdr}.observation` d
JOIN  `{project_id}.{deid_cdr}.observation_ext` e
ON  e.observation_id = d.observation_id

'''
df1=pd.read_gbq(query, dialect='standard')

df1

# + [markdown] papermill={"duration": 0.023649, "end_time": "2021-02-02T22:30:39.115495", "exception": false, "start_time": "2021-02-02T22:30:39.091846", "status": "completed"} tags=[]
# # 3 Verify that all structured concepts related  to COVID is suppressed in EHR tables
#   
#   DC-891
#
# 756055,4100065,37311061,439676,37311060,45763724

# + papermill={"duration": 2.338821, "end_time": "2021-02-02T22:30:41.501415", "exception": false, "start_time": "2021-02-02T22:30:39.162594", "status": "completed"} tags=[]
query = f'''
SELECT COUNT (*) AS n_row_not_pass FROM `{project_id}.{deid_cdr}.measurement` WHERE measurement_concept_id = 756055 
'''
df1=pd.read_gbq(query, dialect='standard')
if df1.loc[0].sum()==0:
 df = df.append({'query' : 'Query3 COVID concepts suppression in measurement table', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query3 COVID concepts suppression in measurement table', 'result' : ''},  
                ignore_index = True) 
df1

# + [markdown] papermill={"duration": 0.023649, "end_time": "2021-02-02T22:30:39.115495", "exception": false, "start_time": "2021-02-02T22:30:39.091846", "status": "completed"} tags=[]
# # 4 Verify that all structured concepts related  to COVID is suppressed in EHR condition_occurrence
#   
#   DC-891
#
# 756055,4100065,37311061,439676,37311060,45763724

# + papermill={"duration": 2.338821, "end_time": "2021-02-02T22:30:41.501415", "exception": false, "start_time": "2021-02-02T22:30:39.162594", "status": "completed"} tags=[]
query = f'''

SELECT COUNT (*) AS n_row_not_pass FROM `{project_id}.{deid_cdr}.condition_occurrence`
WHERE condition_concept_id IN  (4100065, 37311061, 439676)
'''
df1=pd.read_gbq(query, dialect='standard')
if df1.loc[0].sum()==0:
 df = df.append({'query' : 'Query4 COVID concepts suppression in condition table', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query4 COVID concepts suppression in condition table', 'result' : ''},  
                ignore_index = True) 
df1

# + [markdown] papermill={"duration": 0.023649, "end_time": "2021-02-02T22:30:39.115495", "exception": false, "start_time": "2021-02-02T22:30:39.091846", "status": "completed"} tags=[]
# # 5 Verify that all structured concepts related  to COVID is suppressed in EHR observation
#   
#   DC-891
#
# 756055,4100065,37311061,439676,37311060,45763724

# + papermill={"duration": 2.338821, "end_time": "2021-02-02T22:30:41.501415", "exception": false, "start_time": "2021-02-02T22:30:39.162594", "status": "completed"} tags=[]
query = f'''

SELECT COUNT (*) AS n_row_not_pass  
FROM `{project_id}.{deid_cdr}.observation` 
WHERE observation_concept_id IN  (37311060, 45763724) 

'''
df1=pd.read_gbq(query, dialect='standard')
if df1.loc[0].sum()==0:
 df = df.append({'query' : 'Query5 COVID concepts suppression in observation table', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query5 COVID concepts suppression in observation table', 'result' : ''},  
                ignore_index = True) 
df1
# -

# # Summary_deid_COPE_survey

# if not pass, will be highlighted in red
df = df.mask(df.isin(['Null','']))
df.style.highlight_null(null_color='red').set_properties(**{'text-align': 'left'})
