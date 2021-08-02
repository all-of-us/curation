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
# see spread sheet COPE - All Surveys Privacy Rules for details
#
# https://docs.google.com/spreadsheets/d/1UuUVcRdlp2HkBaVdROFsM4ZX_bfffg6ZoEbqj94MlXU/edit#gid=0
#
#  Related tickets [DC-892] [DC-1752]
#  
#  [DC-1752] Refactor analysis 1 so that it provides the observation_source_concept_id, concept_code, concept_name, vocabulary_id, row count per cope survey concept (example query below). Reword the title text to read: Verify that the COPE Survey concepts identified to be suppressed as de-identification action have been removed. 
#
#  [DC-1784] 1310144, 1310145, 1310148, 715725, 715724
#  
# The following concepts should be suppressed
#
# 715711, 1333327, 1333326, 1333014, 1333118, 1332742,1333324 ,1333012 ,1333234,
#
# 903632,702686,715714, 715724, 715725, 715726, 1310054, 1310058, 1310066, 1310146, 1310147, 1333234, 1310065,
#
# 596884, 596885, 596886, 596887, 596888, 596889, 1310137,1333016,1310148,1310145,1310144

# +
query = f'''

SELECT observation_source_concept_id, concept_name,concept_code,vocabulary_id,observation_concept_id,COUNT(1) AS n_row_not_pass FROM 
`{project_id}.{deid_cdr}.observation` ob
JOIN `{project_id}.{deid_cdr}.concept` c
ON ob.observation_source_concept_id=c.concept_id
WHERE observation_source_concept_id IN
(715711, 1333327, 1333326, 1333014, 1333118, 1332742,1333324 ,1333012 ,1333234, 
903632,702686,715714, 715724, 715725, 715726, 1310054, 1310058, 1310066, 1310146, 1310147, 1333234, 1310065, 
596884, 596885, 596886, 596887, 596888, 596889, 1310137,1333016,1310148,1310145,1310144)
OR observation_concept_id IN
(715711, 1333327, 1333326, 1333014, 1333118, 1332742,1333324 ,1333012 ,1333234, 
903632,702686,715714, 715724, 715725, 715726, 1310054, 1310058, 1310066, 1310146, 1310147, 1333234, 1310065, 
596884, 596885, 596886, 596887, 596888, 596889, 1310137,1333016,1310148,1310145,1310144)
GROUP BY 1,2,3,4,5
ORDER BY n_row_not_pass DESC

'''
df1=pd.read_gbq(query, dialect='standard')

if df1['n_row_not_pass'].sum()==0:
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
#
# these should be generalized 2100000002,2100000003,2100000004
# -

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

df1.style.format("{:.0f}")

# + [markdown] papermill={"duration": 0.023649, "end_time": "2021-02-02T22:30:39.115495", "exception": false, "start_time": "2021-02-02T22:30:39.091846", "status": "completed"} tags=[]
# # 3 Verify that all structured concepts related  to COVID are NOT suppressed in EHR tables
#   
#   DC-891
#
# 756055,4100065,37311061,439676,37311060,45763724
#
# update, Remove analyses 3, 4, and 5 as suppression of COVID concepts is no longer part of RT privacy requirements,[DC-1752]

# +
query = f'''

SELECT measurement_concept_id, concept_name,concept_code,vocabulary_id,COUNT(1) AS n_row_not_pass FROM 
`{project_id}.{deid_cdr}.measurement` ob
JOIN `{project_id}.{deid_cdr}.concept` c
ON ob.measurement_concept_id=c.concept_id
WHERE measurement_concept_id=756055
GROUP BY 1,2,3,4
ORDER BY n_row_not_pass DESC

'''
df1=pd.read_gbq(query, dialect='standard')

if df1['n_row_not_pass'].sum()==0:
 df = df.append({'query' : 'Query3 No COPE in deid_measurement table', 'result' : ''},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query3 No COPE in deid_measurement table' , 'result' : 'PASS'},  
                ignore_index = True) 
df1

# + [markdown] papermill={"duration": 0.023649, "end_time": "2021-02-02T22:30:39.115495", "exception": false, "start_time": "2021-02-02T22:30:39.091846", "status": "completed"} tags=[]
# # 4 Verify that all structured concepts related  to COVID are NOT suppressed in EHR condition_occurrence
#   
#   DC-891
#
# 756055,4100065,37311061,439676,37311060,45763724
#
# update, Remove analyses 3, 4, and 5 as suppression of COVID concepts is no longer part of RT privacy requirements,[DC-1752]

# +
query = f'''

SELECT condition_concept_id, concept_name,concept_code,vocabulary_id,COUNT(1) AS n_row_not_pass FROM 
`{project_id}.{deid_cdr}.condition_occurrence` ob
JOIN `{project_id}.{deid_cdr}.concept` c
ON ob.condition_concept_id=c.concept_id
WHERE condition_concept_id IN  (4100065, 37311061, 439676)
GROUP BY 1,2,3,4
ORDER BY n_row_not_pass DESC

'''
df1=pd.read_gbq(query, dialect='standard')

if df1['n_row_not_pass'].sum()==0:
 df = df.append({'query' : 'Query4 COVID concepts suppression in deid_observation table', 'result' : ''},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query4 COVID concepts suppression in deid_observation table' , 'result' : 'PASS'},  
                ignore_index = True) 
df1

# + [markdown] papermill={"duration": 0.023649, "end_time": "2021-02-02T22:30:39.115495", "exception": false, "start_time": "2021-02-02T22:30:39.091846", "status": "completed"} tags=[]
# # 5 Verify that all structured concepts related  to COVID are NOT suppressed in EHR observation
#   
#   DC-891
#
# 756055,4100065,37311061,439676,37311060,45763724
#
# update, Remove analyses 3, 4, and 5 as suppression of COVID concepts is no longer part of RT privacy requirements,[DC-1752]

# +
query = f'''

SELECT observation_concept_id, concept_name,concept_code,vocabulary_id,observation_source_concept_id,COUNT(1) AS n_row_not_pass FROM 
`{project_id}.{deid_cdr}.observation` ob
JOIN `{project_id}.{deid_cdr}.concept` c
ON ob.observation_concept_id=c.concept_id
WHERE observation_concept_id IN  (37311060, 45763724) OR observation_source_concept_id IN  (37311060, 45763724)
GROUP BY 1,2,3,4,5
ORDER BY n_row_not_pass DESC

'''
df1=pd.read_gbq(query, dialect='standard')

if df1['n_row_not_pass'].sum()==0:
 df = df.append({'query' : 'Query5 COVID concepts suppression in observation table', 'result' : ''},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query5 COVID concepts suppression in observation table' , 'result' : 'PASS'},  
                ignore_index = True) 
df1
# -

# # 6 Verify these concepts are NOT suppressed in EHR observation
#   
# [DC-1747]
# these concepts 1333015, 	1333023	are not longer suppressed
#
# 1332737, [DC-1665] 
#
# 1333291
#
# 1332904,1333140 should be generalized to 1332737
#
# 1332843 should be generalized. 

# +
query = f'''

SELECT observation_source_concept_id, concept_name,concept_code,vocabulary_id,observation_concept_id,COUNT(1) AS n_row_not_pass FROM 
`{project_id}.{deid_cdr}.observation` ob
JOIN `{project_id}.{deid_cdr}.concept` c
ON ob.observation_source_concept_id=c.concept_id
WHERE observation_source_concept_id IN  (1333015, 1333023, 1332737,1333291,1332904,1333140,1332843) OR observation_concept_id IN  (1333015, 1333023,1332737,1333291,1332904,1333140,1332843 )
GROUP BY 1,2,3,4,5
ORDER BY n_row_not_pass DESC

'''
df1=pd.read_gbq(query, dialect='standard')

if df1['n_row_not_pass'].sum()==0:
 df = df.append({'query' : 'Query6 The concepts are not suppressed in observation table', 'result' : ''},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query6 The concepts are not suppressed in observation table' , 'result' : 'PASS'},  
                ignore_index = True) 
df1
# -

# # 7 Vaccine-related concepts as these EHR-submitted COVID concepts are disallowed from RT 
#
# DC-1752
#

# +

query = f'''
DECLARE vocabulary_tables DEFAULT ['vocabulary', 'concept', 'source_to_concept_map', 
                                   'concept_class', 'concept_synonym', 'concept_ancestor',
                                   'concept_relationship', 'relationship', 'drug_strength'];

DECLARE query STRING;

CREATE OR REPLACE TABLE `aou-res-curation-prod.R2021q3r1_deid_sandbox.concept_usage` (
 concept_id   INT64
,table_name   STRING NOT NULL
,column_name  STRING NOT NULL
,row_count    INT64 NOT NULL
)
OPTIONS (
  description='Concept usage counts in R2021q3r1_deid'
 ,expiration_timestamp=TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 60 DAY)
);

SET query = (
SELECT 
 STRING_AGG('SELECT ' 
     || column_name         || '  AS concept_id '
            || ',"' || table_name  || '" AS table_name '
     || ',"' || column_name || '" AS column_name '
     || ', COUNT(1) AS row_count '
     || 'FROM `' || table_schema || '.' || table_name || '` t '
     || 'GROUP BY 1, 2, 3',
           ' UNION ALL ')
FROM `R2021q3r1_deid.INFORMATION_SCHEMA.COLUMNS` c
JOIN `R2021q3r1_deid.__TABLES__` t
 ON c.table_name = t.table_id
WHERE 
      table_name NOT IN UNNEST(vocabulary_tables)
  AND t.row_count > 0
  AND table_name NOT LIKE '\\\_%'
  AND LOWER(column_name) LIKE '%concept_id%'
);

EXECUTE IMMEDIATE 'INSERT `aou-res-curation-prod.R2021q3r1_deid_sandbox.concept_usage`' || query;

WITH 
vaccine_concept AS
(
 SELECT *
 FROM `aou-res-curation-prod.R2021q3r1_deid.concept` 
 WHERE (
        -- done by name and vocab -- this alone should be enough, no need for others--
        REGEXP_CONTAINS(concept_name, r'(?i)(COVID)') AND
        REGEXP_CONTAINS(concept_name, r'(?i)(VAC)') AND 
        vocabulary_id not in ('PPI')
    ) OR (
        -- done by code  and vocab --
        REGEXP_CONTAINS(concept_code, r'(207)|(208)|(210)|(212)|(213)') --not 211--
        and vocabulary_id = 'CVX'
    ) OR (
        -- done by code and vocab --
        REGEXP_CONTAINS(concept_code, r'(91300)|(91301)|(91302)|(91303)|(0031A)|(0021A)|(0022A)|(0002A)|(0001A)|(0012A)|(0011A)')   --no 91304--
        and vocabulary_id = 'CPT4'
     )
)

SELECT u.* 
FROM
`aou-res-curation-prod.R2021q3r1_deid_sandbox.concept_usage` u
JOIN vaccine_concept c
USING (concept_id)
ORDER BY row_count DESC

'''
df1=pd.read_gbq(query, dialect='standard') 
if df1['row_count'].sum()==0:
 df = df.append({'query' : 'Query7 COVID Vaccine-related concepts suppression in EHR tables', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query7 COVID Vaccine-related concepts suppression in EHR tables' , 'result' : ''},  
                ignore_index = True) 
df1
# -

# # Summary_deid_COPE_survey

# if not pass, will be highlighted in red
df = df.mask(df.isin(['Null','']))
df.style.highlight_null(null_color='red').set_properties(**{'text-align': 'left'})
