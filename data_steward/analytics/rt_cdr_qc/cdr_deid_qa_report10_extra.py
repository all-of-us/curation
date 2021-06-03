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
# #  Below are the queries we ran for extra validation that can be added to the RT _deid validation notebook:
#
# https://precisionmedicineinitiative.atlassian.net/browse/DC-1404
#
#

# + papermill={"duration": 0.709639, "end_time": "2021-02-02T22:30:32.661373", "exception": false, "start_time": "2021-02-02T22:30:31.951734", "status": "completed"} tags=[]
import urllib
import pandas as pd
pd.options.display.max_rows = 120

# + papermill={"duration": 0.023643, "end_time": "2021-02-02T22:30:31.880820", "exception": false, "start_time": "2021-02-02T22:30:31.857177", "status": "completed"} tags=["parameters"]
project_id = ""
com_cdr = ""
deid_cdr = ""
ct_deid=""
ct_deid_sand=""
deid_sand=""
pipeline=""
# -

# df will have a summary in the end
df = pd.DataFrame(columns = ['query', 'result']) 

# + [markdown] papermill={"duration": 0.02327, "end_time": "2021-02-02T22:30:32.708257", "exception": false, "start_time": "2021-02-02T22:30:32.684987", "status": "completed"} tags=[]
# # 1 No person exists over 89 in the dataset:

# + papermill={"duration": 4.105203, "end_time": "2021-02-02T22:30:36.813460", "exception": false, "start_time": "2021-02-02T22:30:32.708257", "status": "completed"} tags=[]
query = f'''

SELECT COUNT(*) as n_participants_over_89 FROM `{project_id}.{deid_cdr}.person`
WHERE person_id IN (
SELECT person_id FROM (
SELECT DISTINCT person_id, EXTRACT(YEAR FROM CURRENT_DATE()) - EXTRACT(YEAR FROM birth_datetime) AS age
FROM `{project_id}.{deid_cdr}.person`) WHERE age > 89)
'''
df1=pd.read_gbq(query, dialect='standard')
if df1.loc[0].sum()==0:
 df = df.append({'query' : 'Query1 No person exists over 89 in the dataset', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query1 No person exists over 89 in the dataset', 'result' : ''},  
                ignore_index = True) 
df1

# + [markdown] papermill={"duration": 0.023633, "end_time": "2021-02-02T22:30:36.860798", "exception": false, "start_time": "2021-02-02T22:30:36.837165", "status": "completed"} tags=[]
# # 2   No original person_id exists in the de-identified dataset:
# -

query = f'''

SELECT COUNT(*) as n_original_person_ids FROM `{project_id}.{deid_cdr}.person`
WHERE person_id IN (
SELECT person_id FROM `{project_id}.{com_cdr}.person`)
'''
df1=pd.read_gbq(query, dialect='standard')
if df1.loc[0].sum()==0:
 df = df.append({'query' : 'Query2 No original person_id exists', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query2 No original person_id exists', 'result' : ''},  
                ignore_index = True) 
df1

# # 3  These columns should be null, zero, or blank, queries should return 0 results:
#
# a. non null provider_id in condition_occurrence table:

query = f'''

SELECT COUNT(*) AS non_null_provider_ids FROM `{project_id}.{deid_cdr}.condition_occurrence`
WHERE provider_id IS NOT NULL
'''
df1=pd.read_gbq(query, dialect='standard')
if df1.loc[0].sum()==0:
 df = df.append({'query' : 'Query3a non null provider_id in condition_occurrence table', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query3a non null provider_id in condition_occurrence table', 'result' : ''},  
                ignore_index = True) 
df1

# b. non null cause_concept_id, cause_source_value, cause_source_concept_id in death table:

query = f'''

SELECT COUNT(*) AS non_null_values FROM `{project_id}.{deid_cdr}.death`
WHERE cause_concept_id IS NOT NULL OR cause_source_value IS NOT NULL OR cause_source_concept_id IS NOT NULL

'''
df1=pd.read_gbq(query, dialect='standard')
if df1.loc[0].sum()==0:
 df = df.append({'query' : 'Query3b non null cause_concept_id, cause_source_value, cause_source_concept_id in death table', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query3b non null cause_concept_id, cause_source_value, cause_source_concept_id in death table', 'result' : ''},  
                ignore_index = True) 
df1

# c. non null provider_id in device_exposure table:

query = f'''
SELECT COUNT(*) AS non_null_provider_ids FROM `{project_id}.{deid_cdr}.device_exposure`
WHERE provider_id IS NOT NULL

'''
df1=pd.read_gbq(query, dialect='standard')
if df1.loc[0].sum()==0:
 df = df.append({'query' : 'Query3c non null provider_id in device_exposure table', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query3c non null provider_id in device_exposure table', 'result' : ''},  
                ignore_index = True) 
df1

# d. non null value_source_value in measurement table:

query = f'''
SELECT COUNT(*) AS non_null_value_source_value FROM `{project_id}.{deid_cdr}.measurement`
WHERE value_source_value IS NOT NULL

'''
df1=pd.read_gbq(query, dialect='standard')
if df1.loc[0].sum()==0:
 df = df.append({'query' : 'Query3d non null value_source_value in measurement table', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query3d non null value_source_value in measurement table', 'result' : ''},  
                ignore_index = True) 
df1

#   e. non null value_source_value, value_as_string, and provider_id in observation table:

query = f'''
SELECT COUNT(*) AS non_null_values FROM `{project_id}.{deid_cdr}.observation`
WHERE value_source_value IS NOT NULL OR value_as_string IS NOT NULL OR provider_id IS NOT NULL
'''
df1=pd.read_gbq(query, dialect='standard')
if df1.loc[0].sum()==0:
 df = df.append({'query' : 'Query3e non null value_source_value, value_as_string, and provider_id in observation table', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query3e non null value_source_value, value_as_string, and provider_id in observation table', 'result' : ''},  
                ignore_index = True) 
df1

# f. non null values in person table:

query = f'''
SELECT COUNT(*) AS non_null_values FROM `{project_id}.{deid_cdr}.person` WHERE
month_of_birth IS NOT NULL OR day_of_birth IS NOT NULL OR location_id IS NOT NULL
OR provider_id IS NOT NULL OR care_site_id IS NOT NULL OR person_source_value IS NOT NULL
OR gender_source_value IS NOT NULL OR gender_source_concept_id IS NOT NULL OR race_source_value IS NOT NULL
OR race_source_concept_id IS NOT NULL OR ethnicity_source_value IS NOT NULL OR ethnicity_source_concept_id IS NOT NULL

'''
df1=pd.read_gbq(query, dialect='standard')
if df1.loc[0].sum()==0:
 df = df.append({'query' : 'Query3f non null values in person table', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query3f non null values in person table', 'result' : ''},  
                ignore_index = True) 
df1

# g. non zero year_of_birth, race_concept_id, and ethnicity_concept_id in person table:

query = f'''
SELECT COUNT(*) AS non_zero_values FROM `{project_id}.{deid_cdr}.person`
WHERE race_concept_id != 0 OR ethnicity_concept_id != 0 OR year_of_birth != 0

'''
df1=pd.read_gbq(query, dialect='standard')
if df1.loc[0].sum()==0:
 df = df.append({'query' : 'Query3g non zero year_of_birth, race_concept_id, and ethnicity_concept_id in person table:', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query3g non zero year_of_birth, race_concept_id, and ethnicity_concept_id in person table:', 'result' : ''},  
                ignore_index = True) 
df1

# h. non null provider_id in procedure_occurrence table:

query = f'''
SELECT COUNT(*) AS non_null_provider_ids FROM `{project_id}.{deid_cdr}.procedure_occurrence`
WHERE provider_id IS NOT NULL

'''
df1=pd.read_gbq(query, dialect='standard')
if df1.loc[0].sum()==0:
 df = df.append({'query' : 'Query3h non null provider_id in procedure_occurrence table', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query3h non null provider_id in procedure_occurrence table', 'result' : ''},  
                ignore_index = True) 
df1

# i. non null provider_id and care_site_id in visit_occurrence table:

query = f'''
SELECT COUNT(*) AS non_null_values FROM `{project_id}.{deid_cdr}.visit_occurrence`
WHERE provider_id IS NOT NULL OR care_site_id IS NOT NULL
'''
df1=pd.read_gbq(query, dialect='standard')
if df1.loc[0].sum()==0:
 df = df.append({'query' : 'Query3i non null provider_id and care_site_id in visit_occurrence table', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query3i non null provider_id and care_site_id in visit_occurrence table', 'result' : ''},  
                ignore_index = True) 
df1

# + [markdown] papermill={"duration": 0.023649, "end_time": "2021-02-02T22:30:39.115495", "exception": false, "start_time": "2021-02-02T22:30:39.091846", "status": "completed"} tags=[]
# # 4 In the extension tables the srce_id/<omop_table>_id pairs match between the RT and CT:
# -

# ## 4a observation_ext

query = f'''
SELECT COUNT (*) AS n_row_not_pass
FROM `{project_id}.{ct_deid}.observation_ext` c
LEFT JOIN `{project_id}.{deid_cdr}.observation_ext` r
USING (observation_id)
WHERE c.src_id != r.src_id AND r.src_id is not null and c.src_id is not null
-- identify if RT and CT are USING the same masking values
'''
df1=pd.read_gbq(query, dialect='standard')
if df1.loc[0].sum()==0:
 df = df.append({'query' : 'Query4a src_id matching in observation between CT and RT', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query4a src_id matching in observation between CT and RT', 'result' : ''},  
                ignore_index = True) 
df1

# ## 4b sandbox._site_mappings

query = f'''
SELECT COUNT (*) AS n_row_not_pass
FROM `{project_id}.{ct_deid_sand}._site_mappings` as c
LEFT JOIN `{project_id}.{deid_sand}.site_maskings` as r
USING (hpo_id)
WHERE c.src_id != r.src_id
'''
df1=pd.read_gbq(query, dialect='standard')
if df1.loc[0].sum()==0:
 df = df.append({'query' : 'Query4b sandbox.site_maskings matching', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query4b sandbox.site_maskings matching', 'result' : ''},  
                ignore_index = True) 
df1

# ## 4c pipeline_tables.site_maskings

# +
query = f'''

SELECT COUNT (*) AS n_row_not_pass
FROM `{project_id}.{pipeline}.site_maskings` as c
LEFT JOIN `{project_id}.{deid_sand}.site_maskings` as r
USING (hpo_id)
WHERE c.src_id != r.src_id
-- registered tier did use the stabilized maskings for cross pipeline compatibility

'''
df1=pd.read_gbq(query, dialect='standard')
if df1.loc[0].sum()==0:
 df = df.append({'query' : 'Query4c pipeline_tables.site_maskings matching', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query4c pipeline_tables.site_maskings matching', 'result' : ''},  
                ignore_index = True) 

df1

# + [markdown] papermill={"duration": 0.023649, "end_time": "2021-02-02T22:30:39.115495", "exception": false, "start_time": "2021-02-02T22:30:39.091846", "status": "completed"} tags=[]
# # 5 No participants should be FROM any of these states (1585299, 1585304, 1585284, 1585315, 1585271, 1585263, 1585306, 1585274, 1585270, 1585411, 1585313, 1585409, 1585262, 1585309, 1585307, 1585275):

# + papermill={"duration": 2.338821, "end_time": "2021-02-02T22:30:41.501415", "exception": false, "start_time": "2021-02-02T22:30:39.162594", "status": "completed"} tags=[]
query = f'''

SELECT COUNT(1) as num_of_invalid_states
FROM `{project_id}.{deid_cdr}.observation`
WHERE
observation_source_concept_id = 1585249 and
value_source_concept_id IN (1585299, 1585304, 1585284, 1585315, 1585271, 1585263, 1585306, 1585274, 1585270, 
1585411, 1585313, 1585409, 1585262, 1585309, 1585307, 1585275)
'''
df1=pd.read_gbq(query, dialect='standard')
if df1.loc[0].sum()==0:
 df = df.append({'query' : 'Query5 No participants in states', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query5 No participants in states', 'result' : ''},  
                ignore_index = True) 
df1

# + [markdown] papermill={"duration": 0.023649, "end_time": "2021-02-02T22:30:39.115495", "exception": false, "start_time": "2021-02-02T22:30:39.091846", "status": "completed"} tags=[]
# # 6 A participant should have only one gender identity record in the observation table:

# + papermill={"duration": 2.338821, "end_time": "2021-02-02T22:30:41.501415", "exception": false, "start_time": "2021-02-02T22:30:39.162594", "status": "completed"} tags=[]
query = f'''
WITH df1 AS (
SELECT value_source_concept_id, value_as_concept_id, count(person_id) as n_answers
FROM `{project_id}.{deid_cdr}.observation`
WHERE observation_source_concept_id = 1585838
GROUP BY person_id, value_source_concept_id, value_as_concept_id
HAVING count(person_id) > 1)

SELECT COUNT (*) AS n_row_not_pass FROM df1

'''
df1=pd.read_gbq(query, dialect='standard')
if df1.loc[0].sum()==0:
 df = df.append({'query' : 'Query6 only one gender identity record in the observation', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query6 only one gender identity record in the observation', 'result' : ''},  
                ignore_index = True) 
df1

# + [markdown] papermill={"duration": 0.023649, "end_time": "2021-02-02T22:30:39.115495", "exception": false, "start_time": "2021-02-02T22:30:39.091846", "status": "completed"} tags=[]
# # 7  A participant has one race answer (excluding ethnicity answers) in observation table:
# -

# correct one by francis
query = f'''
WITH df1 AS (
SELECT person_id, count(value_source_concept_id) as countp
FROM `{project_id}.{deid_cdr}.observation`
WHERE observation_source_concept_id = 1586140 AND value_as_concept_id !=1586147
GROUP BY person_id
)
SELECT COUNT (*) AS n_row_not_pass FROM df1
WHERE countp >1

'''
df1=pd.read_gbq(query, dialect='standard')
if df1.loc[0].sum()==0:
 df = df.append({'query' : 'Query7 has one race answer in observation', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query7 has one race answer in observation', 'result' : ''},  
                ignore_index = True) 
df1

# # 8  Any response that isn’t straight (1585900) should be generalized to (2000000003):

# +
query = f'''
WITH df1 AS (
SELECT person_id
FROM `aou-res-curation-prod.{com_cdr}.observation` ob
WHERE ob.observation_source_concept_id = 1585899
AND value_source_concept_id !=1585900)

SELECT COUNT (*) AS n_row_not_pass
FROM `aou-res-curation-prod.{deid_cdr}.observation` ob_deid
WHERE ob_deid.person_id in (SELECT person_id FROM df1)
and ob_deid.observation_source_concept_id = 1585899
and ob_deid.value_source_concept_id !=2000000003
'''
df1=pd.read_gbq(query, dialect='standard')
if df1.loc[0].sum()==0:
 df = df.append({'query' : 'Query8 non_straight gender be generalized', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query8 non_straight gender be generalized', 'result' : ''},  
                ignore_index = True) 

df1
# -

# # 9 Sex at birth should be limited to 1585847, 1585846, and 2000000009:
#
# need some work here
#
# is orignal sql answering the question?

query = f'''
SELECT COUNT(*) as n_row_not_pass
FROM `{project_id}.{deid_cdr}.observation`
WHERE observation_source_concept_id = 1585845
AND value_source_concept_id not in (1585847, 1585846,2000000009)
'''
df1=pd.read_gbq(query, dialect='standard')
if df1.loc[0].sum()==0:
 df = df.append({'query' : 'Query9 correct sex_at_birth concept_id', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query9 correct sex_at_birth concept_id', 'result' : ''},  
                ignore_index = True) 
df1

# # 10 Education levels ( value_source_concept_id) should be limited to 2000000007, 2000000006, 1585945, 43021808, 903079, 1177221, 1585946, 4260980, and 903096:

# +
query = f'''
SELECT COUNT (*) as n_row_not_pass
FROM `{project_id}.{deid_cdr}.observation`
WHERE observation_source_concept_id = 1585940
AND value_source_concept_id NOT IN (2000000007, 2000000006, 1585945, 43021808, 903079, 
1177221, 1585946, 4260980, 903096)
'''
df1=pd.read_gbq(query, dialect='standard')
if df1.loc[0].sum()==0:
 df = df.append({'query' : 'Query10 correct education level concept_id', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query10 correct education level concept_id', 'result' : ''},  
                ignore_index = True) 

df1
# -

# # 11. Employment records should be restricted to 2000000005 and 2000000004:
# need work
#
# questions: is other two ok?

query = f'''
SELECT COUNT (*) as n_row_not_pass
FROM `{project_id}.{deid_cdr}.observation`
WHERE observation_source_concept_id = 1585952
And value_source_concept_id not in (2000000005, 2000000004,903079,903096)
'''
df1=pd.read_gbq(query, dialect='standard')
if df1.loc[0].sum()==0:
 df = df.append({'query' : 'Query11 correct Employment records concept_id', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query11 correct Employment records concept_id', 'result' : ''},  
                ignore_index = True) 
df1

# # 12. questionnaire_response_id should be the same between RT and CT:

query = f'''
SELECT COUNT (*) AS n_row_not_pass
FROM `{project_id}.{ct_deid}.observation` c
LEFT JOIN `{project_id}.{deid_cdr}.observation` r
USING (observation_id)
WHERE c.questionnaire_response_id != r.questionnaire_response_id
AND r.questionnaire_response_id IS NOT NULL
AND c.questionnaire_response_id IS NOT NULL
'''
df1=pd.read_gbq(query, dialect='standard')
if df1.loc[0].sum()==0:
 df = df.append({'query' : 'Query12  same questionnaire_response_id in RT and CT ', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query12 same questionnaire_response_id in RT and CT', 'result' : ''},  
                ignore_index = True) 
df1

# # Summary_deid_extra_validation

# if not pass, will be highlighted in red
df = df.mask(df.isin(['Null','']))
df.style.highlight_null(null_color='red').set_properties(**{'text-align': 'left'})
