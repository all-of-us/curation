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
# #  QA queries on new CDR_deid_clean 

# + papermill={"duration": 0.709639, "end_time": "2021-02-02T22:30:32.661373", "exception": false, "start_time": "2021-02-02T22:30:31.951734", "status": "completed"} tags=[]
import urllib
import pandas as pd
pd.options.display.max_rows = 120

# + papermill={"duration": 0.023643, "end_time": "2021-02-02T22:30:31.880820", "exception": false, "start_time": "2021-02-02T22:30:31.857177", "status": "completed"} tags=["parameters"]
project_id = ""
deid_clean = ""
# -

# df will have a summary in the end
df = pd.DataFrame(columns = ['query', 'result']) 

# ## QA queries on new CDR_deid_clean drop rows with 0 OR null

# + [markdown] papermill={"duration": 0.02327, "end_time": "2021-02-02T22:30:32.708257", "exception": false, "start_time": "2021-02-02T22:30:32.684987", "status": "completed"} tags=[]
# # 1 Verify that in observation table if observation_source_concept_id AND the observation_concept_id, both of those fields are null OR zero, the row should be removed. 

# + papermill={"duration": 4.105203, "end_time": "2021-02-02T22:30:36.813460", "exception": false, "start_time": "2021-02-02T22:30:32.708257", "status": "completed"} tags=[]
query = f'''

SELECT 
SUM(CASE WHEN observation_source_concept_id = 0 AND observation_concept_id = 0 THEN 1 ELSE 0 END) AS n_observation_source_concept_id_both_0,
SUM(CASE WHEN observation_source_concept_id IS NULL AND observation_concept_id IS NULL THEN 1 ELSE 0 END) AS n_observation_source_concept_id_both_null,
SUM(CASE WHEN observation_source_concept_id = 0 AND observation_concept_id IS NULL THEN 1 ELSE 0 END) AS n_observation_source_concept_id_either_0,
SUM(CASE WHEN observation_source_concept_id IS NULL AND observation_concept_id=0 THEN 1 ELSE 0 END) AS n_observation_source_concept_id_either_null


FROM `{project_id}.{deid_clean}.observation`
'''
df1=pd.read_gbq(query, dialect='standard')
if df1.loc[0].sum()==0:
 df = df.append({'query' : 'Query1 observation', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query1 observation', 'result' : ''},  
                ignore_index = True) 
df1.T

# + [markdown] papermill={"duration": 0.023633, "end_time": "2021-02-02T22:30:36.860798", "exception": false, "start_time": "2021-02-02T22:30:36.837165", "status": "completed"} tags=[]
# # 2   Verify that in condition_occurrence if condition_occurrence_source_concept_id AND the condition_occurrence_concept_id both of those fields are null OR zero, the row should be removed. 
# -

query = f'''

SELECT  
SUM(CASE WHEN condition_source_concept_id = 0 AND condition_concept_id = 0 THEN 1 ELSE 0 END) AS n_condition_source_concept_id_both_0,
SUM(CASE WHEN condition_source_concept_id IS NULL AND condition_concept_id IS NULL THEN 1 ELSE 0 END) AS n_condition_source_concept_id_both_null,
SUM(CASE WHEN condition_source_concept_id = 0 AND condition_concept_id IS NULL THEN 1 ELSE 0 END) AS n_condition_source_concept_id_either_0,
SUM(CASE WHEN condition_source_concept_id IS NULL AND condition_concept_id=0 THEN 1 ELSE 0 END) AS n_condition_source_concept_id_either_null


FROM `{project_id}.{deid_clean}.condition_occurrence`
WHERE (condition_source_concept_id = 0 AND  condition_concept_id=0)
OR ( condition_source_concept_id IS NULL AND condition_concept_id IS NULL)
'''
df1=pd.read_gbq(query, dialect='standard')
if df1.loc[0].sum()==0:
 df = df.append({'query' : 'Query2 condition', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query2 condition', 'result' : ''},  
                ignore_index = True) 
df1.T

# # 3  Verify that in procedure_occurrence table if procedure_occurrence_source_concept_id AND the procedure_occurrence_concept_id both of those fields are null OR zero, the row should be removed. 

query = f'''

SELECT 
SUM(CASE WHEN procedure_source_concept_id = 0 AND procedure_concept_id = 0 THEN 1 ELSE 0 END) AS n_procedure_source_concept_id_both_0,
SUM(CASE WHEN procedure_source_concept_id IS NULL AND procedure_concept_id IS NULL THEN 1 ELSE 0 END) AS n_procedure_source_concept_id_both_null,
SUM(CASE WHEN procedure_source_concept_id = 0 AND procedure_concept_id IS NULL THEN 1 ELSE 0 END) AS n_procedure_source_concept_id_either_0,
SUM(CASE WHEN procedure_source_concept_id IS NULL AND procedure_concept_id=0 THEN 1 ELSE 0 END) AS n_procedure_source_concept_id_either_null

FROM `{project_id}.{deid_clean}.procedure_occurrence`
'''
df1=pd.read_gbq(query, dialect='standard')
if df1.loc[0].sum()==0:
 df = df.append({'query' : 'Query3 procedure', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query3 procedure', 'result' : ''},  
                ignore_index = True) 
df1.T

# + [markdown] papermill={"duration": 0.023649, "end_time": "2021-02-02T22:30:39.115495", "exception": false, "start_time": "2021-02-02T22:30:39.091846", "status": "completed"} tags=[]
# # 4 Verify that in visit_occurrence table if visit_occurrence_source_concept_id AND the visit_occurrence_concept_id both of those fields are null OR zero, the row should be removed. 

# + papermill={"duration": 2.338821, "end_time": "2021-02-02T22:30:41.501415", "exception": false, "start_time": "2021-02-02T22:30:39.162594", "status": "completed"} tags=[]
query = f'''

SELECT
SUM(CASE WHEN visit_source_concept_id = 0 AND visit_concept_id = 0 THEN 1 ELSE 0 END) AS n_visit_source_concept_id_both_0,
SUM(CASE WHEN visit_source_concept_id IS NULL AND visit_concept_id IS NULL THEN 1 ELSE 0 END) AS n_visit_source_concept_id_both_null,
SUM(CASE WHEN visit_source_concept_id = 0 AND visit_concept_id IS NULL THEN 1 ELSE 0 END) AS n_visit_source_concept_id_either_0,
SUM(CASE WHEN visit_source_concept_id IS NULL AND visit_concept_id =0 THEN 1 ELSE 0 END) AS n_visit_source_concept_id_either_null
FROM `{project_id}.{deid_clean}.visit_occurrence`
'''
df1=pd.read_gbq(query, dialect='standard')
if df1.loc[0].sum()==0:
 df = df.append({'query' : 'Query4 visit', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query4 visit', 'result' : ''},  
                ignore_index = True) 
df1.T


# + [markdown] papermill={"duration": 0.023649, "end_time": "2021-02-02T22:30:39.115495", "exception": false, "start_time": "2021-02-02T22:30:39.091846", "status": "completed"} tags=[]
# # 5 Verify that in drug_exposure table if drug_exposure_source_concept_id AND the drug_exposure_concept_id both of those fields are null OR zero, the row should be removed. 

# + papermill={"duration": 2.338821, "end_time": "2021-02-02T22:30:41.501415", "exception": false, "start_time": "2021-02-02T22:30:39.162594", "status": "completed"} tags=[]
query = f'''

SELECT 
SUM(CASE WHEN drug_source_concept_id = 0 AND drug_concept_id = 0 THEN 1 ELSE 0 END) AS n_drug_source_concept_id_both_0,
SUM(CASE WHEN drug_source_concept_id IS NULL AND drug_concept_id IS NULL THEN 1 ELSE 0 END) AS n_drug_source_concept_id_both_null,
SUM(CASE WHEN drug_source_concept_id = 0 AND drug_concept_id IS NULL THEN 1 ELSE 0 END) AS n_drug_source_concept_id_either_0,
SUM(CASE WHEN drug_source_concept_id IS NULL AND drug_concept_id =0 THEN 1 ELSE 0 END) AS n_drug_source_concept_id_either_null
FROM `{project_id}.{deid_clean}.drug_exposure`
'''
df1=pd.read_gbq(query, dialect='standard')
if df1.loc[0].sum()==0:
 df = df.append({'query' : 'Query5 drug_exposure', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query5 drug_exposure', 'result' : ''},  
                ignore_index = True) 
df1.T

# + [markdown] papermill={"duration": 0.023649, "end_time": "2021-02-02T22:30:39.115495", "exception": false, "start_time": "2021-02-02T22:30:39.091846", "status": "completed"} tags=[]
# # 6 Verify that in device_exposure table if device_exposure_source_concept_id AND the device_exposure_concept_id both of those fields are null OR zero, the row should be removed. 

# + papermill={"duration": 2.338821, "end_time": "2021-02-02T22:30:41.501415", "exception": false, "start_time": "2021-02-02T22:30:39.162594", "status": "completed"} tags=[]
query = f'''

SELECT 
SUM(CASE WHEN device_source_concept_id = 0 AND device_concept_id = 0 THEN 1 ELSE 0 END) AS n_device_source_concept_id_both_0,
SUM(CASE WHEN device_source_concept_id IS NULL AND device_concept_id IS NULL THEN 1 ELSE 0 END) AS n_device_source_concept_id_both_null,
SUM(CASE WHEN device_source_concept_id = 0 AND device_concept_id IS NULL THEN 1 ELSE 0 END) AS n_device_source_concept_id_either_0,
SUM(CASE WHEN device_source_concept_id IS NULL AND device_concept_id =0 THEN 1 ELSE 0 END) AS n_device_source_concept_id_either_null
FROM `{project_id}.{deid_clean}.device_exposure`
'''
df1=pd.read_gbq(query, dialect='standard')
if df1.loc[0].sum()==0:
 df = df.append({'query' : 'Query6 device', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query6 device', 'result' : ''},  
                ignore_index = True) 
df1.T

# + [markdown] papermill={"duration": 0.023649, "end_time": "2021-02-02T22:30:39.115495", "exception": false, "start_time": "2021-02-02T22:30:39.091846", "status": "completed"} tags=[]
# # 7 Verify that in measurement table if measurement_source_concept_id AND the measurement_concept_id both of those fields are null OR zero, the row should be removed. 

# + papermill={"duration": 2.338821, "end_time": "2021-02-02T22:30:41.501415", "exception": false, "start_time": "2021-02-02T22:30:39.162594", "status": "completed"} tags=[]
query = f'''

SELECT
SUM(CASE WHEN measurement_source_concept_id = 0 AND measurement_concept_id = 0 THEN 1 ELSE 0 END) AS n_measurement_source_concept_id_both_0,
SUM(CASE WHEN measurement_source_concept_id IS NULL AND measurement_concept_id IS NULL THEN 1 ELSE 0 END) AS n_measurement_source_concept_id_both_null,
SUM(CASE WHEN measurement_source_concept_id = 0 AND measurement_concept_id IS NULL THEN 1 ELSE 0 END) AS n_measurement_source_concept_id_either_0,
SUM(CASE WHEN measurement_source_concept_id IS NULL AND measurement_concept_id =0 THEN 1 ELSE 0 END) AS n_measurement_source_concept_id_either_null
FROM`{project_id}.{deid_clean}.measurement`
'''
df1=pd.read_gbq(query, dialect='standard')
if df1.loc[0].sum()==0:
 df = df.append({'query' : 'Query7 measurement', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query7 measurement', 'result' : ''},  
                ignore_index = True) 
df1.T
# -

# # 8 Verify that in death table if cause_source_concept_id AND the cause_concept_id both of those fields are null OR zero, the row should be removed. 
#
#

# +
# how to COUNT NaN
# in old cdr_clean , the value is NaN; in contrast, the value is empty in new cdr_clean
query = f'''

SELECT
SUM(CASE WHEN cause_source_concept_id != 0 AND cause_concept_id != 0 THEN 1 ELSE 0 END) AS n_cause_source_concept_id_both_not_0,
SUM(CASE WHEN cause_source_concept_id IS NOT NULL AND cause_concept_id IS NOT NULL THEN 1 ELSE 0 END) AS n_cause_source_concept_id_both_not_null,
SUM(CASE WHEN cause_source_concept_id != 0 AND cause_concept_id IS NOT NULL THEN 1 ELSE 0 END) AS n_cause_source_concept_id_either_0,
SUM(CASE WHEN cause_source_concept_id IS NOT NULL AND cause_concept_id !=0 THEN 1 ELSE 0 END) AS n_cause_source_concept_id_either_null
FROM `{project_id}.{deid_clean}.death`

'''
df1=pd.read_gbq(query, dialect='standard')
if df1.loc[0].sum()==0:
 df = df.append({'query' : 'Query8 cause_source_concept_id/cause_concept_id is null in dealth table', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query8 cause_source_concept_id/cause_concept_id is null in dealth table', 'result' : ''},  
                ignore_index = True) 
 
df1.T
# -

# # 9  check State_of_Residence fields in the person_ext table in deid_clean
#
# Generalization Rules for reference 
#
# DC-939: Validate that a new table Person_ext table in the deid_cdr_clean has the State_of_residence data!
# will not work in deid_cdr though. But this column will be in product person table. 
#
# DC-1011

# +
query=f''' 
SELECT COUNT (*) AS n_row_pass
FROM  `{project_id}.{deid_clean}.person_ext` d
JOIN `{project_id}.{deid_clean}.person` e
ON  d.person_id = e.person_id
WHERE state_of_residence_concept_id IS NOT NULL OR state_of_residence_source_value IS NOT NULL
'''
df1=pd.read_gbq(query, dialect='standard')  

if df1.loc[0].sum()==0:
 df = df.append({'query' : 'Query9 State_of_Residence in person_ext', 'result' : ' '},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query9 State_of_Residence in person_ext', 'result' : 'PASS'},  
                ignore_index = True) 
df1
# -

# # Summary_row_ICD_suppression

# if not pass, will be highlighted in red
df = df.mask(df.isin(['Null','']))
df.style.highlight_null(null_color='red').set_properties(**{'text-align': 'left'})
