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
# #  QA queries on new CDR_deid dateshift
#
# Quality checks performed on a new CDR dataset using QA queries

# + papermill={"duration": 0.709639, "end_time": "2021-02-02T22:30:32.661373", "exception": false, "start_time": "2021-02-02T22:30:31.951734", "status": "completed"} tags=[]
import urllib
import pandas as pd
pd.options.display.max_rows = 120

# + tags=["parameters"]
project_id = ""
com_cdr = ""
deid_cdr = ""
pipeline=""
# -

# df will have a summary in the end
df = pd.DataFrame(columns = ['query', 'result']) 

# + [markdown] papermill={"duration": 0.02327, "end_time": "2021-02-02T22:30:32.708257", "exception": false, "start_time": "2021-02-02T22:30:32.684987", "status": "completed"} tags=[]
# # 1 DS_1 Verify that the field identified to follow the date shift rule as de-identification action in OBSERVATION table have been randomly date shifted.

# + papermill={"duration": 4.105203, "end_time": "2021-02-02T22:30:36.813460", "exception": false, "start_time": "2021-02-02T22:30:32.708257", "status": "completed"} tags=[]
query = f'''

WITH df1 AS (
SELECT
DATE_DIFF(DATE(i.observation_date), DATE(d.observation_date),day)-m.shift as diff
FROM `{project_id}.{pipeline}.pid_rid_mapping` m
JOIN `{project_id}.{com_cdr}.observation` i
ON m.person_id = i.person_id
JOIN `{project_id}.{deid_cdr}.observation` d
ON d.observation_id = i.observation_id)

SELECT COUNT(*) AS n_row_not_pass FROM df1
WHERE diff !=0

'''
df1=pd.read_gbq(query, dialect='standard')
if df1.eq(0).any().any():
 df = df.append({'query' : 'Query1 OBSERVATION', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query1 OBSERVATION', 'result' : ''},  
                ignore_index = True) 
df1
# -

# # 3 DS_3 Verify that the field identified to follow the date shift rule as de-identification action in OBSERVATION_PERIOD table have been randomly date shifted.

# + papermill={"duration": 2.136748, "end_time": "2021-02-02T22:30:39.044867", "exception": false, "start_time": "2021-02-02T22:30:36.908119", "status": "completed"} tags=[]
query = f'''
WITH df1 AS (
SELECT
DATE_DIFF(DATE(i.observation_period_start_date), DATE(d.observation_period_start_date),day)-m.shift as diff
FROM `{project_id}.{pipeline}.pid_rid_mapping` m
JOIN `{project_id}.{com_cdr}.observation_period` i
ON m.person_id = i.person_id
JOIN `{project_id}.{deid_cdr}.observation_period` d
ON d.observation_period_id = i.observation_period_id)

SELECT COUNT(*) AS n_row_not_pass FROM df1
WHERE diff !=0

'''
df1=pd.read_gbq(query, dialect='standard')
if df1.eq(0).any().any():
 df = df.append({'query' : 'Query3 OBSERVATION_PERIOD', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query3 OBSERVATION_PERIOD', 'result' : ''},  
                ignore_index = True) 
df1


# + [markdown] papermill={"duration": 0.023649, "end_time": "2021-02-02T22:30:39.115495", "exception": false, "start_time": "2021-02-02T22:30:39.091846", "status": "completed"} tags=[]
# # 4 DS_4 Verify that the field identified to follow the date shift rule as de-identification action in PERSON table have been randomly date shifted.

# + papermill={"duration": 2.338821, "end_time": "2021-02-02T22:30:41.501415", "exception": false, "start_time": "2021-02-02T22:30:39.162594", "status": "completed"} tags=[]
query = f'''
WITH df1 AS (
SELECT
DATE_DIFF(DATE(i.birth_datetime), DATE(d.birth_datetime),day)-m.shift as diff
FROM `{project_id}.{pipeline}.pid_rid_mapping` m
JOIN `{project_id}.{com_cdr}.person` i
ON m.person_id = i.person_id
JOIN `{project_id}.{deid_cdr}.person` d
ON d.person_id = m.research_id
)
SELECT COUNT(*) AS n_row_not_pass FROM df1
WHERE diff !=0
'''
df1=pd.read_gbq(query, dialect='standard')
if df1.eq(0).any().any():
 df = df.append({'query' : 'Query4 Person table', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query4 Person table', 'result' : ''},  
                ignore_index = True) 
df1

# -

# # 5 DS_5 Verify that the field identified to follow the date shift rule as de-identification action in SPECIMEN table have been randomly date shifted.

# + papermill={"duration": 2.338821, "end_time": "2021-02-02T22:30:41.501415", "exception": false, "start_time": "2021-02-02T22:30:39.162594", "status": "completed"} tags=[]
query = f'''
WITH df1 AS (
SELECT
DATE_DIFF(DATE(i.specimen_date), DATE(d.specimen_date),day)-m.shift as diff
FROM `{project_id}.{pipeline}.pid_rid_mapping` m
JOIN `{project_id}.{com_cdr}.specimen` i
ON m.person_id = i.person_id
JOIN `{project_id}.{deid_cdr}.specimen` d
ON d.specimen_id = i.specimen_id
)
SELECT COUNT(*) AS n_row_not_pass FROM df1
WHERE diff !=0
  
'''
df1=pd.read_gbq(query, dialect='standard')
if df1.eq(0).any().any():
 df = df.append({'query' : 'Query5 SPECIMEN', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query5 SPECIMEN', 'result' : ''},  
                ignore_index = True) 
df1
# -

# # 6 DS_6 Verify that the field identified to follow the date shift rule as de-identification action in DEATH table have been randomly date shifted. 

query = f'''

WITH df1 AS (
SELECT 
DATE_DIFF(DATE(i.death_date), DATE(d.death_date),day)-m.shift as diff
FROM `{project_id}.{pipeline}.pid_rid_mapping` m
JOIN `{project_id}.{com_cdr}.death` i
ON m.person_id = i.person_id
JOIN `{project_id}.{deid_cdr}.death` d
ON m.research_id = d.person_id 
AND i.death_type_concept_id = d.death_type_concept_id
 )
SELECT COUNT (*) AS n_row_not_pass FROM df1
WHERE diff !=0
  
'''
df1=pd.read_gbq(query, dialect='standard')
if df1.eq(0).any().any():
 df = df.append({'query' : 'Query6 Death', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query6 Death', 'result' : ''},  
                ignore_index = True) 
df1

# + [markdown] papermill={"duration": 0.023411, "end_time": "2021-02-02T22:30:39.091846", "exception": false, "start_time": "2021-02-02T22:30:39.068435", "status": "completed"} tags=[]
# # 7 DS_7 Verify that the field identified to follow the date shift rule as de-identification action in VISIT OCCURENCE table have been randomly date shifted. 
# -

query = f'''
WITH df1 AS (
SELECT
DATE_DIFF(DATE(i.visit_start_date), DATE(d.visit_start_date),day)-m.shift as diff
FROM `{project_id}.{pipeline}.pid_rid_mapping` m
JOIN `{project_id}.{com_cdr}.visit_occurrence` i
ON m.person_id = i.person_id
JOIN `{project_id}.{deid_cdr}.visit_occurrence` d
ON d.visit_occurrence_id = i.visit_occurrence_id
)
SELECT COUNT (*) AS n_row_not_pass FROM df1
WHERE diff !=0
  
'''
df1=pd.read_gbq(query, dialect='standard')
if df1.eq(0).any().any():
 df = df.append({'query' : 'Query7 Visit', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query7 Visit', 'result' : ''},  
                ignore_index = True) 
df1

# # 8 DS_8 Verify that the field identified to follow the date shift rule as de-identification action in PROCEDURE OCCURENCE table have been randomly date shifted.
#

query = f'''
WITH df1 as (
SELECT
DATE_DIFF(DATE(i.procedure_date), DATE(d.procedure_date),day)-m.shift as diff
FROM `{project_id}.{pipeline}.pid_rid_mapping` m
JOIN `{project_id}.{com_cdr}.procedure_occurrence` i
ON m.person_id = i.person_id
JOIN `{project_id}.{deid_cdr}.procedure_occurrence` d
ON d.procedure_occurrence_id = i.procedure_occurrence_id
)
SELECT COUNT(*) AS n_row_not_pass FROM df1
WHERE diff !=0
  
'''
df1=pd.read_gbq(query, dialect='standard')
if df1.eq(0).any().any():
 df = df.append({'query' : 'Query8 PROCEDURE', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query8 PROCEDURE', 'result' : ''},  
                ignore_index = True) 
df1

# # 9 DS_9 Verify that the field identified to follow the date shift rule as de-identification action in DRUG EXPOSURE table have been randomly date shifted.

query = f'''
WITH df1 AS (
SELECT
DATE_DIFF(DATE(i.drug_exposure_start_date), DATE(d.drug_exposure_start_date),day)-m.shift as diff
FROM `{project_id}.{pipeline}.pid_rid_mapping` m
JOIN `{project_id}.{com_cdr}.drug_exposure` i
ON m.person_id = i.person_id
JOIN `{project_id}.{deid_cdr}.drug_exposure` d
ON i.drug_exposure_id = d.drug_exposure_id
)
SELECT COUNT(*) AS n_row_not_pass FROM df1
WHERE diff !=0
'''
df9=pd.read_gbq(query, dialect='standard')
if df1.eq(0).any().any():
 df = df.append({'query' : 'Query9 Drug table', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query9 Drug table', 'result' : ''},  
                ignore_index = True) 
df1


# # 10 DS_10 Verify that the field identified to follow the date shift rule as de-identification action in DEVICE EXPOSURE table have been randomly date shifted.

query = f'''
WITH df1 AS (
SELECT
 DATE_DIFF(DATE(i.device_exposure_start_date), DATE(d.device_exposure_start_date),day)-m.shift as diff
FROM `{project_id}.{pipeline}.pid_rid_mapping` m
JOIN `{project_id}.{com_cdr}.device_exposure` i
ON m.person_id = i.person_id
JOIN `{project_id}.{deid_cdr}.device_exposure` d
ON i.device_exposure_id = d.device_exposure_id
  )
SELECT COUNT(*) AS n_row_not_pass FROM df1
WHERE diff !=0
  '''
df1=pd.read_gbq(query, dialect='standard')
if df1.eq(0).any().any():
 df = df.append({'query' : 'Query10 Device', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query10 Device', 'result' : ''},  
                ignore_index = True) 
df1

# # 11 DS_11 Verify that the field identified to follow the date shift rule as de-identification action in CONDITION OCCURENCE table have been randomly date shifted.

query = f'''
WITH df1 AS (
SELECT
DATE_DIFF(DATE(i.condition_start_date), DATE(d.condition_start_date),day)-m.shift as diff
FROM `{project_id}.{pipeline}.pid_rid_mapping` m
JOIN `{project_id}.{com_cdr}.condition_occurrence` i
ON m.person_id = i.person_id
JOIN `{project_id}.{deid_cdr}.condition_occurrence` d
ON i.condition_occurrence_id = d.condition_occurrence_id
  )
SELECT COUNT(*) AS n_row_not_pass FROM df1
WHERE diff !=0
  
'''
df1=pd.read_gbq(query, dialect='standard')
if df1.eq(0).any().any():
 df = df.append({'query' : 'Query11 Condition table', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query11 Condition table', 'result' : ''},  
                ignore_index = True) 
df1

# # 12 DS_12 Verify that the field identified to follow the date shift rule as de-identification action in MEASUREMENT table have been randomly date shifted.

# +
query = f'''
WITH df1 AS (
SELECT
DATE_DIFF(DATE(i.measurement_date), DATE(d.measurement_date),day)-m.shift as diff
FROM `{project_id}.{pipeline}.pid_rid_mapping` m
JOIN `{project_id}.{com_cdr}.measurement` i
ON m.person_id = i.person_id
JOIN `{project_id}.{deid_cdr}.measurement` d
ON d.measurement_id = i.measurement_id
  )
SELECT COUNT(*) AS n_row_not_pass FROM df1
WHERE diff !=0
  
'''
df1=pd.read_gbq(query, dialect='standard')

if df1.eq(0).any().any():
 df = df.append({'query' : 'Query12 Measurement', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query12 Measurement', 'result' : ''},  
                ignore_index = True) 
df1
# -

# # 13 DS_13 Verify the date shift has been implemented following the date shift noted in the deid_map table in the non-deid dataset.

query = f'''
SELECT COUNT (*) AS n_row_not_pass
FROM  `{project_id}.{pipeline}.pid_rid_mapping`
WHERE shift <=0

'''
df1=pd.read_gbq(query, dialect='standard')
if df1.eq(0).any().any():
 df = df.append({'query' : 'Query13 date shifted in non_deid', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query13 date shifited in non_deid', 'result' : ''},  
                ignore_index = True) 
df1

# # 14 DS_14 Verify that  person_id has been replaced by research_id
#
#
# checked total 8 tables including  specimen etc tables in deid. However will be hard to check person or death tables without row_id.

query = f'''
WITH df1 AS (
SELECT COUNT (*) AS n_row_not_pass
FROM  `{project_id}.{com_cdr}.observation` non_deid
JOIN `{project_id}.{pipeline}.pid_rid_mapping` m
ON m.person_id=non_deid.person_id
JOIN `{project_id}.{deid_cdr}.observation` deid USING(observation_id)
WHERE deid.person_id !=m.research_id
),

df2 AS (
SELECT COUNT (*) AS n_row_not_pass
FROM  `{project_id}.{com_cdr}.measurement` non_deid
JOIN `{project_id}.{pipeline}.pid_rid_mapping` m
ON m.person_id=non_deid.person_id
JOIN `{project_id}.{deid_cdr}.measurement` deid USING(measurement_id)
WHERE deid.person_id !=m.research_id
),

df3 AS (
SELECT COUNT (*) AS n_row_not_pass
FROM  `{project_id}.{com_cdr}.condition_occurrence` non_deid
JOIN `{project_id}.{pipeline}.pid_rid_mapping` m
ON m.person_id=non_deid.person_id
JOIN `{project_id}.{deid_cdr}.condition_occurrence` deid USING(condition_occurrence_id)
WHERE deid.person_id !=m.research_id
),

df4 AS (
SELECT COUNT (*) AS n_row_not_pass
FROM  `{project_id}.{com_cdr}.drug_exposure` non_deid
JOIN `{project_id}.{pipeline}.pid_rid_mapping` m
ON m.person_id=non_deid.person_id
JOIN `{project_id}.{deid_cdr}.drug_exposure` deid USING(drug_exposure_id)
WHERE deid.person_id !=m.research_id
),

df5 AS (
SELECT COUNT (*) AS n_row_not_pass
FROM  `{project_id}.{com_cdr}.device_exposure` non_deid
JOIN `{project_id}.{pipeline}.pid_rid_mapping` m
ON m.person_id=non_deid.person_id
JOIN `{project_id}.{deid_cdr}.device_exposure` deid USING(device_exposure_id)
WHERE deid.person_id !=m.research_id
),

df6 AS (
SELECT COUNT (*) AS n_row_not_pass
FROM  `{project_id}.{com_cdr}.procedure_occurrence` non_deid
JOIN `{project_id}.{pipeline}.pid_rid_mapping` m
ON m.person_id=non_deid.person_id
JOIN `{project_id}.{deid_cdr}.procedure_occurrence` deid USING(procedure_occurrence_id)
WHERE deid.person_id !=m.research_id
),

df7 AS (
SELECT COUNT (*) AS n_row_not_pass
FROM  `{project_id}.{com_cdr}.visit_occurrence` non_deid
JOIN `{project_id}.{pipeline}.pid_rid_mapping` m
ON m.person_id=non_deid.person_id
JOIN `{project_id}.{deid_cdr}.visit_occurrence` deid USING(visit_occurrence_id)
WHERE deid.person_id !=m.research_id
),

df8 AS (
SELECT COUNT (*) AS n_row_not_pass
FROM  `{project_id}.{com_cdr}.specimen` non_deid
JOIN `{project_id}.{pipeline}.pid_rid_mapping` m
ON m.person_id=non_deid.person_id
JOIN `{project_id}.{deid_cdr}.specimen` deid USING(specimen_id)
WHERE deid.person_id !=m.research_id
)


SELECT * FROM df1
JOIN df2 USING(n_row_not_pass)
JOIN df3 USING(n_row_not_pass)
JOIN df4 USING(n_row_not_pass)
JOIN df5 USING(n_row_not_pass)
JOIN df6 USING(n_row_not_pass)
JOIN df7 USING(n_row_not_pass)
JOIN df8 USING(n_row_not_pass)



'''
df1=pd.read_gbq(query, dialect='standard')
if df1.eq(0).any().any():
 df = df.append({'query' : 'Query14.3 person_id replaed by research_id in other 8 tables', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query14.3 person_id replaed by research_id in other 8 tables', 'result' : ''},  
                ignore_index = True) 
df1

# # Summary_dateshift

# if not pass, will be highlighted in red
df = df.mask(df.isin(['Null','']))
df.style.highlight_null(null_color='red').set_properties(**{'text-align': 'left'})
