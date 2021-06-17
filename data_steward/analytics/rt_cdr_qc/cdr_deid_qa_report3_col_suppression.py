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

# # QA queries on new CDR column suppression
# Verify all columns identified for suppression in the deid dataset have been set to null.
#
# (Query results: This query returned no results.)

import urllib
import pandas as pd
pd.options.display.max_rows = 120

# + tags=["parameters"]
project_id = ""
deid_cdr=""
# -

# df will have a summary in the end
df = pd.DataFrame(columns = ['query', 'result']) 

# # 1 Verify the following columns in the OBSERVATION table have been set to null:
# value_as_string,
# provider_id,
# value_source_value,
# qualifier_source_value,
# observation_source_value,
# unit_source_value

# +
query=f''' 
SELECT

SUM(CASE WHEN value_as_string IS NOT NULL THEN 1 ELSE 0 END) AS n_value_as_string_not_null,
SUM(CASE WHEN provider_id IS NOT NULL THEN 1 ELSE 0 END) AS n_provider_id_not_null,
SUM(CASE WHEN value_source_value IS NOT NULL THEN 1 ELSE 0 END) AS n_value_source_value_not_null,
SUM(CASE WHEN qualifier_source_value IS NOT NULL THEN 1 ELSE 0 END) AS n_qualifier_source_value_not_null,
SUM(CASE WHEN observation_source_value IS NOT NULL THEN 1 ELSE 0 END) AS n_observation_source_value_not_null,
SUM(CASE WHEN unit_source_value IS NOT NULL THEN 1 ELSE 0 END) AS n_unit_source_value_not_null

FROM `{project_id}.{deid_cdr}.observation` 
 '''
df1=pd.read_gbq(query, dialect='standard')  

if df1.loc[0].sum()==0:
 df = df.append({'query' : 'Query1 observation', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query1 observation', 'result' : ''},  
                ignore_index = True) 
df1.T
# -

# # 2 Verify the following columns in the PERSON table have been set to null:

# ## false errors in new cdr if checking null, due to  year_of_birth, race_concept_id,ethnicity_concept_id , these are set to 0, why not null?
#
# I changed to detect 0 for now, but in the future, better set them to null, instead of 0.

query=f''' 
SELECT
  SUM(CASE WHEN year_of_birth !=0 THEN 1 ELSE 0 END) AS n_year_of_birth_not_null,
SUM(CASE WHEN month_of_birth IS NOT NULL THEN 1 ELSE 0 END) AS n_month_of_birth_not_null,
SUM(CASE WHEN day_of_birth IS NOT NULL THEN 1 ELSE 0 END) AS n_day_of_birth_not_null,
SUM(CASE WHEN race_concept_id !=0  THEN 1 ELSE 0 END) AS n_race_concept_id_not_null,
SUM(CASE WHEN ethnicity_concept_id !=0 THEN 1 ELSE 0 END) AS n_ethnicity_concept_id_not_null,
SUM(CASE WHEN location_id IS NOT NULL THEN 1 ELSE 0 END) AS n_location_id_not_null,
SUM(CASE WHEN provider_id IS NOT NULL THEN 1 ELSE 0 END) AS n_provider_id_not_null,
SUM(CASE WHEN care_site_id IS NOT NULL THEN 1 ELSE 0 END) AS n_care_site_id_not_null,
SUM(CASE WHEN person_source_value IS NOT NULL THEN 1 ELSE 0 END) AS n_person_source_value_not_null,
SUM(CASE WHEN gender_source_value IS NOT NULL THEN 1 ELSE 0 END) AS n_gender_source_value_not_null,
SUM(CASE WHEN gender_source_concept_id IS NOT NULL THEN 1 ELSE 0 END) AS n_gender_source_concept_id_not_null,
SUM(CASE WHEN race_source_value IS NOT NULL THEN 1 ELSE 0 END) AS n_race_source_value_not_null,
SUM(CASE WHEN ethnicity_source_value IS NOT NULL THEN 1 ELSE 0 END) AS n_ethnicity_source_value_not_null,
SUM(CASE WHEN ethnicity_source_concept_id IS NOT NULL THEN 1 ELSE 0 END) AS n_ethnicity_source_concept_id_not_null
FROM `{project_id}.{deid_cdr}.person` 
    '''
df1=pd.read_gbq(query, dialect='standard') 
if df1.loc[0].sum()==0:
 df = df.append({'query' : 'Query2 Person', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query2 Person', 'result' : ''},  
                ignore_index = True) 
df1.T              

# # 3 Verify the following columns in the MEASUREMENT table have been set to null:

query=f''' 
SELECT

SUM(CASE WHEN provider_id IS NOT NULL THEN 1 ELSE 0 END) AS n_provider_id_not_null,
SUM(CASE WHEN measurement_source_value IS NOT NULL THEN 1 ELSE 0 END) AS n_measurement_source_value_not_null,
SUM(CASE WHEN value_source_value IS NOT NULL THEN 1 ELSE 0 END) AS n_value_source_value_not_null

FROM `{project_id}.{deid_cdr}.measurement`
'''
df1=pd.read_gbq(query, dialect='standard')  
if df1.loc[0].sum()==0:
 df = df.append({'query' : 'Query3 Measurement', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query3 Measurement', 'result' : ''},  
                ignore_index = True) 
df1.T  

# # 4 Verify the following columns in the DEATH table have been set to null:

query=f''' 
SELECT
SUM(CASE WHEN cause_concept_id IS NOT NULL THEN 1 ELSE 0 END) AS n_cause_concept_id_not_null,
SUM(CASE WHEN cause_source_value IS NOT NULL THEN 1 ELSE 0 END) AS n_cause_source_value_not_null,
SUM(CASE WHEN cause_source_concept_id IS NOT NULL THEN 1 ELSE 0 END) AS n_cause_source_concept_id_not_null

FROM `{project_id}.{deid_cdr}.death`
'''
df1=pd.read_gbq(query, dialect='standard')  
if df1.loc[0].sum()==0:
 df = df.append({'query' : 'Query4 Death', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query4 Death', 'result' : ''},  
                ignore_index = True) 
df1        

# # 5 Verify the following columnsin the CONDITION_OCCURRENCE table have been set to null:

query=f''' 
SELECT
SUM(CASE WHEN provider_id IS NOT NULL THEN 1 ELSE 0 END) AS n_provider_id_not_null
FROM `{project_id}.{deid_cdr}.condition_occurrence`
'''
df1=pd.read_gbq(query, dialect='standard')  
if df1.loc[0].sum()==0:
 df = df.append({'query' : 'Query5 Condition provider_id', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query5 Condition provider_id', 'result' : ''},  
                ignore_index = True) 
df1        

# # 6 Verify the following columns in the DEVICE_EXPOSURE table have been set to null:
#
# <font color='red'> Uncomment below when visit_detail_id is available

# +
# query6 has error
# no this column called visit_detail_id?
    
query=f''' 
SELECT
SUM(CASE WHEN provider_id IS NOT NULL THEN 1 ELSE 0 END) AS n_provider_id_not_null
-- SUM(CASE WHEN visit_detail_id IS NOT NULL THEN 1 ELSE 0 END) AS n_visit_detail_id_not_null

FROM `{project_id}.{deid_cdr}.device_exposure`
'''
df1=pd.read_gbq(query, dialect='standard')  
if df1.loc[0].sum()==0:
 df = df.append({'query' : 'Query6 provider_id in Device', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query6 provider_id in Device', 'result' : ''},  
                ignore_index = True) 
df1        
# -

# # 7 Verify the following columns in the DRUG_EXPOSURE table have been set to null:
# provider_id
#
# <font color='red'> no visit_detail_id ?
#     
#  Uncomment below when visit_detail_id is available

query=f''' 
SELECT
SUM(CASE WHEN provider_id IS NOT NULL THEN 1 ELSE 0 END) AS n_provider_id_not_null
--SUM(CASE WHEN visit_detail_id IS NOT NULL THEN 1 ELSE 0 END) AS n_visit_detail_id_not_null
FROM `{project_id}.{deid_cdr}.drug_exposure`
'''
df1=pd.read_gbq(query, dialect='standard')  
if df1.loc[0].sum()==0:
 df = df.append({'query' : 'Query7 provider_id in Drug', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query7 provider_id in Drug', 'result' : ''},  
                ignore_index = True) 
df1        

# # 8 Verify the following columns in the VISIT_OCCURRENCE table have been set to null:

query=f''' 
SELECT
SUM(CASE WHEN provider_id IS NOT NULL THEN 1 ELSE 0 END) AS n_provider_id_not_null,
SUM(CASE WHEN care_site_id IS NOT NULL THEN 1 ELSE 0 END) AS n_care_site_id_not_null
FROM `{project_id}.{deid_cdr}.visit_occurrence`
 '''
df1=pd.read_gbq(query, dialect='standard')  
if df1.loc[0].sum()==0:
 df = df.append({'query' : 'Query8 Visit', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query8 Visit', 'result' : ''},  
                ignore_index = True) 
df1        

# # 9 Verify the following columns in the PROCEDURE_OCCURRENCE table have been set to null:

query=f''' 
SELECT
SUM(CASE WHEN provider_id IS NOT NULL THEN 1 ELSE 0 END) AS n_provider_id_not_null
FROM `{project_id}.{deid_cdr}.procedure_occurrence`
'''
df1=pd.read_gbq(query, dialect='standard')  
if df1.loc[0].sum()==0:
 df = df.append({'query' : 'Query9 Procedure provider_id', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query9 Procedure provider_id', 'result' : ''},  
                ignore_index = True) 
df1        

# # 10 Verify the following columns in the SPECIMEN  table have been set to null:

query=f''' 
SELECT
SUM(CASE WHEN specimen_source_value IS NOT NULL THEN 1 ELSE 0 END) AS n_specimen_source_value_not_null,
SUM(CASE WHEN unit_source_value IS NOT NULL THEN 1 ELSE 0 END) AS n_unit_source_value_not_null,
SUM(CASE WHEN specimen_source_id IS NOT NULL THEN 1 ELSE 0 END) AS n_specimen_source_id_not_null,
SUM(CASE WHEN disease_status_source_value IS NOT NULL THEN 1 ELSE 0 END) AS n_disease_status_source_value_not_null,
SUM(CASE WHEN anatomic_site_source_value IS NOT NULL THEN 1 ELSE 0 END) AS n_anatomic_site_source_value_not_null

FROM `{project_id}.{deid_cdr}.specimen`
'''
df1=pd.read_gbq(query, dialect='standard')  
if df1.loc[0].sum()==0:
 df = df.append({'query' : 'Query10 Specimen', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query10 Specimen', 'result' : ''},  
                ignore_index = True) 
df1.T        

# # 11 Verify the following columns in the CARE_SITE  table have been set to null

# ## in new cdr, this table is empty?

query=f''' 
SELECT
SUM(CASE WHEN care_site_name IS NOT NULL THEN 1 ELSE 0 END) AS n_care_site_name_not_null,
SUM(CASE WHEN care_site_source_value IS NOT NULL THEN 1 ELSE 0 END) AS n_care_site_source_value_not_null,
SUM(CASE WHEN place_of_service_source_value IS NOT NULL THEN 1 ELSE 0 END) AS n_place_of_service_source_value_not_null

FROM `{project_id}.{deid_cdr}.care_site`
'''
df1=pd.read_gbq(query, dialect='standard')  
if df1.loc[0].sum()==0:
 df = df.append({'query' : 'Query11 Care_site', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query11 Care_site', 'result' : ''},  
                ignore_index = True) 
df1      

# # 12 Verify the NOTE table is suppressed

query=f''' 
SELECT COUNT (*) AS n_row_not_pass
FROM `{project_id}.{deid_cdr}.note`
'''
df1=pd.read_gbq(query, dialect='standard')  
if df1.loc[0].sum()==0:
 df = df.append({'query' : 'Query12 Note table', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query12 Note table', 'result' : ''},  
                ignore_index = True) 
df1     

# # 13 Verify the NOTE_NLP table  is suppressed
#

query=f''' 
SELECT COUNT (*) AS n_row_not_pass
FROM `{project_id}.{deid_cdr}.__TABLES_SUMMARY__`
WHERE table_id='note_nlp'
'''
df1=pd.read_gbq(query, dialect='standard') 
if df1.loc[0].sum()==0:
 df = df.append({'query' : 'Query13 Note_NLP table', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query13 Note_NLP table', 'result' : ''},  
                ignore_index = True) 
df1     

# # Summary_column_suppression

# if not pass, will be highlighted in red
df = df.mask(df.isin(['Null','']))
df.style.highlight_null(null_color='red').set_properties(**{'text-align': 'left'})
