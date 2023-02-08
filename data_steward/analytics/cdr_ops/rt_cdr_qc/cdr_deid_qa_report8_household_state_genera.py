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

# # QA queries on new CDR_deid household AND state generalization

import urllib
import pandas as pd
from common import JINJA_ENV
from utils import auth
from gcloud.bq import BigQueryClient
from analytics.cdr_ops.notebook_utils import execute, IMPERSONATION_SCOPES
pd.options.display.max_rows = 120

# + tags=["parameters"]
project_id = ""
deid_cdr=""
combine = ""
reg_combine=''
pipeline = ""
deid_sand = ""
pid_threshold=""
run_as=""

# +
impersonation_creds = auth.get_impersonation_credentials(
    run_as, target_scopes=IMPERSONATION_SCOPES)

client = BigQueryClient(project_id, credentials=impersonation_creds)
# -

# df will have a summary in the end
df = pd.DataFrame(columns = ['query', 'result']) 

# # Query1 Verify that if the observation_source_concept_id  field in OBSERVATION table populates: 1585890, the value_as_concept_id field in de-id table should populate : 2000000012
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
query=JINJA_ENV.from_string("""

SELECT COUNT (*) AS n_row_not_pass
FROM `{{project_id}}.{{deid_cdr}}.observation`
WHERE
  observation_source_concept_id = 1585890
  AND value_as_concept_id NOT IN (2000000012,2000000010,903096)
""")
q = query.render(project_id=project_id,deid_cdr=deid_cdr)
df1=execute(client, q)

if df1.loc[0].sum()==0:
 df = df.append({'query' : 'Query1 observation_source_concept_id 1585890', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query1 observation_source_concept_id 1585890', 'result' : 'Failure'},  
                ignore_index = True) 
df1
# -

# # Query2 Verify that if the observation_source_concept_id  field in OBSERVATION table populates: 1333023 , the value_as_concept_id field in de-id table should populate : 2000000012
#
# expected results:
#
# Null is the value poplulated in the value_as_number fields
#
# AND 2000000012, 2000000010 AND 903096 are the values that are populated in value_as_concept_id field in the deid table.
#
# ## one row had error in new cdr

# +
query=JINJA_ENV.from_string("""

SELECT COUNT (*) AS n_row_not_pass
FROM `{{project_id}}.{{deid_cdr}}.observation`
WHERE
  observation_source_concept_id = 1333023
  AND value_as_concept_id NOT IN (2000000012,2000000010,903096)
""")
q = query.render(project_id=project_id,deid_cdr=deid_cdr)
df1=execute(client, q)

if df1.loc[0].sum()==0:
 df = df.append({'query' : 'Query2 observation_source_concept_id 1333023', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query2 observation_source_concept_id 1333023', 'result' : 'Failure'},  
                ignore_index = True) 
df1
# -

query=JINJA_ENV.from_string("""

SELECT *
FROM `{{project_id}}.{{deid_cdr}}.observation`
WHERE
  observation_source_concept_id = 1333023
  AND value_as_concept_id NOT IN (2000000012,2000000010,903096)
""")
q = query.render(project_id=project_id,deid_cdr=deid_cdr)
df1=execute(client, q)
df1

# # Query3 Verify that if the observation_source_concept_id  field in OBSERVATION table populates: 1585889,  the value_as_concept_id field in de-id table should populate : 2000000013
#
# DC-1059
#
# expected results:
#
# Null is the value poplulated in the value_as_number fields 
#
# AND 2000000013, 2000000010 AND 903096 are the values that are populated in value_as_concept_id field in the deid table.

# +
query=JINJA_ENV.from_string("""
SELECT COUNT (*) AS n_row_not_pass
FROM  `{{project_id}}.{{deid_cdr}}.observation`
WHERE
  observation_source_concept_id = 1585889
  AND value_as_concept_id NOT IN (2000000013,2000000010,903096)
""")
q = query.render(project_id=project_id,deid_cdr=deid_cdr)
df1=execute(client, q)

if df1.loc[0].sum()==0:
 df = df.append({'query' : 'Query3 observation_source_concept_id 1585889', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query3 observation_source_concept_id 1585889', 'result' : 'Failure'},  
                ignore_index = True) 
df1
# -

# # Query4 Verify that if the observation_source_concept_id  field in OBSERVATION table populates: 1333015,  the value_as_concept_id field in de-id table should populate : 2000000013
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
query=JINJA_ENV.from_string("""

SELECT COUNT (*) AS n_row_not_pass
FROM  `{{project_id}}.{{deid_cdr}}.observation`
WHERE
  observation_source_concept_id = 1333015
  AND value_as_concept_id NOT IN (2000000013,2000000010,903096)
""")
q = query.render(project_id=project_id,deid_cdr=deid_cdr)
df1=execute(client, q)

if df1.loc[0].sum()==0:
 df = df.append({'query' : 'Query4 observation_source_concept_id 1333015', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query4 observation_source_concept_id 1333015', 'result' : 'Failure'},  
                ignore_index = True) 
df1
# -

# # Query5 update to verifie that value_as_concept_id and value_source_concept_id are set to 2000000011 for states with less than 200 participants.
#
# Set the value_source_concept_id = 2000000011 and value_as_concept_id =2000000011 
#
# DC-2377 and DC-1614, DC-2782, DC-2785

# ## Query5.1 Generalize state info (2000000011) for participants who have EHR data from states other than the state they are currently living in.

# +
query = JINJA_ENV.from_string("""

with df_ehr_site as (
SELECT distinct com.person_id, research_id deid_pid, map_state.State EHR_site_state,'observation' table
FROM `{{project_id}}.{{reg_combine}}.observation` com
join `{{project_id}}.{{reg_combine}}._mapping_observation` map on map.observation_id=com.observation_id
JOIN `{{project_id}}.{{deid_sand}}._deid_map` deid_map on deid_map.person_id=com.person_id
JOIN `{{project_id}}.{{reg_combine}}._mapping_src_hpos_to_allowed_states`  map_state ON map_state.src_hpo_id=map.src_hpo_id
JOIN `{{project_id}}.{{pipeline}}.site_maskings` mask ON hpo_id=map_state.src_hpo_id

union distinct 

SELECT distinct com.person_id, research_id deid_pid, map_state.State EHR_site_state,'condition' table
FROM `{{project_id}}.{{reg_combine}}.condition_occurrence` com
join `{{project_id}}.{{reg_combine}}._mapping_condition_occurrence` map on map.condition_occurrence_id=com.condition_occurrence_id
JOIN `{{project_id}}.{{deid_sand}}._deid_map` deid_map on deid_map.person_id=com.person_id
JOIN `{{project_id}}.{{reg_combine}}._mapping_src_hpos_to_allowed_states`  map_state ON map_state.src_hpo_id=map.src_hpo_id
JOIN `{{project_id}}.{{pipeline}}.site_maskings` mask ON hpo_id=map_state.src_hpo_id

union distinct 

SELECT distinct com.person_id, research_id deid_pid, map_state.State EHR_site_state,'measurement' table
FROM `{{project_id}}.{{reg_combine}}.measurement` com
join `{{project_id}}.{{reg_combine}}._mapping_measurement` map on map.measurement_id=com.measurement_id
JOIN `{{project_id}}.{{deid_sand}}._deid_map` deid_map on deid_map.person_id=com.person_id
JOIN `{{project_id}}.{{reg_combine}}._mapping_src_hpos_to_allowed_states`  map_state ON map_state.src_hpo_id=map.src_hpo_id
JOIN `{{project_id}}.{{pipeline}}.site_maskings` mask ON hpo_id=map_state.src_hpo_id

union distinct 

SELECT distinct com.person_id, research_id deid_pid, map_state.State EHR_site_state,'device_exposure' table
FROM `{{project_id}}.{{reg_combine}}.device_exposure` com
join `{{project_id}}.{{reg_combine}}._mapping_device_exposure` map on map.device_exposure_id=com.device_exposure_id
JOIN `{{project_id}}.{{deid_sand}}._deid_map` deid_map on deid_map.person_id=com.person_id
JOIN `{{project_id}}.{{reg_combine}}._mapping_src_hpos_to_allowed_states`  map_state ON map_state.src_hpo_id=map.src_hpo_id
JOIN `{{project_id}}.{{pipeline}}.site_maskings` mask ON hpo_id=map_state.src_hpo_id

union distinct 

SELECT distinct com.person_id, research_id deid_pid, map_state.State EHR_site_state,'drug_exposure' table
FROM `{{project_id}}.{{reg_combine}}.drug_exposure` com
join `{{project_id}}.{{reg_combine}}._mapping_drug_exposure` map on map.drug_exposure_id=com.drug_exposure_id
JOIN `{{project_id}}.{{deid_sand}}._deid_map` deid_map on deid_map.person_id=com.person_id
JOIN `{{project_id}}.{{reg_combine}}._mapping_src_hpos_to_allowed_states`  map_state ON map_state.src_hpo_id=map.src_hpo_id
JOIN `{{project_id}}.{{pipeline}}.site_maskings` mask ON hpo_id=map_state.src_hpo_id

union distinct 

SELECT distinct com.person_id, research_id deid_pid, map_state.State EHR_site_state,'procedure' table
FROM `{{project_id}}.{{reg_combine}}.procedure_occurrence` com
join `{{project_id}}.{{reg_combine}}._mapping_procedure_occurrence` map on map.procedure_occurrence_id=com.procedure_occurrence_id
JOIN `{{project_id}}.{{deid_sand}}._deid_map` deid_map on deid_map.person_id=com.person_id
JOIN `{{project_id}}.{{reg_combine}}._mapping_src_hpos_to_allowed_states`  map_state ON map_state.src_hpo_id=map.src_hpo_id
JOIN `{{project_id}}.{{pipeline}}.site_maskings` mask ON hpo_id=map_state.src_hpo_id

union distinct 

SELECT distinct com.person_id, research_id deid_pid, map_state.State EHR_site_state,'visit' table
FROM `{{project_id}}.{{reg_combine}}.visit_occurrence` com
join `{{project_id}}.{{reg_combine}}._mapping_visit_occurrence` map on map.visit_occurrence_id=com.visit_occurrence_id
JOIN `{{project_id}}.{{deid_sand}}._deid_map` deid_map on deid_map.person_id=com.person_id
JOIN `{{project_id}}.{{reg_combine}}._mapping_src_hpos_to_allowed_states`  map_state ON map_state.src_hpo_id=map.src_hpo_id
JOIN `{{project_id}}.{{pipeline}}.site_maskings` mask ON hpo_id=map_state.src_hpo_id
),

df2 as (

SELECT distinct deid.person_id,
com.value_source_concept_id, 
com.value_source_value com_residency_state, EHR_site_state
FROM `{{project_id}}.{{deid_cdr}}.observation` deid
join `{{project_id}}.{{reg_combine}}.observation` com on com.observation_id=deid.observation_id
join df_ehr_site on deid.person_id=df_ehr_site.deid_pid
where deid.observation_source_concept_id = 1585249
and deid.value_source_concept_id !=2000000011
and com.value_source_value !=EHR_site_state 
)
   
select count (*) AS row_counts_failure_state_generalization from df2

""")

q = query.render(project_id=project_id,deid_cdr=deid_cdr,reg_combine=reg_combine,deid_sand=deid_sand,pipeline=pipeline)

df1=execute(client, q)

if df1.loc[0].sum()==0:
 df = df.append({'query' : 'Query5.1 state generalization to 2000000011', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query5.1 state generalization to 2000000011', 'result' : 'Failure'},  
                ignore_index = True) 
df1

# -

# ## Query5.2 Generalize state info for participants where the identified number of participants living in the state without EHR records from a different state < 200 (the generalization threshold)”

# +
query=JINJA_ENV.from_string(""" 
with df_state_pid_200 as (
select value_source_concept_id,
count(distinct person_id) as participant_count
FROM `{{project_id}}.{{reg_combine}}.observation` 
where observation_source_concept_id = 1585249
group by 1
having participant_count < 200
)

SELECT COUNT (*) AS n_row_not_pass_state_pid_200
FROM `{{project_id}}.{{deid_cdr}}.observation` 
WHERE observation_source_concept_id = 1585249
and (value_source_concept_id !=2000000011 or value_as_concept_id !=2000000011) 
and value_source_concept_id in (select value_source_concept_id from df_state_pid_200)
""")
q = query.render(project_id=project_id,deid_cdr=deid_cdr,pid_threshold=pid_threshold,reg_combine=reg_combine)
df1=execute(client, q)

if df1.loc[0].sum()==0:
 df = df.append({'query' : 'Query5.2 state generalization if counts <200', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query5.2 state generalization if counts <200', 'result' : 'Failure'},  
                ignore_index = True) 
df1


# -

# # Summary_deid_household AND state generalization

# +
def highlight_cells(val):
    color = 'red' if 'Failure' in val else 'white'
    return f'background-color: {color}' 

df.style.applymap(highlight_cells).set_properties(**{'text-align': 'left'})
