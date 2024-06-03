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
from common import JINJA_ENV
from utils import auth
from gcloud.bq import BigQueryClient
from analytics.cdr_ops.notebook_utils import execute, IMPERSONATION_SCOPES
pd.options.display.max_rows = 120

# + papermill={"duration": 0.023643, "end_time": "2021-02-02T22:30:31.880820", "exception": false, "start_time": "2021-02-02T22:30:31.857177", "status": "completed"} tags=["parameters"]
project_id = ""
deid_clean_cdr = ""
run_as = ""

# +
impersonation_creds = auth.get_impersonation_credentials(
    run_as, target_scopes=IMPERSONATION_SCOPES)

client = BigQueryClient(project_id, credentials=impersonation_creds)
# -

# summary will have a summary in the end
summary = pd.DataFrame(columns=['query', 'result'])

# ## QA queries on new CDR_deid_clean drop rows with 0 OR null

# + [markdown] papermill={"duration": 0.02327, "end_time": "2021-02-02T22:30:32.708257", "exception": false, "start_time": "2021-02-02T22:30:32.684987", "status": "completed"} tags=[]
# # 1 Verify that in observation table if observation_source_concept_id AND the observation_concept_id, both of those fields are null OR zero, the row should be removed.

# + papermill={"duration": 4.105203, "end_time": "2021-02-02T22:30:36.813460", "exception": false, "start_time": "2021-02-02T22:30:32.708257", "status": "completed"} tags=[]
query = JINJA_ENV.from_string("""

SELECT 
SUM(CASE WHEN observation_source_concept_id = 0 AND observation_concept_id = 0 THEN 1 ELSE 0 END) AS n_observation_source_concept_id_both_0,
SUM(CASE WHEN observation_source_concept_id IS NULL AND observation_concept_id IS NULL THEN 1 ELSE 0 END) AS n_observation_source_concept_id_both_null,
SUM(CASE WHEN observation_source_concept_id = 0 AND observation_concept_id IS NULL THEN 1 ELSE 0 END) AS n_observation_source_concept_id_either_0,
SUM(CASE WHEN observation_source_concept_id IS NULL AND observation_concept_id=0 THEN 1 ELSE 0 END) AS n_observation_source_concept_id_either_null

FROM `{{project_id}}.{{deid_clean_cdr}}.observation`
""")
q = query.render(project_id=project_id, deid_clean_cdr=deid_clean_cdr)
result = execute(client, q)
if result.loc[0].sum() == 0:
    summary = summary.append({
        'query': 'Query1 observation',
        'result': 'PASS'
    },
                   ignore_index=True)
else:
    summary = summary.append({
        'query': 'Query1 observation',
        'result': 'Failure'
    },
                   ignore_index=True)
result.T

# + [markdown] papermill={"duration": 0.023633, "end_time": "2021-02-02T22:30:36.860798", "exception": false, "start_time": "2021-02-02T22:30:36.837165", "status": "completed"} tags=[]
# # 2   Verify that in condition_occurrence if condition_occurrence_source_concept_id AND the condition_occurrence_concept_id both of those fields are null OR zero, the row should be removed.
# -

query = JINJA_ENV.from_string("""

SELECT  
SUM(CASE WHEN condition_source_concept_id = 0 AND condition_concept_id = 0 THEN 1 ELSE 0 END) AS n_condition_source_concept_id_both_0,
SUM(CASE WHEN condition_source_concept_id IS NULL AND condition_concept_id IS NULL THEN 1 ELSE 0 END) AS n_condition_source_concept_id_both_null,
SUM(CASE WHEN condition_source_concept_id = 0 AND condition_concept_id IS NULL THEN 1 ELSE 0 END) AS n_condition_source_concept_id_either_0,
SUM(CASE WHEN condition_source_concept_id IS NULL AND condition_concept_id=0 THEN 1 ELSE 0 END) AS n_condition_source_concept_id_either_null


FROM `{{project_id}}.{{deid_clean_cdr}}.condition_occurrence`
WHERE (condition_source_concept_id = 0 AND  condition_concept_id=0)
OR ( condition_source_concept_id IS NULL AND condition_concept_id IS NULL)
""")
q = query.render(project_id=project_id, deid_clean_cdr=deid_clean_cdr)
result = execute(client, q)
if result.loc[0].sum() == 0:
    summary = summary.append({
        'query': 'Query2 condition',
        'result': 'PASS'
    },
                   ignore_index=True)
else:
    summary = summary.append({
        'query': 'Query2 condition',
        'result': 'Failure'
    },
                   ignore_index=True)
result.T

# # 3  Verify that in procedure_occurrence table if procedure_occurrence_source_concept_id AND the procedure_occurrence_concept_id both of those fields are null OR zero, the row should be removed.

query = JINJA_ENV.from_string("""

SELECT 
SUM(CASE WHEN procedure_source_concept_id = 0 AND procedure_concept_id = 0 THEN 1 ELSE 0 END) AS n_procedure_source_concept_id_both_0,
SUM(CASE WHEN procedure_source_concept_id IS NULL AND procedure_concept_id IS NULL THEN 1 ELSE 0 END) AS n_procedure_source_concept_id_both_null,
SUM(CASE WHEN procedure_source_concept_id = 0 AND procedure_concept_id IS NULL THEN 1 ELSE 0 END) AS n_procedure_source_concept_id_either_0,
SUM(CASE WHEN procedure_source_concept_id IS NULL AND procedure_concept_id=0 THEN 1 ELSE 0 END) AS n_procedure_source_concept_id_either_null

FROM `{{project_id}}.{{deid_clean_cdr}}.procedure_occurrence`
""")
q = query.render(project_id=project_id, deid_clean_cdr=deid_clean_cdr)
result = execute(client, q)
if result.loc[0].sum() == 0:
    summary = summary.append({
        'query': 'Query3 procedure',
        'result': 'PASS'
    },
                   ignore_index=True)
else:
    summary = summary.append({
        'query': 'Query3 procedure',
        'result': 'Failure'
    },
                   ignore_index=True)
result.T

# + [markdown] papermill={"duration": 0.023649, "end_time": "2021-02-02T22:30:39.115495", "exception": false, "start_time": "2021-02-02T22:30:39.091846", "status": "completed"} tags=[]
# # 4 Verify that in visit_occurrence table if visit_occurrence_source_concept_id AND the visit_occurrence_concept_id both of those fields are null OR zero, the row should be removed.

# + papermill={"duration": 2.338821, "end_time": "2021-02-02T22:30:41.501415", "exception": false, "start_time": "2021-02-02T22:30:39.162594", "status": "completed"} tags=[]
query = JINJA_ENV.from_string("""

SELECT
SUM(CASE WHEN visit_source_concept_id = 0 AND visit_concept_id = 0 THEN 1 ELSE 0 END) AS n_visit_source_concept_id_both_0,
SUM(CASE WHEN visit_source_concept_id IS NULL AND visit_concept_id IS NULL THEN 1 ELSE 0 END) AS n_visit_source_concept_id_both_null,
SUM(CASE WHEN visit_source_concept_id = 0 AND visit_concept_id IS NULL THEN 1 ELSE 0 END) AS n_visit_source_concept_id_either_0,
SUM(CASE WHEN visit_source_concept_id IS NULL AND visit_concept_id =0 THEN 1 ELSE 0 END) AS n_visit_source_concept_id_either_null
FROM `{{project_id}}.{{deid_clean_cdr}}.visit_occurrence`
""")
q = query.render(project_id=project_id, deid_clean_cdr=deid_clean_cdr)
result = execute(client, q)
if result.loc[0].sum() == 0:
    summary = summary.append({
        'query': 'Query4 visit',
        'result': 'PASS'
    },
                   ignore_index=True)
else:
    summary = summary.append({
        'query': 'Query4 visit',
        'result': 'Failure'
    },
                   ignore_index=True)
result.T


# + [markdown] papermill={"duration": 0.023649, "end_time": "2021-02-02T22:30:39.115495", "exception": false, "start_time": "2021-02-02T22:30:39.091846", "status": "completed"} tags=[]
# # 5 Verify that in drug_exposure table if drug_exposure_source_concept_id AND the drug_exposure_concept_id both of those fields are null OR zero, the row should be removed.

# + papermill={"duration": 2.338821, "end_time": "2021-02-02T22:30:41.501415", "exception": false, "start_time": "2021-02-02T22:30:39.162594", "status": "completed"} tags=[]
query = JINJA_ENV.from_string("""

SELECT 
SUM(CASE WHEN drug_source_concept_id = 0 AND drug_concept_id = 0 THEN 1 ELSE 0 END) AS n_drug_source_concept_id_both_0,
SUM(CASE WHEN drug_source_concept_id IS NULL AND drug_concept_id IS NULL THEN 1 ELSE 0 END) AS n_drug_source_concept_id_both_null,
SUM(CASE WHEN drug_source_concept_id = 0 AND drug_concept_id IS NULL THEN 1 ELSE 0 END) AS n_drug_source_concept_id_either_0,
SUM(CASE WHEN drug_source_concept_id IS NULL AND drug_concept_id =0 THEN 1 ELSE 0 END) AS n_drug_source_concept_id_either_null
FROM `{{project_id}}.{{deid_clean_cdr}}.drug_exposure`
""")
q = query.render(project_id=project_id, deid_clean_cdr=deid_clean_cdr)
result = execute(client, q)
if result.loc[0].sum() == 0:
    summary = summary.append({
        'query': 'Query5 drug_exposure',
        'result': 'PASS'
    },
                   ignore_index=True)
else:
    summary = summary.append({
        'query': 'Query5 drug_exposure',
        'result': 'Failure'
    },
                   ignore_index=True)
result.T

# + [markdown] papermill={"duration": 0.023649, "end_time": "2021-02-02T22:30:39.115495", "exception": false, "start_time": "2021-02-02T22:30:39.091846", "status": "completed"} tags=[]
# # 6 Verify that in device_exposure table if device_exposure_source_concept_id AND the device_exposure_concept_id both of those fields are null OR zero, the row should be removed.

# + papermill={"duration": 2.338821, "end_time": "2021-02-02T22:30:41.501415", "exception": false, "start_time": "2021-02-02T22:30:39.162594", "status": "completed"} tags=[]
query = JINJA_ENV.from_string("""

SELECT 
SUM(CASE WHEN device_source_concept_id = 0 AND device_concept_id = 0 THEN 1 ELSE 0 END) AS n_device_source_concept_id_both_0,
SUM(CASE WHEN device_source_concept_id IS NULL AND device_concept_id IS NULL THEN 1 ELSE 0 END) AS n_device_source_concept_id_both_null,
SUM(CASE WHEN device_source_concept_id = 0 AND device_concept_id IS NULL THEN 1 ELSE 0 END) AS n_device_source_concept_id_either_0,
SUM(CASE WHEN device_source_concept_id IS NULL AND device_concept_id =0 THEN 1 ELSE 0 END) AS n_device_source_concept_id_either_null
FROM `{{project_id}}.{{deid_clean_cdr}}.device_exposure`
""")
q = query.render(project_id=project_id, deid_clean_cdr=deid_clean_cdr)
result = execute(client, q)
if result.loc[0].sum() == 0:
    summary = summary.append({
        'query': 'Query6 device',
        'result': 'PASS'
    },
                   ignore_index=True)
else:
    summary = summary.append({
        'query': 'Query6 device',
        'result': 'Failure'
    },
                   ignore_index=True)
result.T

# + [markdown] papermill={"duration": 0.023649, "end_time": "2021-02-02T22:30:39.115495", "exception": false, "start_time": "2021-02-02T22:30:39.091846", "status": "completed"} tags=[]
# # 7 Verify that in measurement table if measurement_source_concept_id AND the measurement_concept_id both of those fields are null OR zero, the row should be removed.

# + papermill={"duration": 2.338821, "end_time": "2021-02-02T22:30:41.501415", "exception": false, "start_time": "2021-02-02T22:30:39.162594", "status": "completed"} tags=[]
query = JINJA_ENV.from_string("""

SELECT
SUM(CASE WHEN measurement_source_concept_id = 0 AND measurement_concept_id = 0 THEN 1 ELSE 0 END) AS n_measurement_source_concept_id_both_0,
SUM(CASE WHEN measurement_source_concept_id IS NULL AND measurement_concept_id IS NULL THEN 1 ELSE 0 END) AS n_measurement_source_concept_id_both_null,
SUM(CASE WHEN measurement_source_concept_id = 0 AND measurement_concept_id IS NULL THEN 1 ELSE 0 END) AS n_measurement_source_concept_id_either_0,
SUM(CASE WHEN measurement_source_concept_id IS NULL AND measurement_concept_id =0 THEN 1 ELSE 0 END) AS n_measurement_source_concept_id_either_null
FROM`{{project_id}}.{{deid_clean_cdr}}.measurement`
""")
q = query.render(project_id=project_id, deid_clean_cdr=deid_clean_cdr)
result = execute(client, q)
if result.loc[0].sum() == 0:
    summary = summary.append({
        'query': 'Query7 measurement',
        'result': 'PASS'
    },
                   ignore_index=True)
else:
    summary = summary.append({
        'query': 'Query7, measurement',
        'result': 'Failure'
    },
                   ignore_index=True)
result.T
# -

# # 8  check State_of_Residence fields in the person_ext table in deid_clean
#
# Generalization Rules for reference
#
# DC-939: Validate that a new table Person_ext table in the deid_cdr_clean has the State_of_residence data!
# will not work in deid_cdr though. But this column will be in product person table.
#
# DC-1011

# +
query = JINJA_ENV.from_string("""
SELECT COUNT (*) AS n_row_pass
FROM  `{{project_id}}.{{deid_clean_cdr}}.person_ext` d
JOIN `{{project_id}}.{{deid_clean_cdr}}.person` e
ON  d.person_id = e.person_id
WHERE state_of_residence_concept_id IS NOT NULL OR state_of_residence_source_value IS NOT NULL
""")
q = query.render(project_id=project_id, deid_clean_cdr=deid_clean_cdr)
result = execute(client, q)

if result.loc[0].sum() == 0:
    summary = summary.append(
        {
            'query': 'Query8, State_of_Residence in person_ext',
            'result': ' Failure'
        },
        ignore_index=True)
else:
    summary = summary.append(
        {
            'query': 'Query8, State_of_Residence in person_ext',
            'result': 'PASS'
        },
        ignore_index=True)
result
# -

Query9 "Race Ethnicity: person_ext self reported population DC-3789"
Verify that the person_ext self_reported_population fields are populated correctly. 

# has to be deid_clean
query = JINJA_ENV.from_string("""
WITH obs as 
(SELECT person_id, races, c_races 
    FROM (
      SELECT person_id, ARRAY_TO_STRING(ARRAY_AGG(CAST(value_source_concept_id AS STRING)), '|') races, ARRAY_TO_STRING(ARRAY_AGG(CAST(value_as_concept_id AS STRING)), '|') c_races
      FROM (
        SELECT person_id, value_as_concept_id, value_source_concept_id
        FROM `{{project_id}}.{{deid_clean_cdr}}.observation`
        WHERE observation_source_concept_id = 1586140
        ORDER BY person_id, value_source_concept_id)
      GROUP BY 1))

SELECT DISTINCT races, c_races, race_source_value, ethnicity_source_value,  race_source_concept_id, race_concept_id, self_reported_population_source_value,  self_reported_population_source_concept_id, self_reported_population_concept_id
FROM obs
LEFT JOIN `{{project_id}}.{{deid_clean_cdr}}.person`
USING (person_id)
LEFT JOIN `{{project_id}}.{{deid_clean_cdr}}.person_ext`
USING (person_id)
WHERE 
-- check srp column multi pop --
  (REGEXP_CONTAINS(obs.c_races,  r'\|')  AND self_reported_population_source_value != 'WhatRaceEthnicity_GeneralizedMultPopulations' )
-- check srp column single pop not hispanic--
  OR (NOT (REGEXP_CONTAINS(obs.c_races,  r'\|') )) AND (race_source_value != self_reported_population_source_value AND (race_source_value = 'AoUDRC_NoneIndicated' AND self_reported_population_source_value != 'WhatRaceEthnicity_Hispanic'))
-- check srp column single pop hispanic--
  OR (race_source_value = 'AoUDRC_NoneIndicated' AND self_reported_population_source_value != 'WhatRaceEthnicity_Hispanic')
-- check only expected srpsv exist --
  OR (self_reported_population_source_value NOT IN ('WhatRaceEthnicity_GeneralizedMultPopulations','WhatRaceEthnicity_GeneralizedPopulation', 'WhatRaceEthnicity_Black','WhatRaceEthnicity_White','WhatRaceEthnicity_Asian' ,'WhatRaceEthnicity_Hispanic','PMI_PreferNotToAnswer', 'PMI_Skip', 'WhatRaceEthnicity_RaceEthnicityNoneOfThese','WhatRaceEthnicity_AIAN',
'WhatRaceEthnicity_MENA','WhatRaceEthnicity_NHPI'))
-- check for expected concept_ids per srpsv --
  OR (self_reported_population_source_value = 'WhatRaceEthnicity_GeneralizedMultPopulations' AND (self_reported_population_concept_id != 2000000008 OR self_reported_population_source_concept_id != 2000000008))
  OR (self_reported_population_source_value = 'WhatRaceEthnicity_GeneralizedPopulation' AND (self_reported_population_concept_id != 2000000001 OR self_reported_population_source_concept_id != 2000000001))
  OR (self_reported_population_source_value = 'WhatRaceEthnicity_Black' AND (self_reported_population_concept_id != 8516 OR self_reported_population_source_concept_id != 1586143))
  OR (self_reported_population_source_value = 'WhatRaceEthnicity_White' AND (self_reported_population_concept_id != 8527 OR self_reported_population_source_concept_id != 1586146))
  OR (self_reported_population_source_value = 'WhatRaceEthnicity_Asian' AND (self_reported_population_concept_id != 8515 OR self_reported_population_source_concept_id != 1586142))
  OR (self_reported_population_source_value = 'WhatRaceEthnicity_Hispanic' AND (self_reported_population_concept_id != 1586147 OR self_reported_population_source_concept_id != 1586147))
  OR (self_reported_population_source_value = 'PMI_PreferNotToAnswer' AND (self_reported_population_concept_id != 1177221 OR self_reported_population_source_concept_id != 903079))
  OR (self_reported_population_source_value = 'PMI_Skip' AND (self_reported_population_concept_id != 903096 OR self_reported_population_source_concept_id != 903096))
  OR (self_reported_population_source_value = 'WhatRaceEthnicity_RaceEthnicityNoneOfThese' AND (self_reported_population_concept_id != 45882607 OR self_reported_population_source_concept_id != 1586148))
  OR (self_reported_population_source_value = 'WhatRaceEthnicity_AIAN' AND (self_reported_population_concept_id != 8657 OR self_reported_population_source_concept_id != 1586141)) -- ct only --
  OR (self_reported_population_source_value = 'WhatRaceEthnicity_MENA' AND (self_reported_population_concept_id != 38003615 OR self_reported_population_source_concept_id != 1586144)) -- ct only --
  OR (self_reported_population_source_value = 'WhatRaceEthnicity_NHPI' AND (self_reported_population_concept_id != 8557 OR self_reported_population_source_concept_id != 1586145)) -- ct only --
ORDER BY 1,2

""")
q = query.render(project_id=project_id, deid_clean_cdr=deid_clean_cdr)
df1 = execute(client, q)
if df1.eq(0).any().any():
    summary = summary.append(
        {
            'query':
                'Query 9 Race Ethnicity: person_ext self reported population',
            'result':
                'PASS'
        },
        ignore_index=True)
else:
    summary = summary.append(
        {
            'query':
                'Query 9 Race Ethnicity: person_ext self reported population',
            'result':
                'Failure'
        },
        ignore_index=True)
df1

# # Summary_row_ICD_suppression


# +
def highlight_cells(val):
    color = 'red' if 'Failure' in val else 'white'
    return f'background-color: {color}'


summary.style.applymap(highlight_cells).set_properties(**{'text-align': 'left'})
