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
from common import JINJA_ENV
from utils import auth
from gcloud.bq import BigQueryClient
from analytics.cdr_ops.notebook_utils import execute, IMPERSONATION_SCOPES
pd.options.display.max_rows = 120

# + tags=["parameters"]
project_id = ""
com_cdr = ""
deid_cdr = ""
deid_questionnaire_response_map_dataset = ""
pipeline = ""
run_as = ""

# +
impersonation_creds = auth.get_impersonation_credentials(
    run_as, target_scopes=IMPERSONATION_SCOPES)

client = BigQueryClient(project_id, credentials=impersonation_creds)
# -

# df will have a summary in the end
df = pd.DataFrame(columns=['query', 'result'])

# + [markdown] papermill={"duration": 0.02327, "end_time": "2021-02-02T22:30:32.708257", "exception": false, "start_time": "2021-02-02T22:30:32.684987", "status": "completed"} tags=[]
# # 1 DS_1 Verify that the field identified to follow the date shift rule as de-identification action in OBSERVATION table have been randomly date shifted.

# + papermill={"duration": 4.105203, "end_time": "2021-02-02T22:30:36.813460", "exception": false, "start_time": "2021-02-02T22:30:32.708257", "status": "completed"} tags=[]
query = JINJA_ENV.from_string("""

WITH df1 AS (
SELECT
DATE_DIFF(DATE(i.observation_date), DATE(d.observation_date),day)-m.shift as diff
FROM `{{project_id}}.{{pipeline}}.pid_rid_mapping` m
JOIN `{{project_id}}.{{com_cdr}}.observation` i
ON m.person_id = i.person_id
JOIN `{{project_id}}.{{deid_cdr}}.observation` d
ON d.observation_id = i.observation_id)

SELECT COUNT(*) AS n_row_not_pass FROM df1
WHERE diff !=0

""")
q = query.render(project_id=project_id,
                 pipeline=pipeline,
                 com_cdr=com_cdr,
                 deid_cdr=deid_cdr)
df1 = execute(client, q)
if df1.eq(0).any().any():
    df = df.append({
        'query': 'Query1 OBSERVATION',
        'result': 'PASS'
    },
                   ignore_index=True)
else:
    df = df.append({
        'query': 'Query1 OBSERVATION',
        'result': 'Failure'
    },
                   ignore_index=True)
df1
# -

# # 3 DS_3 Verify that the field identified to follow the date shift rule as de-identification action in OBSERVATION_PERIOD table have been randomly date shifted.

# + papermill={"duration": 2.136748, "end_time": "2021-02-02T22:30:39.044867", "exception": false, "start_time": "2021-02-02T22:30:36.908119", "status": "completed"} tags=[]
query = JINJA_ENV.from_string("""
WITH df1 AS (
SELECT
DATE_DIFF(DATE(i.observation_period_start_date), DATE(d.observation_period_start_date),day)-m.shift as diff
FROM `{{project_id}}.{{pipeline}}.pid_rid_mapping` m
JOIN `{{project_id}}.{{com_cdr}}.observation_period` i
ON m.person_id = i.person_id
JOIN `{{project_id}}.{{deid_cdr}}.observation_period` d
ON d.observation_period_id = i.observation_period_id)

SELECT COUNT(*) AS n_row_not_pass FROM df1
WHERE diff !=0

""")
q = query.render(project_id=project_id,
                 pipeline=pipeline,
                 com_cdr=com_cdr,
                 deid_cdr=deid_cdr)
df1 = execute(client, q)
if df1.eq(0).any().any():
    df = df.append({
        'query': 'Query3 OBSERVATION_PERIOD',
        'result': 'PASS'
    },
                   ignore_index=True)
else:
    df = df.append({
        'query': 'Query3 OBSERVATION_PERIOD',
        'result': 'Failure'
    },
                   ignore_index=True)
df1

# + [markdown] papermill={"duration": 0.023649, "end_time": "2021-02-02T22:30:39.115495", "exception": false, "start_time": "2021-02-02T22:30:39.091846", "status": "completed"} tags=[]
# # 4 DS_4 Verify that the field identified to follow the date shift rule as de-identification action in PERSON table have been randomly date shifted.

# + papermill={"duration": 2.338821, "end_time": "2021-02-02T22:30:41.501415", "exception": false, "start_time": "2021-02-02T22:30:39.162594", "status": "completed"} tags=[]
query = JINJA_ENV.from_string("""
WITH df1 AS (
SELECT
DATE_DIFF(DATE(i.birth_datetime), DATE(d.birth_datetime),day)-m.shift as diff
FROM `{{project_id}}.{{pipeline}}.pid_rid_mapping` m
JOIN `{{project_id}}.{{com_cdr}}.person` i
ON m.person_id = i.person_id
JOIN `{{project_id}}.{{deid_cdr}}.person` d
ON d.person_id = m.research_id
)
SELECT COUNT(*) AS n_row_not_pass FROM df1
WHERE diff !=0
""")
q = query.render(project_id=project_id,
                 pipeline=pipeline,
                 com_cdr=com_cdr,
                 deid_cdr=deid_cdr)
df1 = execute(client, q)
if df1.eq(0).any().any():
    df = df.append({
        'query': 'Query4 Person table',
        'result': 'PASS'
    },
                   ignore_index=True)
else:
    df = df.append({
        'query': 'Query4 Person table',
        'result': 'Failure'
    },
                   ignore_index=True)
df1

# -

# # 5 DS_5 Verify that the field identified to follow the date shift rule as de-identification action in SPECIMEN table have been randomly date shifted.

# + papermill={"duration": 2.338821, "end_time": "2021-02-02T22:30:41.501415", "exception": false, "start_time": "2021-02-02T22:30:39.162594", "status": "completed"} tags=[]
query = JINJA_ENV.from_string("""
WITH df1 AS (
SELECT
DATE_DIFF(DATE(i.specimen_date), DATE(d.specimen_date),day)-m.shift as diff
FROM `{{project_id}}.{{pipeline}}.pid_rid_mapping` m
JOIN `{{project_id}}.{{com_cdr}}.specimen` i
ON m.person_id = i.person_id
JOIN `{{project_id}}.{{deid_cdr}}.specimen` d
ON d.specimen_id = i.specimen_id
)
SELECT COUNT(*) AS n_row_not_pass FROM df1
WHERE diff !=0
  
""")
q = query.render(project_id=project_id,
                 pipeline=pipeline,
                 com_cdr=com_cdr,
                 deid_cdr=deid_cdr)
df1 = execute(client, q)
if df1.eq(0).any().any():
    df = df.append({
        'query': 'Query5 SPECIMEN',
        'result': 'PASS'
    },
                   ignore_index=True)
else:
    df = df.append({
        'query': 'Query5 SPECIMEN',
        'result': 'Failure'
    },
                   ignore_index=True)
df1
# -

# # 6 DS_6 Verify that the field identified to follow the date shift rule as de-identification action in AOU_DEATH table have been randomly date shifted.

query = JINJA_ENV.from_string("""

WITH df1 AS (
SELECT 
DATE_DIFF(DATE(i.death_date), DATE(d.death_date),day)-m.shift as diff
FROM `{{project_id}}.{{pipeline}}.pid_rid_mapping` m
JOIN `{{project_id}}.{{com_cdr}}.aou_death` i
ON m.person_id = i.person_id
JOIN `{{project_id}}.{{deid_cdr}}.aou_death` d
ON d.aou_death_id = i.aou_death_id 
WHERE i.death_date IS NOT NULL
)
SELECT COUNT (*) AS n_row_not_pass FROM df1
WHERE diff !=0
  
""")
q = query.render(project_id=project_id,
                 pipeline=pipeline,
                 com_cdr=com_cdr,
                 deid_cdr=deid_cdr)
df1 = execute(client, q)
if df1.eq(0).any().any():
    df = df.append({
        'query': 'Query6 AOU_DEATH',
        'result': 'PASS'
    },
                   ignore_index=True)
else:
    df = df.append({
        'query': 'Query6 AOU_DEATH',
        'result': 'Failure'
    },
                   ignore_index=True)
df1

# + [markdown] papermill={"duration": 0.023411, "end_time": "2021-02-02T22:30:39.091846", "exception": false, "start_time": "2021-02-02T22:30:39.068435", "status": "completed"} tags=[]
# # 7 DS_7 Verify that the field identified to follow the date shift rule as de-identification action in VISIT OCCURENCE table have been randomly date shifted.
# -

query = JINJA_ENV.from_string("""
WITH df1 AS (
SELECT
DATE_DIFF(DATE(i.visit_start_date), DATE(d.visit_start_date),day)-m.shift as diff
FROM `{{project_id}}.{{pipeline}}.pid_rid_mapping` m
JOIN `{{project_id}}.{{com_cdr}}.visit_occurrence` i
ON m.person_id = i.person_id
JOIN `{{project_id}}.{{deid_cdr}}.visit_occurrence` d
ON d.visit_occurrence_id = i.visit_occurrence_id
)
SELECT COUNT (*) AS n_row_not_pass FROM df1
WHERE diff !=0
  
""")
q = query.render(project_id=project_id,
                 pipeline=pipeline,
                 com_cdr=com_cdr,
                 deid_cdr=deid_cdr)
df1 = execute(client, q)
if df1.eq(0).any().any():
    df = df.append({
        'query': 'Query7 Visit',
        'result': 'PASS'
    },
                   ignore_index=True)
else:
    df = df.append({
        'query': 'Query7 Visit',
        'result': 'Failure'
    },
                   ignore_index=True)
df1

# # 8 DS_8 Verify that the field identified to follow the date shift rule as de-identification action in PROCEDURE OCCURENCE table have been randomly date shifted.
#

query = JINJA_ENV.from_string("""
WITH df1 as (
SELECT
DATE_DIFF(DATE(i.procedure_date), DATE(d.procedure_date),day)-m.shift as diff
FROM `{{project_id}}.{{pipeline}}.pid_rid_mapping` m
JOIN `{{project_id}}.{{com_cdr}}.procedure_occurrence` i
ON m.person_id = i.person_id
JOIN `{{project_id}}.{{deid_cdr}}.procedure_occurrence` d
ON d.procedure_occurrence_id = i.procedure_occurrence_id
)
SELECT COUNT(*) AS n_row_not_pass FROM df1
WHERE diff !=0
  
""")
q = query.render(project_id=project_id,
                 pipeline=pipeline,
                 com_cdr=com_cdr,
                 deid_cdr=deid_cdr)
df1 = execute(client, q)
if df1.eq(0).any().any():
    df = df.append({
        'query': 'Query8 PROCEDURE',
        'result': 'PASS'
    },
                   ignore_index=True)
else:
    df = df.append({
        'query': 'Query8 PROCEDURE',
        'result': 'Failure'
    },
                   ignore_index=True)
df1

# # 9 DS_9 Verify that the field identified to follow the date shift rule as de-identification action in DRUG EXPOSURE table have been randomly date shifted.

# +
query = JINJA_ENV.from_string("""
WITH df1 AS (
SELECT
DATE_DIFF(DATE(i.drug_exposure_start_date), DATE(d.drug_exposure_start_date),day)-m.shift as diff
FROM `{{project_id}}.{{pipeline}}.pid_rid_mapping` m
JOIN `{{project_id}}.{{com_cdr}}.drug_exposure` i
ON m.person_id = i.person_id
JOIN `{{project_id}}.{{deid_cdr}}.drug_exposure` d
ON i.drug_exposure_id = d.drug_exposure_id
)
SELECT COUNT(*) AS n_row_not_pass FROM df1
WHERE diff !=0
""")
q = query.render(project_id=project_id,
                 pipeline=pipeline,
                 com_cdr=com_cdr,
                 deid_cdr=deid_cdr)
df1 = execute(client, q)

if df1.eq(0).any().any():
    df = df.append({
        'query': 'Query9 Drug table',
        'result': 'PASS'
    },
                   ignore_index=True)
else:
    df = df.append({
        'query': 'Query9 Drug table',
        'result': 'Failure'
    },
                   ignore_index=True)
df1
# -

# # 10 DS_10 Verify that the field identified to follow the date shift rule as de-identification action in DEVICE EXPOSURE table have been randomly date shifted.

query = JINJA_ENV.from_string("""
WITH df1 AS (
SELECT
 DATE_DIFF(DATE(i.device_exposure_start_date), DATE(d.device_exposure_start_date),day)-m.shift as diff
FROM `{{project_id}}.{{pipeline}}.pid_rid_mapping` m
JOIN `{{project_id}}.{{com_cdr}}.device_exposure` i
ON m.person_id = i.person_id
JOIN `{{project_id}}.{{deid_cdr}}.device_exposure` d
ON i.device_exposure_id = d.device_exposure_id
  )
SELECT COUNT(*) AS n_row_not_pass FROM df1
WHERE diff !=0
  """)
q = query.render(project_id=project_id,
                 pipeline=pipeline,
                 com_cdr=com_cdr,
                 deid_cdr=deid_cdr)
df1 = execute(client, q)
if df1.eq(0).any().any():
    df = df.append({
        'query': 'Query10 Device',
        'result': 'PASS'
    },
                   ignore_index=True)
else:
    df = df.append({
        'query': 'Query10 Device',
        'result': 'Failure'
    },
                   ignore_index=True)
df1

# # 11 DS_11 Verify that the field identified to follow the date shift rule as de-identification action in CONDITION OCCURENCE table have been randomly date shifted.

query = JINJA_ENV.from_string("""
WITH df1 AS (
SELECT
DATE_DIFF(DATE(i.condition_start_date), DATE(d.condition_start_date),day)-m.shift as diff
FROM `{{project_id}}.{{pipeline}}.pid_rid_mapping` m
JOIN `{{project_id}}.{{com_cdr}}.condition_occurrence` i
ON m.person_id = i.person_id
JOIN `{{project_id}}.{{deid_cdr}}.condition_occurrence` d
ON i.condition_occurrence_id = d.condition_occurrence_id
  )
SELECT COUNT(*) AS n_row_not_pass FROM df1
WHERE diff !=0
  
""")
q = query.render(project_id=project_id,
                 pipeline=pipeline,
                 com_cdr=com_cdr,
                 deid_cdr=deid_cdr)
df1 = execute(client, q)
if df1.eq(0).any().any():
    df = df.append({
        'query': 'Query11 Condition table',
        'result': 'PASS'
    },
                   ignore_index=True)
else:
    df = df.append({
        'query': 'Query11 Condition table',
        'result': 'Failure'
    },
                   ignore_index=True)
df1

# # 12 DS_12 Verify that the field identified to follow the date shift rule as de-identification action in MEASUREMENT table have been randomly date shifted.

# +
query = JINJA_ENV.from_string("""
WITH df1 AS (
SELECT
DATE_DIFF(DATE(i.measurement_date), DATE(d.measurement_date),day)-m.shift as diff
FROM `{{project_id}}.{{pipeline}}.pid_rid_mapping` m
JOIN `{{project_id}}.{{com_cdr}}.measurement` i
ON m.person_id = i.person_id
JOIN `{{project_id}}.{{deid_cdr}}.measurement` d
ON d.measurement_id = i.measurement_id
  )
SELECT COUNT(*) AS n_row_not_pass FROM df1
WHERE diff !=0
  
""")
q = query.render(project_id=project_id,
                 pipeline=pipeline,
                 com_cdr=com_cdr,
                 deid_cdr=deid_cdr)
df1 = execute(client, q)

if df1.eq(0).any().any():
    df = df.append({
        'query': 'Query12 Measurement',
        'result': 'PASS'
    },
                   ignore_index=True)
else:
    df = df.append({
        'query': 'Query12 Measurement',
        'result': 'Failure'
    },
                   ignore_index=True)
df1
# -

# # Q13 DS_13 Verify that the field identified to follow the date shift rule as de-identification action in SURVEY_CONDUCT table have been randomly date shifted.
#
# this is a new table, only avaiable in new cdr 2022q4r4

# +
query = JINJA_ENV.from_string("""
WITH df1 AS (
SELECT
DATE_DIFF(DATE(i.survey_start_date), DATE(d.survey_start_date),day)-m.shift as diff
FROM `{{project_id}}.{{pipeline}}.pid_rid_mapping` m
JOIN `{{project_id}}.{{com_cdr}}.survey_conduct` i
ON m.person_id = i.person_id
JOIN `{{project_id}}.{{deid_cdr}}.survey_conduct` d
ON d.survey_conduct_id = i.survey_conduct_id
  )
SELECT COUNT(*) AS n_row_not_pass FROM df1
WHERE diff !=0

""")
q = query.render(project_id=project_id,
                 pipeline=pipeline,
                 com_cdr=com_cdr,
                 deid_cdr=deid_cdr)
df1 = execute(client, q)

if df1.eq(0).any().any():
    df = df.append({
        'query': 'Query13 Survey Conduct',
        'result': 'PASS'
    },
                   ignore_index=True)
else:
    df = df.append({
        'query': 'Query13 Survey Conduct',
        'result': 'Failure'
    },
                   ignore_index=True)
df1
# -

# # Q14 DS_14 Verify the date shift has been implemented following the date shift noted in the deid_map table in the non-deid dataset.

query = JINJA_ENV.from_string("""
SELECT COUNT (*) AS n_row_not_pass
FROM  `{{project_id}}.{{pipeline}}.primary_pid_rid_mapping`
WHERE shift NOT BETWEEN 1 AND 364

""")
q = query.render(project_id=project_id,
                 pipeline=pipeline,
                 com_cdr=com_cdr,
                 deid_cdr=deid_cdr)
df1 = execute(client, q)
if df1.eq(0).any().any():
    df = df.append(
        {
            'query': 'Query14 date shifted in non_deid',
            'result': 'PASS'
        },
        ignore_index=True)
else:
    df = df.append(
        {
            'query': 'Query14 date shifited in non_deid',
            'result': 'Failure'
        },
        ignore_index=True)
df1

# # Q15 DS_15 Verify that  person_id has been replaced by research_id
#
#
# checked total 8 tables including  specimen etc tables in deid. However will be hard to check person or death tables without row_id.

query = JINJA_ENV.from_string("""
WITH df1 AS (
SELECT COUNT (*) AS n_row_not_pass
FROM  `{{project_id}}.{{com_cdr}}.observation` non_deid
JOIN `{{project_id}}.{{pipeline}}.pid_rid_mapping` m
ON m.person_id=non_deid.person_id
JOIN `{{project_id}}.{{deid_cdr}}.observation` deid USING(observation_id)
WHERE deid.person_id !=m.research_id
),

df2 AS (
SELECT COUNT (*) AS n_row_not_pass
FROM  `{{project_id}}.{{com_cdr}}.measurement` non_deid
JOIN `{{project_id}}.{{pipeline}}.pid_rid_mapping` m
ON m.person_id=non_deid.person_id
JOIN `{{project_id}}.{{deid_cdr}}.measurement` deid USING(measurement_id)
WHERE deid.person_id !=m.research_id
),

df3 AS (
SELECT COUNT (*) AS n_row_not_pass
FROM  `{{project_id}}.{{com_cdr}}.condition_occurrence` non_deid
JOIN `{{project_id}}.{{pipeline}}.pid_rid_mapping` m
ON m.person_id=non_deid.person_id
JOIN `{{project_id}}.{{deid_cdr}}.condition_occurrence` deid USING(condition_occurrence_id)
WHERE deid.person_id !=m.research_id
),

df4 AS (
SELECT COUNT (*) AS n_row_not_pass
FROM  `{{project_id}}.{{com_cdr}}.drug_exposure` non_deid
JOIN `{{project_id}}.{{pipeline}}.pid_rid_mapping` m
ON m.person_id=non_deid.person_id
JOIN `{{project_id}}.{{deid_cdr}}.drug_exposure` deid USING(drug_exposure_id)
WHERE deid.person_id !=m.research_id
),

df5 AS (
SELECT COUNT (*) AS n_row_not_pass
FROM  `{{project_id}}.{{com_cdr}}.device_exposure` non_deid
JOIN `{{project_id}}.{{pipeline}}.pid_rid_mapping` m
ON m.person_id=non_deid.person_id
JOIN `{{project_id}}.{{deid_cdr}}.device_exposure` deid USING(device_exposure_id)
WHERE deid.person_id !=m.research_id
),

df6 AS (
SELECT COUNT (*) AS n_row_not_pass
FROM  `{{project_id}}.{{com_cdr}}.procedure_occurrence` non_deid
JOIN `{{project_id}}.{{pipeline}}.pid_rid_mapping` m
ON m.person_id=non_deid.person_id
JOIN `{{project_id}}.{{deid_cdr}}.procedure_occurrence` deid USING(procedure_occurrence_id)
WHERE deid.person_id !=m.research_id
),

df7 AS (
SELECT COUNT (*) AS n_row_not_pass
FROM  `{{project_id}}.{{com_cdr}}.visit_occurrence` non_deid
JOIN `{{project_id}}.{{pipeline}}.pid_rid_mapping` m
ON m.person_id=non_deid.person_id
JOIN `{{project_id}}.{{deid_cdr}}.visit_occurrence` deid USING(visit_occurrence_id)
WHERE deid.person_id !=m.research_id
),

df8 AS (
SELECT COUNT (*) AS n_row_not_pass
FROM  `{{project_id}}.{{com_cdr}}.specimen` non_deid
JOIN `{{project_id}}.{{pipeline}}.pid_rid_mapping` m
ON m.person_id=non_deid.person_id
JOIN `{{project_id}}.{{deid_cdr}}.specimen` deid USING(specimen_id)
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



""")
q = query.render(project_id=project_id,
                 pipeline=pipeline,
                 com_cdr=com_cdr,
                 deid_cdr=deid_cdr)
df1 = execute(client, q)
if df1.eq(0).any().any():
    df = df.append(
        {
            'query':
                'Query15 person_id replaed by research_id in other 8 tables',
            'result':
                'PASS'
        },
        ignore_index=True)
else:
    df = df.append(
        {
            'query':
                'Query15 person_id replaed by research_id in other 8 tables',
            'result':
                'Failure'
        },
        ignore_index=True)
df1

# # Q16 Verify that  questionnaire_response_id/survey_conduct_id has been replaced by research_response_id

query = JINJA_ENV.from_string("""
WITH df1 AS (
SELECT COUNT (*) AS n_row_not_pass
FROM  `{{project_id}}.{{com_cdr}}.survey_conduct` non_deid
JOIN `{{project_id}}.{{deid_questionnaire_response_map_dataset}}._deid_questionnaire_response_map` m
ON m.questionnaire_response_id=non_deid.survey_conduct_id
JOIN `{{project_id}}.{{deid_cdr}}.survey_conduct` deid USING(survey_conduct_id)
WHERE deid.survey_conduct_id != m.research_response_id
),

df2 AS (
SELECT COUNT (*) AS n_row_not_pass
FROM  `{{project_id}}.{{com_cdr}}.survey_conduct` non_deid
JOIN `{{project_id}}.{{deid_questionnaire_response_map_dataset}}._deid_questionnaire_response_map` m
ON m.questionnaire_response_id=non_deid.survey_conduct_id
JOIN `{{project_id}}.{{deid_cdr}}.survey_conduct` deid USING(survey_conduct_id)
WHERE SAFE_CAST(deid.survey_source_identifier AS INT64) != m.research_response_id
),
df3 AS (
SELECT COUNT (*) AS n_row_not_pass
FROM  `{{project_id}}.{{com_cdr}}.observation` non_deid
JOIN `{{project_id}}.{{deid_questionnaire_response_map_dataset}}._deid_questionnaire_response_map` m
ON m.questionnaire_response_id=non_deid.questionnaire_response_id
JOIN `{{project_id}}.{{deid_cdr}}.observation` deid USING(observation_id)
WHERE deid.questionnaire_response_id != m.research_response_id
)

SELECT * FROM df1
JOIN df2 USING(n_row_not_pass)
JOIN df3 USING(n_row_not_pass)

""")
q = query.render(project_id=project_id,
                 pipeline=pipeline,
                 com_cdr=com_cdr,
                 deid_cdr=deid_cdr,
                 deid_questionnaire_response_map_dataset=
                 deid_questionnaire_response_map_dataset)
df1 = execute(client, q)
if df1.eq(0).any().any():
    df = df.append(
        {
            'query':
                'Query16 questionnaire_response_id/survey_conduct_id/survey_source_identifier replaced by research_response_id',
            'result':
                'PASS'
        },
        ignore_index=True)
else:
    df = df.append(
        {
            'query':
                'Query16 questionnaire_response_id/survey_conduct_id/survey_source_identifier replaced by research_response_id',
            'result':
                'Failure'
        },
        ignore_index=True)
df1

# # Summary_dateshift


# +
def highlight_cells(val):
    color = 'red' if 'Failure' in val else 'white'
    return f'background-color: {color}'


df.style.applymap(highlight_cells).set_properties(**{'text-align': 'left'})
# -
