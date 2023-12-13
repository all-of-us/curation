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
from common import JINJA_ENV
from utils import auth
from gcloud.bq import BigQueryClient
from analytics.cdr_ops.notebook_utils import execute, IMPERSONATION_SCOPES
pd.options.display.max_rows = 120

# + papermill={"duration": 0.023643, "end_time": "2021-02-02T22:30:31.880820", "exception": false, "start_time": "2021-02-02T22:30:31.857177", "status": "completed"} tags=["parameters"]
# Parameters
project_id = ""  # The project to examine
com_cdr = ""  # The comibend dataset
deid_base_cdr = ""  # the deid dataset
pipeline = ""  # the pipeline tables
run_as = ""  # The account used to run checks


# +
impersonation_creds = auth.get_impersonation_credentials(
    run_as, target_scopes=IMPERSONATION_SCOPES)

client = BigQueryClient(project_id, credentials=impersonation_creds)
# -

# df will have a summary in the end
df = pd.DataFrame(columns = ['query', 'result'])

# + [markdown] papermill={"duration": 0.02327, "end_time": "2021-02-02T22:30:32.708257", "exception": false, "start_time": "2021-02-02T22:30:32.684987", "status": "completed"} tags=[]
# # 1 done Verify that the COPE Survey Data identified to be suppressed as de-identification action in OBSERVATION table have been removed from the de-id dataset.
#
# these concept_ids should be suppressed as shown in the spread sheet 'COPE - All Surveys Privacy Rules', and was temporally saved to curation_sandbox.temp_cope_privacy_rules. Moving forward, we only need to update this table accordingly.
#
# https://docs.google.com/spreadsheets/d/1UuUVcRdlp2HkBaVdROFsM4ZX_bfffg6ZoEbqj94MlXU/edit#gid=0
#
# DC-2373, DC-2374 done
# -

# these concept_ids should be suppressed
query = JINJA_ENV.from_string("""
select OMOP_conceptID,New_Requirement
from  `{{project_id}}.curation_sandbox.temp_cope_privacy_rules`
where New_Requirement like 'suppress%' or New_Requirement like 'row suppression'
""")
q = query.render(project_id=project_id,sandbox=sandbox)
result = execute(client, q)
result.shape

result


query = JINJA_ENV.from_string("""
SELECT observation_source_concept_id, concept_name,concept_code,vocabulary_id,
observation_concept_id,
COUNT(1) AS n_row_not_pass
FROM `{{project_id}}.{{deid_cdr}}.observation` ob
JOIN `{{project_id}}.{{deid_cdr}}.concept` c
ON ob.observation_source_concept_id=c.concept_id
WHERE observation_source_concept_id IN
(select OMOP_conceptID from `{{project_id}}.curation_sandbox.temp_cope_privacy_rules`
where New_Requirement like 'suppress%' or New_Requirement like 'row suppression')
OR observation_concept_id IN
(select OMOP_conceptID from `{{project_id}}.curation_sandbox.temp_cope_privacy_rules`
where New_Requirement like 'suppress%' or New_Requirement like 'row suppression')
GROUP BY 1,2,3,4,5
ORDER BY n_row_not_pass DESC
""")
q = query.render(project_id=project_id,sandbox=sandbox,deid_cdr=deid_cdr)
result = execute(client, q)
result.shape

result

if df1['n_row_not_pass'].sum()==0:
 df = df.append({'query' : 'Query1 No COPE in deid_observation table', 'result' : 'Pass'},
                ignore_index = True)
else:
 df = df.append({'query' : 'Query1 No COPE in deid_observation table' , 'result' : 'Failure'},
                ignore_index = True)

# + [markdown] papermill={"duration": 0.023633, "end_time": "2021-02-02T22:30:36.860798", "exception": false, "start_time": "2021-02-02T22:30:36.837165", "status": "completed"} tags=[]
# # 2 done   Verify if a survey version is provided for the COPE survey.
#
# [DC-1040]
#
# expected results: all the person_id and the questionnaire_response_id has a survey_version_concept_id
# original sql missed something.
#
# these should be generalized 2100000002,2100000003,2100000004
#
# new update/question as 05062022, there are more survey version which are not included in the original table? shoud I bring this up in the meeting??
# -

query = JINJA_ENV.from_string("""
SELECT survey_version_concept_id,
count (*) row_counts,
CASE WHEN
  COUNT(*) > 0
  THEN 0 ELSE 1
END
 AS Failure_row_counts

FROM `{{project_id}}.{{deid_cdr}}.concept` c1
LEFT JOIN `{{project_id}}.{{deid_cdr}}.concept_relationship` cr ON cr.concept_id_2 = c1.concept_id
JOIN `{{project_id}}.{{deid_cdr}}.observation` ob on ob.observation_concept_id=c1.concept_id
LEFT JOIN `{{project_id}}.{{deid_cdr}}.observation_ext` ext USING(observation_id)
WHERE
 cr.concept_id_1 IN (1333174,1333343,1333207,1333310,1332811,1332812,1332715,1332813,1333101,1332814,1332815,1332816,1332817,1332818)
 AND cr.relationship_id = "PPI parent code of"
 group by 1
 order by row_counts
 """)
q = query.render(project_id=project_id,deid_cdr=deid_cdr)
df1=execute(client, q)
df1.shape

df1

if df1['Failure_row_counts'].sum()==0:
 df = df.append({'query' : 'Query2 survey version provided', 'result' : 'Pass'},
                ignore_index = True)
else:
 df = df.append({'query' : 'Query2 survey version provided', 'result' : 'Failure'},
                ignore_index = True)

# + [markdown] papermill={"duration": 0.023649, "end_time": "2021-02-02T22:30:39.115495", "exception": false, "start_time": "2021-02-02T22:30:39.091846", "status": "completed"} tags=[]
# # 3 done no change Verify that all structured concepts related  to COVID are NOT suppressed in EHR tables
#
#   DC-891
#
# 756055,4100065,37311061,439676,37311060,45763724
#
# update, Remove analyses 3, 4, and 5 as suppression of COVID concepts is no longer part of RT privacy requirements,[DC-1752]
# -

query = JINJA_ENV.from_string("""

SELECT measurement_concept_id, concept_name,concept_code,vocabulary_id,
COUNT(1) AS n_row_not_pass,
CASE WHEN
  COUNT(*) > 0
  THEN 0 ELSE 1
END
 AS Failure_row_counts
FROM `{{project_id}}.{{deid_cdr}}.measurement` ob
JOIN `{{project_id}}.{{deid_cdr}}.concept` c
ON ob.measurement_concept_id=c.concept_id
WHERE measurement_concept_id=756055
GROUP BY 1,2,3,4
ORDER BY n_row_not_pass DESC

 """)
q = query.render(project_id=project_id,deid_cdr=deid_cdr)
df1=execute(client, q)
df1.shape

df1

if df1['Failure_row_counts'].sum()==0:
 df = df.append({'query' : 'Query3 No COPE in deid_measurement table', 'result' : 'Pass'},
                ignore_index = True)
else:
 df = df.append({'query' : 'Query3 No COPE in deid_measurement table' , 'result' : 'Failure'},
                ignore_index = True)

# + [markdown] papermill={"duration": 0.023649, "end_time": "2021-02-02T22:30:39.115495", "exception": false, "start_time": "2021-02-02T22:30:39.091846", "status": "completed"} tags=[]
# # 4 done no change Verify that all structured concepts related  to COVID are NOT suppressed in EHR condition_occurrence
#
#   DC-891
#
# 756055,4100065,37311061,439676,37311060,45763724
#
# update, Remove analyses 3, 4, and 5 as suppression of COVID concepts is no longer part of RT privacy requirements,[DC-1752]
# -

query = JINJA_ENV.from_string("""

SELECT condition_concept_id, concept_name,concept_code,vocabulary_id,
COUNT(1) AS n_row_not_pass,
CASE WHEN
  COUNT(*) > 0
  THEN 0 ELSE 1
END
 AS Failure_row_counts
FROM  `{{project_id}}.{{deid_cdr}}.condition_occurrence` ob
JOIN `{{project_id}}.{{deid_cdr}}.concept` c
ON ob.condition_concept_id=c.concept_id
WHERE condition_concept_id IN  (4100065, 37311061, 439676)
GROUP BY 1,2,3,4
ORDER BY n_row_not_pass DESC

 """)
q = query.render(project_id=project_id,deid_cdr=deid_cdr)
df1=execute(client, q)
df1.shape

df1

if df1['Failure_row_counts'].sum()==0:
 df = df.append({'query' : 'Query4 COVID concepts suppression in deid_observation table', 'result' : 'Pass'},
                ignore_index = True)
else:
 df = df.append({'query' : 'Query4 COVID concepts suppression in deid_observation table' , 'result' : 'Failure'},
                ignore_index = True)


# + [markdown] papermill={"duration": 0.023649, "end_time": "2021-02-02T22:30:39.115495", "exception": false, "start_time": "2021-02-02T22:30:39.091846", "status": "completed"} tags=[]
# # 5 done no change Verify that all structured concepts related  to COVID are NOT suppressed in EHR observation
#
#   DC-891
#
# 756055,4100065,37311061,439676,37311060,45763724
#
# update, Remove analyses 3, 4, and 5 as suppression of COVID concepts is no longer part of RT privacy requirements,[DC-1752]
# -

query = JINJA_ENV.from_string("""

SELECT observation_concept_id, concept_name,concept_code,vocabulary_id,observation_source_concept_id,
COUNT(1) AS n_row_not_pass,
CASE WHEN
  COUNT(*) > 0
  THEN 0 ELSE 1
END
 AS Failure_row_counts
FROM `{{project_id}}.{{deid_cdr}}.observation` ob
JOIN `{{project_id}}.{{deid_cdr}}.concept` c
ON ob.observation_concept_id=c.concept_id
WHERE observation_concept_id IN  (37311060, 45763724) OR observation_source_concept_id IN  (37311060, 45763724)
GROUP BY 1,2,3,4,5
ORDER BY n_row_not_pass DESC
""")
q = query.render(project_id=project_id,deid_cdr=deid_cdr)
df1=execute(client, q)
df1.shape


df1

if df1['Failure_row_counts'].sum()==0:
 df = df.append({'query' : 'Query5 COVID concepts suppression in observation table', 'result' : 'Pass'},
                ignore_index = True)
else:
 df = df.append({'query' : 'Query5 COVID concepts suppression in observation table' , 'result' : 'Failure'},
                ignore_index = True)

# # 6 done updated Verify these concepts are NOT suppressed in EHR observation
#
# [DC-1747]
# these concepts 1333015, 	1333023	are not longer suppressed
#
# 1332737, [DC-1665]
#
# 1333291
#
# 1332904,1333140 should be generalized to 1332737 , # update ?need to rewrite??
#
# 1332843 should be generalized.

query = JINJA_ENV.from_string("""

SELECT observation_source_concept_id, concept_name,concept_code,vocabulary_id,observation_concept_id,
COUNT(1) AS n_row_pass,
CASE WHEN
  COUNT(*) > 0
  THEN 0 ELSE 1
END
 AS Failure_row_counts

FROM `{{project_id}}.{{deid_base_cdr}}.observation` ob
JOIN `{{project_id}}.{{deid_base_cdr}}.concept` c
ON ob.observation_source_concept_id=c.concept_id
WHERE observation_source_concept_id IN  (1333015, 1333023, 1332737,1333291,1332904,1333140,1332843)
OR observation_concept_id IN  (1333015, 1333023,1332737,1333291,1332904,1333140,1332843 )
GROUP BY 1,2,3,4,5
ORDER BY n_row_pass DESC
""")
q = query.render(project_id=project_id,
                 deid_base_cdr=deid_base_cdr)
df1=execute(client, q)
df1.shape


df1

if (df1['Failure_row_counts'].sum()==0) and (df1[df1['observation_source_concept_id'].isin(['1332904','1333140'])].empty) :
 df = df.append({'query' : 'Query6 The concepts are not suppressed in observation table', 'result' : 'Pass'},
                ignore_index = True)
else:
 df = df.append({'query' : 'Query6 The concepts are not suppressed in observation table' , 'result' : 'Failure'},
                ignore_index = True)

# # 7 done Vaccine-related concepts as these EHR-submitted COVID concepts are allowed from RT
# DC-2374
# this query was from DC-1752

query = JINJA_ENV.from_string("""

DECLARE vocabulary_tables DEFAULT ['vocabulary', 'concept', 'source_to_concept_map',
                                   'concept_class', 'concept_synonym', 'concept_ancestor',
                                   'concept_relationship', 'relationship', 'drug_strength'];
SELECT table_name,column_name

FROM `{{project_id}}.{{deid_base_cdr}}.INFORMATION_SCHEMA.COLUMNS` c
JOIN `{{project_id}}.{{deid_base_cdr}}.__TABLES__` t
 ON c.table_name = t.table_id
WHERE
     table_name NOT IN UNNEST(vocabulary_tables) and
  t.row_count > 0
  AND table_name NOT LIKE '\\\_%'
  AND table_name in ('procedure_occurrence','drug_exposure')
  AND column_name in ('procedure_concept_id','procedure_source_concept_id','drug_concept_id','drug_source_concept_id')
""")
q = query.render(project_id=project_id,
                 deid_base_cdr=deid_base_cdr)
target_tables = execute(client, q)
target_tables.shape


# +
#table_name="drug_exposure"
#@column_name="drug_concept_id"

def my_sql(table_name, column_name):

    query = JINJA_ENV.from_string("""
SELECT
'procedure_occurrence' AS table_name,
'procedure_concept_id' AS column_name,
concept_id_in_combined,
COUNT(*) AS row_counts,
CASE WHEN
  COUNT(*) > 0 AND sub.concept_id_in_combined IS NOT NULL
  THEN 0 ELSE 1
END
 AS Failure_row_counts
FROM `aou-res-curation-prod.R2023q3r5_deid.procedure_occurrence` c
JOIN (
  SELECT concept_id as concept_id_in_combined
        FROM `aou-res-curation-prod.2023q3r2_combined.procedure_occurrence` c
        JOIN `aou-res-curation-prod.R2023q3r5_deid.concept`
        on concept_id=procedure_concept_id
        WHERE (REGEXP_CONTAINS(concept_name, r'(?i)(COVID)') AND
              REGEXP_CONTAINS(concept_name, r'(?i)(VAC)') AND
        vocabulary_id not in ('PPI'))
     OR (
        REGEXP_CONTAINS(concept_code, r'(207)|(208)|(210)|(212)|(213)')         and vocabulary_id = 'CVX'
    ) OR (
        REGEXP_CONTAINS(concept_code, r'(91300)|(91301)|(91302)|(91303)|(0031A)|(0021A)|(0022A)|(0002A)|(0001A)|(0012A)|(0011A)')           and vocabulary_id = 'CPT4'
     )
AND  domain_id LIKE '%LEFT(c.domain_id, 3)%'
 ) sub
  on concept_id_in_combined=procedure_concept_id
  GROUP BY concept_id_in_combined

""")
    q = query.render(project_id=project_id,deid_cdr=deid_cdr,table_name=table_name,column_name=column_name)
    df11=execute(client, q)
    return df11


# -

# use a loop to get table name AND column name AND run sql function
result = [my_sql(table_name, column_name) for table_name, column_name in zip(target_tables['table_name'], target_tables['column_name'])]
result
# if Row_count is '0' in "Combined" dataset as well, '0' showing up in this check is not a problem

# +
# AND then get the result back FROM loop result list
n=len(target_tables.index)
res2 = pd.DataFrame(result[0])

for x in range(1,n):
  res2=res2.append(result[x])

#res2=res2.sort_values(by='row_counts_failure', ascending=False)
res2
# -

if res2['Failure_row_counts'].sum()==0:
 df = df.append({'query' : 'Query7 COVID Vaccine-related concepts NOT suppressed in EHR tables', 'result' : 'Pass'},
                ignore_index = True)
else:
 df = df.append({'query' : 'Query7 COVID Vaccine-related concepts NOT suppressed in EHR tables' , 'result' : 'Failure'},
                ignore_index = True)


# # Summary_deid_COPE_survey

# +
def highlight_cells(val):
    color = 'red' if 'Failure' in val else 'white'
    return f'background-color: {color}'

df.style.applymap(highlight_cells).set_properties(**{'text-align': 'left'})
# -


