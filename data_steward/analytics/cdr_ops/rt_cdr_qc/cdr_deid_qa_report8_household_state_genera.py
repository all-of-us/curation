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
from common import (CONDITION_OCCURRENCE, DEVICE_EXPOSURE, DRUG_EXPOSURE,
                    JINJA_ENV, MEASUREMENT, OBSERVATION, PROCEDURE_OCCURRENCE,
                    VISIT_OCCURRENCE)
from utils import auth
from gcloud.bq import BigQueryClient
from analytics.cdr_ops.notebook_utils import execute, IMPERSONATION_SCOPES
pd.options.display.max_rows = 120

# + tags=["parameters"]
project_id = ""
deid_cdr = ""
combine = ""
reg_combine = ''
pipeline = ""
deid_sand = ""
run_as = ""

# +
impersonation_creds = auth.get_impersonation_credentials(
    run_as, target_scopes=IMPERSONATION_SCOPES)

client = BigQueryClient(project_id, credentials=impersonation_creds)
# -

# df will have a summary in the end
df = pd.DataFrame(columns=['query', 'result'])

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
query = JINJA_ENV.from_string("""

SELECT COUNT (*) AS n_row_not_pass
FROM `{{project_id}}.{{deid_cdr}}.observation`
WHERE
  observation_source_concept_id = 1585890
  AND value_as_concept_id NOT IN (2000000012,2000000010,903096)
""")
q = query.render(project_id=project_id, deid_cdr=deid_cdr)
df1 = execute(client, q)

if df1.loc[0].sum() == 0:
    df = df.append(
        {
            'query': 'Query1 observation_source_concept_id 1585890',
            'result': 'PASS'
        },
        ignore_index=True)
else:
    df = df.append(
        {
            'query': 'Query1 observation_source_concept_id 1585890',
            'result': 'Failure'
        },
        ignore_index=True)
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
query = JINJA_ENV.from_string("""

SELECT COUNT (*) AS n_row_not_pass
FROM `{{project_id}}.{{deid_cdr}}.observation`
WHERE
  observation_source_concept_id = 1333023
  AND value_as_concept_id NOT IN (2000000012,2000000010,903096)
""")
q = query.render(project_id=project_id, deid_cdr=deid_cdr)
df1 = execute(client, q)

if df1.loc[0].sum() == 0:
    df = df.append(
        {
            'query': 'Query2 observation_source_concept_id 1333023',
            'result': 'PASS'
        },
        ignore_index=True)
else:
    df = df.append(
        {
            'query': 'Query2 observation_source_concept_id 1333023',
            'result': 'Failure'
        },
        ignore_index=True)
df1
# -

query = JINJA_ENV.from_string("""

SELECT *
FROM `{{project_id}}.{{deid_cdr}}.observation`
WHERE
  observation_source_concept_id = 1333023
  AND value_as_concept_id NOT IN (2000000012,2000000010,903096)
""")
q = query.render(project_id=project_id, deid_cdr=deid_cdr)
df1 = execute(client, q)
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
query = JINJA_ENV.from_string("""
SELECT COUNT (*) AS n_row_not_pass
FROM  `{{project_id}}.{{deid_cdr}}.observation`
WHERE
  observation_source_concept_id = 1585889
  AND value_as_concept_id NOT IN (2000000013,2000000010,903096)
""")
q = query.render(project_id=project_id, deid_cdr=deid_cdr)
df1 = execute(client, q)

if df1.loc[0].sum() == 0:
    df = df.append(
        {
            'query': 'Query3 observation_source_concept_id 1585889',
            'result': 'PASS'
        },
        ignore_index=True)
else:
    df = df.append(
        {
            'query': 'Query3 observation_source_concept_id 1585889',
            'result': 'Failure'
        },
        ignore_index=True)
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
query = JINJA_ENV.from_string("""

SELECT COUNT (*) AS n_row_not_pass
FROM  `{{project_id}}.{{deid_cdr}}.observation`
WHERE
  observation_source_concept_id = 1333015
  AND value_as_concept_id NOT IN (2000000013,2000000010,903096)
""")
q = query.render(project_id=project_id, deid_cdr=deid_cdr)
df1 = execute(client, q)

if df1.loc[0].sum() == 0:
    df = df.append(
        {
            'query': 'Query4 observation_source_concept_id 1333015',
            'result': 'PASS'
        },
        ignore_index=True)
else:
    df = df.append(
        {
            'query': 'Query4 observation_source_concept_id 1333015',
            'result': 'Failure'
        },
        ignore_index=True)
df1
# -

# # Query5 State generalization cleaning rules
# `ConflictingHpoStateGeneralize` and `GeneralizeStateByPopulation` updates participants' state records in observation to
# value_source_concept_id = 2000000011 and value_as_concept_id = 2000000011 based on the criteria.
# Query5.1 and Query5.2 check if the state generalization is working as expected.
#
# Related tickets: DC-2377, DC-1614, DC-2782, DC-2785, DC-3504

# ## Query5.1 Generalize state info (2000000011) for participants who have EHR data from states other than the state they are currently living in.
# The CR `ConflictingHpoStateGeneralize` takes care of this criteria. Look at the CR's sandbox tables and related logics if this QC fails.

# +
query = JINJA_ENV.from_string("""

with participant_hpo_sites as (

{% for table in tables %}
SELECT DISTINCT deid.person_id, mask.value_source_concept_id
FROM `{{project_id}}.{{deid_cdr}}.{{table}}` deid
JOIN `{{project_id}}.{{deid_cdr}}.{{table}}_ext` ext ON deid.{{table}}_id = ext.{{table}}_id
JOIN `{{project_id}}.{{pipeline}}.site_maskings` mask ON ext.src_id = mask.src_id

{% if not loop.last -%} UNION DISTINCT {% endif %}

{% endfor %}
),

df2 as (

SELECT DISTINCT deid.person_id
FROM `{{project_id}}.{{deid_cdr}}.observation` deid
JOIN participant_hpo_sites hpo ON deid.person_id = hpo.person_id
WHERE deid.observation_source_concept_id = 1585249
AND deid.value_source_concept_id != hpo.value_source_concept_id
AND (deid.value_source_concept_id != 2000000011 OR 
     deid.value_as_concept_id != 2000000011
    )
)
   
select count (*) AS row_counts_failure_state_generalization from df2

""").render(project_id=project_id,
            deid_cdr=deid_cdr,
            reg_combine=reg_combine,
            deid_sand=deid_sand,
            pipeline=pipeline,
            tables=[
                CONDITION_OCCURRENCE, DEVICE_EXPOSURE, DRUG_EXPOSURE,
                MEASUREMENT, OBSERVATION, PROCEDURE_OCCURRENCE, VISIT_OCCURRENCE
            ])

df1 = execute(client, query)

if df1.loc[0].sum() == 0:
    df = df.append(
        {
            'query': 'Query5.1 state generalization to 2000000011',
            'result': 'PASS'
        },
        ignore_index=True)
else:
    df = df.append(
        {
            'query': 'Query5.1 state generalization to 2000000011',
            'result': 'Failure'
        },
        ignore_index=True)
df1

# -

# ## Query5.2 Generalize state info for participants where the identified number of participants living in the state without EHR records from a different state < 200 (the generalization threshold)”
# The CR `GeneralizeStateByPopulation` takes care of this criteria. Look at the CR's sandbox tables and related logics if this QC fails.

# +
query = JINJA_ENV.from_string(""" 
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
q = query.render(project_id=project_id,
                 deid_cdr=deid_cdr,
                 reg_combine=reg_combine)
df1 = execute(client, q)

if df1.loc[0].sum() == 0:
    df = df.append(
        {
            'query': 'Query5.2 state generalization if counts <200',
            'result': 'PASS'
        },
        ignore_index=True)
else:
    df = df.append(
        {
            'query': 'Query5.2 state generalization if counts <200',
            'result': 'Failure'
        },
        ignore_index=True)
df1

# -

# # Summary_deid_household AND state generalization


# +
def highlight_cells(val):
    color = 'red' if 'Failure' in val else 'white'
    return f'background-color: {color}'


df.style.applymap(highlight_cells).set_properties(**{'text-align': 'left'})
