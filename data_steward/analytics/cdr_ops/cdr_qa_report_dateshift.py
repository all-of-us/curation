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

# + papermill={"duration": 0.023643, "end_time": "2021-02-02T22:30:31.880820", "exception": false, "start_time": "2021-02-02T22:30:31.857177", "status": "completed"} tags=["parameters"]
project_id = ""
com_cdr = ""
new_cdr = ""
#dataset_id=""

# + papermill={"duration": 0.046903, "end_time": "2021-02-02T22:30:31.927723", "exception": false, "start_time": "2021-02-02T22:30:31.880820", "status": "completed"} tags=["injected-parameters"]
# Parameters
#project_id = "aou-res-curation-prod"
#com_cdr = "2020q4r1_combined_release"
#new_cdr = "R2020q4r1_deid"


# + [markdown] papermill={"duration": 0.024011, "end_time": "2021-02-02T22:30:31.951734", "exception": false, "start_time": "2021-02-02T22:30:31.927723", "status": "completed"} tags=[]
# #  QA queries on new CDR dateshift
#
# Quality checks performed on a new CDR dataset using QA queries

# + papermill={"duration": 0.709639, "end_time": "2021-02-02T22:30:32.661373", "exception": false, "start_time": "2021-02-02T22:30:31.951734", "status": "completed"} tags=[]
import urllib
import pandas as pd

pd.options.display.max_rows = 120

# + [markdown] papermill={"duration": 0.02327, "end_time": "2021-02-02T22:30:32.708257", "exception": false, "start_time": "2021-02-02T22:30:32.684987", "status": "completed"} tags=[]
# # QA query 1

# + papermill={"duration": 4.105203, "end_time": "2021-02-02T22:30:36.813460", "exception": false, "start_time": "2021-02-02T22:30:32.708257", "status": "completed"} tags=[]
#newcdr='2020q4r1_combined_release'
query = f'''

SELECT

d.observation_id AS observation_id,

i.observation_id AS observation_deid,

m.research_id AS person_id,

m.person_id AS research_id,

d.observation_date AS date_d,

i.observation_date AS date_i,

m.shift

FROM

`{project_id}.pipeline_tables.pid_rid_mapping` m

JOIN

`{project_id}.{com_cdr}.observation` i

ON

m.person_id = i.person_id

JOIN

`{project_id}.{new_cdr}.observation` d

ON

d.observation_id = i.observation_id


limit 5


'''
pd.read_gbq(query, dialect='standard')

# + [markdown] papermill={"duration": 0.023633, "end_time": "2021-02-02T22:30:36.860798", "exception": false, "start_time": "2021-02-02T22:30:36.837165", "status": "completed"} tags=[]
# ##  QA  query 2

# + papermill={"duration": 2.136748, "end_time": "2021-02-02T22:30:39.044867", "exception": false, "start_time": "2021-02-02T22:30:36.908119", "status": "completed"} tags=[]
query = f'''




SELECT

d.observation_id AS observation_id,

d.observation_source_value AS observation_source_value,

i.observation_id AS observation_deid,

m.research_id AS person_id,

m.person_id AS research_id,

d.observation_date AS date_d,

i.observation_date AS date_i,

m.shift

FROM

`{project_id}.pipeline_tables.pid_rid_mapping` m

JOIN

`{project_id}.{com_cdr}.observation` i

ON

m.person_id = i.person_id

JOIN

`{project_id}.{new_cdr}.observation` d

ON

d.observation_id = i.observation_id

Where d.observation_source_value LIKE 'PIIBirthInformation_BirthDate'

limit 5

'''
pd.read_gbq(query, dialect='standard')
# -

# # query 3

# + papermill={"duration": 2.136748, "end_time": "2021-02-02T22:30:39.044867", "exception": false, "start_time": "2021-02-02T22:30:36.908119", "status": "completed"} tags=[]
query = f'''

 SELECT

d.observation_period_id AS observation_period_id,

m.research_id AS person_id,

m.person_id AS research_id,

d.observation_period_start_date AS date_d,

i.observation_period_start_date  AS date_i,

m.shift,

FROM

`{project_id}.pipeline_tables.pid_rid_mapping` m

JOIN

`{project_id}.{com_cdr}.observation_period` i

ON

m.person_id = i.person_id

JOIN

`{project_id}.{new_cdr}.observation_period` d

ON

d.observation_period_id = i.observation_period_id

limit 5

'''
pd.read_gbq(query, dialect='standard')

# + [markdown] papermill={"duration": 0.023649, "end_time": "2021-02-02T22:30:39.115495", "exception": false, "start_time": "2021-02-02T22:30:39.091846", "status": "completed"} tags=[]
# # query 4

# + papermill={"duration": 2.338821, "end_time": "2021-02-02T22:30:41.501415", "exception": false, "start_time": "2021-02-02T22:30:39.162594", "status": "completed"} tags=[]
query = f'''

 SELECT

m.person_id AS research_id,

i.person_id AS person_I,

m.research_id AS person_id,

d.person_id AS person_D,

d.birth_datetime AS date_d,

i.birth_datetime  AS date_i,

m.shift

FROM

`{project_id}.pipeline_tables.pid_rid_mapping` m

JOIN

`{project_id}.{com_cdr}.person` i

ON

m.person_id = i.person_id

JOIN

`{project_id}.{new_cdr}.person` d

ON

d.person_id = m.research_id


limit 5
  


'''
pd.read_gbq(query, dialect='standard')
# -

# # query 5

# + papermill={"duration": 2.338821, "end_time": "2021-02-02T22:30:41.501415", "exception": false, "start_time": "2021-02-02T22:30:39.162594", "status": "completed"} tags=[]
query = f'''

 SELECT

d.specimen_id AS specimen_id,

m.person_id AS research_id,

i.person_id AS person_I,

m.research_id AS person_id,

d.person_id AS person_D,

d.specimen_date AS date_d,

i.specimen_date  AS date_i,

m.shift

FROM


`{project_id}.pipeline_tables.pid_rid_mapping` m

JOIN

`{project_id}.{com_cdr}.specimen` i

ON

m.person_id = i.person_id

JOIN

`{project_id}.{new_cdr}.specimen` d

ON

d.person_id = m.research_id


limit 5
  


'''
pd.read_gbq(query, dialect='standard')
# -

# # query6 

query = f'''

 SELECT

m.person_id AS research_id,

i.person_id AS person_I,

m.research_id AS person_id,

d.person_id AS person_D,

d.death_date AS date_d,

i.death_date  AS date_i,

m.shift

FROM

`{project_id}.pipeline_tables.pid_rid_mapping` m

JOIN

`{project_id}.{com_cdr}.death` i

ON

m.person_id = i.person_id

JOIN

`{project_id}.{new_cdr}.death` d

ON

m.research_id = d.person_id 

 limit 5

  


'''
pd.read_gbq(query, dialect='standard')

# + [markdown] papermill={"duration": 0.023411, "end_time": "2021-02-02T22:30:39.091846", "exception": false, "start_time": "2021-02-02T22:30:39.068435", "status": "completed"} tags=[]
# # query 7 
# -

query = f'''

 SELECT

d.visit_occurrence_id AS visit_occurrence_id,

m.research_id AS person_id,

d.person_id AS person_D,

m.person_id AS research_id,

i.person_id AS person_I,

d.visit_start_date AS  visit_start_date_d,

i.visit_start_date  AS visit_start_date_i,

m.shift

FROM


`{project_id}.pipeline_tables.pid_rid_mapping` m

JOIN

`{project_id}.{com_cdr}.visit_occurrence` i

ON

m.person_id = i.person_id

JOIN

`{project_id}.{new_cdr}.visit_occurrence` d

ON

d.visit_occurrence_id = i.visit_occurrence_id
limit 5

  


'''
pd.read_gbq(query, dialect='standard')

# # query 8
#

query = f'''

 SELECT

m.person_id AS research_id,

i.person_id AS person_I,

m.research_id AS person_id,

d.person_id AS person_D,

d.procedure_date AS procedure_date_d,

i.procedure_date  AS procedure_date_i,

m.shift

FROM


`{project_id}.pipeline_tables.pid_rid_mapping` m

JOIN

`{project_id}.{com_cdr}.procedure_occurrence` i

ON

m.person_id = i.person_id

JOIN

`{project_id}.{new_cdr}.procedure_occurrence` d

ON

d.person_id = m.research_id


limit 5
  


'''
pd.read_gbq(query, dialect='standard')

# # query 9

query = f'''

SELECT

m.person_id AS research_id,

i.person_id AS person_I,

m.research_id AS person_id,

d.person_id AS person_D,

d.drug_exposure_start_date AS date_d,

i.drug_exposure_start_date AS date_i,

m.shift

FROM

`{project_id}.pipeline_tables.pid_rid_mapping` m

JOIN

`{project_id}.{com_cdr}.drug_exposure` i

ON

m.person_id = i.person_id

JOIN

`{project_id}.{new_cdr}.drug_exposure` d

ON

m.research_id = d.person_id

 limit 5


'''
pd.read_gbq(query, dialect='standard')


# # query 10

query = f'''

SELECT

d.device_exposure_id AS device_exposure_id,

  m.person_id AS research_id,

  i.person_id AS person_I,

  m.research_id AS person_id,

  d.person_id AS person_D,

  d.device_exposure_start_date AS date_d,

  i.device_exposure_start_date AS date_i,

  m.shift

FROM

  `{project_id}.pipeline_tables.pid_rid_mapping` m

JOIN

  `{project_id}.{com_cdr}.device_exposure` i

ON

  m.person_id = i.person_id

JOIN

  `{project_id}.{new_cdr}.device_exposure` d

ON

  i.device_exposure_id = d.device_exposure_id
  limit 5
  


'''
pd.read_gbq(query, dialect='standard')

# # query 11

query = f'''
SELECT

d.condition_occurrence_id AS condition_occurrence_id,

m.person_id AS research_id,

i.person_id AS person_I,

m.research_id AS person_id,

d.person_id AS person_D,

d.condition_start_date  AS  date_d,

i.condition_start_date  AS date_i,

m.shift

FROM

`{project_id}.pipeline_tables.pid_rid_mapping` m

JOIN

`{project_id}.{com_cdr}.condition_occurrence` i

ON

m.person_id = i.person_id

JOIN

`{project_id}.{new_cdr}.condition_occurrence` d

ON

  i.condition_occurrence_id = d.condition_occurrence_id


limit 5
  


'''
pd.read_gbq(query, dialect='standard')

# # query 12

query = f'''
SELECT

m.research_id AS person_id,

d.person_id AS person_D,

m.person_id AS research_id,

i.person_id AS person_I,

d.measurement_date AS date_d,

i.measurement_date AS date_i,

m.shift

FROM

`{project_id}.pipeline_tables.pid_rid_mapping` m

JOIN

`{project_id}.{com_cdr}.measurement` i

ON

m.person_id = i.person_id

JOIN

`{project_id}.{new_cdr}.measurement` d

ON

d.person_id = m.research_id


limit 5
  


'''
pd.read_gbq(query, dialect='standard')

# # query13 question, 
# is this to double check specific person_id?

query = f'''
SELECT

  person_id,

  shift

FROM

  `{project_id}.pipeline_tables.pid_rid_mapping`

WHERE

  person_id = 1225138
  limit 5


'''
pd.read_gbq(query, dialect='standard')

# # query 14 error
# no this '2020q4r1_combined_release_deid' dataset?
# changed to 2020q4r1_combined_release' and it worked
#
#
# no this 'combined20190912'
# changed to combined20191004_clean' and it worked

query = f'''


SELECT 

id.person_id as participant_id,

deid.person_id as research_id



FROM 

`aou-res-curation-prod.2020q4r1_combined_release.observation` deid

JOIN

`aou-res-curation-prod.combined20191004_clean.observation` id using (observation_id)


limit 5
  


'''
pd.read_gbq(query, dialect='standard')

# # done


