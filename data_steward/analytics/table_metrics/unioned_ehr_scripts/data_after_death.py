# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:light
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.4.2
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

from google.cloud import bigquery
# %reload_ext google.cloud.bigquery
client = bigquery.Client()
# %load_ext google.cloud.bigquery

# +
from notebooks import parameters
DATASET = parameters.EHR_OPS_Q1_2019
LOOKUP_TABLES = parameters.LOOKUP_TABLES

print(f"Dataset to use: {DATASET}")
print(f"Lookup tables: {LOOKUP_TABLES}")

# +
import warnings

warnings.filterwarnings('ignore')
import pandas as pd
import matplotlib.pyplot as plt
import os

plt.style.use('ggplot')
pd.options.display.max_rows = 999
pd.options.display.max_columns = 999
pd.options.display.max_colwidth = 999


def cstr(s, color='black'):
    return "<text style=color:{}>{}</text>".format(color, s)


# -

cwd = os.getcwd()
cwd = str(cwd)
print("Current working directory is: {cwd}".format(cwd=cwd))

# ### Get the list of HPO IDs
#
# ### NOTE: This assumes that all of the relevant HPOs have a person table.

hpo_id_query = f"""
SELECT REPLACE(table_id, '_person', '') AS src_hpo_id
FROM
`{DATASET}.__TABLES__`
WHERE table_id LIKE '%person' 
AND table_id 
NOT LIKE '%unioned_ehr_%' 
AND table_id NOT LIKE '\\\_%'
"""

site_df = pd.io.gbq.read_gbq(hpo_id_query, dialect='standard')

get_full_names = f"""
select * from {LOOKUP_TABLES}
"""

full_names_df = pd.io.gbq.read_gbq(get_full_names, dialect='standard')

# +
full_names_df.columns = ['org_id', 'src_hpo_id', 'site_name', 'display_order']
columns_to_use = ['src_hpo_id', 'site_name']

full_names_df = full_names_df[columns_to_use]
full_names_df['src_hpo_id'] = full_names_df['src_hpo_id'].str.lower()

# +
cols_to_join = ['src_hpo_id']

site_df = pd.merge(site_df, full_names_df, on=['src_hpo_id'], how='left')
# -

# # No data point exists beyond 30 days of the death date. (Achilles rule_id #3)

# ## Visit Occurrence Table

# +
######################################
print('Getting the data from the database...')
######################################

temporal_df = pd.io.gbq.read_gbq('''
    SELECT
        src_hpo_id,
        COUNT(*) AS total,
        sum(case when (DATE_DIFF(visit_start_date, death_date, DAY)>30) then 1 else 0 end) as wrong_death_date
    FROM
       `{DATASET}.unioned_ehr_visit_occurrence` AS t1
    INNER JOIN
        `{DATASET}.unioned_ehr_death` AS t2
        ON
            t1.person_id=t2.person_id
    INNER JOIN
        (SELECT
            DISTINCT * 
        FROM
             `{DATASET}._mapping_visit_occurrence`)  AS t3
    ON
        t1.visit_occurrence_id=t3.visit_occurrence_id
    GROUP BY
        1
    '''.format(DATASET=DATASET), dialect='standard')
temporal_df.shape

print(temporal_df.shape[0], 'records received.')
# -

temporal_df['failure_rate'] = round(
    100 * temporal_df['wrong_death_date'] / temporal_df['total'], 1)
temporal_df

# - main reason death date entered as default value ("1890")

visit_occurrence = temporal_df.rename(
    columns={"failure_rate": "visit_occurrence"})
visit_occurrence = visit_occurrence[["src_hpo_id", "visit_occurrence"]]
visit_occurrence = visit_occurrence.fillna(0)
visit_occurrence

# ## Condition Occurrence Table

# +
######################################
print('Getting the data from the database...')
######################################

temporal_df = pd.io.gbq.read_gbq('''
    SELECT
        src_hpo_id,
        COUNT(*) AS total,
        sum(case when (DATE_DIFF(condition_start_date, death_date, DAY)>30) then 1 else 0 end) as wrong_death_date
    FROM
       `{DATASET}.unioned_ehr_condition_occurrence` AS t1
    INNER JOIN
        `{DATASET}.unioned_ehr_death` AS t2
        ON
            t1.person_id=t2.person_id
    INNER JOIN
        (SELECT
            DISTINCT * 
        FROM
             `{DATASET}._mapping_condition_occurrence`)  AS t3
    ON
        t1.condition_occurrence_id=t3.condition_occurrence_id
    GROUP BY
        1
    '''.format(DATASET=DATASET),
                                 dialect='standard')
temporal_df.shape

print(temporal_df.shape[0], 'records received.')
# -

temporal_df['failure_rate'] = round(
    100 * temporal_df['wrong_death_date'] / temporal_df['total'], 1)
temporal_df

condition_occurrence = temporal_df.rename(
    columns={"failure_rate": "condition_occurrence"})
condition_occurrence = condition_occurrence[[
    "src_hpo_id", "condition_occurrence"
]]
condition_occurrence = condition_occurrence.fillna(0)
condition_occurrence

# ## Drug Exposure Table

# +
######################################
print('Getting the data from the database...')
######################################

temporal_df = pd.io.gbq.read_gbq('''
    SELECT
        src_hpo_id,
        COUNT(*) AS total,
        sum(case when (DATE_DIFF(drug_exposure_start_date, death_date, DAY)>30) then 1 else 0 end) as wrong_death_date
    FROM
       `{DATASET}.unioned_ehr_drug_exposure` AS t1
    INNER JOIN
        `{DATASET}.unioned_ehr_death` AS t2
        ON
            t1.person_id=t2.person_id
    INNER JOIN
        (SELECT
            DISTINCT * 
        FROM
             `{DATASET}._mapping_drug_exposure`)  AS t3
    ON
        t1.drug_exposure_id=t3.drug_exposure_id
    GROUP BY
        1
    '''.format(DATASET=DATASET), dialect='standard')
temporal_df.shape

print(temporal_df.shape[0], 'records received.')
# -

temporal_df['failure_rate'] = round(
    100 * temporal_df['wrong_death_date'] / temporal_df['total'], 1)
temporal_df

drug_exposure = temporal_df.rename(columns={"failure_rate": "drug_exposure"})
drug_exposure = drug_exposure[["src_hpo_id", "drug_exposure"]]
drug_exposure = drug_exposure.fillna(0)
drug_exposure

# ## Measurement Table

# +
######################################
print('Getting the data from the database...')
######################################

temporal_df = pd.io.gbq.read_gbq('''
    SELECT
        src_hpo_id,
        COUNT(*) AS total,
        sum(case when (DATE_DIFF(measurement_date, death_date, DAY)>30) then 1 else 0 end) as wrong_death_date
    FROM
       `{DATASET}.unioned_ehr_measurement` AS t1
    INNER JOIN
        `{DATASET}.unioned_ehr_death` AS t2
        ON
            t1.person_id=t2.person_id
    INNER JOIN
        (SELECT
            DISTINCT * 
        FROM
             `{DATASET}._mapping_measurement`)  AS t3
    ON
        t1.measurement_id=t3.measurement_id
    GROUP BY
        1
    '''.format(DATASET=DATASET),dialect='standard')
temporal_df.shape

print(temporal_df.shape[0], 'records received.')
# -

temporal_df['failure_rate'] =  round(
    100 * temporal_df['wrong_death_date'] / temporal_df['total'], 1)
temporal_df

measurement = temporal_df.rename(columns={"failure_rate": "measurement"})
measurement = measurement[["src_hpo_id", "measurement"]]
measurement = measurement.fillna(0)
measurement

# ## Procedure Occurrence Table

# +
######################################
print('Getting the data from the database...')
######################################

temporal_df = pd.io.gbq.read_gbq('''
    SELECT
        src_hpo_id,
        COUNT(*) AS total,
        sum(case when (DATE_DIFF(procedure_date, death_date, DAY)>30) then 1 else 0 end) as wrong_death_date
    FROM
       `{DATASET}.unioned_ehr_procedure_occurrence` AS t1
    INNER JOIN
        `{DATASET}.unioned_ehr_death` AS t2
        ON
            t1.person_id=t2.person_id
    INNER JOIN
        (SELECT
            DISTINCT * 
        FROM
             `{DATASET}._mapping_procedure_occurrence`)  AS t3
    ON
        t1.procedure_occurrence_id=t3.procedure_occurrence_id
    GROUP BY
        1
    '''.format(DATASET=DATASET), dialect='standard')
temporal_df.shape

print(temporal_df.shape[0], 'records received.')
# -

temporal_df['failure_rate'] = round(
    100 * temporal_df['wrong_death_date'] / temporal_df['total'], 1)
temporal_df

procedure_occurrence = temporal_df.rename(
    columns={"failure_rate": "procedure_occurrence"})
procedure_occurrence = procedure_occurrence[[
    "src_hpo_id", "procedure_occurrence"
]]
procedure_occurrence = procedure_occurrence.fillna(0)
procedure_occurrence

# ## Observation Table

# +
######################################
print('Getting the data from the database...')
######################################

temporal_df = pd.io.gbq.read_gbq('''
    SELECT
        src_hpo_id,
        COUNT(*) AS total,
        sum(case when (DATE_DIFF(observation_date, death_date, DAY)>30) then 1 else 0 end) as wrong_death_date
    FROM
       `{DATASET}.unioned_ehr_observation` AS t1
    INNER JOIN
        `{DATASET}.unioned_ehr_death` AS t2
        ON
            t1.person_id=t2.person_id
    INNER JOIN
        (SELECT
            DISTINCT * 
        FROM
             `{DATASET}._mapping_observation`)  AS t3
    ON
        t1.observation_id=t3.observation_id
    GROUP BY
        1
    '''.format(DATASET=DATASET), dialect='standard')

temporal_df.shape

print(temporal_df.shape[0], 'records received.')
# -

temporal_df['failure_rate'] = round(
    100 * temporal_df['wrong_death_date'] / temporal_df['total'], 1)
temporal_df

observation = temporal_df.rename(columns={"failure_rate": "observation"})

observation = observation[["src_hpo_id", "observation"]]
observation = observation.fillna(0)
observation

# ## 4. Success Rate Temporal Data Points - Data After Death Date

datas = [
    condition_occurrence, drug_exposure, measurement, procedure_occurrence,
    observation]

master_df = visit_occurrence

for filename in datas:
    master_df = pd.merge(master_df, filename, on='src_hpo_id', how='outer')

master_df

success_rate = pd.merge(master_df, site_df, how='outer', on='src_hpo_id')
success_rate = success_rate.fillna(0)

success_rate

success_rate.to_csv("{cwd}/data_after_death.csv".format(cwd = cwd))


