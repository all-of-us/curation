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

# ### NOTE: This notebook only looks at the following three tables:
# - Visit Occurrence, Condition Occurrence, and Measurement
#
# ### The following three tables are excluded:
# - Observation, Procedure Occurrence, Measurement
#
# ### The aforementioned three tables are excluded because there is neither a "start" nor an "end" date fields in this table. There is only a single "date" and "datetime" field. This prevents the 'end date' from preceding the 'start date' since neither exists.

from google.cloud import bigquery
# %reload_ext google.cloud.bigquery
client = bigquery.Client()
# %load_ext google.cloud.bigquery

# +
from notebooks import parameters
DATASET = parameters.UNIONED_Q4_2018
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

site_df = full_names_df
# -

# # All temporal data points should be consistent such that end dates should NOT be before a start date.

# ## Visit Occurrence Table

# +
######################################
print('Getting the data from the database...')
######################################

temporal_df = pd.io.gbq.read_gbq('''
    SELECT
        COUNT(*) AS total,
        sum(case when (t1.visit_start_date>t1.visit_end_date) then 1 else 0 end) as wrong_date
    FROM
       `{DATASET}.visit_occurrence` AS t1

    '''.format(DATASET=DATASET),
                                 dialect='standard')
temporal_df.shape

print(temporal_df.shape[0], 'records received.')
# -

temporal_df

# ### Visit Occurrence Table By Site

# +
######################################
print('Getting the data from the database...')
######################################

temporal_df = pd.io.gbq.read_gbq('''
    SELECT
        src_hpo_id,
        COUNT(*) AS total_rows,
        sum(case when (t1.visit_start_date>t1.visit_end_date) then 1 else 0 end) as wrong_date_rows
    FROM
       `{DATASET}.visit_occurrence` AS t1
    INNER JOIN
        (SELECT
            DISTINCT * 
        FROM
             `{DATASET}._mapping_visit_occurrence`)  AS t2
    ON
        t1.visit_occurrence_id=t2.visit_occurrence_id
    WHERE
        LOWER(src_hpo_id) NOT LIKE '%rdr%'
    GROUP BY
        1
    ORDER BY
        3
    '''.format(DATASET=DATASET),
                                 dialect='standard')

print(temporal_df.shape[0], 'records received.')
# -

temporal_df['failure_rate'] = round(
    100 * temporal_df['wrong_date_rows'] / temporal_df['total_rows'], 1)
temporal_df

visit_occurrence = temporal_df.rename(
    columns={"failure_rate": "visit_occurrence"})
visit_occurrence = visit_occurrence[["src_hpo_id", "visit_occurrence"]]
visit_occurrence = visit_occurrence.fillna(0)
visit_occurrence

total_wrong = temporal_df['wrong_date_rows'].sum()
total_wrong

total_row = temporal_df['total_rows'].sum()
percent = round(100 - 100 * (total_wrong / (total_row)), 1)
percent

# ## Condition Occurrence Table

# +
######################################
print('Getting the data from the database...')
######################################

temporal_df = pd.io.gbq.read_gbq('''
    SELECT
        COUNT(*) AS total,
        sum(case when (t1.condition_start_date>t1.condition_end_date) then 1 else 0 end) as wrong_date
    FROM
       `{DATASET}.condition_occurrence` AS t1
    '''.format(DATASET=DATASET),
                                 dialect='standard')
temporal_df.shape

print(temporal_df.shape[0], 'records received.')
# -

temporal_df

# +
# print("success rate for condition_occurrence is: ",round(100-100*(temporal_df.iloc[0,1]/temporal_df.iloc[0,0]),1))
# -

# ### Condition Occurrence Table By Site

# +
######################################
print('Getting the data from the database...')
######################################

temporal_df = pd.io.gbq.read_gbq('''
    SELECT
        src_hpo_id,
        COUNT(*) AS total_rows,
        sum(case when (t1.condition_start_date>t1.condition_end_date) then 1 else 0 end) as wrong_date_rows
    FROM
       `{DATASET}.condition_occurrence` AS t1
    INNER JOIN
        (SELECT
            DISTINCT * 
        FROM
             `{DATASET}._mapping_condition_occurrence`)  AS t2
    ON
        t1.condition_occurrence_id=t2.condition_occurrence_id
    WHERE
        LOWER(src_hpo_id) NOT LIKE '%rdr%'
    GROUP BY
        1
    ORDER BY
        3
    '''.format(DATASET=DATASET),
                                 dialect='standard')
temporal_df.shape

print(temporal_df.shape[0], 'records received.')
# -

temporal_df['failure_rate'] = round(
    100 * temporal_df['wrong_date_rows'] / temporal_df['total_rows'], 1)
temporal_df

condition_occurrence = temporal_df.rename(
    columns={"failure_rate": "condition_occurrence"})
condition_occurrence = condition_occurrence[[
    "src_hpo_id", "condition_occurrence"
]]
condition_occurrence = condition_occurrence.fillna(0)
condition_occurrence

total_wrong = temporal_df['wrong_date_rows'].sum()
total_wrong

total_row = temporal_df['total_rows'].sum()
percent = round(100 - 100 * (total_wrong / (total_row)), 1)
percent

# ## Drug Exposure Table

# +
######################################
print('Getting the data from the database...')
######################################

temporal_df = pd.io.gbq.read_gbq('''
    SELECT
        COUNT(*) AS total,
        sum(case when (t1.drug_exposure_start_date>t1.drug_exposure_end_date) then 1 else 0 end) as wrong_date
    FROM
       `{DATASET}.drug_exposure` AS t1
    '''.format(DATASET=DATASET),
                                 dialect='standard')
temporal_df.shape

print(temporal_df.shape[0], 'records received.')
# -

temporal_df

# +
# print("success rate for drug_exposure is: ",round(100-100*(temporal_df.iloc[0,1]/temporal_df.iloc[0,0]),1))
# -

# ### Drug Exposure Table By Site

# +
######################################
print('Getting the data from the database...')
######################################

temporal_df = pd.io.gbq.read_gbq('''
    SELECT
        src_hpo_id,
        COUNT(*) AS total_rows,
        sum(case when (t1.drug_exposure_start_date>t1.drug_exposure_end_date) then 1 else 0 end) as wrong_date_rows
    FROM
       `{DATASET}.drug_exposure` AS t1
    INNER JOIN
        (SELECT
            DISTINCT * 
        FROM
             `{DATASET}._mapping_drug_exposure`)  AS t2
    ON
        t1.drug_exposure_id=t2.drug_exposure_id
    WHERE
        LOWER(src_hpo_id) NOT LIKE '%rdr%'
    GROUP BY
        1
    '''.format(DATASET=DATASET),
                                 dialect='standard')
temporal_df.shape

print(temporal_df.shape[0], 'records received.')
# -

temporal_df['failure_rate'] = round(
    100 * temporal_df['wrong_date_rows'] / temporal_df['total_rows'], 1)
temporal_df

drug_exposure = temporal_df.rename(columns={"failure_rate": "drug_exposure"})
drug_exposure = drug_exposure[["src_hpo_id", "drug_exposure"]]
drug_exposure = drug_exposure.fillna(0)
drug_exposure

total_wrong = temporal_df['wrong_date_rows'].sum()
total_wrong

total_row = temporal_df['total_rows'].sum()
percent = round(100 - 100 * (total_wrong / (total_row)), 1)
percent

# ## Temporal Data Points - End Dates Before Start Dates

# +

success_rate = pd.merge(visit_occurrence,
                       condition_occurrence,
                       how='outer',
                       on='src_hpo_id')
success_rate = pd.merge(success_rate, drug_exposure, how='outer', on='src_hpo_id')



success_rate = pd.merge(success_rate, site_df, how='outer', on='src_hpo_id')
success_rate = success_rate.fillna(0)
success_rate
# -

success_rate.to_csv("{cwd}/end_before_begin.csv".format(cwd = cwd))
