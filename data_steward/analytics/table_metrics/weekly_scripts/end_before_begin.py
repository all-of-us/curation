# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:light
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.3.0
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

client = bigquery.Client()

# %load_ext google.cloud.bigquery

# %reload_ext google.cloud.bigquery

# +
from notebooks import parameters
DATASET = parameters.LATEST_DATASET

print("Dataset to use: {DATASET}".format(DATASET = DATASET))

# +
#######################################
print('Setting everything up...')
#######################################

import warnings

warnings.filterwarnings('ignore')
import pandas as pd
import matplotlib.pyplot as plt
# %matplotlib inline
import os


plt.style.use('ggplot')
pd.options.display.max_rows = 999
pd.options.display.max_columns = 999
pd.options.display.max_colwidth = 999


def cstr(s, color='black'):
    return "<text style=color:{}>{}</text>".format(color, s)


print('done.')
# -

cwd = os.getcwd()
cwd = str(cwd)
print("Current working directory is: {cwd}".format(cwd=cwd))

# +
dic = {
    'src_hpo_id': [
        "saou_uab_selma", "saou_uab_hunt", "saou_tul", "pitt_temple",
        "saou_lsu", "trans_am_meyers", "trans_am_essentia", "saou_ummc",
        "seec_miami", "seec_morehouse", "seec_emory", "uamc_banner", "pitt",
        "nyc_cu", "ipmc_uic", "trans_am_spectrum", "tach_hfhs", "nec_bmc",
        "cpmc_uci", "nec_phs", "nyc_cornell", "ipmc_nu", "nyc_hh",
        "ipmc_uchicago", "aouw_mcri", "syhc", "cpmc_ceders", "seec_ufl",
        "saou_uab", "trans_am_baylor", "cpmc_ucsd", "ecchc", "chci", "aouw_uwh",
        "cpmc_usc", "hrhc", "ipmc_northshore", "chs", "cpmc_ucsf", "jhchc",
        "aouw_mcw", "cpmc_ucd", "ipmc_rush", "va", "saou_umc"
    ],
    'HPO': [
        "UAB Selma", "UAB Huntsville", "Tulane University", "Temple University",
        "Louisiana State University",
        "Reliant Medical Group (Meyers Primary Care)",
        "Essentia Health Superior Clinic", "University of Mississippi",
        "SouthEast Enrollment Center Miami",
        "SouthEast Enrollment Center Morehouse",
        "SouthEast Enrollment Center Emory", "Banner Health",
        "University of Pittsburgh", "Columbia University Medical Center",
        "University of Illinois Chicago", "Spectrum Health",
        "Henry Ford Health System", "Boston Medical Center", "UC Irvine",
        "Partners HealthCare", "Weill Cornell Medical Center",
        "Northwestern Memorial Hospital", "Harlem Hospital",
        "University of Chicago", "Marshfield Clinic",
        "San Ysidro Health Center", "Cedars-Sinai", "University of Florida",
        "University of Alabama at Birmingham", "Baylor", "UC San Diego",
        "Eau Claire Cooperative Health Center", "Community Health Center, Inc.",
        "UW Health (University of Wisconsin Madison)",
        "University of Southern California", "HRHCare",
        "NorthShore University Health System", "Cherokee Health Systems",
        "UC San Francisco", "Jackson-Hinds CHC", "Medical College of Wisconsin",
        "UC Davis", "Rush University", 
        "United States Department of Veterans Affairs - Boston",
        "University Medical Center (UA Tuscaloosa)"
    ]
}

site_df = pd.DataFrame(data=dic)
site_df

# +
######################################
print('Getting the data from the database...')
######################################

site_map = pd.io.gbq.read_gbq('''
    select distinct * from (
    SELECT
            DISTINCT(src_hpo_id) as src_hpo_id
    FROM
         `{DATASET}._mapping_visit_occurrence`
         
    UNION ALL
    SELECT
            DISTINCT(src_hpo_id) as src_hpo_id
    FROM
         `{DATASET}._mapping_care_site`
         
    UNION ALL
    SELECT
            DISTINCT(src_hpo_id) as src_hpo_id
    FROM
         `{DATASET}._mapping_condition_occurrence`  
         
    UNION ALL
    SELECT
            DISTINCT(src_hpo_id) as src_hpo_id
    FROM
         `{DATASET}._mapping_device_exposure`

    UNION ALL
    SELECT
            DISTINCT(src_hpo_id) as src_hpo_id
    FROM
         `{DATASET}._mapping_drug_exposure`
         
    UNION ALL
    SELECT
            DISTINCT(src_hpo_id) as src_hpo_id
    FROM
         `{DATASET}._mapping_location`         
         
         
    UNION ALL
    SELECT
            DISTINCT(src_hpo_id) as src_hpo_id
    FROM
         `{DATASET}._mapping_measurement`         
         
         
    UNION ALL
    SELECT
            DISTINCT(src_hpo_id) as src_hpo_id
    FROM
         `{DATASET}._mapping_note`        
         
         
    UNION ALL
    SELECT
            DISTINCT(src_hpo_id) as src_hpo_id
    FROM
         `{DATASET}._mapping_observation`         
         
    UNION ALL
    SELECT
            DISTINCT(src_hpo_id) as src_hpo_id
    FROM
         `{DATASET}._mapping_person`         
         
    UNION ALL
    SELECT
            DISTINCT(src_hpo_id) as src_hpo_id
    FROM
         `{DATASET}._mapping_procedure_occurrence`         
         
         
    UNION ALL
    SELECT
            DISTINCT(src_hpo_id) as src_hpo_id
    FROM
         `{DATASET}._mapping_provider`
         
    UNION ALL
    SELECT
            DISTINCT(src_hpo_id) as src_hpo_id
    FROM
         `{DATASET}._mapping_specimen`
    
    UNION ALL
    SELECT
            DISTINCT(src_hpo_id) as src_hpo_id
    FROM
         `{DATASET}._mapping_visit_occurrence`   
    )     
    '''.format(DATASET=DATASET),
                              dialect='standard')
print(site_map.shape[0], 'records received.')
# -

site_df = pd.merge(site_map, site_df, how='outer', on='src_hpo_id')

site_df

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
       `{DATASET}.unioned_ehr_visit_occurrence` AS t1

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
       `{DATASET}.unioned_ehr_visit_occurrence` AS t1
    INNER JOIN
        (SELECT
            DISTINCT * 
        FROM
             `{DATASET}._mapping_visit_occurrence`)  AS t2
    ON
        t1.visit_occurrence_id=t2.visit_occurrence_id
    GROUP BY
        1
    ORDER BY
        3
    '''.format(DATASET=DATASET),
                                 dialect='standard')

print(temporal_df.shape[0], 'records received.')
# -

temporal_df['success_rate'] = 100 - round(
    100 * temporal_df['wrong_date_rows'] / temporal_df['total_rows'], 1)
temporal_df

visit_occurrence = temporal_df.rename(
    columns={"success_rate": "visit_occurrence"})
visit_occurrence = visit_occurrence[["src_hpo_id", "visit_occurrence"]]
visit_occurrence = visit_occurrence.fillna(100)
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
       `{DATASET}.unioned_ehr_condition_occurrence` AS t1
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
       `{DATASET}.unioned_ehr_condition_occurrence` AS t1
    INNER JOIN
        (SELECT
            DISTINCT * 
        FROM
             `{DATASET}._mapping_condition_occurrence`)  AS t2
    ON
        t1.condition_occurrence_id=t2.condition_occurrence_id
    GROUP BY
        1
    ORDER BY
        3
    '''.format(DATASET=DATASET),
                                 dialect='standard')
temporal_df.shape

print(temporal_df.shape[0], 'records received.')
# -

temporal_df['success_rate'] = 100 - round(
    100 * temporal_df['wrong_date_rows'] / temporal_df['total_rows'], 1)
temporal_df

condition_occurrence = temporal_df.rename(
    columns={"success_rate": "condition_occurrence"})
condition_occurrence = condition_occurrence[[
    "src_hpo_id", "condition_occurrence"
]]
condition_occurrence = condition_occurrence.fillna(100)
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
       `{DATASET}.unioned_ehr_drug_exposure` AS t1
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
       `{DATASET}.unioned_ehr_drug_exposure` AS t1
    INNER JOIN
        (SELECT
            DISTINCT * 
        FROM
             `{DATASET}._mapping_drug_exposure`)  AS t2
    ON
        t1.drug_exposure_id=t2.drug_exposure_id
    GROUP BY
        1
    '''.format(DATASET=DATASET),
                                 dialect='standard')
temporal_df.shape

print(temporal_df.shape[0], 'records received.')
# -

temporal_df['success_rate'] = 100 - round(
    100 * temporal_df['wrong_date_rows'] / temporal_df['total_rows'], 1)
temporal_df

drug_exposure = temporal_df.rename(columns={"success_rate": "drug_exposure"})
drug_exposure = drug_exposure[["src_hpo_id", "drug_exposure"]]
drug_exposure = drug_exposure.fillna(100)
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
success_rate = success_rate.fillna(100)
success_rate
# -

success_rate.to_csv("{cwd}/end_before_begin.csv".format(cwd = cwd))
