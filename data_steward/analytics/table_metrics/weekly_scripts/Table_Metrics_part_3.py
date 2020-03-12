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

from google.cloud import bigquery

client = bigquery.Client()

# %load_ext google.cloud.bigquery

# %reload_ext google.cloud.bigquery

# +
#######################################
print('Setting everything up...')
#######################################

import warnings

warnings.filterwarnings('ignore')
import pandas_gbq
import pandas as pd
import numpy as np
import matplotlib
import matplotlib.pyplot as plt
import matplotlib.lines as mlines
from matplotlib.lines import Line2D

import matplotlib.ticker as ticker
import matplotlib.cm as cm
import matplotlib as mpl

import matplotlib.pyplot as plt
# %matplotlib inline

import os
import sys
from datetime import datetime
from datetime import date
from datetime import time
from datetime import timedelta
import time
import math

DATASET = ''

plt.style.use('ggplot')
pd.options.display.max_rows = 999
pd.options.display.max_columns = 999
pd.options.display.max_colwidth = 999

from IPython.display import HTML as html_print


def cstr(s, color='black'):
    return "<text style=color:{}>{}</text>".format(color, s)


print('done.')
# -

cwd = os.getcwd()
cwd = str(cwd)
print(cwd)

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
         `{}._mapping_visit_occurrence`
         
    UNION ALL
    SELECT
            DISTINCT(src_hpo_id) as src_hpo_id
    FROM
         `{}._mapping_care_site`
         
    UNION ALL
    SELECT
            DISTINCT(src_hpo_id) as src_hpo_id
    FROM
         `{}._mapping_condition_occurrence`  
         
    UNION ALL
    SELECT
            DISTINCT(src_hpo_id) as src_hpo_id
    FROM
         `{}._mapping_device_exposure`

    UNION ALL
    SELECT
            DISTINCT(src_hpo_id) as src_hpo_id
    FROM
         `{}._mapping_drug_exposure`
         
    UNION ALL
    SELECT
            DISTINCT(src_hpo_id) as src_hpo_id
    FROM
         `{}._mapping_location`         
         
         
    UNION ALL
    SELECT
            DISTINCT(src_hpo_id) as src_hpo_id
    FROM
         `{}._mapping_measurement`         
         
         
    UNION ALL
    SELECT
            DISTINCT(src_hpo_id) as src_hpo_id
    FROM
         `{}._mapping_note`        
         
         
    UNION ALL
    SELECT
            DISTINCT(src_hpo_id) as src_hpo_id
    FROM
         `{}._mapping_observation`         
         
    UNION ALL
    SELECT
            DISTINCT(src_hpo_id) as src_hpo_id
    FROM
         `{}._mapping_person`         
         
    UNION ALL
    SELECT
            DISTINCT(src_hpo_id) as src_hpo_id
    FROM
         `{}._mapping_procedure_occurrence`         
         
         
    UNION ALL
    SELECT
            DISTINCT(src_hpo_id) as src_hpo_id
    FROM
         `{}._mapping_provider`
         
    UNION ALL
    SELECT
            DISTINCT(src_hpo_id) as src_hpo_id
    FROM
         `{}._mapping_specimen`
    
    UNION ALL
    SELECT
            DISTINCT(src_hpo_id) as src_hpo_id
    FROM
         `{}._mapping_visit_occurrence`   
    )     
    '''.format(DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET,
               DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET,
               DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET,
               DATASET, DATASET, DATASET, DATASET, DATASET),
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
       `{}.unioned_ehr_visit_occurrence` AS t1

    '''.format(DATASET, DATASET, DATASET, DATASET, DATASET, DATASET),
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
       `{}.unioned_ehr_visit_occurrence` AS t1
    INNER JOIN
        (SELECT
            DISTINCT * 
        FROM
             `{}._mapping_visit_occurrence`)  AS t2
    ON
        t1.visit_occurrence_id=t2.visit_occurrence_id
    GROUP BY
        1
    ORDER BY
        3
    '''.format(DATASET, DATASET, DATASET, DATASET, DATASET, DATASET),
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
       `{}.unioned_ehr_condition_occurrence` AS t1
    '''.format(DATASET, DATASET, DATASET, DATASET, DATASET, DATASET),
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
       `{}.unioned_ehr_condition_occurrence` AS t1
    INNER JOIN
        (SELECT
            DISTINCT * 
        FROM
             `{}._mapping_condition_occurrence`)  AS t2
    ON
        t1.condition_occurrence_id=t2.condition_occurrence_id
    GROUP BY
        1
    ORDER BY
        3
    '''.format(DATASET, DATASET, DATASET, DATASET, DATASET, DATASET),
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
       `{}.unioned_ehr_drug_exposure` AS t1
    '''.format(DATASET, DATASET, DATASET, DATASET, DATASET, DATASET),
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
       `{}.unioned_ehr_drug_exposure` AS t1
    INNER JOIN
        (SELECT
            DISTINCT * 
        FROM
             `{}._mapping_drug_exposure`)  AS t2
    ON
        t1.drug_exposure_id=t2.drug_exposure_id
    GROUP BY
        1
    '''.format(DATASET, DATASET, DATASET, DATASET, DATASET, DATASET),
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

# ## Device Exposure Table

# +
######################################
print('Getting the data from the database...')
######################################

temporal_df = pd.io.gbq.read_gbq('''
    SELECT
        COUNT(*) AS total,
        sum(case when (t1.device_exposure_start_date>t1.device_exposure_end_date) then 1 else 0 end) as wrong_date
    FROM
       `{}.unioned_ehr_device_exposure` AS t1
    '''.format(DATASET, DATASET, DATASET, DATASET, DATASET, DATASET),
                                 dialect='standard')
temporal_df.shape

print(temporal_df.shape[0], 'records received.')
# -

temporal_df

print("success rate for device is: ",
      round(100 - 100 * (temporal_df.iloc[0, 1] / temporal_df.iloc[0, 0]), 1))

# ### Device Exposure Table By Site

# +
######################################
print('Getting the data from the database...')
######################################

temporal_df = pd.io.gbq.read_gbq('''
    SELECT
        src_hpo_id,
        COUNT(*) AS total_rows,
        sum(case when (t1.device_exposure_start_date>t1.device_exposure_end_date) then 1 else 0 end) as wrong_date_rows
    FROM
       `{}.unioned_ehr_device_exposure` AS t1
    INNER JOIN
        (SELECT
            DISTINCT * 
        FROM
             `{}._mapping_device_exposure`)  AS t2
    ON
        t1.device_exposure_id=t2.device_exposure_id
    GROUP BY
        1
    '''.format(DATASET, DATASET, DATASET, DATASET, DATASET, DATASET),
                                 dialect='standard')
temporal_df.shape

print(temporal_df.shape[0], 'records received.')
# -

temporal_df['success_rate'] = 100 - round(
    100 * temporal_df['wrong_date_rows'] / temporal_df['total_rows'], 1)
temporal_df

device_exposure = temporal_df.rename(columns={"success_rate": "device_exposure"})
device_exposure = device_exposure[["src_hpo_id", "device_exposure"]]
device_exposure = device_exposure.fillna(100)
device_exposure

total_wrong = temporal_df['wrong_date_rows'].sum()
total_wrong

total_row = temporal_df['total_rows'].sum()
percent = round(100 - 100 * (total_wrong / (total_row)), 1)
percent

temporal_df

# ## Temporal Data Points - End Dates Before Start Dates

# +

success_rate = pd.merge(visit_occurrence,
                       condition_occurrence,
                       how='outer',
                       on='src_hpo_id')
success_rate = pd.merge(success_rate, drug_exposure, how='outer', on='src_hpo_id')
success_rate = pd.merge(success_rate,
                       device_exposure,
                       how='outer',
                       on='src_hpo_id')
success_rate = pd.merge(success_rate, site_df, how='outer', on='src_hpo_id')
success_rate = success_rate.fillna(100)
success_rate
# -

success_rate.to_csv("{cwd}\end_before_begin.csv".format(cwd = cwd))

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
       `{}.unioned_ehr_visit_occurrence` AS t1
    INNER JOIN
        `{}.unioned_ehr_death` AS t2
        ON
            t1.person_id=t2.person_id
    INNER JOIN
        (SELECT
            DISTINCT * 
        FROM
             `{}._mapping_visit_occurrence`)  AS t3
    ON
        t1.visit_occurrence_id=t3.visit_occurrence_id
    GROUP BY
        1
    '''.format(DATASET, DATASET, DATASET, DATASET, DATASET, DATASET),
                                 dialect='standard')
temporal_df.shape

print(temporal_df.shape[0], 'records received.')
# -

temporal_df['success_rate'] = 100 - round(
    100 * temporal_df['wrong_death_date'] / temporal_df['total'], 1)
temporal_df

# - main reason death date entered as default value ("1890")

visit_occurrence = temporal_df.rename(
    columns={"success_rate": "visit_occurrence"})
visit_occurrence = visit_occurrence[["src_hpo_id", "visit_occurrence"]]
visit_occurrence = visit_occurrence.fillna(100)
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
       `{}.unioned_ehr_condition_occurrence` AS t1
    INNER JOIN
        `{}.unioned_ehr_death` AS t2
        ON
            t1.person_id=t2.person_id
    INNER JOIN
        (SELECT
            DISTINCT * 
        FROM
             `{}._mapping_condition_occurrence`)  AS t3
    ON
        t1.condition_occurrence_id=t3.condition_occurrence_id
    GROUP BY
        1
    '''.format(DATASET, DATASET, DATASET, DATASET, DATASET, DATASET),
                                 dialect='standard')
temporal_df.shape

print(temporal_df.shape[0], 'records received.')
# -

temporal_df['success_rate'] = 100 - round(
    100 * temporal_df['wrong_death_date'] / temporal_df['total'], 1)
temporal_df

condition_occurrence = temporal_df.rename(
    columns={"success_rate": "condition_occurrence"})
condition_occurrence = condition_occurrence[[
    "src_hpo_id", "condition_occurrence"
]]
condition_occurrence = condition_occurrence.fillna(100)
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
       `{}.unioned_ehr_drug_exposure` AS t1
    INNER JOIN
        `{}.unioned_ehr_death` AS t2
        ON
            t1.person_id=t2.person_id
    INNER JOIN
        (SELECT
            DISTINCT * 
        FROM
             `{}._mapping_drug_exposure`)  AS t3
    ON
        t1.drug_exposure_id=t3.drug_exposure_id
    GROUP BY
        1
    '''.format(DATASET, DATASET, DATASET, DATASET, DATASET, DATASET),
                                 dialect='standard')
temporal_df.shape

print(temporal_df.shape[0], 'records received.')
# -

temporal_df['success_rate'] = 100 - round(
    100 * temporal_df['wrong_death_date'] / temporal_df['total'], 1)
temporal_df

drug_exposure = temporal_df.rename(columns={"success_rate": "drug_exposure"})
drug_exposure = drug_exposure[["src_hpo_id", "drug_exposure"]]
drug_exposure = drug_exposure.fillna(100)
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
       `{}.unioned_ehr_measurement` AS t1
    INNER JOIN
        `{}.unioned_ehr_death` AS t2
        ON
            t1.person_id=t2.person_id
    INNER JOIN
        (SELECT
            DISTINCT * 
        FROM
             `{}._mapping_measurement`)  AS t3
    ON
        t1.measurement_id=t3.measurement_id
    GROUP BY
        1
    '''.format(DATASET, DATASET, DATASET, DATASET, DATASET, DATASET),
                                 dialect='standard')
temporal_df.shape

print(temporal_df.shape[0], 'records received.')
# -

temporal_df['success_rate'] = 100 - round(
    100 * temporal_df['wrong_death_date'] / temporal_df['total'], 1)
temporal_df

measurement = temporal_df.rename(columns={"success_rate": "measurement"})
measurement = measurement[["src_hpo_id", "measurement"]]
measurement = measurement.fillna(100)
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
       `{}.unioned_ehr_procedure_occurrence` AS t1
    INNER JOIN
        `{}.unioned_ehr_death` AS t2
        ON
            t1.person_id=t2.person_id
    INNER JOIN
        (SELECT
            DISTINCT * 
        FROM
             `{}._mapping_procedure_occurrence`)  AS t3
    ON
        t1.procedure_occurrence_id=t3.procedure_occurrence_id
    GROUP BY
        1
    '''.format(DATASET, DATASET, DATASET, DATASET, DATASET, DATASET),
                                 dialect='standard')
temporal_df.shape

print(temporal_df.shape[0], 'records received.')
# -

temporal_df['success_rate'] = 100 - round(
    100 * temporal_df['wrong_death_date'] / temporal_df['total'], 1)
temporal_df

procedure_occurrence = temporal_df.rename(
    columns={"success_rate": "procedure_occurrence"})
procedure_occurrence = procedure_occurrence[[
    "src_hpo_id", "procedure_occurrence"
]]
procedure_occurrence = procedure_occurrence.fillna(100)
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
       `{}.unioned_ehr_observation` AS t1
    INNER JOIN
        `{}.unioned_ehr_death` AS t2
        ON
            t1.person_id=t2.person_id
    INNER JOIN
        (SELECT
            DISTINCT * 
        FROM
             `{}._mapping_observation`)  AS t3
    ON
        t1.observation_id=t3.observation_id
    GROUP BY
        1
    '''.format(DATASET, DATASET, DATASET, DATASET, DATASET, DATASET),
                                 dialect='standard')
temporal_df.shape

print(temporal_df.shape[0], 'records received.')
# -

temporal_df['success_rate'] = 100 - round(
    100 * temporal_df['wrong_death_date'] / temporal_df['total'], 1)
temporal_df

observation = temporal_df.rename(columns={"success_rate": "observation"})
observation = observation[["src_hpo_id", "observation"]]
observation = observation.fillna(100)
observation

# ## Device Exposure Table

# +
######################################
print('Getting the data from the database...')
######################################

temporal_df = pd.io.gbq.read_gbq('''
    SELECT
        src_hpo_id,
        COUNT(*) AS total,
        sum(case when (DATE_DIFF(device_exposure_start_date, death_date, DAY)>30) then 1 else 0 end) as wrong_death_date
    FROM
       `{}.unioned_ehr_device_exposure` AS t1
    INNER JOIN
        `{}.unioned_ehr_death` AS t2
        ON
            t1.person_id=t2.person_id
    INNER JOIN
        (SELECT
            DISTINCT * 
        FROM
             `{}._mapping_device_exposure`)  AS t3
    ON
        t1.device_exposure_id=t3.device_exposure_id
    GROUP BY
        1
    '''.format(DATASET, DATASET, DATASET, DATASET, DATASET, DATASET),
                                 dialect='standard')
temporal_df.shape

print(temporal_df.shape[0], 'records received.')
# -

temporal_df['success_rate'] = 100 - round(
    100 * temporal_df['wrong_death_date'] / temporal_df['total'], 1)
temporal_df

device_exposure = temporal_df.rename(columns={"success_rate": "device_exposure"})
device_exposure = device_exposure[["src_hpo_id", "device_exposure"]]
device_exposure = device_exposure.fillna(100)
device_exposure

# ## 4. Success Rate Temporal Data Points - Data After Death Date

datas = [
    condition_occurrence, drug_exposure, measurement, procedure_occurrence,
    observation, device_exposure
]

master_df = visit_occurrence

for filename in datas:
    master_df = pd.merge(master_df, filename, on='src_hpo_id', how='outer')

master_df

success_rate = pd.merge(master_df, site_df, how='outer', on='src_hpo_id')
success_rate = success_rate.fillna(100)

success_rate

success_rate.to_csv("{cwd}\data_after_death.csv".format(cwd = cwd))

# # Age of participant should NOT be below 18 and should NOT be too high (Achilles rule_id #20 and 21)

# ## Count number of unique participants with age <18

# +

######################################
print('Getting the data from the database...')
######################################

birth_df = pd.io.gbq.read_gbq('''
    SELECT
        COUNT(*) AS total,
        sum(case when (DATE_DIFF(CURRENT_DATE, EXTRACT(DATE FROM birth_datetime), YEAR)<18) then 1 else 0 end) as minors_in_dataset
         
    FROM
       `{}.unioned_ehr_person` AS t1
    '''.format(DATASET, DATASET, DATASET, DATASET, DATASET, DATASET),
                              dialect='standard')
print(birth_df.shape[0], 'records received.')
# -

birth_df

# +
######################################
print('Getting the data from the database...')
######################################

birth_df = pd.io.gbq.read_gbq('''
    SELECT
        person_id          
    FROM
       `{}.unioned_ehr_person` AS t1
    where 
        (DATE_DIFF(CURRENT_DATE, EXTRACT(DATE FROM birth_datetime), YEAR)<18)
    '''.format(DATASET, DATASET, DATASET, DATASET, DATASET, DATASET),
                              dialect='standard')
print(birth_df.shape[0], 'records received.')
# -

# ## Count number of unique participants with age >120

# +

######################################
print('Getting the data from the database...')
######################################

birth_df = pd.io.gbq.read_gbq('''
    SELECT
        COUNT(*) AS total,
        sum(case when (DATE_DIFF(CURRENT_DATE, EXTRACT(DATE FROM birth_datetime), YEAR)>120) then 1 else 0 end) as over_120_in_dataset
         
    FROM
       `{}.unioned_ehr_person` AS t1
    '''.format(DATASET, DATASET, DATASET, DATASET, DATASET, DATASET),
                              dialect='standard')
print(birth_df.shape[0], 'records received.')

# +
######################################
print('Getting the data from the database...')
######################################

birth_df = pd.io.gbq.read_gbq('''
    SELECT
        person_id          
    FROM
       `{}.unioned_ehr_person` AS t1
    where 
        DATE_DIFF(CURRENT_DATE, EXTRACT(DATE FROM birth_datetime), YEAR)>120
    '''.format(DATASET, DATASET, DATASET, DATASET, DATASET, DATASET),
                              dialect='standard')
print(birth_df.shape[0], 'records received.')
# -

birth_df

# ## Histogram

# +

######################################
print('Getting the data from the database...')
######################################

birth_df = pd.io.gbq.read_gbq('''
    SELECT
        DATE_DIFF(CURRENT_DATE, EXTRACT(DATE FROM birth_datetime), YEAR) as AGE    
    FROM
       `{}.unioned_ehr_person` AS t1
    '''.format(DATASET, DATASET, DATASET, DATASET, DATASET, DATASET),
                              dialect='standard')
print(birth_df.shape[0], 'records received.')
# -

birth_df.head()

birth_df['AGE'].hist(bins=88)

# # Participant should have supporting data in either lab results or drugs if he/she has a condition code for diabetes.

# ## Determine those who have diabetes according to the 'condition' table

persons_with_conditions_related_to_diabetes_query = """
CREATE TABLE `{DATASET}.persons_with_diabetes_according_to_condition_table`
OPTIONS
(expiration_timestamp=TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 3 MINUTE)
)
AS
SELECT
DISTINCT
mco.src_hpo_id, p.person_id
FROM
`{DATASET}.unioned_ehr_person` p
JOIN
`{DATASET}.unioned_ehr_condition_occurrence` co
ON
p.person_id = co.person_id
JOIN
`{DATASET}.concept` c
ON
co.condition_concept_id = c.concept_id
JOIN
`{DATASET}._mapping_condition_occurrence` mco
ON
co.condition_occurrence_id = mco.condition_occurrence_id 
WHERE
LOWER(c.concept_name) LIKE '%diabetes%'
AND
(invalid_reason is null or invalid_reason = '')
GROUP BY 1, 2
ORDER BY 1, 2 DESC
""".format(DATASET = DATASET)

persons_with_conditions_related_to_diabetes = pd.io.gbq.read_gbq(
    persons_with_conditions_related_to_diabetes_query, dialect = 'standard')

num_persons_w_diabetes_query = """
SELECT
DISTINCT
COUNT(p.person_id) as num_with_diab
FROM
`{DATASET}.persons_with_diabetes_according_to_condition_table` p
""".format(DATASET = DATASET)

num_persons_w_diabetes = pd.io.gbq.read_gbq(num_persons_w_diabetes_query, dialect = 'standard')

# +
diabetics = num_persons_w_diabetes['num_with_diab'][0]

print("There are {diabetics} persons with diabetes in the total dataset".format(diabetics = diabetics))
# -

diabetics_per_site_query = """
SELECT
DISTINCT
p.src_hpo_id, COUNT(DISTINCT p.person_id) as num_with_diab
FROM
`{DATASET}.persons_with_diabetes_according_to_condition_table` p
GROUP BY 1
ORDER BY num_with_diab DESC
""".format(DATASET = DATASET)

diabetics_per_site = pd.io.gbq.read_gbq(diabetics_per_site_query, dialect = 'standard')

diabetics_per_site

# ## Drug

create_table_with_substantiating_diabetic_drug_concept_ids = """
CREATE TABLE `{DATASET}.substantiating_diabetic_drug_concept_ids`
OPTIONS (
expiration_timestamp=TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 3 MINUTE)
) AS
SELECT
DISTINCT
ca.descendant_concept_id 
FROM
`{DATASET}.concept` c
JOIN
`{DATASET}.concept_ancestor` ca
ON
c.concept_id = ca.ancestor_concept_id 
WHERE
ca.ancestor_concept_id  IN
(1529331,1530014,1594973,1583722,1597756,1560171,19067100,1559684,1503297,1510202,1502826,
1525215,1516766,1547504,1580747,1502809,1515249)
AND
(c.invalid_reason is NULL 
or 
C.invalid_reason = '')
""".format(DATASET = DATASET)

substantiating_diabetic_drug_concept_ids = pd.io.gbq.read_gbq(create_table_with_substantiating_diabetic_drug_concept_ids, dialect = 'standard')

# +
######################################
print('Getting the data from the database...')
######################################

persons_w_t2d_by_condition_and_substantiating_drugs_query = """
SELECT
DISTINCT
p.src_hpo_id, COUNT(DISTINCT p.person_id) as num_with_diab_and_drugs
FROM
`{DATASET}.persons_with_diabetes_according_to_condition_table` p
RIGHT JOIN
`{DATASET}.unioned_ehr_drug_exposure` de  -- get the relevant drugs
ON
p.person_id = de.person_id
RIGHT JOIN
`{DATASET}.substantiating_diabetic_drug_concept_ids` t2drugs  -- only focus on the drugs that substantiate diabetes
ON
de.drug_concept_id = t2drugs.descendant_concept_id 
GROUP BY 1
ORDER BY num_with_diab_and_drugs DESC
""".format(DATASET = DATASET)


diabetics_with_substantiating_drugs = pd.io.gbq.read_gbq(persons_w_t2d_by_condition_and_substantiating_drugs_query, dialect='standard')
# -

diabetics_with_substantiating_drugs

diabetics_with_substantiating_drugs.shape

# ## glucose_lab

valid_glucose_measurements_query = """
CREATE TABLE `{DATASET}.valid_glucose_labs`
OPTIONS (
expiration_timestamp=TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 3 MINUTE)
) AS
SELECT
DISTINCT
c.concept_id, c.concept_name
FROM
`{DATASET}.concept` c
JOIN
`{DATASET}.concept_ancestor` ca
ON
c.concept_id = ca.descendant_concept_id
WHERE
ca.ancestor_concept_id IN (40795740)
AND
c.invalid_reason IS NULL
OR
c.invalid_reason = ''
""".format(DATASET = DATASET)

valid_glucose_measurements = pd.io.gbq.read_gbq(valid_glucose_measurements_query, dialect='standard')

# #### diabetic persons who have at least one 'glucose' measurement

diabetics_with_glucose_measurement_query = """
SELECT
DISTINCT
p.src_hpo_id, COUNT(DISTINCT p.person_id) as num_with_diab_and_glucose
FROM
`{DATASET}.persons_with_diabetes_according_to_condition_table` p
RIGHT JOIN
`{DATASET}.unioned_ehr_measurement` m
ON
p.person_id = m.person_id -- get the persons with measurements
RIGHT JOIN
`{DATASET}.valid_glucose_labs` vgl
ON
vgl.concept_id = m.measurement_concept_id -- only get those with the substantiating labs
GROUP BY 1
ORDER BY num_with_diab_and_glucose DESC
""".format(DATASET = DATASET)

diabetics_with_glucose_measurement = pd.io.gbq.read_gbq(diabetics_with_glucose_measurement_query, dialect='standard')

diabetics_with_glucose_measurement.shape

# ## a1c

hemoglobin_a1c_desc_query = """
CREATE TABLE `{DATASET}.a1c_descendants`
OPTIONS (
expiration_timestamp=TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 3 MINUTE)
)
AS
SELECT
DISTINCT
ca.descendant_concept_id as concept_id
FROM
`{DATASET}.concept_ancestor` ca
WHERE
ca.ancestor_concept_id IN (40789263)
""".format(DATASET = DATASET)

hemoglobin_a1c_desc = pd.io.gbq.read_gbq(hemoglobin_a1c_desc_query, dialect='standard')

diabetics_with_a1c_measurement_query = """
SELECT
DISTINCT
p.src_hpo_id, COUNT(DISTINCT p.person_id) as num_with_diab_and_a1c
FROM
`{DATASET}.persons_with_diabetes_according_to_condition_table` p
RIGHT JOIN
`{DATASET}.unioned_ehr_measurement` m
ON
p.person_id = m.person_id -- get the persons with measurements
RIGHT JOIN
`{DATASET}.a1c_descendants` a1c
ON
a1c.concept_id = m.measurement_concept_id -- only get those with the substantiating labs
GROUP BY 1
ORDER BY num_with_diab_and_a1c DESC
""".format(DATASET = DATASET)

# +
diabetics_with_a1c_measurement = pd.io.gbq.read_gbq(diabetics_with_a1c_measurement_query, dialect='standard')

diabetics_with_a1c_measurement.shape
# -

# ## insulin

# +
######################################
print('Getting the data from the database...')
######################################

persons_with_insulin_query = """
SELECT
DISTINCT
p.src_hpo_id, COUNT(DISTINCT p.person_id) as num_with_diab_and_insulin
FROM
`{DATASET}.persons_with_diabetes_according_to_condition_table` p
RIGHT JOIN
`{DATASET}.unioned_ehr_drug_exposure` de
ON
de.person_id = p.person_id -- get the persons with measurements
RIGHT JOIN
`{DATASET}.concept` c
ON
de.drug_concept_id = c.concept_id
WHERE
LOWER(c.concept_name) LIKE '%insulin%'  -- generous for detecting insulin
GROUP BY 1
ORDER BY num_with_diab_and_insulin DESC
""".format(DATASET = DATASET)
# -

diabetics_with_insulin = pd.io.gbq.read_gbq(persons_with_insulin_query, dialect='standard')

final_diabetic_df = pd.merge(diabetics_per_site, diabetics_with_substantiating_drugs, on = 'src_hpo_id')

final_diabetic_df['diabetics_w_drugs'] = round(final_diabetic_df['num_with_diab_and_drugs'] / final_diabetic_df['num_with_diab'] * 100, 2)

final_diabetic_df = pd.merge(final_diabetic_df, diabetics_with_glucose_measurement, on = 'src_hpo_id')

final_diabetic_df['diabetics_w_glucose'] = round(final_diabetic_df['num_with_diab_and_glucose'] / final_diabetic_df['num_with_diab'] * 100, 2)

final_diabetic_df = pd.merge(final_diabetic_df, diabetics_with_a1c_measurement, on = 'src_hpo_id')

final_diabetic_df['diabetics_w_a1c'] = round(final_diabetic_df['num_with_diab_and_a1c'] / final_diabetic_df['num_with_diab'] * 100, 2)

final_diabetic_df = pd.merge(final_diabetic_df, diabetics_with_insulin, on = 'src_hpo_id')

final_diabetic_df['diabetics_w_insulin'] = round(final_diabetic_df['num_with_diab_and_insulin'] / final_diabetic_df['num_with_diab'] * 100, 2)

final_diabetic_df = final_diabetic_df.sort_values(by='diabetics_w_glucose', ascending = False)

final_diabetic_df

success_rate.to_csv("{cwd}\data_after_death.csv".format(cwd = cwd))


