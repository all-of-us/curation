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

Lipid = (40782589, 40795800, 40772572)

CBC = (40789356, 40789120, 40789179, 40772748, 40782735,
40789182, 40786033, 40779159)

CBCwDiff = (40785788, 40785796, 40779195, 40795733, 40795725,
40772531, 40779190, 40785793, 40779191, 40782561, 40789266)

CMP = (3049187, 3053283, 40775801, 40779224, 40782562, 40782579, 40785850,
40785861, 40785869, 40789180, 40789190, 40789527, 40791227, 40792413, 40792440,
40795730, 40795740, 40795754)

Physical_Measurement = (40654163, 40655804, 40654162, 40655805, 40654167, 40654164)

all_measurements = Lipid + CBC + CBCwDiff + CMP + Physical_Measurement

# # Improve the Definitions of Measurement Integration

# ## Lipid

len(Lipid)

num_lipids = len(set(Lipid))

df = pd.io.gbq.read_gbq('''
SELECT
    a.src_hpo_id, 
    round(COUNT(a.src_hpo_id) / {num_lipids} * 100, 2) perc_ancestors
FROM
     (
     SELECT
         DISTINCT mm.src_hpo_id, ca.ancestor_concept_id -- logs an ancestor_concept if it is found
     FROM
         `{DATASET}.unioned_ehr_measurement` m
     JOIN -- to get the site info
         `{DATASET}._mapping_measurement` mm
     ON
         m.measurement_id = mm.measurement_id
     JOIN
         `{DATASET}.concept` c
     ON
         c.concept_id = m.measurement_concept_id
     JOIN -- ensuring you 'navigate up' the hierarchy
         `{DATASET}.concept_ancestor` ca
     ON
         m.measurement_concept_id = ca.descendant_concept_id
     WHERE
         ca.ancestor_concept_id IN {Lipid}
     ) a
 GROUP BY 1
 ORDER BY perc_ancestors DESC, a.src_hpo_id
    '''.format(DATASET=DATASET, num_lipids=num_lipids,Lipid=Lipid),
                        dialect='standard')
df.shape

df_Lipid = df.rename(columns={"perc_ancestors": 'Lipid'})

df_Lipid.head(100)

# ## CBC

len(CBC)

num_cbc = len(set(CBC))

df = pd.io.gbq.read_gbq('''
SELECT
    a.src_hpo_id, 
    round(COUNT(a.src_hpo_id) / {num_cbc} * 100, 2) perc_ancestors
FROM
     (
     SELECT
         DISTINCT mm.src_hpo_id, ca.ancestor_concept_id -- logs an ancestor_concept if it is found
     FROM
         `{DATASET}.unioned_ehr_measurement` m
     JOIN -- to get the site info
         `{DATASET}._mapping_measurement` mm
     ON
         m.measurement_id = mm.measurement_id
     JOIN
         `{DATASET}.concept` c
     ON
         c.concept_id = m.measurement_concept_id
     JOIN -- ensuring you 'navigate up' the hierarchy
         `{DATASET}.concept_ancestor` ca
     ON
         m.measurement_concept_id = ca.descendant_concept_id
     WHERE
         ca.ancestor_concept_id IN {CBC}
     ) a
 GROUP BY 1
 ORDER BY perc_ancestors DESC, a.src_hpo_id
    '''.format(num_cbc=num_cbc, DATASET=DATASET, CBC=CBC),
                        dialect='standard')
df.shape

df_CBC = df.rename(columns={"perc_ancestors": 'CBC'})

df_CBC.head(100)

# ## CBCwDiff

len(CBCwDiff)

num_cbc_w_diff = len(set(CBCwDiff))

df = pd.io.gbq.read_gbq('''
SELECT
    a.src_hpo_id, 
    round(COUNT(a.src_hpo_id) / {num_cbc_w_diff} * 100, 2) perc_ancestors
FROM
     (
     SELECT
         DISTINCT mm.src_hpo_id, ca.ancestor_concept_id -- logs an ancestor_concept if it is found
     FROM
         `{DATASET}.unioned_ehr_measurement` m
     JOIN -- to get the site info
         `{DATASET}._mapping_measurement` mm
     ON
         m.measurement_id = mm.measurement_id
     JOIN
         `{DATASET}.concept` c
     ON
         c.concept_id = m.measurement_concept_id
     JOIN -- ensuring you 'navigate up' the hierarchy
         `{DATASET}.concept_ancestor` ca
     ON
         m.measurement_concept_id = ca.descendant_concept_id
     WHERE
         ca.ancestor_concept_id IN {CBCwDiff}
     ) a
 GROUP BY 1
 ORDER BY perc_ancestors DESC, a.src_hpo_id
    '''.format(num_cbc_w_diff=num_cbc_w_diff, DATASET=DATASET, 
               CBCwDiff=CBCwDiff),
                        dialect='standard')
df.shape

df_CBCwDiff = df.rename(columns={"perc_ancestors": 'CBCwDiff'})

df_CBCwDiff.head(100)

# ## CMP

len(CMP)

num_cmp = len(set(CMP))

df = pd.io.gbq.read_gbq('''
SELECT
    a.src_hpo_id, 
    round(COUNT(a.src_hpo_id) / {num_cmp} * 100, 2) perc_ancestors
FROM
     (
     SELECT
         DISTINCT mm.src_hpo_id, ca.ancestor_concept_id -- logs an ancestor_concept if it is found
     FROM
         `{DATASET}.unioned_ehr_measurement` m
     JOIN -- to get the site info
         `{DATASET}._mapping_measurement` mm
     ON
         m.measurement_id = mm.measurement_id
     JOIN
         `{DATASET}.concept` c
     ON
         c.concept_id = m.measurement_concept_id
     JOIN -- ensuring you 'navigate up' the hierarchy
         `{DATASET}.concept_ancestor` ca
     ON
         m.measurement_concept_id = ca.descendant_concept_id
     WHERE
         ca.ancestor_concept_id IN {CMP}
     ) a
 GROUP BY 1
 ORDER BY perc_ancestors DESC, a.src_hpo_id
    '''.format(num_cmp=num_cmp, DATASET=DATASET, CMP=CMP),
                        dialect='standard')
df.shape

df_CMP = df.rename(columns={"perc_ancestors": 'CMP'})

df_CMP.head(100)

# ## Physical_Measurement

len(Physical_Measurement)

num_pms = len(set(Physical_Measurement))

df = pd.io.gbq.read_gbq('''
SELECT
    a.src_hpo_id, 
    round(COUNT(a.src_hpo_id) / {num_pms} * 100, 2) perc_ancestors
FROM
     (
     SELECT
         DISTINCT mm.src_hpo_id, ca.ancestor_concept_id -- logs an ancestor_concept if it is found
     FROM
         `{DATASET}.unioned_ehr_measurement` m
     JOIN -- to get the site info
         `{DATASET}._mapping_measurement` mm
     ON
         m.measurement_id = mm.measurement_id
     JOIN
         `{DATASET}.concept` c
     ON
         c.concept_id = m.measurement_concept_id
     JOIN -- ensuring you 'navigate up' the hierarchy
         `{DATASET}.concept_ancestor` ca
     ON
         m.measurement_concept_id = ca.descendant_concept_id
     WHERE
         ca.ancestor_concept_id IN {Physical_Measurement}
     ) a
 GROUP BY 1
 ORDER BY perc_ancestors DESC, a.src_hpo_id
    '''.format(num_pms=num_pms, DATASET=DATASET, Physical_Measurement=Physical_Measurement),
                        dialect='standard')
df.shape

df_Physical_Measurement = df.rename(
    columns={"perc_ancestors": 'Physical_Measurement'})

df_Physical_Measurement.head(100)

# ## All Measurements

len(all_measurements)

num_all_measurements = len(set(all_measurements))

df = pd.io.gbq.read_gbq('''
SELECT
    a.src_hpo_id, 
    round(COUNT(a.src_hpo_id) / {num_all_measurements} * 100, 2) perc_ancestors
FROM
     (
     SELECT
         DISTINCT mm.src_hpo_id, ca.ancestor_concept_id -- logs an ancestor_concept if it is found
     FROM
         `{DATASET}.unioned_ehr_measurement` m
     JOIN -- to get the site info
         `{DATASET}._mapping_measurement` mm
     ON
         m.measurement_id = mm.measurement_id
     JOIN
         `{DATASET}.concept` c
     ON
         c.concept_id = m.measurement_concept_id
     JOIN -- ensuring you 'navigate up' the hierarchy
         `{DATASET}.concept_ancestor` ca
     ON
         m.measurement_concept_id = ca.descendant_concept_id
     WHERE
         ca.ancestor_concept_id IN {all_measurements}
     ) a
 GROUP BY 1
 ORDER BY perc_ancestors DESC, a.src_hpo_id
    '''.format(num_all_measurements=num_all_measurements, DATASET=DATASET,
              all_measurements=all_measurements),
                        dialect='standard')
df.shape

df_all_measurements = df.rename(columns={"perc_ancestors": 'All_Measurements'})

df_all_measurements.head(100)

# ## Sites combined

sites_measurement = pd.merge(df_Physical_Measurement,
                             df_CMP,
                             how='outer',
                             on='src_hpo_id')
sites_measurement = pd.merge(sites_measurement,
                             df_CBCwDiff,
                             how='outer',
                             on='src_hpo_id')
sites_measurement = pd.merge(sites_measurement,
                             df_CBC,
                             how='outer',
                             on='src_hpo_id')
sites_measurement = pd.merge(sites_measurement,
                             df_Lipid,
                             how='outer',
                             on='src_hpo_id')
sites_measurement = pd.merge(sites_measurement,
                             df_all_measurements,
                             how='outer',
                             on='src_hpo_id')

sites_measurement[["Physical_Measurement","CMP","CBCwDiff","CBC","Lipid","All_Measurements"]]\
    =sites_measurement[["Physical_Measurement","CMP","CBCwDiff","CBC","Lipid","All_Measurements"]]

sites_measurement = pd.merge(sites_measurement,
                             site_df,
                             how='outer',
                             on='src_hpo_id')
sites_measurement = sites_measurement.fillna(0)

sites_measurement

sites_measurement = sites_measurement.sort_values(by='All_Measurements', ascending = False)

sites_measurement

sites_measurement.to_csv("{cwd}/sites_measurement.csv".format(cwd = cwd))


