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

# +
# #!pip install --upgrade google-cloud-bigquery[pandas]
# -

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

DATASET = ''

plt.style.use('ggplot')
pd.options.display.max_rows = 999
pd.options.display.max_columns = 999
pd.options.display.max_colwidth = 999

from IPython.display import HTML as html_print


def cstr(s, color='black'):
    return "<text style=color:{}>{}</text>".format(color, s)


print('done.')

# +
dic = {'src_hpo_id': ["pitt_temple", "saou_lsu", "trans_am_meyers", "trans_am_essentia", "saou_ummc", "seec_miami",
                      "seec_morehouse", "seec_emory", "uamc_banner", "pitt", "nyc_cu", "ipmc_uic", "trans_am_spectrum",
                      "tach_hfhs", "nec_bmc", "cpmc_uci", "nec_phs", "nyc_cornell", "ipmc_nu", "nyc_hh",
                      "ipmc_uchicago", "aouw_mcri", "syhc", "cpmc_ceders", "seec_ufl", "saou_uab", "trans_am_baylor",
                      "cpmc_ucsd", "ecchc", "chci", "aouw_uwh", "cpmc_usc", "hrhc", "ipmc_northshore", "chs",
                      "cpmc_ucsf", "jhchc", "aouw_mcw", "cpmc_ucd", "ipmc_rush"],
       'HPO': ["Temple University", "Louisiana State University", "Reliant Medical Group (Meyers Primary Care)",
               "Essentia Health Superior Clinic", "University of Mississippi", "SouthEast Enrollment Center Miami",
               "SouthEast Enrollment Center Morehouse", "SouthEast Enrollment Center Emory", "Banner Health",
               "University of Pittsburgh", "Columbia University Medical Center", "University of Illinois Chicago",
               "Spectrum Health", "Henry Ford Health System", "Boston Medical Center", "UC Irvine",
               "Partners HealthCare", "Weill Cornell Medical Center", "Northwestern Memorial Hospital",
               "Harlem Hospital", "University of Chicago", "Marshfield Clinic", "San Ysidro Health Center",
               "Cedars-Sinai", "University of Florida", "University of Alabama at Birmingham", "Baylor", "UC San Diego",
               "Eau Claire Cooperative Health Center", "Community Health Center, Inc.",
               "UW Health (University of Wisconsin Madison)", "University of Southern California", "HRHCare",
               "NorthShore University Health System", "Cherokee Health Systems", "UC San Francisco",
               "Jackson-Hinds CHC", "Medical College of Wisconsin", "UC Davis", "Rush University"]}

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
    '''.format(DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET
               , DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET
               , DATASET, DATASET, DATASET, DATASET),
                              dialect='standard')
print(site_map.shape[0], 'records received.')
# -

site_df = pd.merge(site_map, site_df, how='outer', on='src_hpo_id')

site_df

# # Foreign key references (i.e. visit_occurrence_id in the condition table) should be valid.

# ## Person

print(("There is no _mapping table for person table so I could not separete results by sites "))

# ### gender_concept_id

# +
######################################
print('Getting the data from the database...')
######################################

foreign_key_df = pd.io.gbq.read_gbq('''
    SELECT
        gender_concept_id,
        concept_name,
        COUNT(*) AS cnt
    FROM
       `{}.unioned_ehr_person` AS p
    INNER JOIN
        `{}.concept` AS c
        ON
            p.gender_concept_id=c.concept_id
    GROUP BY
        1,2
    '''.format(DATASET, DATASET, DATASET, DATASET, DATASET, DATASET),
                                    dialect='standard')
print(foreign_key_df.shape[0], 'records received.')
# -

foreign_key_df

foreign_key_df.loc[foreign_key_df["gender_concept_id"] == 0, ["cnt"]]

# +
# success_rate=100-round(100*(foreign_key_df.loc[foreign_key_df["gender_concept_id"]==0,["cnt"]])/sum(foreign_key_df.iloc[:,2]),1)
# print("success rate for gender_concept_id is: ", success_rate.iloc[0,0]) 
# -

foreign_key_df

# ### race_concept_id

# +
######################################
print('Getting the data from the database...')
######################################

foreign_key_df = pd.io.gbq.read_gbq('''
    SELECT
        race_concept_id,
        concept_name,
        COUNT(*) AS cnt
    FROM
       `{}.unioned_ehr_person` AS p
    INNER JOIN
        `{}.concept` AS c
        ON
            p.race_concept_id=c.concept_id
    GROUP BY
        1,2
    '''.format(DATASET, DATASET, DATASET, DATASET, DATASET, DATASET),
                                    dialect='standard')
foreign_key_df.shape

print(foreign_key_df.shape[0], 'records received.')
# -

foreign_key_df

# +
# success_rate=100-round(100*(foreign_key_df.loc[foreign_key_df["race_concept_id"]==0,["cnt"]])/sum(foreign_key_df.iloc[:,2]),1)
# print("success rate for race_concept_id is: ", success_rate.iloc[0,0]) 
# -

# ### ethnicity_concept_id

# +
######################################
print('Getting the data from the database...')
######################################

foreign_key_df = pd.io.gbq.read_gbq('''
    SELECT
        ethnicity_concept_id,
        concept_name,
        COUNT(*) AS cnt
    FROM
       `{}.unioned_ehr_person` AS p
    INNER JOIN
        `{}.concept` AS c
        ON
            p.ethnicity_concept_id=c.concept_id
    GROUP BY
        1,2
    '''.format(DATASET, DATASET, DATASET, DATASET, DATASET, DATASET),
                                    dialect='standard')
foreign_key_df.shape

print(foreign_key_df.shape[0], 'records received.')
# -

foreign_key_df

# +
# success_rate=100-round(100*(foreign_key_df.loc[foreign_key_df["ethnicity_concept_id"]==0,["cnt"]])/sum(foreign_key_df.iloc[:,2]),1)
# print("success rate for ethnicity_concept_id is: ", round(success_rate.iloc[0,0],1)) 
# -

# ### location_id

# +
######################################
print('Getting the data from the database...')
######################################

foreign_key_df = pd.io.gbq.read_gbq('''
    SELECT
    location_id,
    COUNT(*) AS total_cnt
    FROM
       `{}.unioned_ehr_person` AS p
    GROUP BY
        1
    '''.format(DATASET, DATASET, DATASET, DATASET, DATASET, DATASET),
                                    dialect='standard')
foreign_key_df.shape

print(foreign_key_df.shape[0], 'records received.')
# -

foreign_key_df.head()

# +

print("location_id is NULL ")
# -

# ### provider_id

# +
######################################
print('Getting the data from the database...')
######################################

foreign_key_df = pd.io.gbq.read_gbq('''
    SELECT
        provider_id,
        COUNT(*) AS cnt
    FROM
       `{}.unioned_ehr_person` AS p
    GROUP BY
        1
    '''.format(DATASET, DATASET, DATASET, DATASET, DATASET, DATASET),
                                    dialect='standard')
foreign_key_df.shape

print(foreign_key_df.shape[0], 'records received.')
# -

foreign_key_df.head()

# +

print("provider_id is NULL ")
# -

# ### care_site_id

# +
######################################
print('Getting the data from the database...')
######################################

foreign_key_df = pd.io.gbq.read_gbq('''
    SELECT
        care_site_id,
        COUNT(*) AS cnt
    FROM
       `{}.unioned_ehr_person` AS p
    GROUP BY
        1
    '''.format(DATASET, DATASET, DATASET, DATASET, DATASET, DATASET),
                                    dialect='standard')
foreign_key_df.shape

print(foreign_key_df.shape[0], 'records received.')
# -

foreign_key_df.head()

# +

print("care_site_id is NULL ")
# -

# ### gender_source_concept_id

# +
######################################
print('Getting the data from the database...')
######################################

foreign_key_df = pd.io.gbq.read_gbq('''
    SELECT
        gender_source_concept_id,
        COUNT(*) AS cnt
    FROM
       `{}.unioned_ehr_person` AS p
    GROUP BY
        1
    '''.format(DATASET, DATASET, DATASET, DATASET, DATASET, DATASET),
                                    dialect='standard')
foreign_key_df.shape

print(foreign_key_df.shape[0], 'records received.')
# -

foreign_key_df

# +

print("gender_source_concept_id is NULL ")
# -

# ### race_source_concept_id

# +
######################################
print('Getting the data from the database...')
######################################

foreign_key_df = pd.io.gbq.read_gbq('''
    SELECT
        race_source_concept_id,
        concept_name,
        COUNT(*) AS cnt
    FROM
       `{}.unioned_ehr_person` AS p
    INNER JOIN
        `{}.concept` AS c
        ON
            p.race_source_concept_id=c.concept_id
    GROUP BY
        1,2
    '''.format(DATASET, DATASET, DATASET, DATASET, DATASET, DATASET),
                                    dialect='standard')
foreign_key_df.shape

print(foreign_key_df.shape[0], 'records received.')
# -

foreign_key_df

# +

print("race_source_concept_id is NULL ")
# -

# ### ethnicity_source_concept_id

# +
######################################
print('Getting the data from the database...')
######################################

foreign_key_df = pd.io.gbq.read_gbq('''
    SELECT
        ethnicity_source_concept_id,
        COUNT(*) AS cnt
    FROM
       `{}.unioned_ehr_person` AS p
    GROUP BY
        1
    '''.format(DATASET, DATASET, DATASET, DATASET, DATASET, DATASET),
                                    dialect='standard')
foreign_key_df.shape

print(foreign_key_df.shape[0], 'records received.')
# -

foreign_key_df

# +

print("ethnicity_source_concept_id is NULL ")
# -

# ## VISIT_OCCURANCE TABLE

# ### person_id

# +
######################################
print('Getting the data from the database...')
######################################

foreign_key_df = pd.io.gbq.read_gbq('''
    SELECT
        COUNT(*) AS total,
        sum(case when (vo.person_id is null or vo.person_id=0) then 1 else 0 end) as missing
    FROM
       `{}.unioned_ehr_visit_occurrence` AS vo
    INNER JOIN
        `{}.unioned_ehr_observation` AS o
        ON
            vo.person_id=o.person_id
    WHERE 
        o.observation_source_concept_id=1586099 and o.value_as_concept_id=45877994
    '''.format(DATASET, DATASET, DATASET, DATASET, DATASET, DATASET),
                                    dialect='standard')
foreign_key_df.shape

print(foreign_key_df.shape[0], 'records received.')
# -

foreign_key_df

# ### visit_concept_id

# +
######################################
print('Getting the data from the database...')
######################################

foreign_key_df = pd.io.gbq.read_gbq('''
    SELECT
        concept_name,
        visit_concept_id,
        COUNT(*)
    FROM
       `{}.unioned_ehr_visit_occurrence` AS vo
    INNER JOIN
        `{}.concept` AS c
        ON
            vo.visit_concept_id=c.concept_id
    GROUP BY
        1,2
    ORDER BY
        2
    '''.format(DATASET, DATASET, DATASET, DATASET, DATASET, DATASET),
                                    dialect='standard')
foreign_key_df.shape

print(foreign_key_df.shape[0], 'records received.')
# -

foreign_key_df.head()

# #### visit_concept_id BY SITE

# +
######################################
print('Getting the data from the database...')
######################################

visit_occurrence_visit_concept_id_df = pd.io.gbq.read_gbq('''
    WITH
        hpo_counts AS (
            SELECT
                src_hpo_id,
                COUNT(vo.person_id) as total_counts
            FROM
               `{}.unioned_ehr_visit_occurrence` AS vo
            INNER JOIN
                `{}.concept` AS c
                ON
                    vo.visit_concept_id=c.concept_id
            LEFT OUTER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{}._mapping_visit_occurrence`) AS mvo
                ON
                    vo.visit_occurrence_id=mvo.visit_occurrence_id
            GROUP BY
                1
        ),

        hpo_missing_counts AS (
            SELECT
                mvo.src_hpo_id,
                COUNT(vo.person_id) as missing_counts
            FROM
               `{}.unioned_ehr_visit_occurrence` AS vo
            INNER JOIN
                `{}.concept` AS c
                ON
                    vo.visit_concept_id=c.concept_id
            LEFT OUTER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{}._mapping_visit_occurrence`) AS mvo
                ON
                    vo.visit_occurrence_id=mvo.visit_occurrence_id
            WHERE
                (vo.visit_concept_id is null or vo.visit_concept_id=0) 
            GROUP BY
                1
        )

    SELECT
        hpo_counts.src_hpo_id,
        missing_counts,
        total_counts,
        round(100-100*(missing_counts/total_counts),1) AS success_rate
    FROM
       hpo_counts
    FULL OUTER JOIN
        hpo_missing_counts
            ON
                hpo_missing_counts.src_hpo_id=hpo_counts.src_hpo_id
    ORDER BY
        4
    '''.format(DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET,
               DATASET),
                                                          dialect='standard')
visit_occurrence_visit_concept_id_df.shape

print(visit_occurrence_visit_concept_id_df.shape[0], 'records received.')
# -

visit_occurrence_visit_concept_id_df

visit_occurrence_visit_concept_id_df = visit_occurrence_visit_concept_id_df.rename(
    columns={"success_rate": "visit_occurrence_visit_concept_id"})
visit_occurrence_visit_concept_id_df = visit_occurrence_visit_concept_id_df[
    ["src_hpo_id", "visit_occurrence_visit_concept_id"]]
visit_occurrence_visit_concept_id_df = visit_occurrence_visit_concept_id_df.fillna(100)
visit_occurrence_visit_concept_id_df

# ### visit_type_concept_id

# +
######################################
print('Getting the data from the database...')
######################################

foreign_key_df = pd.io.gbq.read_gbq('''
    SELECT
        concept_name,
        visit_type_concept_id,
        COUNT(*)
    FROM
       `{}.unioned_ehr_visit_occurrence` AS vo
    INNER JOIN
        `{}.concept` AS c
        ON
            vo.visit_type_concept_id=c.concept_id
    GROUP BY
        1,2
    '''.format(DATASET, DATASET, DATASET, DATASET, DATASET, DATASET),
                                    dialect='standard')
foreign_key_df.shape

print(foreign_key_df.shape[0], 'records received.')
# -

foreign_key_df

success_rate = 100 - round(
    100 * (foreign_key_df.loc[foreign_key_df["visit_type_concept_id"] == 0, ["f0_"]]) / sum(foreign_key_df.iloc[:, 2]),
    1)
print("success rate for visit_concept_id is: ", success_rate.iloc[0, 0])

# #### visit_type_concept_id BY SITE

# +
######################################
print('Getting the data from the database...')
######################################

visit_occurrence_visit_type_concept_id = pd.io.gbq.read_gbq('''
    WITH
        hpo_counts AS (
            SELECT
                src_hpo_id,
                COUNT(vo.person_id) as total_counts
            FROM
               `{}.unioned_ehr_visit_occurrence` AS vo
            INNER JOIN
                `{}.concept` AS c
                ON
                    vo.visit_type_concept_id=c.concept_id
            INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{}._mapping_visit_occurrence`) AS mvo
                ON
                    vo.visit_occurrence_id=mvo.visit_occurrence_id
            GROUP BY
                1
        ),

        hpo_missing_counts AS (
            SELECT
                mvo.src_hpo_id,
                COUNT(vo.person_id) as missing_counts
            FROM
               `{}.unioned_ehr_visit_occurrence` AS vo
            INNER JOIN
                `{}.concept` AS c
                ON
                    vo.visit_type_concept_id=c.concept_id
            INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{}._mapping_visit_occurrence`) AS mvo
                ON
                    vo.visit_occurrence_id=mvo.visit_occurrence_id
            WHERE
                (vo.visit_type_concept_id is null or vo.visit_type_concept_id=0) 
            GROUP BY
                1
        )

    SELECT
        hpo_counts.src_hpo_id,
        missing_counts,
        total_counts,
        round(100-100*(missing_counts/total_counts),1) AS success_rate
    FROM
       hpo_counts
    FULL OUTER JOIN
        hpo_missing_counts
            ON
                hpo_missing_counts.src_hpo_id=hpo_counts.src_hpo_id
    ORDER BY
        4
    '''.format(DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET,
               DATASET),
                                                            dialect='standard')
visit_occurrence_visit_type_concept_id.shape

print(visit_occurrence_visit_type_concept_id.shape[0], 'records received.')
# -

visit_occurrence_visit_type_concept_id

visit_occurrence_visit_type_concept_id = visit_occurrence_visit_type_concept_id.rename(
    columns={"success_rate": "visit_occurrence_visit_type_concept_id"})
visit_occurrence_visit_type_concept_id = visit_occurrence_visit_type_concept_id[
    ["src_hpo_id", "visit_occurrence_visit_type_concept_id"]]
visit_occurrence_visit_type_concept_id = visit_occurrence_visit_type_concept_id.fillna(100)
visit_occurrence_visit_type_concept_id

# ### provider_id

# +
######################################
print('Getting the data from the database...')
######################################

foreign_key_df = pd.io.gbq.read_gbq('''
    SELECT
        vo.provider_id,
        COUNT(*) AS cnt
    FROM
       `{}.unioned_ehr_visit_occurrence` AS vo
    GROUP BY
        1
    ORDER BY
        2    
    '''.format(DATASET, DATASET, DATASET, DATASET, DATASET, DATASET),
                                    dialect='standard')
foreign_key_df.shape

print(foreign_key_df.shape[0], 'records received.')
# -

foreign_key_df.tail(10)

100 - round(100 * (foreign_key_df.loc[foreign_key_df["provider_id"].isnull(), ["cnt"]].iloc[0, 0]
                   + foreign_key_df.loc[(foreign_key_df["provider_id"] == 0), ["cnt"]].iloc[0, 0]) / sum(
    foreign_key_df.iloc[:, 1]), 1)

total_missing = foreign_key_df.loc[foreign_key_df["provider_id"].isnull(), ["cnt"]].iloc[0, 0] + \
                foreign_key_df.loc[(foreign_key_df["provider_id"] == 0), ["cnt"]].iloc[0, 0]
total_missing

# #### provider_id by sites

# +
######################################
print('Getting the data from the database...')
######################################

visit_occurrence_provider_id_df = pd.io.gbq.read_gbq('''
    WITH
        hpo_counts AS (
            SELECT
                mvo.src_hpo_id,
                COUNT(vo.person_id) as total_counts
            FROM
               `{}.unioned_ehr_visit_occurrence` AS vo
            INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{}._mapping_visit_occurrence`) AS mvo
                ON
                    vo.visit_occurrence_id=mvo.visit_occurrence_id
            GROUP BY
                1
        ),

        hpo_missing_counts AS (
            SELECT
                mvo.src_hpo_id,
                COUNT(vo.person_id) as missing_counts
            FROM
               `{}.unioned_ehr_visit_occurrence` AS vo
            INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{}._mapping_visit_occurrence`) AS mvo
                ON
                    vo.visit_occurrence_id=mvo.visit_occurrence_id
            GROUP BY
                1
        )

    SELECT
        hpo_counts.src_hpo_id,
        missing_counts,
        total_counts,
        round(100-100*(missing_counts/total_counts),1) AS success_rate
    FROM
       hpo_counts
    FULL OUTER JOIN
        hpo_missing_counts
            ON
                hpo_missing_counts.src_hpo_id=hpo_counts.src_hpo_id
    ORDER BY
        4
    '''.format(DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET,
               DATASET),
                                                     dialect='standard')
visit_occurrence_provider_id_df.shape

print(visit_occurrence_provider_id_df.shape[0], 'records received.')
# -

visit_occurrence_provider_id_df

visit_occurrence_provider_id_df = visit_occurrence_provider_id_df.rename(
    columns={"success_rate": "visit_occurrence_provider_id"})
visit_occurrence_provider_id_df = visit_occurrence_provider_id_df[["src_hpo_id", "visit_occurrence_provider_id"]]
visit_occurrence_provider_id_df = visit_occurrence_provider_id_df.fillna(100)
visit_occurrence_provider_id_df

# #### care_site_id BY SITE

# +
######################################
print('Getting the data from the database...')
######################################

visit_occurrence_care_site_id_df = pd.io.gbq.read_gbq('''
    WITH
        hpo_counts AS (
            SELECT
                src_hpo_id,
                COUNT(vo.person_id) as total_counts
            FROM
               `{}.unioned_ehr_visit_occurrence` AS vo
            INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{}._mapping_visit_occurrence`) AS mvo
                ON
                    vo.visit_occurrence_id=mvo.visit_occurrence_id
            GROUP BY
                1
        ),

        hpo_missing_counts AS (
            SELECT
                mvo.src_hpo_id,
                COUNT(vo.person_id) as missing_counts
            FROM
               `{}.unioned_ehr_visit_occurrence` AS vo
            INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{}._mapping_visit_occurrence`) AS mvo
                ON
                    vo.visit_occurrence_id=mvo.visit_occurrence_id
            WHERE
                (vo.care_site_id is null or vo.care_site_id=0) 
            GROUP BY
                1
        )

    SELECT
        hpo_counts.src_hpo_id,
        missing_counts,
        total_counts,
        round(100-100*(missing_counts/total_counts),1) AS success_rate
    FROM
       hpo_counts
    FULL OUTER JOIN
        hpo_missing_counts
            ON
                hpo_missing_counts.src_hpo_id=hpo_counts.src_hpo_id
    ORDER BY
        4
    '''.format(DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET,
               DATASET),
                                                      dialect='standard')
foreign_key_df.shape

print(visit_occurrence_care_site_id_df.shape[0], 'records received.')
# -

visit_occurrence_care_site_id_df = visit_occurrence_care_site_id_df.rename(
    columns={"success_rate": "visit_occurrence_care_site_id"})
visit_occurrence_care_site_id_df = visit_occurrence_care_site_id_df[["src_hpo_id", "visit_occurrence_care_site_id"]]
visit_occurrence_care_site_id_df = visit_occurrence_care_site_id_df.fillna(100)
visit_occurrence_care_site_id_df

# #### visit_source_concept_id BY SITE

# +
######################################
print('Getting the data from the database...')
######################################

visit_occurrence_visit_source_concept_id_df = pd.io.gbq.read_gbq('''
    WITH
        hpo_counts AS (
            SELECT
                src_hpo_id,
                COUNT(vo.person_id) as total_counts
            FROM
               `{}.unioned_ehr_visit_occurrence` AS vo
            INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{}._mapping_visit_occurrence`) AS mvo
                ON
                    vo.visit_occurrence_id=mvo.visit_occurrence_id
            GROUP BY
                1
        ),

        hpo_missing_counts AS (
            SELECT
                mvo.src_hpo_id,
                COUNT(vo.person_id) as missing_counts
            FROM
               `{}.unioned_ehr_visit_occurrence` AS vo
            INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{}._mapping_visit_occurrence`) AS mvo
                ON
                    vo.visit_occurrence_id=mvo.visit_occurrence_id
            WHERE
                (vo.visit_source_concept_id is null or vo.visit_source_concept_id=0) 
            GROUP BY
                1
        )

    SELECT
        hpo_counts.src_hpo_id,
        missing_counts,
        total_counts,
        round(100-100*(missing_counts/total_counts),1) AS success_rate
    FROM
       hpo_counts
    FULL OUTER JOIN
        hpo_missing_counts
            ON
                hpo_missing_counts.src_hpo_id=hpo_counts.src_hpo_id
    ORDER BY
        4
    '''.format(DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET,
               DATASET),
                                                                 dialect='standard')
visit_occurrence_visit_source_concept_id_df.shape

print(foreign_key_df.shape[0], 'records received.')
# -

visit_occurrence_visit_source_concept_id_df = visit_occurrence_visit_source_concept_id_df.rename(
    columns={"success_rate": "visit_occurrence_visit_source_concept_id"})
visit_occurrence_visit_source_concept_id_df = visit_occurrence_visit_source_concept_id_df[
    ["src_hpo_id", "visit_occurrence_visit_source_concept_id"]]
visit_occurrence_visit_source_concept_id_df = visit_occurrence_visit_source_concept_id_df.fillna(100)
visit_occurrence_visit_source_concept_id_df

# #### admitting_source_concept_id BY SITE

# +
######################################
print('Getting the data from the database...')
######################################

visit_occurrence_admitting_source_concept_id_df = pd.io.gbq.read_gbq('''
    WITH
        hpo_counts AS (
            SELECT
                src_hpo_id,
                COUNT(vo.person_id) as total_counts
            FROM
               `{}.unioned_ehr_visit_occurrence` AS vo
            INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{}._mapping_visit_occurrence`) AS mvo
                ON
                    vo.visit_occurrence_id=mvo.visit_occurrence_id
            GROUP BY
                1
        ),

        hpo_missing_counts AS (
            SELECT
                mvo.src_hpo_id,
                COUNT(vo.person_id) as missing_counts
            FROM
               `{}.unioned_ehr_visit_occurrence` AS vo
            INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{}._mapping_visit_occurrence`) AS mvo
                ON
                    vo.visit_occurrence_id=mvo.visit_occurrence_id
            WHERE
                (vo.admitting_source_concept_id is null or vo.admitting_source_concept_id=0) 
            GROUP BY
                1
        )

    SELECT
        hpo_counts.src_hpo_id,
        missing_counts,
        total_counts,
        round(100-100*(missing_counts/total_counts),1) AS success_rate
    FROM
       hpo_counts
    FULL OUTER JOIN
        hpo_missing_counts
            ON
                hpo_missing_counts.src_hpo_id=hpo_counts.src_hpo_id
    ORDER BY
        4
    '''.format(DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET,
               DATASET),
                                                                     dialect='standard')
visit_occurrence_admitting_source_concept_id_df.shape

print(foreign_key_df.shape[0], 'records received.')
# -

visit_occurrence_admitting_source_concept_id_df = visit_occurrence_admitting_source_concept_id_df.rename(
    columns={"success_rate": "visit_occurrence_admitting_source_concept_id"})
visit_occurrence_admitting_source_concept_id_df = visit_occurrence_admitting_source_concept_id_df[
    ["src_hpo_id", "visit_occurrence_admitting_source_concept_id"]]
visit_occurrence_admitting_source_concept_id_df = visit_occurrence_admitting_source_concept_id_df.fillna(100)
visit_occurrence_admitting_source_concept_id_df

# #### discharge_to_concept_id BY SITE

# +
######################################
print('Getting the data from the database...')
######################################

visit_occurrence_discharge_to_concept_id_df = pd.io.gbq.read_gbq('''
    WITH
        hpo_counts AS (
            SELECT
                src_hpo_id,
                COUNT(vo.person_id) as total_counts
            FROM
               `{}.unioned_ehr_visit_occurrence` AS vo
            INNER JOIN
                `{}.concept` AS c
                ON
                    vo.discharge_to_concept_id=c.concept_id
            INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{}._mapping_visit_occurrence`) AS mvo
                ON
                    vo.visit_occurrence_id=mvo.visit_occurrence_id
            GROUP BY
                1
        ),

        hpo_missing_counts AS (
            SELECT
                mvo.src_hpo_id,
                COUNT(vo.person_id) as missing_counts
            FROM
               `{}.unioned_ehr_visit_occurrence` AS vo
            INNER JOIN
                `{}.concept` AS c
                ON
                    vo.discharge_to_concept_id=c.concept_id
            INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{}._mapping_visit_occurrence`) AS mvo
                ON
                    vo.visit_occurrence_id=mvo.visit_occurrence_id
            WHERE
                (vo.discharge_to_concept_id is null or vo.discharge_to_concept_id=0) 
            GROUP BY
                1
        )

    SELECT
        hpo_counts.src_hpo_id,
        missing_counts,
        total_counts,
        round(100-100*(missing_counts/total_counts),1) AS success_rate
    FROM
       hpo_counts
    FULL OUTER JOIN
        hpo_missing_counts
            ON
                hpo_missing_counts.src_hpo_id=hpo_counts.src_hpo_id
    ORDER BY
        4
    '''.format(DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET,
               DATASET),
                                                                 dialect='standard')
visit_occurrence_discharge_to_concept_id_df.shape

print(foreign_key_df.shape[0], 'records received.')
# -

visit_occurrence_discharge_to_concept_id_df = visit_occurrence_discharge_to_concept_id_df.rename(
    columns={"success_rate": "visit_occurrence_discharge_to_concept_id"})
visit_occurrence_discharge_to_concept_id_df = visit_occurrence_discharge_to_concept_id_df[
    ["src_hpo_id", "visit_occurrence_discharge_to_concept_id"]]
visit_occurrence_discharge_to_concept_id_df = visit_occurrence_discharge_to_concept_id_df.fillna(100)
visit_occurrence_discharge_to_concept_id_df

# #### preceding_visit_occurrence_id BY SITE

# +
######################################
print('Getting the data from the database...')
######################################

visit_occurrence_preceding_visit_occurrence_id_df = pd.io.gbq.read_gbq('''
    WITH
        hpo_counts AS (
            SELECT
                src_hpo_id,
                COUNT(vo.person_id) as total_counts
            FROM
               `{}.unioned_ehr_visit_occurrence` AS vo
            INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{}._mapping_visit_occurrence`) AS mvo
                ON
                vo.visit_occurrence_id=mvo.visit_occurrence_id
            GROUP BY
                1
        ),

        hpo_missing_counts AS (
            SELECT
                mvo.src_hpo_id,
                COUNT(vo.person_id) as missing_counts
            FROM
               `{}.unioned_ehr_visit_occurrence` AS vo
            INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{}._mapping_visit_occurrence`) AS mvo
                ON
                    vo.visit_occurrence_id=mvo.visit_occurrence_id
            WHERE
                (vo.preceding_visit_occurrence_id is null or vo.preceding_visit_occurrence_id=0) 
            GROUP BY
                1
        )

    SELECT
        hpo_counts.src_hpo_id,
        missing_counts,
        total_counts,
        round(100-100*(missing_counts/total_counts),1) AS success_rate
    FROM
       hpo_counts
    FULL OUTER JOIN
        hpo_missing_counts
            ON
                hpo_missing_counts.src_hpo_id=hpo_counts.src_hpo_id
    ORDER BY
        4
    '''.format(DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET,
               DATASET),
                                                                       dialect='standard')
visit_occurrence_preceding_visit_occurrence_id_df.shape

print(foreign_key_df.shape[0], 'records received.')
# -

visit_occurrence_preceding_visit_occurrence_id_df = visit_occurrence_preceding_visit_occurrence_id_df.rename(
    columns={"success_rate": "visit_occurrence_preceding_visit_occurrence_id"})
visit_occurrence_preceding_visit_occurrence_id_df = visit_occurrence_preceding_visit_occurrence_id_df[
    ["src_hpo_id", "visit_occurrence_preceding_visit_occurrence_id"]]
visit_occurrence_preceding_visit_occurrence_id_df = visit_occurrence_preceding_visit_occurrence_id_df.fillna(100)
visit_occurrence_preceding_visit_occurrence_id_df

# ## Condition Occurrence Table

# #### condition_concept_id BY SITE

# +
######################################
print('Getting the data from the database...')
######################################

condition_occurrence_condition_concept_id_df = pd.io.gbq.read_gbq('''
    WITH
        hpo_counts AS (
            SELECT
                src_hpo_id,
                COUNT(co.person_id) as total_counts
            FROM
               `{}.unioned_ehr_condition_occurrence` AS co
            INNER JOIN
                `{}.concept` AS c
                ON
                    co.condition_concept_id=c.concept_id
            INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                    `{}._mapping_condition_occurrence`)  AS mco
                ON
                    co.condition_occurrence_id=mco.condition_occurrence_id
            GROUP BY
                1
        ),

        hpo_missing_counts AS (
            SELECT
                mco.src_hpo_id,
                COUNT(co.person_id) as missing_counts
            FROM
               `{}.unioned_ehr_condition_occurrence` AS co
            INNER JOIN
                `{}.concept` AS c
                ON
                    co.condition_concept_id=c.concept_id
            INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                    `{}._mapping_condition_occurrence`)  AS mco
                ON
                    co.condition_occurrence_id=mco.condition_occurrence_id
            WHERE
                (co.condition_concept_id is null or co.condition_concept_id=0) 
            GROUP BY
                1
        )

    SELECT
        hpo_counts.src_hpo_id,
        missing_counts,
        total_counts,
        round(100-100*(missing_counts/total_counts),1) AS success_rate
    FROM
       hpo_counts
    FULL OUTER JOIN
        hpo_missing_counts
            ON
                hpo_missing_counts.src_hpo_id=hpo_counts.src_hpo_id
    ORDER BY
        4
    '''.format(DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET,
               DATASET),
                                                                  dialect='standard')
condition_occurrence_condition_concept_id_df.shape

print(condition_occurrence_condition_concept_id_df.shape[0], 'records received.')
# -

condition_occurrence_condition_concept_id_df = condition_occurrence_condition_concept_id_df.rename(
    columns={"success_rate": "condition_occurrence_condition_concept_id"})
condition_occurrence_condition_concept_id_df = condition_occurrence_condition_concept_id_df[
    ["src_hpo_id", "condition_occurrence_condition_concept_id"]]
condition_occurrence_condition_concept_id_df = condition_occurrence_condition_concept_id_df.fillna(100)
condition_occurrence_condition_concept_id_df

# #### condition_type_concept_id BY SITE

# +
######################################
print('Getting the data from the database...')
######################################

condition_occurrence_condition_type_concept_id = pd.io.gbq.read_gbq('''
    WITH
        hpo_counts AS (
            SELECT
                src_hpo_id,
                COUNT(co.person_id) as total_counts
            FROM
               `{}.unioned_ehr_condition_occurrence` AS co
            INNER JOIN
                `{}.concept` AS c
                ON
                    co.condition_type_concept_id=c.concept_id
            INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                    `{}._mapping_condition_occurrence`)  AS mco
                ON
                    co.condition_occurrence_id=mco.condition_occurrence_id
            GROUP BY
                1
        ),

        hpo_missing_counts AS (
            SELECT
                mco.src_hpo_id,
                COUNT(co.person_id) as missing_counts
            FROM
               `{}.unioned_ehr_condition_occurrence` AS co
            INNER JOIN
                `{}.concept` AS c
                ON
                    co.condition_type_concept_id=c.concept_id
            INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                    `{}._mapping_condition_occurrence`)  AS mco
                ON
                    co.condition_occurrence_id=mco.condition_occurrence_id
            WHERE
                (co.condition_type_concept_id is null or co.condition_type_concept_id=0) 
            GROUP BY
                1
        )

    SELECT
        hpo_counts.src_hpo_id,
        missing_counts,
        total_counts,
        round(100-100*(missing_counts/total_counts),1) AS success_rate
    FROM
       hpo_counts
    FULL OUTER JOIN
        hpo_missing_counts
            ON
                hpo_missing_counts.src_hpo_id=hpo_counts.src_hpo_id
    ORDER BY
        4
    '''.format(DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET,
               DATASET),
                                                                    dialect='standard')
condition_occurrence_condition_type_concept_id.shape

print(condition_occurrence_condition_type_concept_id.shape[0], 'records received.')
# -

condition_occurrence_condition_type_concept_id = condition_occurrence_condition_type_concept_id.rename(
    columns={"success_rate": "condition_occurrence_condition_type_concept_id"})
condition_occurrence_condition_type_concept_id = condition_occurrence_condition_type_concept_id[
    ["src_hpo_id", "condition_occurrence_condition_type_concept_id"]]
condition_occurrence_condition_type_concept_id = condition_occurrence_condition_type_concept_id.fillna(100)
condition_occurrence_condition_type_concept_id

# ### Provider_id
#

# +
######################################
print('Getting the data from the database...')
######################################

foreign_key_df = pd.io.gbq.read_gbq('''
    SELECT
        co.provider_id,
        COUNT(*) as cnt
    FROM
       `{}.unioned_ehr_condition_occurrence` AS co
    GROUP BY
        1
    ORDER BY
        2
    '''.format(DATASET, DATASET, DATASET, DATASET, DATASET, DATASET),
                                    dialect='standard')
foreign_key_df.shape

print(foreign_key_df.shape[0], 'records received.')
# -

foreign_key_df.tail()

# ### visit_occurrence_id

# +
######################################
print('Getting the data from the database...')
######################################

foreign_key_df = pd.io.gbq.read_gbq('''
    SELECT
        co.visit_occurrence_id,
        COUNT(*) as cnt
    FROM
       `{}.unioned_ehr_condition_occurrence` AS co
    GROUP BY
        1
    ORDER BY
        2
    '''.format(DATASET, DATASET, DATASET, DATASET, DATASET, DATASET),
                                    dialect='standard')
foreign_key_df.shape

print(foreign_key_df.shape[0], 'records received.')
# -

foreign_key_df.tail()

# ### condition_source_concept_id

# +
######################################
print('Getting the data from the database...')
######################################

foreign_key_df = pd.io.gbq.read_gbq('''
    SELECT
        condition_source_concept_id,
        COUNT(*) as cnt
    FROM
       `{}.unioned_ehr_condition_occurrence` AS co
    GROUP BY
        1
    ORDER BY
        2
    '''.format(DATASET, DATASET, DATASET, DATASET, DATASET, DATASET),
                                    dialect='standard')
foreign_key_df.shape

print(foreign_key_df.shape[0], 'records received.')
# -

foreign_key_df.tail()

success_rate = 100 - round(
    100 * (foreign_key_df.loc[foreign_key_df["condition_source_concept_id"].isnull(), ["cnt"]].iloc[0, 0]
           + foreign_key_df.loc[(foreign_key_df["condition_source_concept_id"] == 0), ["cnt"]].iloc[0, 0]) / sum(
        foreign_key_df.iloc[:, 1]), 1)
print("success rate for condition_source_concept_id is: ", round(success_rate, 1))

# #### condition_source_concept_id BY SITE

# +
######################################
print('Getting the data from the database...')
######################################

condition_occurrence_condition_source_concept_id = pd.io.gbq.read_gbq('''
    WITH
        hpo_counts AS (
            SELECT
                src_hpo_id,
                COUNT(co.person_id) as total_counts
            FROM
               `{}.unioned_ehr_condition_occurrence` AS co
            INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                    `{}._mapping_condition_occurrence`)  AS mco
                ON
                    co.condition_occurrence_id=mco.condition_occurrence_id
            GROUP BY
                1
        ),

        hpo_missing_counts AS (
            SELECT
                mco.src_hpo_id,
                COUNT(co.person_id) as missing_counts
            FROM
               `{}.unioned_ehr_condition_occurrence` AS co
            INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                    `{}._mapping_condition_occurrence`)  AS mco
                ON
                    co.condition_occurrence_id=mco.condition_occurrence_id
            WHERE
                (co.condition_source_concept_id is null or co.condition_source_concept_id=0) 
            GROUP BY
                1
        )

    SELECT
        hpo_counts.src_hpo_id,
        missing_counts,
        total_counts,
        round(100-100*(missing_counts/total_counts),1) AS success_rate
    FROM
       hpo_counts
    FULL OUTER JOIN
        hpo_missing_counts
            ON
                hpo_missing_counts.src_hpo_id=hpo_counts.src_hpo_id
    ORDER BY
        4
    '''.format(DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET,
               DATASET),
                                                                      dialect='standard')
condition_occurrence_condition_source_concept_id.shape

print(condition_occurrence_condition_source_concept_id.shape[0], 'records received.')
# -

condition_occurrence_condition_source_concept_id = condition_occurrence_condition_source_concept_id.rename(
    columns={"success_rate": "condition_occurrence_condition_source_concept_id"})
condition_occurrence_condition_source_concept_id = condition_occurrence_condition_source_concept_id[
    ["src_hpo_id", "condition_occurrence_condition_source_concept_id"]]
condition_occurrence_condition_source_concept_id = condition_occurrence_condition_source_concept_id.fillna(100)
condition_occurrence_condition_source_concept_id

# #### condition_status_concept_id BY SITE

# +
######################################
print('Getting the data from the database...')
######################################

condition_occurrence_condition_status_concept_id = pd.io.gbq.read_gbq('''
    WITH
        hpo_counts AS (
            SELECT
                src_hpo_id,
                COUNT(co.person_id) as total_counts
            FROM
               `{}.unioned_ehr_condition_occurrence` AS co
            INNER JOIN
                `{}.concept` AS c
                ON
                    co.condition_status_concept_id=c.concept_id
            INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                    `{}._mapping_condition_occurrence`)  AS mco
                ON
                    co.condition_occurrence_id=mco.condition_occurrence_id
            GROUP BY
                1
        ),

        hpo_missing_counts AS (
            SELECT
                mco.src_hpo_id,
                COUNT(co.person_id) as missing_counts
            FROM
               `{}.unioned_ehr_condition_occurrence` AS co
            INNER JOIN
                `{}.concept` AS c
                ON
                    co.condition_status_concept_id=c.concept_id
            INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                    `{}._mapping_condition_occurrence`)  AS mco
                ON
                    co.condition_occurrence_id=mco.condition_occurrence_id
            WHERE
                (co.condition_status_concept_id is null or co.condition_status_concept_id=0) 
            GROUP BY
                1
        )

    SELECT
        hpo_counts.src_hpo_id,
        missing_counts,
        total_counts,
        round(100-100*(missing_counts/total_counts),1) AS success_rate
    FROM
       hpo_counts
    FULL OUTER JOIN
        hpo_missing_counts
            ON
                hpo_missing_counts.src_hpo_id=hpo_counts.src_hpo_id
    ORDER BY
        4
    '''.format(DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET,
               DATASET),
                                                                      dialect='standard')
condition_occurrence_condition_status_concept_id.shape

print(condition_occurrence_condition_status_concept_id.shape[0], 'records received.')
# -

condition_occurrence_condition_status_concept_id = condition_occurrence_condition_status_concept_id.rename(
    columns={"success_rate": "condition_occurrence_condition_status_concept_id"})
condition_occurrence_condition_status_concept_id = condition_occurrence_condition_status_concept_id[
    ["src_hpo_id", "condition_occurrence_condition_status_concept_id"]]
condition_occurrence_condition_status_concept_id = condition_occurrence_condition_status_concept_id.fillna(100)
condition_occurrence_condition_status_concept_id

# ## Drug Exposure Table

# ### person_id

# +
######################################
print('Getting the data from the database...')
######################################

foreign_key_df = pd.io.gbq.read_gbq('''
    SELECT
        COUNT(*) AS total,
        sum(case when (de.person_id is null or de.person_id=0) then 1 else 0 end) as missing
    FROM
       `{}.unioned_ehr_drug_exposure` AS de
    INNER JOIN
        `{}.unioned_ehr_observation` AS o
        ON
            de.person_id=o.person_id
    '''.format(DATASET, DATASET, DATASET, DATASET, DATASET, DATASET),
                                    dialect='standard')
foreign_key_df.shape

print(foreign_key_df.shape[0], 'records received.')
# -

foreign_key_df

# +

print("success rate for person_id is: ", round(100 - 100 * (foreign_key_df.iloc[0, 1] / foreign_key_df.iloc[0, 0]), 1))
# -

# #### drug_concept_id BY SITE

# +
######################################
print('Getting the data from the database...')
######################################

drug_exposure_drug_concept_id = pd.io.gbq.read_gbq('''
    WITH
        hpo_counts AS (
            SELECT
                src_hpo_id,
                COUNT(de.person_id) as total_counts
            FROM
               `{}.unioned_ehr_drug_exposure` AS de
            INNER JOIN
                `{}.concept` AS c
                ON
                    de.drug_concept_id=c.concept_id
            INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{}._mapping_drug_exposure`)  AS mde
                ON
                    de.drug_exposure_id=mde.drug_exposure_id
            GROUP BY
                1
        ),

        hpo_missing_counts AS (
            SELECT
                mde.src_hpo_id,
                COUNT(de.person_id) as missing_counts
            FROM
               `{}.unioned_ehr_drug_exposure` AS de
            INNER JOIN
                `{}.concept` AS c
                ON
                    de.drug_concept_id=c.concept_id
            INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{}._mapping_drug_exposure`)  AS mde
                ON
                    de.drug_exposure_id=mde.drug_exposure_id
            WHERE
                (de.drug_concept_id is null or de.drug_concept_id=0) 
            GROUP BY
                1
        )

    SELECT
        hpo_counts.src_hpo_id,
        missing_counts,
        total_counts,
        round(100-100*(missing_counts/total_counts),1) AS success_rate
    FROM
       hpo_counts
    FULL OUTER JOIN
        hpo_missing_counts
            ON
                hpo_missing_counts.src_hpo_id=hpo_counts.src_hpo_id
    ORDER BY
        4
    '''.format(DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET,
               DATASET),
                                                   dialect='standard')
drug_exposure_drug_concept_id.shape

print(drug_exposure_drug_concept_id.shape[0], 'records received.')
# -

drug_exposure_drug_concept_id = drug_exposure_drug_concept_id.rename(
    columns={"success_rate": "drug_exposure_drug_concept_id"})
drug_exposure_drug_concept_id = drug_exposure_drug_concept_id[["src_hpo_id", "drug_exposure_drug_concept_id"]]
drug_exposure_drug_concept_id = drug_exposure_drug_concept_id.fillna(100)
drug_exposure_drug_concept_id

# #### drug_type_concept_id BY SITE

# +
######################################
print('Getting the data from the database...')
######################################

drug_exposure_drug_type_concept_id = pd.io.gbq.read_gbq('''
    WITH
        hpo_counts AS (
            SELECT
                src_hpo_id,
                COUNT(de.person_id) as total_counts
            FROM
               `{}.unioned_ehr_drug_exposure` AS de
            INNER JOIN
                `{}.concept` AS c
                ON
                    de.drug_type_concept_id=c.concept_id
            INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{}._mapping_drug_exposure`)  AS mde
                ON
                    de.drug_exposure_id=mde.drug_exposure_id
            GROUP BY
                1
        ),

        hpo_missing_counts AS (
            SELECT
                mde.src_hpo_id,
                COUNT(de.person_id) as missing_counts
            FROM
               `{}.unioned_ehr_drug_exposure` AS de
            INNER JOIN
                `{}.concept` AS c
                ON
                    de.drug_type_concept_id=c.concept_id
            INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{}._mapping_drug_exposure`)  AS mde
                ON
                    de.drug_exposure_id=mde.drug_exposure_id
            WHERE
                (de.drug_type_concept_id is null or de.drug_type_concept_id=0) 
            GROUP BY
                1
        )
    SELECT
        hpo_counts.src_hpo_id,
        missing_counts,
        total_counts,
        round(100-100*(missing_counts/total_counts),1) AS success_rate
    FROM
       hpo_counts
    FULL OUTER JOIN
        hpo_missing_counts
            ON
                hpo_missing_counts.src_hpo_id=hpo_counts.src_hpo_id
    ORDER BY
        4
    '''.format(DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET,
               DATASET),
                                                        dialect='standard')
drug_exposure_drug_type_concept_id.shape

print(drug_exposure_drug_type_concept_id.shape[0], 'records received.')
# -

drug_exposure_drug_type_concept_id = drug_exposure_drug_type_concept_id.rename(
    columns={"success_rate": "condition_occurrence_drug_type_concept_id"})
drug_exposure_drug_type_concept_id = drug_exposure_drug_type_concept_id[
    ["src_hpo_id", "condition_occurrence_drug_type_concept_id"]]
drug_exposure_drug_type_concept_id = drug_exposure_drug_type_concept_id.fillna(100)
drug_exposure_drug_type_concept_id

# #### route_concept_id BY SITE

# +
######################################
print('Getting the data from the database...')
######################################

drug_exposure_route_concept_id = pd.io.gbq.read_gbq('''
    WITH
        hpo_counts AS (
            SELECT
                src_hpo_id,
                COUNT(de.person_id) as total_counts
            FROM
               `{}.unioned_ehr_drug_exposure` AS de
            INNER JOIN
                `{}.concept` AS c
                ON
                    de.route_concept_id=c.concept_id
            INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{}._mapping_drug_exposure`)  AS mde
                ON
                    de.drug_exposure_id=mde.drug_exposure_id
            GROUP BY
                1
        ),

        hpo_missing_counts AS (
            SELECT
                mde.src_hpo_id,
                COUNT(de.person_id) as missing_counts
            FROM
               `{}.unioned_ehr_drug_exposure` AS de
            INNER JOIN
                `{}.concept` AS c
                ON
                    de.route_concept_id=c.concept_id
            INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{}._mapping_drug_exposure`)  AS mde
                ON
                    de.drug_exposure_id=mde.drug_exposure_id
            WHERE
                (de.route_concept_id is null or de.route_concept_id=0) 
            GROUP BY
                1
        )
    SELECT
        hpo_counts.src_hpo_id,
        missing_counts,
        total_counts,
        round(100-100*(missing_counts/total_counts),1) AS success_rate
    FROM
       hpo_counts
    FULL OUTER JOIN
        hpo_missing_counts
            ON
                hpo_missing_counts.src_hpo_id=hpo_counts.src_hpo_id
    ORDER BY
        4
    '''.format(DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET,
               DATASET),
                                                    dialect='standard')
drug_exposure_route_concept_id.shape

print(drug_exposure_route_concept_id.shape[0], 'records received.')
# -

drug_exposure_route_concept_id = drug_exposure_route_concept_id.rename(
    columns={"success_rate": "drug_exposure_route_concept_id"})
drug_exposure_route_concept_id = drug_exposure_route_concept_id[["src_hpo_id", "drug_exposure_route_concept_id"]]
drug_exposure_route_concept_id = drug_exposure_route_concept_id.fillna(100)
drug_exposure_route_concept_id

# #### provider_id BY SITE

# +
######################################
print('Getting the data from the database...')
######################################

drug_exposure_provider_id = pd.io.gbq.read_gbq('''
    WITH
        hpo_counts AS (
            SELECT
                src_hpo_id,
                COUNT(de.person_id) as total_counts
            FROM
               `{}.unioned_ehr_drug_exposure` AS de
            INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{}._mapping_drug_exposure`)  AS mde
                ON
                    de.drug_exposure_id=mde.drug_exposure_id
            GROUP BY
                1
        ),

        hpo_missing_counts AS (
            SELECT
                mde.src_hpo_id,
                COUNT(de.person_id) as missing_counts
            FROM
               `{}.unioned_ehr_drug_exposure` AS de
            INNER JOIN
                (SELECT
                    DISTINCT * 
                    FROM
                     `{}._mapping_drug_exposure`)  AS mde
                ON
                    de.drug_exposure_id=mde.drug_exposure_id
            WHERE
                (de.provider_id is null or de.provider_id=0) 
            GROUP BY
                1
        )
    SELECT
        hpo_counts.src_hpo_id,
        missing_counts,
        total_counts,
        round(100-100*(missing_counts/total_counts),1) AS success_rate
    FROM
       hpo_counts
    FULL OUTER JOIN
        hpo_missing_counts
            ON
                hpo_missing_counts.src_hpo_id=hpo_counts.src_hpo_id
    ORDER BY
        4
    '''.format(DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET,
               DATASET),
                                               dialect='standard')
drug_exposure_provider_id.shape

print(drug_exposure_provider_id.shape[0], 'records received.')
# -

drug_exposure_provider_id = drug_exposure_provider_id.rename(columns={"success_rate": "drug_exposure_provider_id"})
drug_exposure_provider_id = drug_exposure_provider_id[["src_hpo_id", "drug_exposure_provider_id"]]
drug_exposure_provider_id = drug_exposure_provider_id.fillna(100)
drug_exposure_provider_id

# #### visit_occurrence_id BY SITE

# +
######################################
print('Getting the data from the database...')
######################################

drug_exposure_visit_occurrence_id = pd.io.gbq.read_gbq('''
    WITH
        hpo_counts AS (
            SELECT
                src_hpo_id,
                COUNT(de.person_id) as total_counts
            FROM
               `{}.unioned_ehr_drug_exposure` AS de
            INNER JOIN
                (SELECT
                    DISTINCT * 
                    FROM
                     `{}._mapping_drug_exposure`)  AS mde
                ON
                    de.drug_exposure_id=mde.drug_exposure_id
            GROUP BY
                1
        ),

        hpo_missing_counts AS (
            SELECT
                mde.src_hpo_id,
                COUNT(de.person_id) as missing_counts
            FROM
               `{}.unioned_ehr_drug_exposure` AS de
            INNER JOIN
                (SELECT
                    DISTINCT * 
                    FROM
                     `{}._mapping_drug_exposure`)  AS mde
                ON
                    de.drug_exposure_id=mde.drug_exposure_id
            WHERE
                (de.visit_occurrence_id is null or de.visit_occurrence_id=0) 
            GROUP BY
                1
        )
    SELECT
        hpo_counts.src_hpo_id,
        missing_counts,
        total_counts,
        round(100-100*(missing_counts/total_counts),1) AS success_rate
    FROM
       hpo_counts
    FULL OUTER JOIN
        hpo_missing_counts
            ON
                hpo_missing_counts.src_hpo_id=hpo_counts.src_hpo_id
    ORDER BY
        4
    '''.format(DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET,
               DATASET),
                                                       dialect='standard')
drug_exposure_visit_occurrence_id.shape

print(drug_exposure_visit_occurrence_id.shape[0], 'records received.')
# -

drug_exposure_visit_occurrence_id = drug_exposure_visit_occurrence_id.rename(
    columns={"success_rate": "drug_exposure_visit_occurrence_id"})
drug_exposure_visit_occurrence_id = drug_exposure_visit_occurrence_id[
    ["src_hpo_id", "drug_exposure_visit_occurrence_id"]]
drug_exposure_visit_occurrence_id = drug_exposure_visit_occurrence_id.fillna(100)
drug_exposure_visit_occurrence_id

# #### drug_source_concept_id BY SITE

# +
######################################
print('Getting the data from the database...')
######################################

drug_exposure_drug_source_concept_id = pd.io.gbq.read_gbq('''
    WITH
        hpo_counts AS (
            SELECT
                src_hpo_id,
                COUNT(de.person_id) as total_counts
            FROM
               `{}.unioned_ehr_drug_exposure` AS de
            INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{}._mapping_drug_exposure`)  AS mde
                ON
                    de.drug_exposure_id=mde.drug_exposure_id
            GROUP BY
                1
        ),

        hpo_missing_counts AS (
            SELECT
                mde.src_hpo_id,
                COUNT(de.person_id) as missing_counts
            FROM
               `{}.unioned_ehr_drug_exposure` AS de
            INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{}._mapping_drug_exposure`)  AS mde
                ON
                    de.drug_exposure_id=mde.drug_exposure_id
            WHERE
                (de.drug_source_concept_id is null or de.drug_source_concept_id=0) 
            GROUP BY
                1
        )
    SELECT
        hpo_counts.src_hpo_id,
        missing_counts,
        total_counts,
        round(100-100*(missing_counts/total_counts),1) AS success_rate
    FROM
       hpo_counts
    FULL OUTER JOIN
        hpo_missing_counts
            ON
                hpo_missing_counts.src_hpo_id=hpo_counts.src_hpo_id
    ORDER BY
        4
    '''.format(DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET,
               DATASET),
                                                          dialect='standard')
drug_exposure_drug_source_concept_id.shape

print(drug_exposure_drug_source_concept_id.shape[0], 'records received.')
# -

drug_exposure_drug_source_concept_id = drug_exposure_drug_source_concept_id.rename(
    columns={"success_rate": "drug_exposure_drug_source_concept_id"})
drug_exposure_drug_source_concept_id = drug_exposure_drug_source_concept_id[
    ["src_hpo_id", "drug_exposure_drug_source_concept_id"]]
drug_exposure_drug_source_concept_id = drug_exposure_drug_source_concept_id.fillna(100)
drug_exposure_drug_source_concept_id

# ## Measurement table

# #### measurement_concept_id BY SITE

# +
######################################
print('Getting the data from the database...')
######################################

measurement_measurement_concept_id = pd.io.gbq.read_gbq('''
    WITH
        hpo_counts AS (
            SELECT 
                src_hpo_id,
                COUNT(me.person_id) as total_counts
            FROM
               `{}.unioned_ehr_measurement` AS me
            INNER JOIN
                `{}.concept` AS c
                ON
                    me.measurement_concept_id=c.concept_id
            INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{}._mapping_measurement`)  AS mm
                ON
                    me.measurement_id=mm.measurement_id
            GROUP BY
                1
        ),

        hpo_missing_counts AS (
            SELECT
                mm.src_hpo_id,
                COUNT(me.person_id) as missing_counts
            FROM
               `{}.unioned_ehr_measurement` AS me
            INNER JOIN
                `{}.concept` AS c
                ON
                    me.measurement_concept_id=c.concept_id
            INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{}._mapping_measurement`)  AS mm
                ON
                    me.measurement_id=mm.measurement_id
            WHERE
                (me.measurement_concept_id is null or me.measurement_concept_id=0) 
            GROUP BY
                1
        )

    SELECT
        hpo_counts.src_hpo_id,
        missing_counts,
        total_counts,
        round(100-100*(missing_counts/total_counts),1) AS success_rate
    FROM
       hpo_counts
    FULL OUTER JOIN
        hpo_missing_counts
            ON
                hpo_missing_counts.src_hpo_id=hpo_counts.src_hpo_id
    ORDER BY
        4
    '''.format(DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET,
               DATASET),
                                                        dialect='standard')
measurement_measurement_concept_id.shape

print(measurement_measurement_concept_id.shape[0], 'records received.')
# -

measurement_measurement_concept_id = measurement_measurement_concept_id.rename(
    columns={"success_rate": "measurement_measurement_concept_id"})
measurement_measurement_concept_id = measurement_measurement_concept_id[
    ["src_hpo_id", "measurement_measurement_concept_id"]]
measurement_measurement_concept_id = measurement_measurement_concept_id.fillna(100)
measurement_measurement_concept_id

# #### measurement_type_concept_id BY SITE

# +
######################################
print('Getting the data from the database...')
######################################

measurement_measurement_type_concept_id = pd.io.gbq.read_gbq('''
    WITH
        hpo_counts AS (
            SELECT 
                src_hpo_id,
                COUNT(me.person_id) as total_counts
            FROM
               `{}.unioned_ehr_measurement` AS me
            INNER JOIN
                `{}.concept` AS c
                ON
                    me.measurement_type_concept_id=c.concept_id
            INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{}._mapping_measurement`)  AS mm
                ON
                    me.measurement_id=mm.measurement_id
            GROUP BY
                1
        ),

        hpo_missing_counts AS (
            SELECT
                mm.src_hpo_id,
                COUNT(me.person_id) as missing_counts
            FROM
               `{}.unioned_ehr_measurement` AS me
            INNER JOIN
                `{}.concept` AS c
                ON
                    me.measurement_type_concept_id=c.concept_id
            INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{}._mapping_measurement`)  AS mm
                ON
                    me.measurement_id=mm.measurement_id
            WHERE
                (me.measurement_type_concept_id is null or me.measurement_type_concept_id=0) 
            GROUP BY
                1
        )

    SELECT
        hpo_counts.src_hpo_id,
        missing_counts,
        total_counts,
        round(100-100*(missing_counts/total_counts),1) AS success_rate
    FROM
       hpo_counts
    FULL OUTER JOIN
        hpo_missing_counts
            ON
                hpo_missing_counts.src_hpo_id=hpo_counts.src_hpo_id
    ORDER BY
        4
    '''.format(DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET,
               DATASET),
                                                             dialect='standard')
measurement_measurement_type_concept_id.shape

print(measurement_measurement_type_concept_id.shape[0], 'records received.')
# -

measurement_measurement_type_concept_id = measurement_measurement_type_concept_id.rename(
    columns={"success_rate": "measurement_measurement_type_concept_id"})
measurement_measurement_type_concept_id = measurement_measurement_type_concept_id[
    ["src_hpo_id", "measurement_measurement_type_concept_id"]]
measurement_measurement_type_concept_id = measurement_measurement_type_concept_id.fillna(100)
measurement_measurement_type_concept_id

# #### operator_concept_id BY SITE

# +
######################################
print('Getting the data from the database...')
######################################

measurement_operator_concept_id = pd.io.gbq.read_gbq('''
    WITH
        hpo_counts AS (
            SELECT 
                src_hpo_id,
                COUNT(me.person_id) as total_counts
            FROM
               `{}.unioned_ehr_measurement` AS me
            INNER JOIN
                `{}.concept` AS c
                ON
                    me.operator_concept_id=c.concept_id
            INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{}._mapping_measurement`)  AS mm
                ON
                    me.measurement_id=mm.measurement_id
            GROUP BY
                1
        ),

        hpo_missing_counts AS (
            SELECT
                mm.src_hpo_id,
                COUNT(me.person_id) as missing_counts
            FROM
               `{}.unioned_ehr_measurement` AS me
            INNER JOIN
                `{}.concept` AS c
                ON
                    me.operator_concept_id=c.concept_id
            INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{}._mapping_measurement`)  AS mm
                ON
                    me.measurement_id=mm.measurement_id
            WHERE
                (me.operator_concept_id is null or me.operator_concept_id=0) 
            GROUP BY
                1
        )

    SELECT
        hpo_counts.src_hpo_id,
        missing_counts,
        total_counts,
        round(100-100*(missing_counts/total_counts),1) AS success_rate
    FROM
       hpo_counts
    FULL OUTER JOIN
        hpo_missing_counts
            ON
                hpo_missing_counts.src_hpo_id=hpo_counts.src_hpo_id
    ORDER BY
        4
    '''.format(DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET,
               DATASET),
                                                     dialect='standard')
measurement_operator_concept_id.shape

print(measurement_operator_concept_id.shape[0], 'records received.')
# -

measurement_operator_concept_id = measurement_operator_concept_id.rename(
    columns={"success_rate": "measurement_operator_concept_id"})
measurement_operator_concept_id = measurement_operator_concept_id[["src_hpo_id", "measurement_operator_concept_id"]]
measurement_operator_concept_id = measurement_operator_concept_id.fillna(100)
measurement_operator_concept_id

# #### value_as_concept_id BY SITE

# +
######################################
print('Getting the data from the database...')
######################################

measurement_value_as_concept_id = pd.io.gbq.read_gbq('''
    WITH
        hpo_counts AS (
            SELECT 
                src_hpo_id,
                COUNT(me.person_id) as total_counts
            FROM
               `{}.unioned_ehr_measurement` AS me
            INNER JOIN
                `{}.concept` AS c
                ON
                    me.value_as_concept_id=c.concept_id
            INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{}._mapping_measurement`)  AS mm
                ON
                    me.measurement_id=mm.measurement_id
            GROUP BY
                1
        ),

        hpo_missing_counts AS (
            SELECT
                mm.src_hpo_id,
                COUNT(me.person_id) as missing_counts
            FROM
               `{}.unioned_ehr_measurement` AS me
            INNER JOIN
                `{}.concept` AS c
                ON
                    me.value_as_concept_id=c.concept_id
            INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{}._mapping_measurement`)  AS mm
                ON
                    me.measurement_id=mm.measurement_id
            WHERE
                (me.value_as_concept_id is null or me.value_as_concept_id=0) 
            GROUP BY
                1
        )

    SELECT
        hpo_counts.src_hpo_id,
        missing_counts,
        total_counts,
        round(100-100*(missing_counts/total_counts),1) AS success_rate
    FROM
       hpo_counts
    FULL OUTER JOIN
        hpo_missing_counts
            ON
                hpo_missing_counts.src_hpo_id=hpo_counts.src_hpo_id
    ORDER BY
        4
    '''.format(DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET,
               DATASET),
                                                     dialect='standard')
measurement_value_as_concept_id.shape

print(measurement_value_as_concept_id.shape[0], 'records received.')
# -

measurement_value_as_concept_id = measurement_value_as_concept_id.rename(
    columns={"success_rate": "measurement_value_as_concept_id"})
measurement_value_as_concept_id = measurement_value_as_concept_id[["src_hpo_id", "measurement_value_as_concept_id"]]
measurement_value_as_concept_id = measurement_value_as_concept_id.fillna(100)
measurement_value_as_concept_id

# #### unit_concept_id BY SITE

# +
######################################
print('Getting the data from the database...')
######################################

measurement_unit_concept_id = pd.io.gbq.read_gbq('''
    WITH
        hpo_counts AS (
            SELECT 
                src_hpo_id,
                COUNT(me.person_id) as total_counts
            FROM
               `{}.unioned_ehr_measurement` AS me
            INNER JOIN
                `{}.concept` AS c
                ON
                    me.unit_concept_id=c.concept_id
            INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{}._mapping_measurement`)  AS mm
                ON
                    me.measurement_id=mm.measurement_id
            GROUP BY
                1
        ),

        hpo_missing_counts AS (
            SELECT
                mm.src_hpo_id,
                COUNT(me.person_id) as missing_counts
            FROM
               `{}.unioned_ehr_measurement` AS me
            INNER JOIN
                `{}.concept` AS c
                ON
                    me.unit_concept_id=c.concept_id
            INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{}._mapping_measurement`)  AS mm
                ON
                    me.measurement_id=mm.measurement_id
            WHERE
                (me.unit_concept_id is null or me.unit_concept_id=0) 
            GROUP BY
                1
        )

    SELECT
        hpo_counts.src_hpo_id,
        missing_counts,
        total_counts,
        round(100-100*(missing_counts/total_counts),1) AS success_rate
    FROM
       hpo_counts
    FULL OUTER JOIN
        hpo_missing_counts
            ON
                hpo_missing_counts.src_hpo_id=hpo_counts.src_hpo_id
    ORDER BY
        4
    '''.format(DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET,
               DATASET),
                                                 dialect='standard')
measurement_unit_concept_id.shape

print(measurement_unit_concept_id.shape[0], 'records received.')
# -

measurement_unit_concept_id = measurement_unit_concept_id.rename(
    columns={"success_rate": "measurement_unit_concept_id"})
measurement_unit_concept_id = measurement_unit_concept_id[["src_hpo_id", "measurement_unit_concept_id"]]
measurement_unit_concept_id = measurement_unit_concept_id.fillna(100)
measurement_unit_concept_id

# #### provider_id BY SITE

# +
######################################
print('Getting the data from the database...')
######################################

measurement_provider_id = pd.io.gbq.read_gbq('''
    WITH
        hpo_counts AS (
            SELECT 
                src_hpo_id,
                COUNT(me.person_id) as total_counts
            FROM
               `{}.unioned_ehr_measurement` AS me
            INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{}._mapping_measurement`)  AS mm
                ON
                    me.measurement_id=mm.measurement_id
            GROUP BY
                1
        ),

        hpo_missing_counts AS (
            SELECT
                mm.src_hpo_id,
                COUNT(me.person_id) as missing_counts
            FROM
               `{}.unioned_ehr_measurement` AS me
            INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{}._mapping_measurement`)  AS mm
                ON
                    me.measurement_id=mm.measurement_id
            WHERE
                (me.provider_id is null or me.provider_id=0) 
            GROUP BY
                1
        )

    SELECT
        hpo_counts.src_hpo_id,
        missing_counts,
        total_counts,
        round(100-100*(missing_counts/total_counts),1) AS success_rate
    FROM
       hpo_counts
    FULL OUTER JOIN
        hpo_missing_counts
            ON
                hpo_missing_counts.src_hpo_id=hpo_counts.src_hpo_id
    ORDER BY
        3
    '''.format(DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET,
               DATASET),
                                             dialect='standard')
measurement_provider_id.shape

print(measurement_provider_id.shape[0], 'records received.')
# -

measurement_provider_id = measurement_provider_id.rename(columns={"success_rate": "measurement_provider_id"})
measurement_provider_id = measurement_provider_id[["src_hpo_id", "measurement_provider_id"]]
measurement_provider_id = measurement_provider_id.fillna(100)
measurement_provider_id

# #### visit_occurrence_id BY SITE

# +
######################################
print('Getting the data from the database...')
######################################

measurement_visit_occurrence_id = pd.io.gbq.read_gbq('''
    WITH
        hpo_counts AS (
            SELECT 
                src_hpo_id,
                COUNT(me.person_id) as total_counts
            FROM
               `{}.unioned_ehr_measurement` AS me
            INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{}._mapping_measurement`)  AS mm
                ON
                    me.measurement_id=mm.measurement_id
            GROUP BY
                1
        ),

        hpo_missing_counts AS (
            SELECT
                mm.src_hpo_id,
                COUNT(me.person_id) as missing_counts
            FROM
               `{}.unioned_ehr_measurement` AS me
            INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{}._mapping_measurement`)  AS mm
                ON
                    me.measurement_id=mm.measurement_id
            WHERE
                (me.visit_occurrence_id is null or me.visit_occurrence_id=0) 
            GROUP BY
                1
        )

    SELECT
        hpo_counts.src_hpo_id,
        missing_counts,
        total_counts,
        round(100-100*(missing_counts/total_counts),1) AS success_rate
    FROM
       hpo_counts
    FULL OUTER JOIN
        hpo_missing_counts
            ON
                hpo_missing_counts.src_hpo_id=hpo_counts.src_hpo_id
    ORDER BY
        3
    '''.format(DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET,
               DATASET),
                                                     dialect='standard')
measurement_visit_occurrence_id.shape

print(measurement_visit_occurrence_id.shape[0], 'records received.')
# -

measurement_visit_occurrence_id = measurement_visit_occurrence_id.rename(
    columns={"success_rate": "measurement_visit_occurrence_id"})
measurement_visit_occurrence_id = measurement_visit_occurrence_id[["src_hpo_id", "measurement_visit_occurrence_id"]]
measurement_visit_occurrence_id = measurement_visit_occurrence_id.fillna(100)
measurement_visit_occurrence_id

# #### measurement_source_concept_id BY SITE

# +
######################################
print('Getting the data from the database...')
######################################

measurement_measurement_source_concept_id = pd.io.gbq.read_gbq('''
    WITH
        hpo_counts AS (
            SELECT 
                src_hpo_id,
                COUNT(me.person_id) as total_counts
            FROM
               `{}.unioned_ehr_measurement` AS me
            INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{}._mapping_measurement`)  AS mm
                ON
                    me.measurement_id=mm.measurement_id
            GROUP BY
                1
        ),

        hpo_missing_counts AS (
            SELECT
                mm.src_hpo_id,
                COUNT(me.person_id) as missing_counts
            FROM
               `{}.unioned_ehr_measurement` AS me
            INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{}._mapping_measurement`)  AS mm
                ON
                    me.measurement_id=mm.measurement_id
            WHERE
                (me.measurement_source_concept_id is null or me.measurement_source_concept_id=0) 
            GROUP BY
                1
        )

    SELECT
        hpo_counts.src_hpo_id,
        missing_counts,
        total_counts,
        round(100-100*(missing_counts/total_counts),1) AS success_rate
    FROM
       hpo_counts
    FULL OUTER JOIN
        hpo_missing_counts
            ON
                hpo_missing_counts.src_hpo_id=hpo_counts.src_hpo_id
    ORDER BY
        3
    '''.format(DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET,
               DATASET),
                                                               dialect='standard')
measurement_measurement_source_concept_id.shape

print(measurement_measurement_source_concept_id.shape[0], 'records received.')
# -

measurement_measurement_source_concept_id = measurement_measurement_source_concept_id.rename(
    columns={"success_rate": "measurement_measurement_source_concept_id"})
measurement_measurement_source_concept_id = measurement_measurement_source_concept_id[
    ["src_hpo_id", "measurement_measurement_source_concept_id"]]
measurement_measurement_source_concept_id = measurement_measurement_source_concept_id.fillna(100)
measurement_measurement_source_concept_id

# ## Procedure Occurrence

# #### procedure_concept_id BY SITE

# +
######################################
print('Getting the data from the database...')
######################################

procedure_occurrence_procedure_concept_id = pd.io.gbq.read_gbq('''
    WITH
        hpo_counts AS (
            SELECT 
                src_hpo_id,
                COUNT(t1.person_id) as total_counts
            FROM
                `{}.unioned_ehr_procedure_occurrence` AS t1
            INNER JOIN
                `{}.concept` AS c
                ON
                    t1.procedure_concept_id=c.concept_id
            INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{}._mapping_procedure_occurrence`)  AS t2
                ON
                    t1.procedure_occurrence_id=t2.procedure_occurrence_id
            GROUP BY
                1
        ),

        hpo_missing_counts AS (
            SELECT
                t2.src_hpo_id,
                COUNT(t1.person_id) as missing_counts
            FROM
                `{}.unioned_ehr_procedure_occurrence` AS t1
            INNER JOIN
                `{}.concept` AS c
                ON
                    t1.procedure_concept_id=c.concept_id
            INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{}._mapping_procedure_occurrence`)  AS t2
                ON
                    t1.procedure_occurrence_id=t2.procedure_occurrence_id
            WHERE
                (t1.procedure_concept_id is null or t1.procedure_concept_id=0) 
            GROUP BY
                1
        )

    SELECT
        hpo_counts.src_hpo_id,
        missing_counts,
        total_counts,
        round(100-100*(missing_counts/total_counts),1) AS success_rate
    FROM
       hpo_counts
    FULL OUTER JOIN
        hpo_missing_counts
            ON
                hpo_missing_counts.src_hpo_id=hpo_counts.src_hpo_id
    ORDER BY
        4
    '''.format(DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET,
               DATASET),
                                                               dialect='standard')
procedure_occurrence_procedure_concept_id.shape

print(procedure_occurrence_procedure_concept_id.shape[0], 'records received.')
# -

procedure_occurrence_procedure_concept_id = procedure_occurrence_procedure_concept_id.rename(
    columns={"success_rate": "procedure_occurrence_procedure_concept_id"})
procedure_occurrence_procedure_concept_id = procedure_occurrence_procedure_concept_id[
    ["src_hpo_id", "procedure_occurrence_procedure_concept_id"]]
procedure_occurrence_procedure_concept_id = procedure_occurrence_procedure_concept_id.fillna(100)
procedure_occurrence_procedure_concept_id

# #### procedure_type_concept_id BY SITE

# +
######################################
print('Getting the data from the database...')
######################################

procedure_occurrence_procedure_type_concept_id = pd.io.gbq.read_gbq('''
    WITH
        hpo_counts AS (
            SELECT 
                src_hpo_id,
                COUNT(t1.person_id) as total_counts
            FROM
                `{}.unioned_ehr_procedure_occurrence` AS t1
            INNER JOIN
                `{}.concept` AS c
                ON
                    t1.procedure_type_concept_id=c.concept_id
            INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{}._mapping_procedure_occurrence`)  AS t2
                ON
                    t1.procedure_occurrence_id=t2.procedure_occurrence_id
            GROUP BY
                1
        ),

        hpo_missing_counts AS (
            SELECT
                t2.src_hpo_id,
                COUNT(t1.person_id) as missing_counts
            FROM
                `{}.unioned_ehr_procedure_occurrence` AS t1
            INNER JOIN
                `{}.concept` AS c
                ON
                    t1.procedure_concept_id=c.concept_id
            INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{}._mapping_procedure_occurrence`)  AS t2
                ON
                    t1.procedure_occurrence_id=t2.procedure_occurrence_id
            WHERE
                (t1.procedure_type_concept_id is null or t1.procedure_type_concept_id=0) 
            GROUP BY
                1
        )

    SELECT
        hpo_counts.src_hpo_id,
        missing_counts,
        total_counts,
        round(100-100*(missing_counts/total_counts),1) AS success_rate
    FROM
       hpo_counts
    FULL OUTER JOIN
        hpo_missing_counts
            ON
                hpo_missing_counts.src_hpo_id=hpo_counts.src_hpo_id
    ORDER BY
        4
    '''.format(DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET,
               DATASET),
                                                                    dialect='standard')
procedure_occurrence_procedure_type_concept_id.shape

print(procedure_occurrence_procedure_type_concept_id.shape[0], 'records received.')
# -

procedure_occurrence_procedure_type_concept_id = procedure_occurrence_procedure_type_concept_id.rename(
    columns={"success_rate": "procedure_occurrence_procedure_type_concept_id"})
procedure_occurrence_procedure_type_concept_id = procedure_occurrence_procedure_type_concept_id[
    ["src_hpo_id", "procedure_occurrence_procedure_type_concept_id"]]
procedure_occurrence_procedure_type_concept_id = procedure_occurrence_procedure_type_concept_id.fillna(100)
procedure_occurrence_procedure_type_concept_id

# #### modifier_concept_id BY SITE

# +
######################################
print('Getting the data from the database...')
######################################

procedure_occurrence_modifier_concept_id = pd.io.gbq.read_gbq('''
    WITH
        hpo_counts AS (
            SELECT 
                src_hpo_id,
                COUNT(t1.person_id) as total_counts
            FROM
                `{}.unioned_ehr_procedure_occurrence` AS t1
            INNER JOIN
                `{}.concept` AS c
                ON
                    t1.modifier_concept_id=c.concept_id
            INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{}._mapping_procedure_occurrence`)  AS t2
                ON
                    t1.procedure_occurrence_id=t2.procedure_occurrence_id
            GROUP BY
                1
        ),

        hpo_missing_counts AS (
            SELECT
                t2.src_hpo_id,
                COUNT(t1.person_id) as missing_counts
            FROM
                `{}.unioned_ehr_procedure_occurrence` AS t1
            INNER JOIN
                `{}.concept` AS c
                ON
                    t1.modifier_concept_id=c.concept_id
            INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{}._mapping_procedure_occurrence`)  AS t2
                ON
                    t1.procedure_occurrence_id=t2.procedure_occurrence_id
            WHERE
                (t1.modifier_concept_id is null or t1.modifier_concept_id=0) 
            GROUP BY
                1
        )

    SELECT
        hpo_counts.src_hpo_id,
        missing_counts,
        total_counts,
        round(100-100*(missing_counts/total_counts),1) AS success_rate
    FROM
       hpo_counts
    FULL OUTER JOIN
        hpo_missing_counts
            ON
                hpo_missing_counts.src_hpo_id=hpo_counts.src_hpo_id
    ORDER BY
        4
    '''.format(DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET,
               DATASET),
                                                              dialect='standard')
procedure_occurrence_modifier_concept_id.shape

print(procedure_occurrence_modifier_concept_id.shape[0], 'records received.')
# -

procedure_occurrence_modifier_concept_id = procedure_occurrence_modifier_concept_id.rename(
    columns={"success_rate": "procedure_occurrence_modifier_concept_id"})
procedure_occurrence_modifier_concept_id = procedure_occurrence_modifier_concept_id[
    ["src_hpo_id", "procedure_occurrence_modifier_concept_id"]]
procedure_occurrence_modifier_concept_id = procedure_occurrence_modifier_concept_id.fillna(100)
procedure_occurrence_modifier_concept_id

# #### provider_id BY SITE

# +
######################################
print('Getting the data from the database...')
######################################

procedure_occurrence_provider_id = pd.io.gbq.read_gbq('''
    WITH
        hpo_counts AS (
            SELECT 
                src_hpo_id,
                COUNT(t1.person_id) as total_counts
            FROM
                `{}.unioned_ehr_procedure_occurrence` AS t1
            INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{}._mapping_procedure_occurrence`)  AS t2
                ON
                    t1.procedure_occurrence_id=t2.procedure_occurrence_id
            GROUP BY
                1
        ),

        hpo_missing_counts AS (
            SELECT
                t2.src_hpo_id,
                COUNT(t1.person_id) as missing_counts
            FROM
                `{}.unioned_ehr_procedure_occurrence` AS t1
            INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{}._mapping_procedure_occurrence`)  AS t2
                ON
                    t1.procedure_occurrence_id=t2.procedure_occurrence_id
            WHERE
                (t1.provider_id is null or t1.provider_id=0) 
            GROUP BY
                1
        )

    SELECT
        hpo_counts.src_hpo_id,
        missing_counts,
        total_counts,
        round(100-100*(missing_counts/total_counts),1) AS success_rate
    FROM
       hpo_counts
    FULL OUTER JOIN
        hpo_missing_counts
            ON
                hpo_missing_counts.src_hpo_id=hpo_counts.src_hpo_id
    ORDER BY
        4
    '''.format(DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET,
               DATASET),
                                                      dialect='standard')
procedure_occurrence_provider_id.shape

print(procedure_occurrence_provider_id.shape[0], 'records received.')
# -

procedure_occurrence_provider_id = procedure_occurrence_provider_id.rename(
    columns={"success_rate": "procedure_occurrence_provider_id"})
procedure_occurrence_provider_id = procedure_occurrence_provider_id[["src_hpo_id", "procedure_occurrence_provider_id"]]
procedure_occurrence_provider_id = procedure_occurrence_provider_id.fillna(100)
procedure_occurrence_provider_id

# #### visit_occurrence_id BY SITE

# +
######################################
print('Getting the data from the database...')
######################################

procedure_occurrence_visit_occurrence_id = pd.io.gbq.read_gbq('''
    WITH
        hpo_counts AS (
            SELECT 
                src_hpo_id,
                COUNT(t1.person_id) as total_counts
            FROM
                `{}.unioned_ehr_procedure_occurrence` AS t1
            INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{}._mapping_procedure_occurrence`)  AS t2
                ON
                    t1.procedure_occurrence_id=t2.procedure_occurrence_id
            GROUP BY
                1
        ),

        hpo_missing_counts AS (
            SELECT
                t2.src_hpo_id,
                COUNT(t1.person_id) as missing_counts
            FROM
                `{}.unioned_ehr_procedure_occurrence` AS t1
            INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{}._mapping_procedure_occurrence`)  AS t2
                ON
                    t1.procedure_occurrence_id=t2.procedure_occurrence_id
            WHERE
                (t1.visit_occurrence_id is null or t1.visit_occurrence_id=0) 
            GROUP BY
                1
        )

    SELECT
        hpo_counts.src_hpo_id,
        missing_counts,
        total_counts,
        round(100-100*(missing_counts/total_counts),1) AS success_rate
    FROM
       hpo_counts
    FULL OUTER JOIN
        hpo_missing_counts
            ON
                hpo_missing_counts.src_hpo_id=hpo_counts.src_hpo_id
    ORDER BY
        4
    '''.format(DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET,
               DATASET),
                                                              dialect='standard')
procedure_occurrence_visit_occurrence_id.shape

print(procedure_occurrence_visit_occurrence_id.shape[0], 'records received.')
# -

procedure_occurrence_visit_occurrence_id = procedure_occurrence_visit_occurrence_id.rename(
    columns={"success_rate": "procedure_occurrence_visit_occurrence_id"})
procedure_occurrence_visit_occurrence_id = procedure_occurrence_visit_occurrence_id[
    ["src_hpo_id", "procedure_occurrence_visit_occurrence_id"]]
procedure_occurrence_visit_occurrence_id = procedure_occurrence_visit_occurrence_id.fillna(100)
procedure_occurrence_visit_occurrence_id

# #### procedure_source_concept_id BY SITE

# +
######################################
print('Getting the data from the database...')
######################################

procedure_occurrence_procedure_source_concept_id = pd.io.gbq.read_gbq('''
    WITH
        hpo_counts AS (
            SELECT 
                src_hpo_id,
                COUNT(t1.person_id) as total_counts
            FROM
                `{}.unioned_ehr_procedure_occurrence` AS t1
            INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{}._mapping_procedure_occurrence`)  AS t2
                ON
                    t1.procedure_occurrence_id=t2.procedure_occurrence_id
            GROUP BY
                1
        ),

        hpo_missing_counts AS (
            SELECT
                t2.src_hpo_id,
                COUNT(t1.person_id) as missing_counts
            FROM
                `{}.unioned_ehr_procedure_occurrence` AS t1
            INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{}._mapping_procedure_occurrence`)  AS t2
                ON
                    t1.procedure_occurrence_id=t2.procedure_occurrence_id
            WHERE
                (t1.procedure_source_concept_id is null or t1.procedure_source_concept_id=0) 
            GROUP BY
                1
        )

    SELECT
        hpo_counts.src_hpo_id,
        missing_counts,
        total_counts,
        round(100-100*(missing_counts/total_counts),1) AS success_rate
    FROM
       hpo_counts
    FULL OUTER JOIN
        hpo_missing_counts
            ON
                hpo_missing_counts.src_hpo_id=hpo_counts.src_hpo_id
    ORDER BY
        4
    '''.format(DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET,
               DATASET),
                                                                      dialect='standard')
procedure_occurrence_procedure_source_concept_id.shape

print(procedure_occurrence_procedure_source_concept_id.shape[0], 'records received.')
# -

procedure_occurrence_procedure_source_concept_id = procedure_occurrence_procedure_source_concept_id.rename(
    columns={"success_rate": "procedure_occurrence_procedure_source_concept_id"})
procedure_occurrence_procedure_source_concept_id = procedure_occurrence_procedure_source_concept_id[
    ["src_hpo_id", "procedure_occurrence_procedure_source_concept_id"]]
procedure_occurrence_procedure_source_concept_id = procedure_occurrence_procedure_source_concept_id.fillna(100)
procedure_occurrence_procedure_source_concept_id

# ## Device Exposure

# #### device_concept_id BY SITE

# +
######################################
print('Getting the data from the database...')
######################################

device_exposure_device_concept_id = pd.io.gbq.read_gbq('''
    WITH
        hpo_counts AS (
            SELECT 
                src_hpo_id,
                COUNT(t1.person_id) as total_counts
            FROM
                `{}.unioned_ehr_device_exposure` AS t1
            INNER JOIN
                `{}.concept` AS c
                ON
                    t1.device_concept_id=c.concept_id
            INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{}._mapping_device_exposure`)  AS t2
                ON
                    t1.device_exposure_id=t2.device_exposure_id
            GROUP BY
                1
        ),

        hpo_missing_counts AS (
            SELECT
                t2.src_hpo_id,
                COUNT(t1.person_id) as missing_counts
            FROM
                `{}.unioned_ehr_device_exposure` AS t1
            INNER JOIN
                `{}.concept` AS c
                ON
                    t1.device_concept_id=c.concept_id
            INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{}._mapping_device_exposure`)  AS t2
                ON
                    t1.device_exposure_id=t2.device_exposure_id
            WHERE
                (t1.device_concept_id is null or t1.device_concept_id=0) 
            GROUP BY
                1
        )

    SELECT
        hpo_counts.src_hpo_id,
        missing_counts,
        total_counts,
        round(100-100*(missing_counts/total_counts),1) AS success_rate
    FROM
       hpo_counts
    FULL OUTER JOIN
        hpo_missing_counts
            ON
                hpo_missing_counts.src_hpo_id=hpo_counts.src_hpo_id
    ORDER BY
        4
    '''.format(DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET,
               DATASET),
                                                       dialect='standard')
device_exposure_device_concept_id.shape

print(device_exposure_device_concept_id.shape[0], 'records received.')
# -

device_exposure_device_concept_id = device_exposure_device_concept_id.rename(
    columns={"success_rate": "device_exposure_device_concept_id"})
device_exposure_device_concept_id = device_exposure_device_concept_id[
    ["src_hpo_id", "device_exposure_device_concept_id"]]
device_exposure_device_concept_id = device_exposure_device_concept_id.fillna(100)
device_exposure_device_concept_id

# #### device_type_concept_id BY SITE

# +
######################################
print('Getting the data from the database...')
######################################

device_exposure_device_type_concept_id = pd.io.gbq.read_gbq('''
    WITH
        hpo_counts AS (
            SELECT 
                src_hpo_id,
                COUNT(t1.person_id) as total_counts
            FROM
                `{}.unioned_ehr_device_exposure` AS t1
            INNER JOIN
                `{}.concept` AS c
                ON
                    t1.device_type_concept_id=c.concept_id
            INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{}._mapping_device_exposure`)  AS t2
                ON
                    t1.device_exposure_id=t2.device_exposure_id
            GROUP BY
                1
        ),

        hpo_missing_counts AS (
            SELECT
                t2.src_hpo_id,
                COUNT(t1.person_id) as missing_counts
            FROM
                `{}.unioned_ehr_device_exposure` AS t1
            INNER JOIN
                `{}.concept` AS c
                ON
                    t1.device_type_concept_id=c.concept_id
            INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{}._mapping_device_exposure`)  AS t2
                ON
                    t1.device_exposure_id=t2.device_exposure_id
            WHERE
                (t1.device_type_concept_id is null or t1.device_type_concept_id=0) 
            GROUP BY
                1
        )

    SELECT
        hpo_counts.src_hpo_id,
        missing_counts,
        total_counts,
        round(100-100*(missing_counts/total_counts),1) AS success_rate
    FROM
       hpo_counts
    FULL OUTER JOIN
        hpo_missing_counts
            ON
                hpo_missing_counts.src_hpo_id=hpo_counts.src_hpo_id
    ORDER BY
        4
    '''.format(DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET,
               DATASET),
                                                            dialect='standard')
device_exposure_device_type_concept_id.shape

print(device_exposure_device_type_concept_id.shape[0], 'records received.')
# -

device_exposure_device_type_concept_id = device_exposure_device_type_concept_id.rename(
    columns={"success_rate": "device_exposure_device_type_concept_id"})
device_exposure_device_type_concept_id = device_exposure_device_type_concept_id[
    ["src_hpo_id", "device_exposure_device_type_concept_id"]]
device_exposure_device_type_concept_id = device_exposure_device_type_concept_id.fillna(100)
device_exposure_device_type_concept_id

# #### provider_id BY SITE

# +
######################################
print('Getting the data from the database...')
######################################

device_exposure_provider_id = pd.io.gbq.read_gbq('''
    WITH
        hpo_counts AS (
            SELECT 
                src_hpo_id,
                COUNT(t1.person_id) as total_counts
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
        ),

        hpo_missing_counts AS (
            SELECT
                t2.src_hpo_id,
                COUNT(t1.person_id) as missing_counts
            FROM
                `{}.unioned_ehr_device_exposure` AS t1
            INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{}._mapping_device_exposure`)  AS t2
                ON
                    t1.device_exposure_id=t2.device_exposure_id
            WHERE
                (t1.provider_id is null or t1.provider_id=0) 
            GROUP BY
                1
        )

    SELECT
        hpo_counts.src_hpo_id,
        missing_counts,
        total_counts,
        round(100-100*(missing_counts/total_counts),1) AS success_rate
    FROM
       hpo_counts
    FULL OUTER JOIN
        hpo_missing_counts
            ON
                hpo_missing_counts.src_hpo_id=hpo_counts.src_hpo_id
    ORDER BY
        4
    '''.format(DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET,
               DATASET),
                                                 dialect='standard')
device_exposure_provider_id.shape

print(device_exposure_provider_id.shape[0], 'records received.')
# -

device_exposure_provider_id = device_exposure_provider_id.rename(
    columns={"success_rate": "device_exposure_provider_id"})
device_exposure_provider_id = device_exposure_provider_id[["src_hpo_id", "device_exposure_provider_id"]]
device_exposure_provider_id = device_exposure_provider_id.fillna(100)
device_exposure_provider_id

# #### visit_occurrence_id BY SITE

# +
######################################
print('Getting the data from the database...')
######################################

device_exposure_visit_occurrence_id = pd.io.gbq.read_gbq('''
    WITH
        hpo_counts AS (
            SELECT 
                src_hpo_id,
                COUNT(t1.person_id) as total_counts
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
        ),

        hpo_missing_counts AS (
            SELECT
                t2.src_hpo_id,
                COUNT(t1.person_id) as missing_counts
            FROM
                `{}.unioned_ehr_device_exposure` AS t1
            INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{}._mapping_device_exposure`)  AS t2
                ON
                    t1.device_exposure_id=t2.device_exposure_id
            WHERE
                (t1.visit_occurrence_id is null or t1.visit_occurrence_id=0) 
            GROUP BY
                1
        )

    SELECT
        hpo_counts.src_hpo_id,
        missing_counts,
        total_counts,
        round(100-100*(missing_counts/total_counts),1) AS success_rate
    FROM
       hpo_counts
    FULL OUTER JOIN
        hpo_missing_counts
            ON
                hpo_missing_counts.src_hpo_id=hpo_counts.src_hpo_id
    ORDER BY
        4
    '''.format(DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET,
               DATASET),
                                                         dialect='standard')
device_exposure_visit_occurrence_id.shape

print(device_exposure_visit_occurrence_id.shape[0], 'records received.')
# -

device_exposure_visit_occurrence_id = device_exposure_visit_occurrence_id.rename(
    columns={"success_rate": "device_exposure_visit_occurrence_id"})
device_exposure_visit_occurrence_id = device_exposure_visit_occurrence_id[
    ["src_hpo_id", "device_exposure_visit_occurrence_id"]]
device_exposure_visit_occurrence_id = device_exposure_visit_occurrence_id.fillna(100)
device_exposure_visit_occurrence_id

# #### device_source_concept_id BY SITE

# +
######################################
print('Getting the data from the database...')
######################################

device_exposure_device_source_concept_id = pd.io.gbq.read_gbq('''
    WITH
        hpo_counts AS (
            SELECT 
                src_hpo_id,
                COUNT(t1.person_id) as total_counts
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
        ),

        hpo_missing_counts AS (
            SELECT
                t2.src_hpo_id,
                COUNT(t1.person_id) as missing_counts
            FROM
                `{}.unioned_ehr_device_exposure` AS t1
            INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{}._mapping_device_exposure`)  AS t2
                ON
                    t1.device_exposure_id=t2.device_exposure_id
            WHERE
                (t1.device_source_concept_id is null or t1.device_source_concept_id=0) 
            GROUP BY
                1
        )

    SELECT
        hpo_counts.src_hpo_id,
        missing_counts,
        total_counts,
        round(100-100*(missing_counts/total_counts),1) AS success_rate
    FROM
       hpo_counts
    FULL OUTER JOIN
        hpo_missing_counts
            ON
                hpo_missing_counts.src_hpo_id=hpo_counts.src_hpo_id
    '''.format(DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET,
               DATASET),
                                                              dialect='standard')
device_exposure_device_source_concept_id.shape

print(device_exposure_device_source_concept_id.shape[0], 'records received.')
# -

device_exposure_device_source_concept_id

device_exposure_device_source_concept_id = device_exposure_device_source_concept_id.rename(
    columns={"success_rate": "device_exposure_device_source_concept_id"})
device_exposure_device_source_concept_id = device_exposure_device_source_concept_id[
    ["src_hpo_id", "device_exposure_device_source_concept_id"]]
device_exposure_device_source_concept_id = device_exposure_device_source_concept_id.fillna(100)
device_exposure_device_source_concept_id

datas = [visit_occurrence_visit_type_concept_id,
         visit_occurrence_provider_id_df,
         visit_occurrence_care_site_id_df,
         visit_occurrence_visit_source_concept_id_df,
         visit_occurrence_admitting_source_concept_id_df,
         visit_occurrence_discharge_to_concept_id_df,
         visit_occurrence_preceding_visit_occurrence_id_df,
         condition_occurrence_condition_concept_id_df,
         condition_occurrence_condition_type_concept_id,
         condition_occurrence_condition_source_concept_id,
         condition_occurrence_condition_status_concept_id,
         drug_exposure_drug_concept_id,
         drug_exposure_drug_type_concept_id,
         drug_exposure_route_concept_id,
         drug_exposure_provider_id,
         drug_exposure_visit_occurrence_id,
         drug_exposure_drug_source_concept_id,
         measurement_measurement_concept_id,
         measurement_measurement_type_concept_id,
         measurement_operator_concept_id,
         measurement_value_as_concept_id,
         measurement_unit_concept_id,
         measurement_provider_id,
         measurement_visit_occurrence_id,
         measurement_measurement_source_concept_id,
         procedure_occurrence_procedure_concept_id,
         procedure_occurrence_procedure_type_concept_id,
         procedure_occurrence_modifier_concept_id,
         procedure_occurrence_provider_id,
         procedure_occurrence_visit_occurrence_id,
         procedure_occurrence_procedure_source_concept_id,
         device_exposure_device_concept_id,
         device_exposure_device_type_concept_id,
         device_exposure_provider_id,
         device_exposure_visit_occurrence_id,
         device_exposure_device_source_concept_id]

master_df = visit_occurrence_visit_concept_id_df

for filename in datas:
    master_df = pd.merge(master_df, filename, on='src_hpo_id', how='outer')

# +

master_df = pd.merge(master_df, site_df, on='src_hpo_id', how='outer')
# -

master_df

master_df = master_df.fillna("No Data")
master_df

master_df.to_csv("data\\foreign.csv")
