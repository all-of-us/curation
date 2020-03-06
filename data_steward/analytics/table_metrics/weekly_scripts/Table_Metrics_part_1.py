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

# %reload_ext google.cloud.bigquery

client = bigquery.Client()

# %load_ext google.cloud.bigquery

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

DATASET = ''

import os
import sys
from datetime import datetime
from datetime import date
from datetime import time
from datetime import timedelta
import time

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
    order by 1
    '''.format(DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET,
               DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET,
               DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET,
               DATASET, DATASET, DATASET, DATASET, DATASET),
                              dialect='standard')
print(site_map.shape[0], 'records received.')
# -

site_map

site_df = pd.merge(site_map, site_df, how='outer', on='src_hpo_id')

site_df

# # There should not be duplicate rows.

# ## visit_occurrence table

# +
######################################
print('Getting the data from the database...')
######################################

foreign_key_df = pd.io.gbq.read_gbq('''
    SELECT
       src_hpo_id,
       person_id, visit_concept_id, visit_start_date, visit_start_datetime, visit_end_date, visit_end_datetime,
       visit_type_concept_id, provider_id, care_site_id, visit_source_value, visit_source_concept_id,
       admitting_source_concept_id, admitting_source_value, discharge_to_concept_id,
       discharge_to_source_value, preceding_visit_occurrence_id,
        COUNT(*) as cnt
    FROM
       `{}.unioned_ehr_visit_occurrence` AS t1
    INNER JOIN
        (SELECT
            DISTINCT * 
    FROM
         `{}._mapping_visit_occurrence`) AS t2
    ON
        t1.visit_occurrence_id=t2.visit_occurrence_id
    WHERE
        t1.visit_concept_id!=0 AND t1.visit_concept_id IS NOT NULL AND
        t1.person_id!=0 and t1.person_id IS NOT NULL 
    GROUP BY
        1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
    HAVING 
        COUNT(*) > 1
    ORDER BY
        1,2,3,4,5,6,7,8,9
    '''.format(DATASET, DATASET, DATASET, DATASET, DATASET, DATASET),
                                    dialect='standard')
print(foreign_key_df.shape[0], 'records received.')
# -

foreign_key_df.head()

visit_occurrence = foreign_key_df.groupby(
    ['src_hpo_id']).size().reset_index().rename(columns={
        0: 'visit_occurrence'
    }).sort_values(["visit_occurrence"]).set_index("src_hpo_id")
visit_occurrence = visit_occurrence.reset_index()
visit_occurrence

# ## condition_occurrence table

# +
######################################
print('Getting the data from the database...')
######################################

foreign_key_df = pd.io.gbq.read_gbq('''
    SELECT
     src_hpo_id,
person_id, condition_concept_id, condition_start_date, condition_start_datetime, condition_end_date,
condition_end_datetime, condition_type_concept_id, stop_reason, provider_id, visit_occurrence_id,
condition_source_value, condition_source_concept_id, condition_status_source_value, condition_status_concept_id,
        COUNT(*) as cnt
    FROM
       `{}.unioned_ehr_condition_occurrence` AS t1
    INNER JOIN
        (SELECT
            DISTINCT * 
    FROM
         `{}._mapping_condition_occurrence`) AS t2
    ON
        t1.condition_occurrence_id=t2.condition_occurrence_id
    WHERE
        t1.condition_concept_id!=0 AND t1.condition_concept_id IS NOT NULL AND
        t1.person_id!=0 and t1.person_id IS NOT NULL 
    GROUP BY
        1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
    HAVING 
        COUNT(*) > 1
    ORDER BY
        1,2,3,4,5,6,7,8,9,10,11,12,13,14
    '''.format(DATASET, DATASET, DATASET, DATASET, DATASET, DATASET),
                                    dialect='standard')
print(foreign_key_df.shape[0], 'records received.')
# -

foreign_key_df.head()

condition_occurrence = foreign_key_df.groupby(
    ['src_hpo_id']).size().reset_index().rename(columns={
        0: 'condition_occurrence'
    }).sort_values(["condition_occurrence"]).set_index("src_hpo_id")
condition_occurrence = condition_occurrence.reset_index()
condition_occurrence

# +
# test=foreign_key_df.loc[foreign_key_df["src_hpo_id"]=="cpmc_usc",:]
# -

# test.head()

# +
# ######################################
# print('Getting the data from the database...')
# ######################################

# foreign_key_df = pd.io.gbq.read_gbq('''
#     SELECT
#      src_hpo_id,
# person_id, condition_concept_id, condition_start_date, condition_start_datetime, condition_end_date,
# condition_end_datetime, condition_type_concept_id, stop_reason, provider_id, visit_occurrence_id,
# condition_source_value, condition_source_concept_id, condition_status_source_value, condition_status_concept_id,
#         COUNT(*) as cnt
#     FROM
#        `{}.unioned_ehr_condition_occurrence` AS t1
#     INNER JOIN
#         (SELECT
#             DISTINCT *
#     FROM
#          `{}._mapping_condition_occurrence`) AS t2
#     ON
#         t1.condition_occurrence_id=t2.condition_occurrence_id
#     WHERE
#         t1.condition_concept_id!=0 AND t1.condition_concept_id IS NOT NULL AND
#         t1.person_id=154704129 and t1.person_id IS NOT NULL
#     GROUP BY
#         1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
#     HAVING
#         COUNT(*) > 1
#     ORDER BY
#         1,2,3,4,5,6,7,8,9,10,11,12,13,14
#     '''.format(DATASET, DATASET,DATASET,DATASET,DATASET,DATASET),
#                     dialect='standard')
# print(foreign_key_df.shape[0], 'records received.')

# +
# foreign_key_df

# +
# ######################################
# print('Getting the data from the database...')
# ######################################

# foreign_key_df = pd.io.gbq.read_gbq('''
#     SELECT
#      src_hpo_id,
#      t1.*
#     FROM
#        `{}.unioned_ehr_condition_occurrence` AS t1
#     INNER JOIN
#         (SELECT
#             DISTINCT *
#     FROM
#          `{}._mapping_condition_occurrence`) AS t2
#     ON
#         t1.condition_occurrence_id=t2.condition_occurrence_id
#     WHERE
#         t1.condition_concept_id!=0 AND t1.condition_concept_id IS NOT NULL AND
#         t1.person_id!=0 and t1.person_id IS NOT NULL and t1.person_id=154704129
#         and t1.condition_concept_id=4157332 and t1.condition_source_concept_id=45600511
#         and condition_source_value="C50.919" and condition_start_date="2018-09-12"
#     ORDER BY
#         1,2,3,4,5,6,7,8,9,10,11,12,13,14
#     '''.format(DATASET, DATASET,DATASET,DATASET,DATASET,DATASET),
#                     dialect='standard')
# print(foreign_key_df.shape[0], 'records received.')

# +
# foreign_key_df
# -

# ## drug_exposure table

# +
######################################
print('Getting the data from the database...')
######################################

foreign_key_df = pd.io.gbq.read_gbq('''
    SELECT
        src_hpo_id,
person_id, drug_concept_id, drug_exposure_start_date,drug_exposure_start_datetime, 
drug_exposure_end_date,drug_exposure_end_datetime, verbatim_end_date, drug_type_concept_id,
stop_reason, refills, quantity, 
days_supply, sig, route_concept_id, lot_number, provider_id, visit_occurrence_id, drug_source_value,
drug_source_concept_id, route_source_value, dose_unit_source_value,
        COUNT(*) as cnt
    FROM
       `{}.unioned_ehr_drug_exposure` AS t1
    INNER JOIN
        (SELECT
            DISTINCT * 
    FROM
         `{}._mapping_drug_exposure`) AS t2
    ON
        t1.drug_exposure_id=t2.drug_exposure_id
    WHERE
        t1.drug_concept_id!=0 AND t1.drug_concept_id IS NOT NULL AND
        t1.person_id!=0 and t1.person_id IS NOT NULL 
    GROUP BY
        1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22
    HAVING 
        COUNT(*) > 1
    ORDER BY
        1,2,3
    '''.format(DATASET, DATASET, DATASET, DATASET, DATASET, DATASET),
                                    dialect='standard')
print(foreign_key_df.shape[0], 'records received.')
# -

foreign_key_df.head()

drug_exposure = foreign_key_df.groupby(['src_hpo_id'
                                       ]).size().reset_index().rename(columns={
                                           0: 'drug_exposure'
                                       }).sort_values(["drug_exposure"
                                                      ]).set_index("src_hpo_id")
drug_exposure = drug_exposure.reset_index()
drug_exposure

# ## measurement table

# +
######################################
print('Getting the data from the database...')
######################################

foreign_key_df = pd.io.gbq.read_gbq('''
    SELECT
        src_hpo_id,
person_id, measurement_concept_id, measurement_date, measurement_datetime, measurement_type_concept_id, 
operator_concept_id, value_as_number, value_as_concept_id, unit_concept_id, range_low, 
range_high, provider_id, visit_occurrence_id,
measurement_source_value, measurement_source_concept_id, unit_source_value, value_source_value,
        COUNT(*) as cnt
    FROM
       `{}.unioned_ehr_measurement` AS t1
    INNER JOIN
        (SELECT
            DISTINCT * 
    FROM
         `{}._mapping_measurement`) AS t2
    ON
        t1.measurement_id=t2.measurement_id
    WHERE
        t1.measurement_concept_id!=0 AND t1.measurement_concept_id IS NOT NULL AND
        t1.person_id!=0 and t1.person_id IS NOT NULL 
    GROUP BY
        1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18
    HAVING 
        COUNT(*) > 1
    ORDER BY
        1,2,3
    '''.format(DATASET, DATASET, DATASET, DATASET, DATASET, DATASET),
                                    dialect='standard')
print(foreign_key_df.shape[0], 'records received.')
# -

foreign_key_df.head()

measurement = foreign_key_df.groupby(['src_hpo_id'
                                     ]).size().reset_index().rename(columns={
                                         0: 'measurement'
                                     }).sort_values(["measurement"
                                                    ]).set_index("src_hpo_id")
measurement = measurement.reset_index()
measurement

# ## procedure_occurrence

# +
######################################
print('Getting the data from the database...')
######################################

foreign_key_df = pd.io.gbq.read_gbq('''
    SELECT
        src_hpo_id,
        person_id, procedure_concept_id, procedure_date, procedure_datetime, procedure_type_concept_id, modifier_concept_id,
        quantity, provider_id, visit_occurrence_id, procedure_source_value, procedure_source_concept_id, qualifier_source_value,
        COUNT(*) as cnt
    FROM
       `{}.unioned_ehr_procedure_occurrence` AS t1
    INNER JOIN
        (SELECT
            DISTINCT * 
    FROM
         `{}._mapping_procedure_occurrence`) AS t2
    ON
        t1.procedure_occurrence_id=t2.procedure_occurrence_id
    WHERE
        t1.procedure_concept_id!=0 AND t1.procedure_concept_id IS NOT NULL AND
        t1.person_id!=0 and t1.person_id IS NOT NULL 
    GROUP BY
        1,2,3,4,5,6,7,8,9,10,11,12,13
    HAVING 
        COUNT(*) > 1
    ORDER BY
        1,2,3,4,5,6,7,8,9,10,11,12,13,14
    '''.format(DATASET, DATASET, DATASET, DATASET, DATASET, DATASET),
                                    dialect='standard')
print(foreign_key_df.shape[0], 'records received.')
# -

foreign_key_df.head()

procedure_occurrence = foreign_key_df.groupby(
    ['src_hpo_id']).size().reset_index().rename(columns={
        0: 'procedure_occurrence'
    }).sort_values(["procedure_occurrence"]).set_index("src_hpo_id")
procedure_occurrence = procedure_occurrence.reset_index()
procedure_occurrence

# ## observation table

# +
######################################
print('Getting the data from the database...')
######################################

foreign_key_df = pd.io.gbq.read_gbq('''
    SELECT
        src_hpo_id,
        person_id, observation_concept_id, observation_date, observation_datetime, observation_type_concept_id, value_as_number, 
        value_as_string, value_as_concept_id, qualifier_concept_id, unit_concept_id, provider_id, visit_occurrence_id, 
        observation_source_value, observation_source_concept_id, unit_source_value, qualifier_source_value, value_source_concept_id,
        value_source_value, questionnaire_response_id,
        COUNT(*) as cnt
    FROM
       `{}.unioned_ehr_observation` AS t1
    INNER JOIN
        (SELECT
            DISTINCT * 
    FROM
         `{}._mapping_observation`) AS t2
    ON
        t1.observation_id=t2.observation_id
    WHERE
        t1.observation_concept_id!=0 AND t1.observation_concept_id IS NOT NULL AND
        t1.person_id!=0 and t1.person_id IS NOT NULL 
    GROUP BY
        1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20
    HAVING 
        COUNT(*) > 1
    ORDER BY
        1,2,3,4,5,6,7,8,9,10,11,12,13,14
    '''.format(DATASET, DATASET, DATASET, DATASET, DATASET, DATASET),
                                    dialect='standard')
print(foreign_key_df.shape[0], 'records received.')
# -

foreign_key_df.head()

observation = foreign_key_df.groupby(['src_hpo_id'
                                     ]).size().reset_index().rename(columns={
                                         0: 'observation'
                                     }).sort_values(["observation"
                                                    ]).set_index("src_hpo_id")
observation = observation.reset_index()
observation

# ## provider table

# +
######################################
print('Getting the data from the database...')
######################################

foreign_key_df = pd.io.gbq.read_gbq('''
    SELECT
        provider_name, NPI, DEA, specialty_concept_id, care_site_id, year_of_birth, 
        gender_concept_id, provider_source_value, specialty_source_value, 
        specialty_source_concept_id, gender_source_value, gender_source_concept_id,
        COUNT(*) as cnt
    FROM
       `{}.unioned_ehr_provider` AS t1
    GROUP BY
        1,2,3,4,5,6,7,8,9,10,11,12
    HAVING 
        COUNT(*) > 1
    ORDER BY
        1,2,3,4,5,6,7,8,9,10,11,12
    '''.format(DATASET, DATASET, DATASET, DATASET, DATASET, DATASET),
                                    dialect='standard')
print(foreign_key_df.shape[0], 'records received.')
# -

foreign_key_df.head()

# ## device_exposure table

# +
######################################
print('Getting the data from the database...')
######################################

foreign_key_df = pd.io.gbq.read_gbq('''
    SELECT
        src_hpo_id,
        person_id, device_concept_id, device_exposure_start_date, device_exposure_start_datetime, device_exposure_end_date, 
        device_exposure_end_datetime, device_type_concept_id, unique_device_id, quantity, provider_id, 
        visit_occurrence_id, device_source_value, device_source_concept_id,
        COUNT(*) as cnt
    FROM
       `{}.unioned_ehr_device_exposure` AS t1
    INNER JOIN
        (SELECT
            DISTINCT * 
    FROM
         `{}._mapping_device_exposure`) AS t2
    ON
        t1.device_exposure_id=t2.device_exposure_id
    WHERE
        t1.device_concept_id!=0 AND t1.device_concept_id IS NOT NULL AND
        t1.person_id!=0 and t1.person_id IS NOT NULL 
    GROUP BY
        1,2,3,4,5,6,7,8,9,10,11,12,13,14
    HAVING 
        COUNT(*) > 1
    ORDER BY
        1,2,3,4,5,6,7,8,9,10,11,12,13,14
    '''.format(DATASET, DATASET, DATASET, DATASET, DATASET, DATASET),
                                    dialect='standard')
print(foreign_key_df.shape[0], 'records received.')
# -

foreign_key_df.head()

device_exposure = foreign_key_df.groupby(
    ['src_hpo_id']).size().reset_index().rename(columns={
        0: 'device_exposure'
    }).sort_values(["device_exposure"]).set_index("src_hpo_id")
device_exposure = device_exposure.reset_index()
device_exposure

# ## death table

# +
######################################
print('Getting the data from the database...')
######################################

foreign_key_df = pd.io.gbq.read_gbq('''
    SELECT
        person_id,death_date, death_datetime, death_type_concept_id, cause_concept_id, cause_source_value, cause_source_concept_id,
        COUNT(*) as cnt
    FROM
       `{}.unioned_ehr_death` AS t1
    WHERE
        t1.death_date IS NOT NULL AND t1.person_id IS NOT NULL 
    GROUP BY
        1,2,3,4,5,6,7
    HAVING 
        COUNT(*) > 1    
    ORDER BY
        1,2,3,4,5,6,7
    '''.format(DATASET, DATASET, DATASET, DATASET, DATASET, DATASET),
                                    dialect='standard')
print(foreign_key_df.shape[0], 'records received.')
# -

foreign_key_df.head()

# ## care_site table

# +
######################################
print('Getting the data from the database...')
######################################

foreign_key_df = pd.io.gbq.read_gbq('''
    SELECT
        place_of_service_concept_id, location_id, place_of_service_source_value,
        care_site_name, care_site_source_value,
        COUNT(*) as cnt
    FROM
       `{}.unioned_ehr_care_site` AS t1
    GROUP BY
        1,2,3,4,5
    HAVING 
        COUNT(*) > 1
    ORDER BY
        1,2,3,4,5
    '''.format(DATASET, DATASET, DATASET, DATASET, DATASET, DATASET),
                                    dialect='standard')
print(foreign_key_df.shape[0], 'records received.')
# -

foreign_key_df.head()

# ## Sites combined

sites_success = pd.merge(visit_occurrence,
                         condition_occurrence,
                         how='outer',
                         on='src_hpo_id')
sites_success = pd.merge(sites_success,
                         drug_exposure,
                         how='outer',
                         on='src_hpo_id')
sites_success = pd.merge(sites_success,
                         measurement,
                         how='outer',
                         on='src_hpo_id')
sites_success = pd.merge(sites_success,
                         procedure_occurrence,
                         how='outer',
                         on='src_hpo_id')
sites_success = pd.merge(sites_success,
                         device_exposure,
                         how='outer',
                         on='src_hpo_id')
sites_success = pd.merge(sites_success,
                         observation,
                         how='outer',
                         on='src_hpo_id')


sites_success = sites_success.fillna(0)
sites_success[["visit_occurrence", "condition_occurrence", "drug_exposure", "measurement", "procedure_occurrence",
               "device_exposure", "observation"]] \
    = sites_success[["visit_occurrence", "condition_occurrence", "drug_exposure", "measurement", "procedure_occurrence",
                     "device_exposure", "observation"]].astype(int)

sites_success

sites_success = pd.merge(sites_success, site_df, how='outer', on='src_hpo_id')

sites_success = sites_success.fillna(0)

sites_success

sites_success.to_csv("{cwd}\duplicates.csv".format(cwd = cwd))

# # Below is used to define the 'concept success rate' of the source_concept_ids
#
# ### NOTE: This is not a useful metric for most sites but has 'fringe' cases of utility. Source concept IDs are NOT expected to be of standard concept.

# ### 20.Dataframe (row for each hpo_id) Condition_occurrence table, condition_source_concept_id field

condition_concept_df = pd.io.gbq.read_gbq('''
    WITH
        data1 AS (
            SELECT
                src_hpo_id,
                COUNT(*) AS condition_total_row
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
        ),

        data2 AS (
            SELECT
                src_hpo_id,
                COUNT(*) AS condition_well_defined_row
            FROM
               `{}.unioned_ehr_condition_occurrence` AS t1
            INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{}._mapping_condition_occurrence`)  AS t2
            ON
                t1.condition_occurrence_id=t2.condition_occurrence_id
            INNER JOIN
                `{}.concept` as t3
            ON
                t3.concept_id = t1.condition_source_concept_id
            WHERE 
                 t3.domain_id="Condition" and t3.standard_concept="S"
            GROUP BY
                1
        ),
        
        
        data3 AS (
            SELECT
                src_hpo_id,
                COUNT(*) AS condition_total_zero
            FROM
               `{}.unioned_ehr_condition_occurrence` AS t1
            INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{}._mapping_condition_occurrence`)  AS t2
            ON
                t1.condition_occurrence_id=t2.condition_occurrence_id
            INNER JOIN
                `{}.concept` as t3
            ON
                t3.concept_id = t1.condition_source_concept_id
            WHERE 
                 (t3.concept_id=0 or t3.concept_id is null)
            GROUP BY
                1
        )

    SELECT
        data1.src_hpo_id,
        condition_well_defined_row,
        condition_total_row,
        round(100*(condition_well_defined_row/condition_total_row),1) as condition_success_rate
    FROM
        data1
    LEFT OUTER JOIN
        data2
    ON
        data1.src_hpo_id=data2.src_hpo_id
    LEFT OUTER JOIN
        data3
    ON
        data1.src_hpo_id=data3.src_hpo_id
    ORDER BY
        1 DESC
    '''.format(DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET,
               DATASET, DATASET, DATASET, DATASET, DATASET, DATASET),
                                          dialect='standard')
condition_concept_df.shape

condition_concept_df = condition_concept_df.fillna(0)
condition_concept_df

# # 21.Dataframe (row for each hpo_id) Procedure_occurrence table, procedure_source_concept_id field

procedure_concept_df = pd.io.gbq.read_gbq('''
    WITH
        data1 AS (
            SELECT
                src_hpo_id,
                COUNT(*) AS procedure_total_row
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

        data2 AS (
            SELECT
                src_hpo_id,
                COUNT(*) AS procedure_well_defined_row
            FROM
               `{}.unioned_ehr_procedure_occurrence` AS t1
            INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{}._mapping_procedure_occurrence`)  AS t2
            ON
                t1.procedure_occurrence_id=t2.procedure_occurrence_id
            INNER JOIN
                `{}.concept` as t3
            ON
                t3.concept_id = t1.procedure_source_concept_id
            WHERE 
                 t3.standard_concept="S" and t3.domain_id="Procedure"
            GROUP BY
                1
        )

    SELECT
        data1.src_hpo_id,
        procedure_well_defined_row,
        procedure_total_row,
        round(100*(procedure_well_defined_row/procedure_total_row),1) as procedure_success_rate
    FROM
        data1
    LEFT OUTER JOIN
        data2
    ON
        data1.src_hpo_id=data2.src_hpo_id
    ORDER BY
        1 DESC
    '''.format(DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET,
               DATASET, DATASET, DATASET, DATASET, DATASET, DATASET),
                                          dialect='standard')
procedure_concept_df.shape

procedure_concept_df = procedure_concept_df.fillna(0)
procedure_concept_df

# # 22.Dataframe (row for each hpo_id) Drug_exposures table, drug_source_concept_id  field

drug_concept_df = pd.io.gbq.read_gbq('''
    WITH
        data1 AS (
            SELECT
                src_hpo_id,
                COUNT(*) AS drug_total_row
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
        ),

        data2 AS (
            SELECT
                src_hpo_id,
                COUNT(*) AS drug_well_defined_row
            FROM
               `{}.unioned_ehr_drug_exposure` AS t1
            INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{}._mapping_drug_exposure`)  AS t2
            ON
                t1.drug_exposure_id=t2.drug_exposure_id
            INNER JOIN
                `{}.concept` as t3
            ON
                t3.concept_id = t1.drug_source_concept_id
            WHERE 
                 t3.standard_concept="S" and t3.domain_id="Drug"
            GROUP BY
                1
        )

    SELECT
        data1.src_hpo_id,
        drug_well_defined_row,
        drug_total_row,
        round(100*(drug_well_defined_row/drug_total_row),1) as drug_success_rate
    FROM
        data1
    LEFT OUTER JOIN
        data2
    ON
        data1.src_hpo_id=data2.src_hpo_id
    ORDER BY
        1 DESC
    '''.format(DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET,
               DATASET, DATASET, DATASET, DATASET, DATASET, DATASET),
                                     dialect='standard')
drug_concept_df.shape

drug_concept_df = drug_concept_df.fillna(0)
drug_concept_df

# # 23.Dataframe (row for each hpo_id) Observation table, Observation_source_concept_id field
#
#

observation_concept_df = pd.io.gbq.read_gbq('''
    WITH
        data1 AS (
            SELECT
                src_hpo_id,
                COUNT(*) AS observation_total_row
            FROM
               `{}.unioned_ehr_observation` AS t1
            INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{}._mapping_observation`)  AS t2
            ON
                t1.observation_id=t2.observation_id
            GROUP BY
                1
        ),

        data2 AS (
            SELECT
                src_hpo_id,
                COUNT(*) AS observation_well_defined_row
            FROM
               `{}.unioned_ehr_observation` AS t1
            INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{}._mapping_observation`)  AS t2
            ON
                t1.observation_id=t2.observation_id
            INNER JOIN
                `{}.concept` as t3
            ON
                t3.concept_id = t1.observation_source_concept_id 
            WHERE 
                 t3.standard_concept="S"
            GROUP BY
                1
        )

    SELECT
        data1.src_hpo_id,
        observation_well_defined_row,
        observation_total_row,
        round(100*(observation_well_defined_row/observation_total_row),1) as observation_success_rate
    FROM
        data1
    LEFT OUTER JOIN
        data2
    ON
        data1.src_hpo_id=data2.src_hpo_id
    ORDER BY
        1 DESC
    '''.format(DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET,
               DATASET, DATASET, DATASET, DATASET, DATASET, DATASET),
                                            dialect='standard')
observation_concept_df.shape

observation_concept_df = observation_concept_df.fillna(0)
observation_concept_df

# # 21.Dataframe (row for each hpo_id) Measurement table, measurement_source_concept_id field

measurement_concept_df = pd.io.gbq.read_gbq('''
    WITH
        data1 AS (
            SELECT
                src_hpo_id,
                COUNT(*) AS measurement_total_row
            FROM
               `{}.unioned_ehr_measurement` AS t1
            INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{}._mapping_measurement`)  AS t2
            ON
                t1.measurement_id=t2.measurement_id
            GROUP BY
                1
        ),

        data2 AS (
            SELECT
                src_hpo_id,
                COUNT(*) AS measurement_well_defined_row
            FROM
               `{}.unioned_ehr_measurement` AS t1
            INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{}._mapping_measurement`)  AS t2
            ON
                t1.measurement_id=t2.measurement_id
            INNER JOIN
                `{}.concept` as t3
            ON
                t3.concept_id = t1.measurement_source_concept_id
            WHERE 
                 t3.standard_concept="S" and t3.domain_id="Measurement"
            GROUP BY
                1
        )

    SELECT
        data1.src_hpo_id,
        measurement_well_defined_row,
        measurement_total_row,
        round(100*(measurement_well_defined_row/measurement_total_row),1) as measurement_success_rate
    FROM
        data1
    LEFT OUTER JOIN
        data2
    ON
        data1.src_hpo_id=data2.src_hpo_id
    ORDER BY
        1 DESC
    '''.format(DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET,
               DATASET, DATASET, DATASET, DATASET, DATASET, DATASET),
                                            dialect='standard')
measurement_concept_df.shape

measurement_concept_df = measurement_concept_df.fillna(0)
measurement_concept_df

# # 21.Dataframe (row for each hpo_id) visit_occurrence table, visit_source_concept_id field

visit_concept_df = pd.io.gbq.read_gbq('''
    WITH
        data1 AS (
            SELECT
                src_hpo_id,
                COUNT(*) AS visit_total_row
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
        ),

        data2 AS (
            SELECT
                src_hpo_id,
                COUNT(*) AS visit_well_defined_row
            FROM
               `{}.unioned_ehr_visit_occurrence` AS t1
            INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{}._mapping_visit_occurrence`)  AS t2
            ON
                t1.visit_occurrence_id=t2.visit_occurrence_id
            INNER JOIN
                `{}.concept` as t3
            ON
                t3.concept_id = t1.visit_source_concept_id
            WHERE 
                 t3.standard_concept="S" and t3.domain_id="Visit"
            GROUP BY
                1
        )

    SELECT
        data1.src_hpo_id,
        visit_well_defined_row,
        visit_total_row,
        round(100*(visit_well_defined_row/visit_total_row),1) as visit_success_rate
    FROM
        data1
    LEFT OUTER JOIN
        data2
    ON
        data1.src_hpo_id=data2.src_hpo_id
    ORDER BY
        1 DESC
    '''.format(DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET,
               DATASET, DATASET, DATASET, DATASET, DATASET, DATASET),
                                      dialect='standard')
visit_concept_df.shape

visit_concept_df = visit_concept_df.fillna(0)
visit_concept_df

datas = [
    procedure_concept_df, drug_concept_df, observation_concept_df,
    measurement_concept_df, visit_concept_df
]

master_df = condition_concept_df

for filename in datas:
    master_df = pd.merge(master_df, filename, on='src_hpo_id', how='outer')

master_df

source = pd.merge(master_df, site_df, how='outer', on='src_hpo_id')
source = source.fillna("No Data")
source.to_csv("{cwd}\source_concept_success_rate.csv".format(cwd = cwd))

# # Below is how we calculate **true** concept_success_rate. This involves concept_id fields that are of Standard Concept = 'S' and of the correct domain.
# #### NOTE: Domain enforcement is not necessary for the Observation table

# # 16.Dataframe (row for each hpo_id) Condition_occurrence table, condition_concept_id field

condition_concept_df = pd.io.gbq.read_gbq('''
    WITH
        data1 AS (
            SELECT
                src_hpo_id,
                COUNT(*) AS condition_total_row
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
        ),


        data2 AS (
            SELECT
                src_hpo_id,
                COUNT(*) AS condition_well_defined_row
            FROM
               `{}.unioned_ehr_condition_occurrence` AS t1
            INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{}._mapping_condition_occurrence`)  AS t2
            ON
                t1.condition_occurrence_id=t2.condition_occurrence_id
            INNER JOIN
                `{}.concept` as t3
            ON
                t3.concept_id = t1.condition_concept_id
            WHERE 
                 t3.standard_concept="S" and t3.domain_id="Condition"
            GROUP BY
                1
        ),
        
        data3 AS (
            SELECT
                src_hpo_id,
                COUNT(*) AS condition_total_zeros_or_null
            FROM
               `{}.unioned_ehr_condition_occurrence` AS t1
            INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{}._mapping_condition_occurrence`)  AS t2
            ON
                t1.condition_occurrence_id=t2.condition_occurrence_id
            WHERE 
                (t1.condition_concept_id=0 or t1.condition_concept_id IS NULL)
            GROUP BY
                1
        ),
        
                data4 AS (
            SELECT
                src_hpo_id,
                COUNT(*) AS condition_total_null
            FROM
               `{}.unioned_ehr_condition_occurrence` AS t1
            INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{}._mapping_condition_occurrence`)  AS t2
            ON
                t1.condition_occurrence_id=t2.condition_occurrence_id
            WHERE 
                t1.condition_concept_id IS NULL
            GROUP BY
                1
        )


    SELECT
        data1.src_hpo_id,
        condition_well_defined_row,
        condition_total_row,
        condition_total_zeros_or_null,
        condition_total_null,
        round(100*(condition_well_defined_row/condition_total_row),1) as condition_success_rate
    FROM
        data1
    LEFT OUTER JOIN
        data2
    ON
        data1.src_hpo_id=data2.src_hpo_id
    LEFT OUTER JOIN
        data3
    ON
        data1.src_hpo_id=data3.src_hpo_id
    LEFT OUTER JOIN
        data4
    ON
        data1.src_hpo_id=data4.src_hpo_id
    ORDER BY
        4 DESC
    '''.format(DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET,
               DATASET, DATASET, DATASET, DATASET, DATASET, DATASET),
                                          dialect='standard')
condition_concept_df.shape

condition_concept_df = condition_concept_df.fillna(0)
condition_concept_df

# # 17.Dataframe (row for each hpo_id) Procedure_occurrence table, procedure_concept_id field

procedure_concept_df = pd.io.gbq.read_gbq('''
    WITH
        data1 AS (
            SELECT
                src_hpo_id,
                COUNT(*) AS procedure_total_row
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

        data2 AS (
            SELECT
                src_hpo_id,
                COUNT(*) AS procedure_well_defined_row
            FROM
               `{}.unioned_ehr_procedure_occurrence` AS t1
            INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{}._mapping_procedure_occurrence`)  AS t2
            ON
                t1.procedure_occurrence_id=t2.procedure_occurrence_id
            INNER JOIN
                `{}.concept` as t3
            ON
                t3.concept_id = t1.procedure_concept_id
            WHERE 
                 t3.standard_concept="S" and t3.domain_id="Procedure"
            GROUP BY
                1
        ),
        
        data3 AS (
            SELECT
                src_hpo_id,
                COUNT(*) AS procedure_total_zero_null
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
                 (t1.procedure_concept_id=0 or t1.procedure_concept_id IS NULL)
            GROUP BY
                1
        ),
        
                data4 AS (
            SELECT
                src_hpo_id,
                COUNT(*) AS procedure_total_null
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
                 t1.procedure_concept_id IS NULL
            GROUP BY
                1
        )
        

    SELECT
        data1.src_hpo_id,
        procedure_well_defined_row,
        procedure_total_zero_null,
        procedure_total_null,
        procedure_total_row,
        round(100*(procedure_well_defined_row/procedure_total_row),1) as procedure_success_rate
    FROM
        data1
    LEFT OUTER JOIN
        data2
    ON
        data1.src_hpo_id=data2.src_hpo_id
    LEFT OUTER JOIN
        data3
    ON
        data1.src_hpo_id=data3.src_hpo_id
    LEFT OUTER JOIN
        data4
    ON
        data1.src_hpo_id=data4.src_hpo_id
    ORDER BY
        1 DESC
    '''.format(DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET,
               DATASET, DATASET, DATASET, DATASET, DATASET, DATASET),
                                          dialect='standard')
procedure_concept_df.shape

procedure_concept_df = procedure_concept_df.fillna(0)
procedure_concept_df

# # 18.Dataframe (row for each hpo_id) Drug_exposures table, drug_concept_id field

drug_concept_df = pd.io.gbq.read_gbq('''
    WITH
        data1 AS (
            SELECT
                src_hpo_id,
                COUNT(*) AS drug_total_row
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
        ),

        data2 AS (
            SELECT
                src_hpo_id,
                COUNT(*) AS drug_well_defined_row
            FROM
               `{}.unioned_ehr_drug_exposure` AS t1
            INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{}._mapping_drug_exposure`)  AS t2
            ON
                t1.drug_exposure_id=t2.drug_exposure_id
            INNER JOIN
                `{}.concept` as t3
            ON
                t3.concept_id = t1.drug_concept_id
            WHERE 
                 t3.standard_concept="S" and t3.domain_id="Drug"
            GROUP BY
                1
        ),

        data3 AS (
            SELECT
                src_hpo_id,
                COUNT(*) AS drug_total_zero_null
            FROM
               `{}.unioned_ehr_drug_exposure` AS t1
            INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{}._mapping_drug_exposure`)  AS t2
            ON
                t1.drug_exposure_id=t2.drug_exposure_id
            INNER JOIN
                `{}.concept` as t3
            ON
                t3.concept_id = t1.drug_concept_id
            WHERE 
                 (t1.drug_concept_id=0 OR t1.drug_concept_id IS NULL)
            GROUP BY
                1
        ),
        
                data4 AS (
            SELECT
                src_hpo_id,
                COUNT(*) AS drug_total_null
            FROM
               `{}.unioned_ehr_drug_exposure` AS t1
            INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{}._mapping_drug_exposure`)  AS t2
            ON
                t1.drug_exposure_id=t2.drug_exposure_id
            WHERE 
                 t1.drug_concept_id IS NULL
            GROUP BY
                1
        )
        
    SELECT
        data1.src_hpo_id,
        drug_well_defined_row,
        drug_total_zero_null,
        drug_total_null,
        drug_total_row,
        round(100*(drug_well_defined_row/drug_total_row),1) as drug_success_rate
    FROM
        data1
    LEFT OUTER JOIN
        data2
    ON
        data1.src_hpo_id=data2.src_hpo_id
    LEFT OUTER JOIN
        data3
    ON
        data1.src_hpo_id=data3.src_hpo_id
    LEFT OUTER JOIN
        data4
    ON
        data1.src_hpo_id=data4.src_hpo_id
    ORDER BY
        1 DESC
    '''.format(DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET,
               DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET,
               DATASET, DATASET),
                                     dialect='standard')
drug_concept_df.shape

# +

drug_concept_df = drug_concept_df.fillna(0)
drug_concept_df
# -

# # 19.Dataframe (row for each hpo_id) Observation table, Observation_concept_id field
#

observation_concept_df = pd.io.gbq.read_gbq('''
    WITH
        data1 AS (
            SELECT
                src_hpo_id,
                COUNT(*) AS observation_total_row
            FROM
               `{}.unioned_ehr_observation` AS t1
            INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{}._mapping_observation`)  AS t2
            ON
                t1.observation_id=t2.observation_id
            GROUP BY
                1
        ),

        data2 AS (
            SELECT
                src_hpo_id,
                COUNT(*) AS observation_well_defined_row
            FROM
               `{}.unioned_ehr_observation` AS t1
            INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{}._mapping_observation`)  AS t2
            ON
                t1.observation_id=t2.observation_id
            INNER JOIN
                `{}.concept` as t3
            ON
                t3.concept_id = t1.observation_concept_id
            WHERE 
                 t3.standard_concept="S" 
            GROUP BY
                1
        ),

        data3 AS (
            SELECT
                src_hpo_id,
                COUNT(*) AS observation_total_zero_missing
            FROM
               `{}.unioned_ehr_observation` AS t1
            INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{}._mapping_observation`)  AS t2
            ON
                t1.observation_id=t2.observation_id
            INNER JOIN
                `{}.concept` as t3
            ON
                t3.concept_id = t1.observation_concept_id
            WHERE 
                 (t1.observation_concept_id=0 OR t1.observation_concept_id IS NULL)
            GROUP BY
                1
        ),
        
            data4 AS (
            SELECT
                src_hpo_id,
                COUNT(*) AS observation_total_missing
            FROM
               `{}.unioned_ehr_observation` AS t1
            INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{}._mapping_observation`)  AS t2
            ON
                t1.observation_id=t2.observation_id
            WHERE 
              t1.observation_concept_id IS NULL
            GROUP BY
                1
        )

    SELECT
        data1.src_hpo_id,
        observation_total_zero_missing,
        observation_total_missing,
        observation_well_defined_row,
        observation_total_row,
        round(100*(observation_well_defined_row/observation_total_row),1) as observation_success_rate
    FROM
        data1
    LEFT OUTER JOIN
        data2
    ON
        data1.src_hpo_id=data2.src_hpo_id
    LEFT OUTER JOIN
        data3
    ON
        data1.src_hpo_id=data3.src_hpo_id
    LEFT OUTER JOIN
        data4
    ON
        data1.src_hpo_id=data4.src_hpo_id
    ORDER BY
        1 DESC
    '''.format(DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET,
               DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET,
               DATASET, DATASET, DATASET),
                                            dialect='standard')
observation_concept_df.shape

observation_concept_df = observation_concept_df.fillna(0)
observation_concept_df

# # 19.Dataframe (row for each hpo_id) measurement table, measurement_concept_id field
#

measurement_concept_df = pd.io.gbq.read_gbq('''
    WITH
        data1 AS (
            SELECT
                src_hpo_id,
                COUNT(*) AS measurement_total_row
            FROM
               `{}.unioned_ehr_measurement` AS t1
            INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{}._mapping_measurement`)  AS t2
            ON
                t1.measurement_id=t2.measurement_id
            GROUP BY
                1
        ),

        data2 AS (
            SELECT
                src_hpo_id,
                COUNT(*) AS measurement_well_defined_row
            FROM
               `{}.unioned_ehr_measurement` AS t1
            INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{}._mapping_measurement`)  AS t2
            ON
                t1.measurement_id=t2.measurement_id
            INNER JOIN
                `{}.concept` as t3
            ON
                t3.concept_id = t1.measurement_concept_id
            WHERE 
                 t3.standard_concept="S" and t3.domain_id="Measurement"
            GROUP BY
                1
        ),

        data3 AS (
            SELECT
                src_hpo_id,
                COUNT(*) AS measurement_total_zero_missing
            FROM
               `{}.unioned_ehr_measurement` AS t1
            INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{}._mapping_measurement`)  AS t2
            ON
                t1.measurement_id=t2.measurement_id
            INNER JOIN
                `{}.concept` as t3
            ON
                t3.concept_id = t1.measurement_concept_id
            WHERE 
                 (t1.measurement_concept_id=0 OR t1.measurement_concept_id IS NULL)
            GROUP BY
                1
        ),
        
            data4 AS (
            SELECT
                src_hpo_id,
                COUNT(*) AS measurement_total_missing
            FROM
               `{}.unioned_ehr_measurement` AS t1
            INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{}._mapping_measurement`)  AS t2
            ON
                t1.measurement_id=t2.measurement_id
            WHERE 
              t1.measurement_concept_id IS NULL
            GROUP BY
                1
        )

    SELECT
        data1.src_hpo_id,
        measurement_total_zero_missing,
        measurement_total_missing,
        measurement_well_defined_row,
        measurement_total_row,
        round(100*(measurement_well_defined_row/measurement_total_row),1) as  measurement_success_rate
    FROM
        data1
    LEFT OUTER JOIN
        data2
    ON
        data1.src_hpo_id=data2.src_hpo_id
    LEFT OUTER JOIN
        data3
    ON
        data1.src_hpo_id=data3.src_hpo_id
    LEFT OUTER JOIN
        data4
    ON
        data1.src_hpo_id=data4.src_hpo_id
    ORDER BY
        1 DESC
    '''.format(DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET,
               DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET,
               DATASET, DATASET, DATASET),
                                            dialect='standard')
measurement_concept_df.shape

measurement_concept_df = measurement_concept_df.fillna(0)
measurement_concept_df

# # 17.Dataframe (row for each hpo_id) visit_occurrence table, visit_concept_id field

visit_concept_df = pd.io.gbq.read_gbq('''
    WITH
        data1 AS (
            SELECT
                src_hpo_id,
                COUNT(*) AS visit_total_row
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
        ),

        data2 AS (
            SELECT
                src_hpo_id,
                COUNT(*) AS visit_well_defined_row
            FROM
               `{}.unioned_ehr_visit_occurrence` AS t1
            INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{}._mapping_visit_occurrence`)  AS t2
            ON
                t1.visit_occurrence_id=t2.visit_occurrence_id
            INNER JOIN
                `{}.concept` as t3
            ON
                t3.concept_id = t1.visit_concept_id
            WHERE 
                 t3.standard_concept="S" and t3.domain_id="Visit"
            GROUP BY
                1
        ),
        
        data3 AS (
            SELECT
                src_hpo_id,
                COUNT(*) AS visit_total_zero_null
            FROM
               `{}.unioned_ehr_visit_occurrence` AS t1
            INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{}._mapping_visit_occurrence`)  AS t2
            ON
                t1.visit_occurrence_id=t2.visit_occurrence_id
            WHERE 
                 (t1.visit_concept_id=0 or t1.visit_concept_id IS NULL)
            GROUP BY
                1
        ),
        
                data4 AS (
            SELECT
                src_hpo_id,
                COUNT(*) AS visit_total_null
            FROM
               `{}.unioned_ehr_visit_occurrence` AS t1
            INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{}._mapping_visit_occurrence`)  AS t2
            ON
                t1.visit_occurrence_id=t2.visit_occurrence_id
            WHERE 
                 t1.visit_concept_id IS NULL
            GROUP BY
                1
        )
        

    SELECT
        data1.src_hpo_id,
        visit_well_defined_row,
        visit_total_zero_null,
        visit_total_null,
        visit_total_row,
        round(100*(visit_well_defined_row/visit_total_row),1) as visit_success_rate
    FROM
        data1
    LEFT OUTER JOIN
        data2
    ON
        data1.src_hpo_id=data2.src_hpo_id
    LEFT OUTER JOIN
        data3
    ON
        data1.src_hpo_id=data3.src_hpo_id
    LEFT OUTER JOIN
        data4
    ON
        data1.src_hpo_id=data4.src_hpo_id
    ORDER BY
        1 DESC
    '''.format(DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET,
               DATASET, DATASET, DATASET, DATASET, DATASET, DATASET),
                                      dialect='standard')
visit_concept_df.shape

visit_concept_df = visit_concept_df.fillna(0)
visit_concept_df

# ## Sites combined

# +
datas = [
    drug_concept_df, procedure_concept_df, condition_concept_df,
    measurement_concept_df, visit_concept_df
]

master_df = observation_concept_df

for filename in datas:
    master_df = pd.merge(master_df, filename, on='src_hpo_id', how='outer')
# -

master_df

success_rate = pd.merge(master_df, site_df, how='outer', on='src_hpo_id')
success_rate

success_rate = success_rate.fillna("No Data")
success_rate

success_rate.to_csv("{cwd}\concept.csv".format(cwd = cwd))
