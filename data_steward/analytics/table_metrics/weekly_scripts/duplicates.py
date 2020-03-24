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

# %reload_ext google.cloud.bigquery

client = bigquery.Client()

# %load_ext google.cloud.bigquery

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
       `{DATASET}.unioned_ehr_visit_occurrence` AS t1
    INNER JOIN
        (SELECT
            DISTINCT * 
    FROM
         `{DATASET}._mapping_visit_occurrence`) AS t2
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
    '''.format(DATASET=DATASET),
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
       `{DATASET}.unioned_ehr_condition_occurrence` AS t1
    INNER JOIN
        (SELECT
            DISTINCT * 
    FROM
         `{DATASET}._mapping_condition_occurrence`) AS t2
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
    '''.format(DATASET=DATASET), dialect='standard')
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
       `{DATASET}.unioned_ehr_drug_exposure` AS t1
    INNER JOIN
        (SELECT
            DISTINCT * 
    FROM
         `{DATASET}._mapping_drug_exposure`) AS t2
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
    '''.format(DATASET=DATASET),
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
       `{DATASET}.unioned_ehr_measurement` AS t1
    INNER JOIN
        (SELECT
            DISTINCT * 
    FROM
         `{DATASET}._mapping_measurement`) AS t2
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
    '''.format(DATASET=DATASET),
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
       `{DATASET}.unioned_ehr_procedure_occurrence` AS t1
    INNER JOIN
        (SELECT
            DISTINCT * 
    FROM
         `{DATASET}._mapping_procedure_occurrence`) AS t2
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
    '''.format(DATASET=DATASET),
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
       `{DATASET}.unioned_ehr_observation` AS t1
    INNER JOIN
        (SELECT
            DISTINCT * 
    FROM
         `{DATASET}._mapping_observation`) AS t2
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
    '''.format(DATASET=DATASET),
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

sites_success.to_csv("{cwd}/duplicates.csv".format(cwd = cwd))


