# -*- coding: utf-8 -*-
# ---
# jupyter:
#   jupytext:
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

# # NOTE: `combined` datasets do not have a `_mapping_person` table - this necessitates the use of unioned_ehr datasets (and this notebook therefore more closely resembles the "weekly metrics" notebook)
#
# ### This notebook is intended to show the percentage of rows with 'invalid' 'person_id' (a type of 'foreign key') in each of the 6 canonical tables. 
#
# ### An 'invalid' row would be one where the field value ('person_id') either:
# ###     a. does not exist in the person table
# ###     b. is seemingly associated with another site in the `unioned_ehr_person` table

# ### The notebook will evaluate the following
#
# #### person_id and visit_occurrence_id in the following tables:
#
# - condition occurrence
# - observation
# - drug exposure
# - procedure occurrence
# - measurement
# - visit occurrence

# +
from google.cloud import bigquery

# %reload_ext google.cloud.bigquery

client = bigquery.Client()

# %load_ext google.cloud.bigquery

# +
from notebooks import parameters
DATASET = parameters.UNIONED_EHR_OCT_2019

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
        "University of Pittsburgh", "Columbia University Medicalå Center",
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

# + endofcell="--"
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
         `{DATASET}._mapping_measurement`               
         
         
    UNION ALL
    SELECT
            DISTINCT(src_hpo_id) as src_hpo_id
    FROM
         `{DATASET}._mapping_observation`           
         
    UNION ALL
    SELECT
            DISTINCT(src_hpo_id) as src_hpo_id
    FROM
         `{DATASET}._mapping_procedure_occurrence`         
         
    
    UNION ALL
    SELECT
            DISTINCT(src_hpo_id) as src_hpo_id
    FROM
         `{DATASET}._mapping_visit_occurrence`   
    ) 
    order by 1
    '''.format(DATASET=DATASET), dialect='standard')
print(site_map.shape[0], 'records received.')
# -

site_map

site_df = pd.merge(site_map, site_df, how='outer', on='src_hpo_id')

site_df
# --

site_df = pd.merge(site_map, site_df, how='outer', on='src_hpo_id')
site_df

# # person_id evaluations

# ## Condition Occurrence Table

condition_occurrence_query = f"""
SELECT
DISTINCT
total_rows.src_hpo_id,
-- IFNULL(invalid_row_count.number_rows_w_no_valid_person, 0) as rows_w_no_valid_person,
-- total_rows.total_rows,
round(IFNULL(invalid_row_count.number_rows_w_no_valid_person, 0) / total_rows.total_rows * 100, 2) AS condition_occurrence

FROM

  (SELECT
  DISTINCT
  mco.src_hpo_id, COUNT(mco.src_hpo_id) as total_rows
  FROM
  `{DATASET}.unioned_ehr_condition_occurrence` co
  LEFT JOIN
  `{DATASET}._mapping_condition_occurrence` mco
  ON
  co.condition_occurrence_id = mco.condition_occurrence_id
  GROUP BY 1
  ORDER BY total_rows DESC) total_rows

LEFT JOIN

  (SELECT
  DISTINCT
  mco.src_hpo_id, COUNT(mco.src_hpo_id) as number_rows_w_no_valid_person

  -- to enable site tracing
  FROM
  `{DATASET}.unioned_ehr_condition_occurrence` co
  LEFT JOIN
  `{DATASET}._mapping_condition_occurrence` mco
  ON
  co.condition_occurrence_id = mco.condition_occurrence_id

  -- to enable person/src_hpo_id cross-checking
  LEFT JOIN
  `{DATASET}.unioned_ehr_person` p
  ON
  co.person_id = p.person_id
  LEFT JOIN
  `{DATASET}._mapping_person` mp
  ON
  p.person_id = mp.person_id


  -- anything dropped by the 'left join'
  WHERE
  co.person_id NOT IN
    (
    SELECT
    DISTINCT p.person_id
    FROM
    `{DATASET}.unioned_ehr_person` p
    )

  -- same person_id but traced to different sites
  OR
  mco.src_hpo_id <> mp.src_hpo_id

  GROUP BY 1
  ORDER BY number_rows_w_no_valid_person DESC) invalid_row_count

ON
total_rows.src_hpo_id = invalid_row_count.src_hpo_id

WHERE LOWER(total_rows.src_hpo_id) NOT LIKE '%rdr%'

ORDER BY condition_occurrence DESC
"""

condition_occurrence_df = pd.io.gbq.read_gbq(condition_occurrence_query, dialect ='standard')

condition_occurrence_df

# # Observation

observation_query = f"""
SELECT
DISTINCT
total_rows.src_hpo_id,
-- IFNULL(invalid_row_count.number_rows_w_no_valid_person, 0) as rows_w_no_valid_person,
-- total_rows.total_rows,
round(IFNULL(invalid_row_count.number_rows_w_no_valid_person, 0) / total_rows.total_rows * 100, 2) AS observation

FROM

  (SELECT
  DISTINCT
  mo.src_hpo_id, COUNT(mo.src_hpo_id) as total_rows
  FROM
  `{DATASET}.unioned_ehr_observation` o
  LEFT JOIN
  `{DATASET}._mapping_observation` mo
  ON
  mo.observation_id = o.observation_id
  GROUP BY 1
  ORDER BY total_rows DESC) total_rows

LEFT JOIN

  (SELECT
  DISTINCT
  mo.src_hpo_id, COUNT(mo.src_hpo_id) as number_rows_w_no_valid_person

  -- to enable site tracing
  FROM
  `{DATASET}.unioned_ehr_observation` o
  LEFT JOIN
  `{DATASET}._mapping_observation` mo
  ON
  mo.observation_id = o.observation_id

  -- to enable person/src_hpo_id cross-checking
  LEFT JOIN
  `{DATASET}.unioned_ehr_person` p
  ON
  o.person_id = p.person_id
  LEFT JOIN
  `{DATASET}._mapping_person` mp
  ON
  p.person_id = mp.person_id


  -- anything dropped by the 'left join'
  WHERE
  o.person_id NOT IN
    (
    SELECT
    DISTINCT p.person_id
    FROM
    `{DATASET}.unioned_ehr_person` p
    )

  -- same person_id but traced to different sites
  OR
  mo.src_hpo_id <> mp.src_hpo_id

  GROUP BY 1
  ORDER BY number_rows_w_no_valid_person DESC) invalid_row_count

ON
total_rows.src_hpo_id = invalid_row_count.src_hpo_id

WHERE LOWER(total_rows.src_hpo_id) NOT LIKE '%rdr%'

ORDER BY observation DESC
"""

observation_df = pd.io.gbq.read_gbq(observation_query, dialect ='standard')

observation_df

# # Drug Exposure Table

drug_exposure_query = f"""
SELECT
DISTINCT
total_rows.src_hpo_id,
-- IFNULL(invalid_row_count.number_rows_w_no_valid_person, 0) as rows_w_no_valid_person,
-- total_rows.total_rows,
round(IFNULL(invalid_row_count.number_rows_w_no_valid_person, 0) / total_rows.total_rows * 100, 2) AS drug_exposure

FROM

  (SELECT
  DISTINCT
  mde.src_hpo_id, COUNT(mde.src_hpo_id) as total_rows
  FROM
  `{DATASET}.unioned_ehr_drug_exposure` de
  LEFT JOIN
  `{DATASET}._mapping_drug_exposure` mde
  ON
  de.drug_exposure_id = mde.drug_exposure_id
  GROUP BY 1
  ORDER BY total_rows DESC) total_rows

LEFT JOIN

  (SELECT
  DISTINCT
  mde.src_hpo_id, COUNT(mde.src_hpo_id) as number_rows_w_no_valid_person

  -- to enable site tracing
  FROM
  `{DATASET}.unioned_ehr_drug_exposure` de
  LEFT JOIN
  `{DATASET}._mapping_drug_exposure` mde
  ON
  de.drug_exposure_id = mde.drug_exposure_id

  -- to enable person/src_hpo_id cross-checking
  LEFT JOIN
  `{DATASET}.unioned_ehr_person` p
  ON
  de.person_id = p.person_id
  LEFT JOIN
  `{DATASET}._mapping_person` mp
  ON
  p.person_id = mp.person_id


  -- anything dropped by the 'left join'
  WHERE
  de.person_id NOT IN
    (
    SELECT
    DISTINCT p.person_id
    FROM
    `{DATASET}.unioned_ehr_person` p
    )

  -- same person_id but traced to different sites
  OR
  mde.src_hpo_id <> mp.src_hpo_id

  GROUP BY 1
  ORDER BY number_rows_w_no_valid_person DESC) invalid_row_count

ON

total_rows.src_hpo_id = invalid_row_count.src_hpo_id

WHERE LOWER(total_rows.src_hpo_id) NOT LIKE '%rdr%'

ORDER BY drug_exposure DESC
"""

drug_exposure_df = pd.io.gbq.read_gbq(drug_exposure_query, dialect ='standard')

drug_exposure_df

# # Procedure Occurrence

procedure_occurrence_query = f"""
SELECT
DISTINCT
total_rows.src_hpo_id,
-- IFNULL(invalid_row_count.number_rows_w_no_valid_person, 0) as rows_w_no_valid_person,
-- total_rows.total_rows,
round(IFNULL(invalid_row_count.number_rows_w_no_valid_person, 0) / total_rows.total_rows * 100, 2) AS procedure_occurrence

FROM

  (SELECT
  DISTINCT
  mpo.src_hpo_id, COUNT(mpo.src_hpo_id) as total_rows
  FROM
  `{DATASET}.unioned_ehr_procedure_occurrence` po
  LEFT JOIN
  `{DATASET}._mapping_procedure_occurrence` mpo
  ON
  po.procedure_occurrence_id = mpo.procedure_occurrence_id
  GROUP BY 1
  ORDER BY total_rows DESC) total_rows

LEFT JOIN

  (SELECT
  DISTINCT
  mpo.src_hpo_id, COUNT(mpo.src_hpo_id) as number_rows_w_no_valid_person

  -- to enable site tracing
  FROM
  `{DATASET}.unioned_ehr_procedure_occurrence` po
  LEFT JOIN
  `{DATASET}._mapping_procedure_occurrence` mpo
  ON
  po.procedure_occurrence_id = mpo.procedure_occurrence_id

  -- to enable person/src_hpo_id cross-checking
  LEFT JOIN
  `{DATASET}.unioned_ehr_person` p
  ON
  po.person_id = p.person_id
  LEFT JOIN
  `{DATASET}._mapping_person` mp
  ON
  p.person_id = mp.person_id


  -- anything dropped by the 'left join'
  WHERE
  po.person_id NOT IN
    (
    SELECT
    DISTINCT p.person_id
    FROM
    `{DATASET}.unioned_ehr_person` p
    )

  -- same person_id but traced to different sites
  OR
  mpo.src_hpo_id <> mp.src_hpo_id

  GROUP BY 1
  ORDER BY number_rows_w_no_valid_person DESC) invalid_row_count

ON
total_rows.src_hpo_id = invalid_row_count.src_hpo_id

WHERE LOWER(total_rows.src_hpo_id) NOT LIKE '%rdr%'

ORDER BY procedure_occurrence DESC
"""

procedure_occurrence_df = pd.io.gbq.read_gbq(procedure_occurrence_query, dialect ='standard')

procedure_occurrence_df

# # Measurement

measurement_query = f"""
SELECT
DISTINCT
total_rows.src_hpo_id,
-- IFNULL(invalid_row_count.number_rows_w_no_valid_person, 0) as rows_w_no_valid_person,
-- total_rows.total_rows,
round(IFNULL(invalid_row_count.number_rows_w_no_valid_person, 0) / total_rows.total_rows * 100, 2) AS measurement

FROM

  (SELECT
  DISTINCT
  mm.src_hpo_id, COUNT(mm.src_hpo_id) as total_rows
  FROM
  `{DATASET}.unioned_ehr_measurement` m
  LEFT JOIN
  `{DATASET}._mapping_measurement` mm
  ON
  m.measurement_id = mm.measurement_id
  GROUP BY 1
  ORDER BY total_rows DESC) total_rows

LEFT JOIN

  (SELECT
  DISTINCT
  mm.src_hpo_id, COUNT(mm.src_hpo_id) as number_rows_w_no_valid_person

  -- to enable site tracing
  FROM
  `{DATASET}.unioned_ehr_measurement` m
  LEFT JOIN
  `{DATASET}._mapping_measurement` mm
  ON
  m.measurement_id = mm.measurement_id

  -- to enable person/src_hpo_id cross-checking
  LEFT JOIN
  `{DATASET}.unioned_ehr_person` p
  ON
  m.person_id = p.person_id
  LEFT JOIN
  `{DATASET}._mapping_person` mp
  ON
  p.person_id = mp.person_id


  -- anything dropped by the 'left join'
  WHERE
  m.person_id NOT IN
    (
    SELECT
    DISTINCT p.person_id
    FROM
    `{DATASET}.unioned_ehr_person` p
    )

  -- same person_id but traced to different sites
  OR
  mm.src_hpo_id <> mp.src_hpo_id

  GROUP BY 1
  ORDER BY number_rows_w_no_valid_person DESC) invalid_row_count

ON
total_rows.src_hpo_id = invalid_row_count.src_hpo_id

WHERE LOWER(total_rows.src_hpo_id) NOT LIKE '%rdr%'

ORDER BY measurement DESC
"""

measurement_df = pd.io.gbq.read_gbq(measurement_query, dialect ='standard')

measurement_df

# # Visit Occurrence

visit_occurrence_query = f"""
SELECT
DISTINCT
total_rows.src_hpo_id,
-- IFNULL(invalid_row_count.number_rows_w_no_valid_person, 0) as rows_w_no_valid_person,
-- total_rows.total_rows,
round(IFNULL(invalid_row_count.number_rows_w_no_valid_person, 0) / total_rows.total_rows * 100, 2) AS visit_occurrence

FROM

  (SELECT
  DISTINCT
  mvo.src_hpo_id, COUNT(mvo.src_hpo_id) as total_rows
  FROM
  `{DATASET}.unioned_ehr_visit_occurrence` vo
  LEFT JOIN
  `{DATASET}._mapping_visit_occurrence` mvo
  ON
  vo.visit_occurrence_id = mvo.visit_occurrence_id
  GROUP BY 1
  ORDER BY total_rows DESC) total_rows

LEFT JOIN

  (SELECT
  DISTINCT
  mvo.src_hpo_id, COUNT(mvo.src_hpo_id) as number_rows_w_no_valid_person

  -- to enable site tracing
  FROM
  `{DATASET}.unioned_ehr_visit_occurrence` vo
  LEFT JOIN
  `{DATASET}._mapping_visit_occurrence` mvo
  ON
  vo.visit_occurrence_id = mvo.visit_occurrence_id

  -- to enable person/src_hpo_id cross-checking
  LEFT JOIN
  `{DATASET}.unioned_ehr_person` p
  ON
  vo.person_id = p.person_id
  LEFT JOIN
  `{DATASET}._mapping_person` mp
  ON
  p.person_id = mp.person_id


  -- anything dropped by the 'left join'
  WHERE
  vo.person_id NOT IN
    (
    SELECT
    DISTINCT p.person_id
    FROM
    `{DATASET}.unioned_ehr_person` p
    )

  -- same person_id but traced to different sites
  OR
  mvo.src_hpo_id <> mp.src_hpo_id

  GROUP BY 1
  ORDER BY number_rows_w_no_valid_person DESC) invalid_row_count

ON
total_rows.src_hpo_id = invalid_row_count.src_hpo_id

WHERE LOWER(total_rows.src_hpo_id) NOT LIKE '%rdr%'

ORDER BY visit_occurrence DESC
"""

visit_occurrence_df = pd.io.gbq.read_gbq(visit_occurrence_query, dialect ='standard')

visit_occurrence_df

# # Bringing it all together

# +
person_id_foreign_key_df = pd.merge(
    site_df, observation_df, how='outer', on='src_hpo_id')

person_id_foreign_key_df = pd.merge(
    person_id_foreign_key_df, measurement_df, how='outer', on='src_hpo_id')

person_id_foreign_key_df = pd.merge(
    person_id_foreign_key_df, visit_occurrence_df, how='outer', on='src_hpo_id')

person_id_foreign_key_df = pd.merge(
    person_id_foreign_key_df, procedure_occurrence_df, how='outer', on='src_hpo_id')

person_id_foreign_key_df = pd.merge(
    person_id_foreign_key_df, drug_exposure_df, how='outer', on='src_hpo_id')

person_id_foreign_key_df = pd.merge(
    person_id_foreign_key_df, condition_occurrence_df, how='outer', on='src_hpo_id')

# +
person_id_foreign_key_df = person_id_foreign_key_df.fillna(0)

person_id_foreign_key_df
# -

person_id_foreign_key_df.to_csv(f"{cwd}/person_id_failure_rate.csv")
