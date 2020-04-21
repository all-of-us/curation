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

# ### This notebook is intended to show the percentage of rows where there are 'erroneous dates' in the 6 canonical tables. The 6 canonical tables are as follows:
# - Condition Occurrence
# - Procedure Occurrence
# - Visit Occurrence
# - Drug Exposure
# - Measurement
# - Observation
#
# ### Erroneous dates are those that precede 1900 for the observation table or precede 1980 for all other tables

# +
from google.cloud import bigquery

# %reload_ext google.cloud.bigquery

client = bigquery.Client()

# %load_ext google.cloud.bigquery

# +
from notebooks import parameters
DATASET = parameters.UNIONED_Q3_2019

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

# ## Observation Table

observation_query = """
SELECT
total.src_hpo_id,
-- IFNULL(bad_date.num_bad_rows, 0) AS num_bad_rows, 
-- IFNULL(total.num_rows, 0) AS num_rows,
ROUND(IFNULL(bad_date.num_bad_rows, 0) / IFNULL(total.num_rows, 0) * 100 , 2) as observation

FROM

  (SELECT
  DISTINCT
  mo.src_hpo_id, COUNT(*) as num_rows
  FROM
  `{DATASET}.observation` o
  JOIN
  `{DATASET}._mapping_observation` mo
  ON
  o.observation_id = mo.observation_id
  GROUP BY 1
  ORDER BY num_rows DESC) total

LEFT JOIN

  (SELECT
  DISTINCT
  mo.src_hpo_id, COUNT(*) as num_bad_rows
  FROM
  `{DATASET}.observation` o
  JOIN
  `{DATASET}._mapping_observation` mo
  ON
  o.observation_id = mo.observation_id
  WHERE
  o.observation_datetime < CAST('1900-01-01 00:00:00' AS TIMESTAMP)
  OR
  o.observation_date < CAST('1900-01-01' as DATE)
  GROUP BY 1
  ORDER BY num_bad_rows DESC) bad_date
  
ON
bad_date.src_hpo_id = total.src_hpo_id

WHERE
LOWER(total.src_hpo_id) NOT LIKE '%rdr%'

GROUP BY 1, 2 --, 3, 4
ORDER BY observation DESC
""".format(DATASET=DATASET)

observation_df = pd.io.gbq.read_gbq(observation_query, dialect ='standard')

observation_df

# ## Condition Occurrence Table

condition_query = """
SELECT
total.src_hpo_id,
-- IFNULL(bad_date.num_bad_rows, 0) AS num_bad_rows, 
-- IFNULL(total.num_rows, 0) AS num_rows,
ROUND(IFNULL(bad_date.num_bad_rows, 0) / IFNULL(total.num_rows, 0) * 100 , 2) as condition_occurrence

FROM

  (SELECT
  DISTINCT
  mco.src_hpo_id, COUNT(*) as num_rows
  FROM
  `{DATASET}.condition_occurrence` co
  JOIN
  `{DATASET}._mapping_condition_occurrence` mco
  ON
  co.condition_occurrence_id = mco.condition_occurrence_id
  GROUP BY 1
  ORDER BY num_rows DESC) total

LEFT JOIN

  (SELECT
  DISTINCT
  mco.src_hpo_id, COUNT(*) as num_bad_rows
  FROM
  `{DATASET}.condition_occurrence` co
  JOIN
  `{DATASET}._mapping_condition_occurrence` mco
  ON
  co.condition_occurrence_id = mco.condition_occurrence_id
  
  WHERE
  co.condition_start_datetime < CAST('1980-01-01 00:00:00' AS TIMESTAMP)
  OR
  co.condition_start_date < CAST('1980-01-01' as DATE)
  
  OR
  
  co.condition_end_datetime < CAST('1980-01-01 00:00:00' AS TIMESTAMP)
  OR
  co.condition_end_date < CAST('1980-01-01' as DATE)
  
  GROUP BY 1
  ORDER BY num_bad_rows DESC) bad_date
  
ON
bad_date.src_hpo_id = total.src_hpo_id

WHERE
LOWER(total.src_hpo_id) NOT LIKE '%rdr%'

GROUP BY 1, 2 --, 3, 4
ORDER BY condition_occurrence DESC
""".format(DATASET=DATASET)

condition_occurrence_df = pd.io.gbq.read_gbq(condition_query, dialect ='standard')

condition_occurrence_df

# ## Procedure Occurrence Table

procedure_query = """
SELECT
total.src_hpo_id,
-- IFNULL(bad_date.num_bad_rows, 0) AS num_bad_rows, 
-- IFNULL(total.num_rows, 0) AS num_rows,
ROUND(IFNULL(bad_date.num_bad_rows, 0) / IFNULL(total.num_rows, 0) * 100 , 2) as procedure_occurrence

FROM

  (SELECT
  DISTINCT
  mpo.src_hpo_id, COUNT(*) as num_rows
  FROM
  `{DATASET}.procedure_occurrence` po
  JOIN
  `{DATASET}._mapping_procedure_occurrence` mpo
  ON
  po.procedure_occurrence_id = mpo.procedure_occurrence_id
  GROUP BY 1
  ORDER BY num_rows DESC) total

LEFT JOIN

  (SELECT
  DISTINCT
  mpo.src_hpo_id, COUNT(*) as num_bad_rows
  FROM
  `{DATASET}.procedure_occurrence` po
  JOIN
  `{DATASET}._mapping_procedure_occurrence` mpo
  ON
  po.procedure_occurrence_id = mpo.procedure_occurrence_id
  
  WHERE
  po.procedure_datetime < CAST('1980-01-01 00:00:00' AS TIMESTAMP)
  OR
  po.procedure_date < CAST('1980-01-01' as DATE)
  
  GROUP BY 1
  ORDER BY num_bad_rows DESC) bad_date
  
ON
bad_date.src_hpo_id = total.src_hpo_id

WHERE
LOWER(total.src_hpo_id) NOT LIKE '%rdr%'

GROUP BY 1, 2 --, 3, 4
ORDER BY procedure_occurrence DESC
""".format(DATASET=DATASET)

procedure_occurrence_df = pd.io.gbq.read_gbq(procedure_query, dialect ='standard')

procedure_occurrence_df

# ## Visit Occurrence

visit_query = """
SELECT
total.src_hpo_id,
-- IFNULL(bad_date.num_bad_rows, 0) AS num_bad_rows, 
-- IFNULL(total.num_rows, 0) AS num_rows,
ROUND(IFNULL(bad_date.num_bad_rows, 0) / IFNULL(total.num_rows, 0) * 100 , 2) as visit_occurrence

FROM

  (SELECT
  DISTINCT
  mvo.src_hpo_id, COUNT(*) as num_rows
  FROM
  `{DATASET}.visit_occurrence` vo
  JOIN
  `{DATASET}._mapping_visit_occurrence` mvo
  ON
  vo.visit_occurrence_id = mvo.visit_occurrence_id
  GROUP BY 1
  ORDER BY num_rows DESC) total

LEFT JOIN

  (SELECT
  DISTINCT
  mvo.src_hpo_id, COUNT(*) as num_bad_rows
  FROM
  `{DATASET}.visit_occurrence` vo
  JOIN
  `{DATASET}._mapping_visit_occurrence` mvo
  ON
  vo.visit_occurrence_id = mvo.visit_occurrence_id
  
  WHERE
  vo.visit_start_datetime < CAST('1980-01-01 00:00:00' AS TIMESTAMP)
  OR
  vo.visit_start_date < CAST('1980-01-01' as DATE)
  
  OR
  
  vo.visit_end_datetime < CAST('1980-01-01 00:00:00' AS TIMESTAMP)
  OR
  vo.visit_end_date < CAST('1980-01-01' as DATE)
  
  GROUP BY 1
  ORDER BY num_bad_rows DESC) bad_date
  
ON
bad_date.src_hpo_id = total.src_hpo_id

WHERE
LOWER(total.src_hpo_id) NOT LIKE '%rdr%'

GROUP BY 1, 2 --, 3, 4
ORDER BY visit_occurrence DESC
""".format(DATASET=DATASET)

visit_occurrence_df = pd.io.gbq.read_gbq(visit_query, dialect ='standard')

visit_occurrence_df

# # Drug Exposure

drug_query = """
SELECT
total.src_hpo_id,
-- IFNULL(bad_date.num_bad_rows, 0) AS num_bad_rows, 
-- IFNULL(total.num_rows, 0) AS num_rows,
ROUND(IFNULL(bad_date.num_bad_rows, 0) / IFNULL(total.num_rows, 0) * 100 , 2) as drug_exposure

FROM

  (SELECT
  DISTINCT
  mde.src_hpo_id, COUNT(*) as num_rows
  FROM
  `{DATASET}.drug_exposure` de
  JOIN
  `{DATASET}._mapping_drug_exposure` mde
  ON
  de.drug_exposure_id = mde.drug_exposure_id
  GROUP BY 1
  ORDER BY num_rows DESC) total

LEFT JOIN

  (SELECT
  DISTINCT
  mde.src_hpo_id, COUNT(*) as num_bad_rows
  FROM
  `{DATASET}.drug_exposure` de
  JOIN
  `{DATASET}._mapping_drug_exposure` mde
  ON
  de.drug_exposure_id = mde.drug_exposure_id
  
  WHERE
  de.drug_exposure_start_datetime < CAST('1980-01-01 00:00:00' AS TIMESTAMP)
  OR
  de.drug_exposure_start_date < CAST('1980-01-01' as DATE)
  
  OR
  
  de.drug_exposure_end_datetime < CAST('1980-01-01 00:00:00' AS TIMESTAMP)
  OR
  de.drug_exposure_end_date < CAST('1980-01-01' as DATE)
  
  GROUP BY 1
  ORDER BY num_bad_rows DESC) bad_date
  
ON
bad_date.src_hpo_id = total.src_hpo_id

WHERE
LOWER(total.src_hpo_id) NOT LIKE '%rdr%'

GROUP BY 1, 2 --, 3, 4
ORDER BY drug_exposure DESC
""".format(DATASET=DATASET)

drug_exposure_df = pd.io.gbq.read_gbq(drug_query, dialect ='standard')

drug_exposure_df

# ## Measurement

measurement_query = """
SELECT
total.src_hpo_id,
-- IFNULL(bad_date.num_bad_rows, 0) AS num_bad_rows, 
-- IFNULL(total.num_rows, 0) AS num_rows,
ROUND(IFNULL(bad_date.num_bad_rows, 0) / IFNULL(total.num_rows, 0) * 100 , 2) as measurement

FROM

  (SELECT
  DISTINCT
  mm.src_hpo_id, COUNT(*) as num_rows
  FROM
  `{DATASET}.measurement` m
  JOIN
  `{DATASET}._mapping_measurement` mm
  ON
  m.measurement_id = mm.measurement_id
  GROUP BY 1
  ORDER BY num_rows DESC) total

LEFT JOIN

  (SELECT
  DISTINCT
  mm.src_hpo_id, COUNT(*) as num_bad_rows
  FROM
  `{DATASET}.measurement` m
  JOIN
  `{DATASET}._mapping_measurement` mm
  ON
  m.measurement_id = mm.measurement_id
  WHERE
  m.measurement_datetime < CAST('1900-01-01 00:00:00' AS TIMESTAMP)
  OR
  m.measurement_date < CAST('1900-01-01' as DATE)
  GROUP BY 1
  ORDER BY num_bad_rows DESC) bad_date
  
ON
bad_date.src_hpo_id = total.src_hpo_id

WHERE
LOWER(total.src_hpo_id) NOT LIKE '%rdr%'

GROUP BY 1, 2 --, 3, 4
ORDER BY measurement DESC
""".format(DATASET=DATASET)

measurement_df = pd.io.gbq.read_gbq(measurement_query, dialect ='standard')

measurement_df

# ## Bring it all together

# +
erroneous_date_df = pd.merge(
    site_df, observation_df, how='outer', on='src_hpo_id')

erroneous_date_df = pd.merge(
    erroneous_date_df, measurement_df, how='outer', on='src_hpo_id')

erroneous_date_df = pd.merge(
    erroneous_date_df, visit_occurrence_df, how='outer', on='src_hpo_id')

erroneous_date_df = pd.merge(
    erroneous_date_df, procedure_occurrence_df, how='outer', on='src_hpo_id')

erroneous_date_df = pd.merge(
    erroneous_date_df, drug_exposure_df, how='outer', on='src_hpo_id')

erroneous_date_df = pd.merge(
    erroneous_date_df, condition_occurrence_df, how='outer', on='src_hpo_id')

# +
erroneous_date_df = erroneous_date_df.fillna(0)

erroneous_date_df
# -

erroneous_date_df.to_csv("{cwd}/erroneous_dates.csv".format(cwd = cwd))


