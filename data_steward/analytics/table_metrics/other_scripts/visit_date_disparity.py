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

# +
# #!pip install --upgrade google-cloud-bigquery[pandas]
# -

from google.cloud import bigquery

# %reload_ext google.cloud.bigquery

client = bigquery.Client()

# %load_ext google.cloud.bigquery

# + endofcell="--"
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

from notebooks import parameters

import matplotlib.ticker as ticker
import matplotlib.cm as cm
import matplotlib as mpl

import matplotlib.pyplot as plt

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

cwd = os.getcwd()
cwd = str(cwd)
print(cwd)

print('done.')
# -
# --

# +
DATASET = parameters.Q2_2019

print("Dataset to use: {DATASET}".format(DATASET = DATASET))

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
# # +
######################################
print('Getting additional sites that may not already be in the dataframe')
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
    WHERE src_hpo_id NOT LIKE '%rdr%'
    order by 1
    '''.format(DATASET = DATASET),
                              dialect='standard')
print(site_map.shape[0], 'records received.')
# -

site_map

site_df = pd.merge(site_map, site_df, how='outer', on='src_hpo_id')

site_df
# --

# ### The below query is used to generate a 'procedure/visit dataframe'. This dataframe shows the difference between the start/end times for the same visit_occurrence_id with respect to the procedure table.
#
# ### Each row shows information for:
# - The difference between the visit start date and the procedure date
# - The difference between the visit end date and the procedure date
# - The difference between the visit start datetime (as a date) and the procedure date
# - The difference between the visit end datetime (as a date) and the procedure date
# - The difference between the visit start datetime (as a date) and the procedure datetime (as a date)
# - The difference between the visit end datetime (as a date) and the procedure datetime (as a date)
# - The sum of all the values listed above
#
# ### While we will only be using the 'total number of bad rows' at this time, the other columns may be useful for subsequent analyses down the line

p_v_query = """
SELECT
DISTINCT
a.*, 
(a.procedure_vis_start_diff + a.procedure_vis_end_diff + a.procedure_vis_start_dt_diff + a.procedure_vis_end_dt_diff + a.procedure_dt_vis_start_dt_diff + a.procedure_dt_vis_end_dt_diff) as total_diff
FROM 
( SELECT
  mpo.src_hpo_id, COUNT(mpo.src_hpo_id) as num_bad_records, 
  IFNULL(ABS(DATE_DIFF(po.procedure_date, vo.visit_start_date, DAY)), 0) as procedure_vis_start_diff,
  IFNULL(ABS(DATE_DIFF(po.procedure_date, vo.visit_end_date, DAY)), 0) as procedure_vis_end_diff,
  IFNULL(ABS(DATE_DIFF(CAST(vo.visit_start_datetime AS DATE), po.procedure_date, DAY)), 0) as procedure_vis_start_dt_diff,
  IFNULL(ABS(DATE_DIFF(CAST(vo.visit_end_datetime AS DATE), po.procedure_date, DAY)), 0) as procedure_vis_end_dt_diff,
  IFNULL(ABS(DATE_DIFF(CAST(po.procedure_datetime AS DATE), CAST(vo.visit_start_datetime AS DATE), DAY)), 0) as procedure_dt_vis_start_dt_diff,
  IFNULL(ABS(DATE_DIFF(CAST(po.procedure_datetime AS DATE), CAST(vo.visit_end_datetime AS DATE), DAY)), 0) as procedure_dt_vis_end_dt_diff,
  (
  ABS(DATE_DIFF(po.procedure_date, vo.visit_start_date, DAY)) = 
  ABS(DATE_DIFF(po.procedure_date, vo.visit_end_date, DAY)) 
  AND
  ABS(DATE_DIFF(po.procedure_date, vo.visit_end_date, DAY)) =
  ABS(DATE_DIFF(CAST(vo.visit_start_datetime AS DATE), po.procedure_date, DAY)) 
  AND
  ABS(DATE_DIFF(CAST(vo.visit_start_datetime AS DATE), po.procedure_date, DAY)) =
  ABS(DATE_DIFF(CAST(vo.visit_end_datetime AS DATE), po.procedure_date, DAY))
  AND
  ABS(DATE_DIFF(CAST(vo.visit_end_datetime AS DATE), po.procedure_date, DAY)) = 
  ABS(DATE_DIFF(CAST(po.procedure_datetime AS DATE), CAST(vo.visit_start_datetime AS DATE), DAY)) 
  AND
  ABS(DATE_DIFF(CAST(po.procedure_datetime AS DATE), CAST(vo.visit_start_datetime AS DATE), DAY)) = 
  ABS(DATE_DIFF(CAST(po.procedure_datetime AS DATE), CAST(vo.visit_end_datetime AS DATE), DAY))
  ) as all_discrepancies_equal
  FROM
  `{DATASET}.procedure_occurrence` po
  LEFT JOIN
  `{DATASET}._mapping_procedure_occurrence` mpo
  ON
  po.procedure_occurrence_id = mpo.procedure_occurrence_id
  LEFT JOIN
  `{DATASET}.visit_occurrence` vo
  ON
  po.visit_occurrence_id = vo.visit_occurrence_id
  WHERE
    -- must have populated visit occurrence id
    (
    po.visit_occurrence_id IS NOT NULL
    AND
    po.visit_occurrence_id <> 0
    AND
    vo.visit_occurrence_id IS NOT NULL
    AND
    vo.visit_occurrence_id <> 0
    )
  AND
    (
    -- problem with procedure date
    (po.procedure_date < vo.visit_start_date
    OR
    po.procedure_date > vo.visit_end_date)
    OR 
    -- problem with datetime
    (po.procedure_datetime < vo.visit_start_datetime
    OR
    po.procedure_datetime > vo.visit_end_datetime )
    OR
    -- problem with the datetime (extracting date for comparison)
    (po.procedure_date < CAST(vo.visit_start_datetime AS DATE)
    OR
    po.procedure_date > CAST(vo.visit_end_datetime AS DATE))
    
    OR
    
    --problem with the datetime
    (CAST(po.procedure_datetime AS DATE) < CAST(vo.visit_start_datetime AS DATE)
    OR
    CAST(po.procedure_datetime AS DATE) > CAST(vo.visit_end_datetime AS DATE)
    )
    )
  GROUP BY mpo.src_hpo_id, po.procedure_date, vo.visit_start_date, vo.visit_end_date, vo.visit_start_datetime, vo.visit_end_datetime, po.procedure_datetime
  ORDER BY all_discrepancies_equal ASC, num_bad_records DESC
) a
WHERE
-- cannot compare date/datetime date accurately because of problem with UTC dates not converting properly. give 'wiggle room ' of 1
(
a.procedure_vis_start_dt_diff > 1
OR
a.procedure_vis_end_dt_diff > 1
OR
a.procedure_vis_start_diff > 0
OR
a.procedure_vis_end_diff > 0
OR
a.procedure_dt_vis_start_dt_diff > 0
OR
a.procedure_dt_vis_end_dt_diff > 0
)
AND
a.src_hpo_id NOT LIKE '%rdr%'
ORDER BY src_hpo_id ASC, num_bad_records DESC, total_diff DESC, all_discrepancies_equal ASC
""".format(DATASET = DATASET)

procedure_visit_df = pd.io.gbq.read_gbq(p_v_query, dialect='standard')



procedure_visit_df

# ### Now let's make the dataframe a little more condensed - only show the total number of 'bad records' for each site

bad_procedure_records_df = procedure_visit_df.groupby('src_hpo_id')['num_bad_records'].sum().to_frame()

bad_procedure_records_df

num_total_procedure_records_query = """
SELECT
DISTINCT
mp.src_hpo_id, count(p.procedure_occurrence_id) as num_total_records
FROM
`{DATASET}.procedure_occurrence`p
JOIN
`{DATASET}._mapping_procedure_occurrence` mp
ON
p.procedure_occurrence_id = mp.procedure_occurrence_id
WHERE mp.src_hpo_id NOT LIKE '%rdr%'
GROUP BY 1
ORDER BY num_total_records DESC
""".format(DATASET = DATASET)

total_procedure_df = pd.io.gbq.read_gbq(num_total_procedure_records_query, dialect='standard')

total_procedure_df = pd.merge(total_procedure_df, site_df, how='outer', on='src_hpo_id')

total_procedure_df = total_procedure_df[['src_hpo_id', 'HPO', 'num_total_records']]

final_procedure_df = pd.merge(total_procedure_df, bad_procedure_records_df, how='outer', on='src_hpo_id') 

final_procedure_df = final_procedure_df.fillna(0)

# ### Now we can actually calculate the 'tangible success rate'

final_procedure_df['procedure_date_adherece'] = \
round((final_procedure_df['num_total_records'] - final_procedure_df['num_bad_records']) / final_procedure_df['num_total_records'] * 100, 2)

# +
final_procedure_df = final_procedure_df.fillna(0)

final_procedure_df = final_procedure_df.sort_values(by=['procedure_date_adherece'], ascending = False)
# -

final_procedure_df

# ### to ensure all the dataframes are easy to ultimately merge, let's create a dataframe that only has the success rates and HPOs

short_procedure_df = final_procedure_df.drop(columns=['num_total_records', 'num_bad_records', 'HPO'])

# # Now let's move to the observation table

observation_visit_query = """
SELECT
DISTINCT
a.*, 
(a.observation_vis_start_diff + a.observation_vis_end_diff + a.observation_vis_start_dt_diff + a.observation_vis_end_dt_diff + a.observation_dt_vis_start_dt_diff + a.observation_dt_vis_end_dt_diff) as total_diff
FROM 
( SELECT
  mo.src_hpo_id, COUNT(mo.src_hpo_id) as num_bad_records, 
  IFNULL(ABS(DATE_DIFF(o.observation_date, vo.visit_start_date, DAY)), 0) as observation_vis_start_diff,
  IFNULL(ABS(DATE_DIFF(o.observation_date, vo.visit_end_date, DAY)), 0) as observation_vis_end_diff,
  IFNULL(ABS(DATE_DIFF(CAST(vo.visit_start_datetime AS DATE), o.observation_date, DAY)), 0) as observation_vis_start_dt_diff,
  IFNULL(ABS(DATE_DIFF(CAST(vo.visit_end_datetime AS DATE), o.observation_date, DAY)), 0) as observation_vis_end_dt_diff,
  IFNULL(ABS(DATE_DIFF(CAST(o.observation_datetime AS DATE), CAST(vo.visit_start_datetime AS DATE), DAY)), 0) as observation_dt_vis_start_dt_diff,
  IFNULL(ABS(DATE_DIFF(CAST(o.observation_datetime AS DATE), CAST(vo.visit_end_datetime AS DATE), DAY)), 0) as observation_dt_vis_end_dt_diff,

  (
  ABS(DATE_DIFF(o.observation_date, vo.visit_start_date, DAY)) = 
  ABS(DATE_DIFF(o.observation_date, vo.visit_end_date, DAY)) 
  AND
  ABS(DATE_DIFF(o.observation_date, vo.visit_end_date, DAY)) =
  ABS(DATE_DIFF(CAST(vo.visit_start_datetime AS DATE), o.observation_date, DAY)) 
  AND
  ABS(DATE_DIFF(CAST(vo.visit_start_datetime AS DATE), o.observation_date, DAY)) =
  ABS(DATE_DIFF(CAST(vo.visit_end_datetime AS DATE), o.observation_date, DAY))
  AND
  ABS(DATE_DIFF(CAST(vo.visit_end_datetime AS DATE), o.observation_date, DAY)) = 
  ABS(DATE_DIFF(CAST(o.observation_datetime AS DATE), CAST(vo.visit_start_datetime AS DATE), DAY)) 
  AND
  ABS(DATE_DIFF(CAST(o.observation_datetime AS DATE), CAST(vo.visit_start_datetime AS DATE), DAY)) = 
  ABS(DATE_DIFF(CAST(o.observation_datetime AS DATE), CAST(vo.visit_end_datetime AS DATE), DAY))
  ) as all_discrepancies_equal

  FROM
  `{DATASET}.observation` o
  LEFT JOIN
  `{DATASET}._mapping_observation` mo
  ON
  o.observation_id = mo.observation_id
  LEFT JOIN
  `{DATASET}.visit_occurrence` vo
  ON
  o.visit_occurrence_id = vo.visit_occurrence_id

  WHERE
    -- must have populated visit occurrence id
    (
    o.visit_occurrence_id IS NOT NULL
    AND
    o.visit_occurrence_id <> 0
    AND
    vo.visit_occurrence_id IS NOT NULL
    AND
    vo.visit_occurrence_id <> 0
    )

  AND
    (
    -- problem with procedure date
    (o.observation_date < vo.visit_start_date
    OR
    o.observation_date > vo.visit_end_date)

    OR 
    -- problem with datetime
    (o.observation_datetime < vo.visit_start_datetime
    OR
    o.observation_datetime > vo.visit_end_datetime )

    OR
    -- problem with the datetime (extracting date for comparison)
    (o.observation_date < CAST(vo.visit_start_datetime AS DATE)
    OR
    o.observation_date > CAST(vo.visit_end_datetime AS DATE))
    
    OR
    
    --problem with the datetime
    (CAST(o.observation_datetime AS DATE) < CAST(vo.visit_start_datetime AS DATE)
    OR
    CAST(o.observation_datetime AS DATE) > CAST(vo.visit_end_datetime AS DATE)
    )
    )

  GROUP BY mo.src_hpo_id, o.observation_date, vo.visit_start_date, vo.visit_end_date, vo.visit_start_datetime, vo.visit_end_datetime, o.observation_datetime
  ORDER BY all_discrepancies_equal ASC, num_bad_records DESC
) a
WHERE
-- cannot compare date/datetime date accurately because of problem with UTC dates not converting properly. give 'wiggle room ' of 1
(
a.observation_vis_start_dt_diff > 1
OR
a.observation_vis_end_dt_diff > 1
OR
a.observation_vis_start_diff > 0
OR
a.observation_vis_end_diff > 0
OR
a.observation_dt_vis_start_dt_diff > 0
OR
a.observation_dt_vis_end_dt_diff > 0
)
AND
a.src_hpo_id NOT LIKE '%rdr%'
ORDER BY src_hpo_id ASC, num_bad_records DESC, total_diff DESC, all_discrepancies_equal ASC
""".format(DATASET = DATASET)


observation_visit_df = pd.io.gbq.read_gbq(observation_visit_query, dialect='standard')

# ### Now let's make the dataframe a little more condensed - only show the total number of 'bad records' for each site

bad_observation_records_df = observation_visit_df.groupby('src_hpo_id')['num_bad_records'].sum().to_frame()

num_total_observation_records_query = """
SELECT
DISTINCT
mo.src_hpo_id, count(o.observation_id) as num_total_records
FROM
`{DATASET}.observation`o
JOIN
`{DATASET}._mapping_observation` mo
ON
o.observation_id = mo.observation_id
WHERE mo.src_hpo_id NOT LIKE '%rdr%'
GROUP BY 1
ORDER BY num_total_records DESC
""".format(DATASET = DATASET)

total_observation_df = pd.io.gbq.read_gbq(num_total_observation_records_query, dialect='standard')

# +
total_observation_df = pd.merge(total_observation_df, site_df, how='outer', on='src_hpo_id')

total_observation_df = total_observation_df[['src_hpo_id', 'HPO', 'num_total_records']]
# -

final_observation_df = pd.merge(total_observation_df, bad_observation_records_df, how='outer', on='src_hpo_id') 

final_observation_df = final_observation_df.fillna(0)

# ### Now we can actually calculate the 'tangible success rate'

final_observation_df['observation_date_adherence'] = \
round((final_observation_df['num_total_records'] - final_observation_df['num_bad_records']) / final_observation_df['num_total_records'] * 100, 2)

# +
final_observation_df = final_observation_df.fillna(0)

final_observation_df = final_observation_df.sort_values(by=['observation_date_adherence'], ascending = False)
# -

# ### Creating a shorter df

short_observation_df = final_observation_df.drop(columns=['num_total_records', 'num_bad_records', 'HPO'])

short_observation_df

# # Next up: the measurement table

measurement_visit_query = """
SELECT
DISTINCT
a.*, 
(a.measurement_vis_start_diff + a.measurement_vis_end_diff + a.measurement_vis_start_dt_diff + a.measurement_vis_end_dt_diff + a.measurement_dt_vis_start_dt_diff + a.measurement_dt_vis_end_dt_diff) as total_diff
FROM 
( SELECT
  mm.src_hpo_id, COUNT(mm.src_hpo_id) as num_bad_records, 
  IFNULL(ABS(DATE_DIFF(m.measurement_date, vo.visit_start_date, DAY)), 0) as measurement_vis_start_diff,
  IFNULL(ABS(DATE_DIFF(m.measurement_date, vo.visit_end_date, DAY)), 0) as measurement_vis_end_diff,
  IFNULL(ABS(DATE_DIFF(CAST(vo.visit_start_datetime AS DATE), m.measurement_date, DAY)), 0) as measurement_vis_start_dt_diff,
  IFNULL(ABS(DATE_DIFF(CAST(vo.visit_end_datetime AS DATE), m.measurement_date, DAY)), 0) as measurement_vis_end_dt_diff,
  IFNULL(ABS(DATE_DIFF(CAST(m.measurement_datetime AS DATE), CAST(vo.visit_start_datetime AS DATE), DAY)), 0) as measurement_dt_vis_start_dt_diff,
  IFNULL(ABS(DATE_DIFF(CAST(m.measurement_datetime AS DATE), CAST(vo.visit_end_datetime AS DATE), DAY)), 0) as measurement_dt_vis_end_dt_diff,

  (
  ABS(DATE_DIFF(m.measurement_date, vo.visit_start_date, DAY)) = 
  ABS(DATE_DIFF(m.measurement_date, vo.visit_end_date, DAY)) 
  AND
  ABS(DATE_DIFF(m.measurement_date, vo.visit_end_date, DAY)) =
  ABS(DATE_DIFF(CAST(vo.visit_start_datetime AS DATE), m.measurement_date, DAY)) 
  AND
  ABS(DATE_DIFF(CAST(vo.visit_start_datetime AS DATE), m.measurement_date, DAY)) =
  ABS(DATE_DIFF(CAST(vo.visit_end_datetime AS DATE), m.measurement_date, DAY))
  AND
  ABS(DATE_DIFF(CAST(vo.visit_end_datetime AS DATE), m.measurement_date, DAY)) = 
  ABS(DATE_DIFF(CAST(m.measurement_datetime AS DATE), CAST(vo.visit_start_datetime AS DATE), DAY)) 
  AND
  ABS(DATE_DIFF(CAST(m.measurement_datetime AS DATE), CAST(vo.visit_start_datetime AS DATE), DAY)) = 
  ABS(DATE_DIFF(CAST(m.measurement_datetime AS DATE), CAST(vo.visit_end_datetime AS DATE), DAY))
  ) as all_discrepancies_equal

  FROM
  `{DATASET}.measurement` m
  LEFT JOIN
  `{DATASET}._mapping_measurement` mm
  ON
  m.measurement_id = mm.measurement_id
  LEFT JOIN
  `{DATASET}.visit_occurrence` vo
  ON
  m.visit_occurrence_id = vo.visit_occurrence_id

  WHERE
    -- must have populated visit occurrence id
    (
    m.visit_occurrence_id IS NOT NULL
    AND
    m.visit_occurrence_id <> 0
    AND
    vo.visit_occurrence_id IS NOT NULL
    AND
    vo.visit_occurrence_id <> 0
    )

  AND
    (
    -- problem with procedure date
    (m.measurement_date < vo.visit_start_date
    OR
    m.measurement_date > vo.visit_end_date)

    OR 
    -- problem with datetime
    (m.measurement_datetime < vo.visit_start_datetime
    OR
    m.measurement_datetime > vo.visit_end_datetime )

    OR
    -- problem with the datetime (extracting date for comparison)
    (m.measurement_date < CAST(vo.visit_start_datetime AS DATE)
    OR
    m.measurement_date > CAST(vo.visit_end_datetime AS DATE))
    
    OR
    
    --problem with the datetime
    (CAST(m.measurement_datetime AS DATE) < CAST(vo.visit_start_datetime AS DATE)
    OR
    CAST(m.measurement_datetime AS DATE) > CAST(vo.visit_end_datetime AS DATE)
    )
    )

  GROUP BY mm.src_hpo_id, m.measurement_date, vo.visit_start_date, vo.visit_end_date, vo.visit_start_datetime, vo.visit_end_datetime, m.measurement_datetime
  ORDER BY all_discrepancies_equal ASC, num_bad_records DESC
) a
WHERE
-- cannot compare date/datetime date accurately because of problem with UTC dates not converting properly. give 'wiggle room ' of 1
(
a.measurement_vis_start_dt_diff > 1
OR
a.measurement_vis_end_dt_diff > 1
OR
a.measurement_vis_start_diff > 0
OR
a.measurement_vis_end_diff > 0
OR
a.measurement_dt_vis_start_dt_diff > 0
OR
a.measurement_dt_vis_end_dt_diff > 0
)
AND a.src_hpo_id NOT LIKE '%rdr%'
ORDER BY src_hpo_id ASC, num_bad_records DESC, total_diff DESC, all_discrepancies_equal ASC
""".format(DATASET = DATASET)

measurement_visit_df = pd.io.gbq.read_gbq(measurement_visit_query, dialect='standard')

# ### Now let's make the dataframe a little more condensed - only show the total number of 'bad records' for each site

bad_measurement_records_df = measurement_visit_df.groupby('src_hpo_id')['num_bad_records'].sum().to_frame()

num_total_measurement_records_query = """
SELECT
DISTINCT
mm.src_hpo_id, count(m.measurement_id) as num_total_records
FROM
`{DATASET}.measurement` m
JOIN
`{DATASET}._mapping_measurement` mm
ON
m.measurement_id = mm.measurement_id
WHERE mm.src_hpo_id NOT LIKE '%rdr%'
GROUP BY 1
ORDER BY num_total_records DESC
""".format(DATASET = DATASET)

total_measurement_df = pd.io.gbq.read_gbq(num_total_measurement_records_query, dialect='standard')

# +
total_measurement_df = pd.merge(total_measurement_df, site_df, how='outer', on='src_hpo_id')

total_measurement_df = total_measurement_df[['src_hpo_id', 'HPO', 'num_total_records']]

# +
final_measurment_df = pd.merge(total_measurement_df, bad_measurement_records_df, how='outer', on='src_hpo_id') 

final_measurment_df = final_measurment_df.fillna(0)
# -

# ### Now we can actually calculate the 'tangible success rate'

final_measurment_df['measurement_date_adherence'] = \
round((final_measurment_df['num_total_records'] - final_measurment_df['num_bad_records']) / final_measurment_df['num_total_records'] * 100, 2)

# +
final_measurment_df = final_measurment_df.fillna(0)

final_measurment_df = final_measurment_df.sort_values(by=['measurement_date_adherence'], ascending = False)

# +
### Creating a shorter df

# +
short_measurement_df = final_measurment_df.drop(columns=['num_total_records', 'num_bad_records', 'HPO'])

short_measurement_df
# -
# # Next up: the condition table


condition_visit_query = """
SELECT
DISTINCT
a.*, 
(a.condition_vis_start_diff + a.condition_vis_start_dt_diff + a.condition_dt_vis_start_dt_diff) as total_diff
FROM 
( SELECT
  mco.src_hpo_id, COUNT(mco.src_hpo_id) as num_bad_records, 
  IFNULL(ABS(DATE_DIFF(co.condition_start_date, vo.visit_start_date, DAY)), 0) as condition_vis_start_diff,
  IFNULL(ABS(DATE_DIFF(CAST(vo.visit_start_datetime AS DATE), co.condition_start_date, DAY)), 0) as condition_vis_start_dt_diff,
  IFNULL(ABS(DATE_DIFF(CAST(co.condition_start_datetime AS DATE), CAST(vo.visit_start_datetime AS DATE), DAY)), 0) as condition_dt_vis_start_dt_diff,
  
  (
  ABS(DATE_DIFF(co.condition_start_date, vo.visit_start_date, DAY)) = 
  ABS(DATE_DIFF(CAST(vo.visit_start_datetime AS DATE), co.condition_start_date, DAY)) 
  AND
  ABS(DATE_DIFF(CAST(vo.visit_start_datetime AS DATE), co.condition_start_date, DAY)) =
  ABS(DATE_DIFF(CAST(co.condition_start_datetime AS DATE), CAST(vo.visit_start_datetime AS DATE), DAY)) 
  ) as all_discrepancies_equal

  FROM
  `{DATASET}.condition_occurrence` co
  LEFT JOIN
  `{DATASET}._mapping_condition_occurrence` mco
  ON
  co.condition_occurrence_id = mco.condition_occurrence_id
  LEFT JOIN
  `{DATASET}.visit_occurrence` vo
  ON
  co.visit_occurrence_id = vo.visit_occurrence_id

  WHERE
    -- must have populated visit occurrence id
    (
    co.visit_occurrence_id IS NOT NULL
    AND
    co.visit_occurrence_id <> 0
    AND
    vo.visit_occurrence_id IS NOT NULL
    AND
    vo.visit_occurrence_id <> 0
    )

  AND
    (
    -- problem with procedure date
    (co.condition_start_date < vo.visit_start_date)

    OR 
    -- problem with datetime
    (co.condition_start_datetime < vo.visit_start_datetime)

    OR
    -- problem with the datetime (extracting date for comparison)
    (co.condition_start_date < CAST(vo.visit_start_datetime AS DATE))
    
    OR
    
    --problem with the datetime
    (CAST(co.condition_start_datetime AS DATE) < CAST(vo.visit_start_datetime AS DATE))
    )

  GROUP BY mco.src_hpo_id, co.condition_start_date, vo.visit_start_date, vo.visit_end_date, vo.visit_start_datetime, vo.visit_end_datetime, co.condition_start_datetime
  ORDER BY all_discrepancies_equal ASC, num_bad_records DESC
) a
WHERE
-- cannot compare date/datetime date accurately because of problem with UTC dates not converting properly. give 'wiggle room ' of 1
(
a.condition_vis_start_dt_diff > 1
OR
a.condition_vis_start_diff > 0
OR
a.condition_dt_vis_start_dt_diff > 0
)
AND a.src_hpo_id NOT LIKE '%rdr%'
ORDER BY src_hpo_id ASC, num_bad_records DESC, total_diff DESC, all_discrepancies_equal ASC
""".format(DATASET = DATASET)

condition_visit_df = pd.io.gbq.read_gbq(condition_visit_query, dialect='standard')

# ### Now let's make the dataframe a little more condensed - only show the total number of 'bad records' for each site

bad_condition_records_df = condition_visit_df.groupby('src_hpo_id')['num_bad_records'].sum().to_frame()

num_total_conditions_query = """
SELECT
DISTINCT
mco.src_hpo_id, count(co.condition_occurrence_id) as num_total_records
FROM
`{DATASET}.condition_occurrence` co
JOIN
`{DATASET}._mapping_condition_occurrence` mco
ON
co.condition_occurrence_id = mco.condition_occurrence_id
WHERE mco.src_hpo_id NOT LIKE '%rdr%'
GROUP BY 1
ORDER BY num_total_records DESC
""".format(DATASET = DATASET)

total_condition_df = pd.io.gbq.read_gbq(num_total_conditions_query, dialect='standard')

# +
total_condition_df = pd.merge(total_condition_df, site_df, how='outer', on='src_hpo_id')

total_condition_df = total_condition_df[['src_hpo_id', 'HPO', 'num_total_records']]

# +
final_condition_df = pd.merge(total_condition_df, bad_condition_records_df, how='outer', on='src_hpo_id') 

final_condition_df = final_condition_df.fillna(0)
# -

# ### Now we can actually calculate the 'tangible success rate'

final_condition_df['condition_date_adherence'] = \
round((final_condition_df['num_total_records'] - final_condition_df['num_bad_records']) / final_condition_df['num_total_records'] * 100, 2)

# +
final_condition_df = final_condition_df.fillna(0)

final_condition_df = final_condition_df.sort_values(by=['condition_date_adherence'], ascending = False)
# -

# ### Creating a shorter df

# +
short_condition_df = final_condition_df.drop(columns=['num_total_records', 'num_bad_records', 'HPO'])

short_condition_df
# -

# # Last but not least - the drug exposure table

drug_visit_query = """
SELECT
DISTINCT
a.*, 
(a.drug_vis_start_diff + a.drug_vis_start_dt_diff + a.drug_dt_vis_start_dt_diff) as total_diff
FROM 
( SELECT
  mde.src_hpo_id, COUNT(mde.src_hpo_id) as num_bad_records, 
  IFNULL(ABS(DATE_DIFF(de.drug_exposure_start_date, vo.visit_start_date, DAY)), 0) as drug_vis_start_diff,
  IFNULL(ABS(DATE_DIFF(CAST(vo.visit_start_datetime AS DATE), de.drug_exposure_start_date, DAY)), 0) as drug_vis_start_dt_diff,
  IFNULL(ABS(DATE_DIFF(CAST(de.drug_exposure_start_datetime AS DATE), CAST(vo.visit_start_datetime AS DATE), DAY)), 0) as drug_dt_vis_start_dt_diff,
  
  (
  ABS(DATE_DIFF(de.drug_exposure_start_date, vo.visit_start_date, DAY)) = 
  ABS(DATE_DIFF(CAST(vo.visit_start_datetime AS DATE), de.drug_exposure_start_date, DAY)) 
  AND
  ABS(DATE_DIFF(CAST(vo.visit_start_datetime AS DATE), de.drug_exposure_start_date, DAY)) =
  ABS(DATE_DIFF(CAST(de.drug_exposure_start_datetime AS DATE), CAST(vo.visit_start_datetime AS DATE), DAY)) 
  ) as all_discrepancies_equal

  FROM
  `{DATASET}.drug_exposure` de
  LEFT JOIN
  `{DATASET}._mapping_drug_exposure` mde
  ON
  de.drug_exposure_id = mde.drug_exposure_id
  LEFT JOIN
  `{DATASET}.visit_occurrence` vo
  ON
  de.visit_occurrence_id = vo.visit_occurrence_id

  WHERE
    -- must have populated visit occurrence id
    (
    de.visit_occurrence_id IS NOT NULL
    AND
    de.visit_occurrence_id <> 0
    AND
    vo.visit_occurrence_id IS NOT NULL
    AND
    vo.visit_occurrence_id <> 0
    )

  AND
    (
    -- problem with procedure date
    (de.drug_exposure_start_date < vo.visit_start_date)

    OR 
    -- problem with datetime
    (de.drug_exposure_start_datetime < vo.visit_start_datetime)

    OR
    -- problem with the datetime (extracting date for comparison)
    (de.drug_exposure_start_date < CAST(vo.visit_start_datetime AS DATE))
    
    OR
    
    --problem with the datetime
    (CAST(de.drug_exposure_start_datetime AS DATE) < CAST(vo.visit_start_datetime AS DATE))
    )

  GROUP BY mde.src_hpo_id, de.drug_exposure_start_date, vo.visit_start_date, vo.visit_end_date, vo.visit_start_datetime, vo.visit_end_datetime, de.drug_exposure_start_datetime
  ORDER BY all_discrepancies_equal ASC, num_bad_records DESC
) a
WHERE
-- cannot compare date/datetime date accurately because of problem with UTC dates not converting properly. give 'wiggle room ' of 1
(
a.drug_vis_start_dt_diff > 1
OR
a.drug_vis_start_diff > 0
OR
a.drug_dt_vis_start_dt_diff > 0
)
AND
a.src_hpo_id NOT LIKE '%rdr%'
ORDER BY src_hpo_id ASC, num_bad_records DESC, total_diff DESC, all_discrepancies_equal ASC
""".format(DATASET = DATASET)

drug_visit_df = pd.io.gbq.read_gbq(drug_visit_query, dialect='standard')

# ### Now let's make the dataframe a little more condensed - only show the total number of 'bad records' for each site

bad_drug_records_df = drug_visit_df.groupby('src_hpo_id')['num_bad_records'].sum().to_frame()

num_total_drugs_query = """
SELECT
DISTINCT
mde.src_hpo_id, count(de.drug_exposure_id) as num_total_records
FROM
`{DATASET}.drug_exposure` de
JOIN
`{DATASET}._mapping_drug_exposure` mde
ON
de.drug_exposure_id = mde.drug_exposure_id
AND
mde.src_hpo_id NOT LIKE '%rdr%'
GROUP BY 1
ORDER BY num_total_records DESC
""".format(DATASET = DATASET)

total_drug_df = pd.io.gbq.read_gbq(num_total_drugs_query, dialect='standard')

# +
total_drug_df = pd.merge(total_drug_df, site_df, how='outer', on='src_hpo_id')

total_drug_df = total_drug_df[['src_hpo_id', 'HPO', 'num_total_records']]

# +
final_drug_df = pd.merge(total_drug_df, bad_drug_records_df, how='outer', on='src_hpo_id') 

final_drug_df = final_drug_df.fillna(0)
# -

# ### Now we can actually calculate the 'tangible success rate'

final_drug_df['drug_date_adherence'] = \
round((final_drug_df['num_total_records'] - final_drug_df['num_bad_records']) / final_drug_df['num_total_records'] * 100, 2)

# +
final_drug_df = final_drug_df.fillna(0)

final_drug_df = final_drug_df.sort_values(by=['drug_date_adherence'], ascending = False)
# -

# ### Creating a shorter dataframe

# +
short_drug_df = final_drug_df.drop(columns=['num_total_records', 'num_bad_records', 'HPO'])

short_drug_df
# -

final_success_df = 0

final_success_df = pd.merge(short_drug_df, site_df, how='outer', on='src_hpo_id') 
final_success_df = final_success_df[['src_hpo_id', 'HPO', 'drug_date_adherence']]  #rearrang columnds

# +
final_success_df = pd.merge(final_success_df, short_observation_df, how='outer', on='src_hpo_id') 
final_success_df = pd.merge(final_success_df, short_procedure_df, how='outer', on='src_hpo_id')
final_success_df = pd.merge(final_success_df, short_measurement_df, how='outer', on='src_hpo_id') 
final_success_df = pd.merge(final_success_df, short_condition_df, how='outer', on='src_hpo_id')

final_success_df
# -

final_success_df.to_csv("{cwd}/visit_date_disparity.csv".format(cwd = cwd))


