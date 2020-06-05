# ---
# jupyter:
#   jupytext:
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

from google.cloud import bigquery
# %reload_ext google.cloud.bigquery
client = bigquery.Client()
# %load_ext google.cloud.bigquery

# +
from notebooks import parameters
DATASET = parameters.LATEST_DATASET
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
#
# ### NOTE: This assumes that all of the relevant HPOs have a person table.

hpo_id_query = f"""
SELECT REPLACE(table_id, '_person', '') AS src_hpo_id
FROM
`{DATASET}.__TABLES__`
WHERE table_id LIKE '%person' 
AND table_id 
NOT LIKE '%unioned_ehr_%' 
AND table_id NOT LIKE '\\\_%'
"""

site_df = pd.io.gbq.read_gbq(hpo_id_query, dialect='standard')

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

site_df = pd.merge(site_df, full_names_df, on=['src_hpo_id'], how='left')
# -

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
  `{DATASET}.unioned_ehr_procedure_occurrence` po
  LEFT JOIN
  `{DATASET}._mapping_procedure_occurrence` mpo
  ON
  po.procedure_occurrence_id = mpo.procedure_occurrence_id
  LEFT JOIN
  `{DATASET}.unioned_ehr_visit_occurrence` vo
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
`{DATASET}.unioned_ehr_procedure_occurrence`p
JOIN
`{DATASET}._mapping_procedure_occurrence` mp
ON
p.procedure_occurrence_id = mp.procedure_occurrence_id
GROUP BY 1
ORDER BY num_total_records DESC
""".format(DATASET = DATASET)

total_procedure_df = pd.io.gbq.read_gbq(num_total_procedure_records_query, dialect='standard')

total_procedure_df = pd.merge(total_procedure_df, site_df, how='outer', on='src_hpo_id')

total_procedure_df = total_procedure_df[['src_hpo_id', 'num_total_records']]

final_procedure_df = pd.merge(total_procedure_df, bad_procedure_records_df, how='outer', on='src_hpo_id') 

final_procedure_df = final_procedure_df.fillna(0)

# ### Now we can actually calculate the 'tangible success rate'

final_procedure_df['procedure_failure_rate'] = \
round((final_procedure_df['num_bad_records']) / final_procedure_df['num_total_records'] * 100, 2)

# +
final_procedure_df = final_procedure_df.fillna(0)

final_procedure_df = final_procedure_df.sort_values(by=['procedure_failure_rate'], ascending = False)
# -

final_procedure_df

# ### to ensure all the dataframes are easy to ultimately merge, let's create a dataframe that only has the success rates and HPOs

short_procedure_df = final_procedure_df.drop(columns=['num_total_records', 'num_bad_records'])

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
  `{DATASET}.unioned_ehr_observation` o
  LEFT JOIN
  `{DATASET}._mapping_observation` mo
  ON
  o.observation_id = mo.observation_id
  LEFT JOIN
  `{DATASET}.unioned_ehr_visit_occurrence` vo
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
`{DATASET}.unioned_ehr_observation`o
JOIN
`{DATASET}._mapping_observation` mo
ON
o.observation_id = mo.observation_id
GROUP BY 1
ORDER BY num_total_records DESC
""".format(DATASET = DATASET)

total_observation_df = pd.io.gbq.read_gbq(num_total_observation_records_query, dialect='standard')

# +
total_observation_df = pd.merge(total_observation_df, site_df, how='outer', on='src_hpo_id')

total_observation_df = total_observation_df[['src_hpo_id', 'num_total_records']]
# -

final_observation_df = pd.merge(total_observation_df, bad_observation_records_df, how='outer', on='src_hpo_id') 

final_observation_df = final_observation_df.fillna(0)

# ### Now we can actually calculate the 'tangible success rate'

final_observation_df['observation_date_failure'] = \
round((final_observation_df['num_bad_records']) / final_observation_df['num_total_records'] * 100, 2)

# +
final_observation_df = final_observation_df.fillna(0)

final_observation_df = final_observation_df.sort_values(by=['observation_date_failure'], ascending = False)
# -

# ### Creating a shorter df

short_observation_df = final_observation_df.drop(columns=['num_total_records', 'num_bad_records'])

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
  `{DATASET}.unioned_ehr_measurement` m
  LEFT JOIN
  `{DATASET}._mapping_measurement` mm
  ON
  m.measurement_id = mm.measurement_id
  LEFT JOIN
  `{DATASET}.unioned_ehr_visit_occurrence` vo
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
`{DATASET}.unioned_ehr_measurement` m
JOIN
`{DATASET}._mapping_measurement` mm
ON
m.measurement_id = mm.measurement_id
GROUP BY 1
ORDER BY num_total_records DESC
""".format(DATASET = DATASET)

total_measurement_df = pd.io.gbq.read_gbq(num_total_measurement_records_query, dialect='standard')

# +
total_measurement_df = pd.merge(total_measurement_df, site_df, how='outer', on='src_hpo_id')

total_measurement_df = total_measurement_df[['src_hpo_id', 'num_total_records']]

# +
final_measurment_df = pd.merge(total_measurement_df, bad_measurement_records_df, how='outer', on='src_hpo_id') 

final_measurment_df = final_measurment_df.fillna(0)
# -

# ### Now we can actually calculate the 'tangible success rate'

final_measurment_df['measurement_date_failure'] = \
round((final_measurment_df['num_bad_records']) / final_measurment_df['num_total_records'] * 100, 2)

# +
final_measurment_df = final_measurment_df.fillna(0)

final_measurment_df = final_measurment_df.sort_values(by=['measurement_date_failure'], ascending = False)

# +
### Creating a shorter df

# +
short_measurement_df = final_measurment_df.drop(columns=['num_total_records', 'num_bad_records'])

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
  `{DATASET}.unioned_ehr_condition_occurrence` co
  LEFT JOIN
  `{DATASET}._mapping_condition_occurrence` mco
  ON
  co.condition_occurrence_id = mco.condition_occurrence_id
  LEFT JOIN
  `{DATASET}.unioned_ehr_visit_occurrence` vo
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
`{DATASET}.unioned_ehr_condition_occurrence` co
JOIN
`{DATASET}._mapping_condition_occurrence` mco
ON
co.condition_occurrence_id = mco.condition_occurrence_id
GROUP BY 1
ORDER BY num_total_records DESC
""".format(DATASET = DATASET)

total_condition_df = pd.io.gbq.read_gbq(num_total_conditions_query, dialect='standard')

# +
total_condition_df = pd.merge(total_condition_df, site_df, how='outer', on='src_hpo_id')

total_condition_df = total_condition_df[['src_hpo_id', 'num_total_records']]

# +
final_condition_df = pd.merge(total_condition_df, bad_condition_records_df, how='outer', on='src_hpo_id') 

final_condition_df = final_condition_df.fillna(0)
# -

# ### Now we can actually calculate the 'tangible success rate'

final_condition_df['condition_date_failure'] = \
round((final_condition_df['num_bad_records']) / final_condition_df['num_total_records'] * 100, 2)

# +
final_condition_df = final_condition_df.fillna(0)

final_condition_df = final_condition_df.sort_values(by=['condition_date_failure'], ascending = False)
# -

# ### Creating a shorter df

# +
short_condition_df = final_condition_df.drop(columns=['num_total_records', 'num_bad_records'])

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
  `{DATASET}.unioned_ehr_drug_exposure` de
  LEFT JOIN
  `{DATASET}._mapping_drug_exposure` mde
  ON
  de.drug_exposure_id = mde.drug_exposure_id
  LEFT JOIN
  `{DATASET}.unioned_ehr_visit_occurrence` vo
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
`{DATASET}.unioned_ehr_drug_exposure` de
JOIN
`{DATASET}._mapping_drug_exposure` mde
ON
de.drug_exposure_id = mde.drug_exposure_id
GROUP BY 1
ORDER BY num_total_records DESC
""".format(DATASET = DATASET)

total_drug_df = pd.io.gbq.read_gbq(num_total_drugs_query, dialect='standard')

# +
total_drug_df = pd.merge(total_drug_df, site_df, how='outer', on='src_hpo_id')

total_drug_df = total_drug_df[['src_hpo_id', 'num_total_records']]

# +
final_drug_df = pd.merge(total_drug_df, bad_drug_records_df, how='outer', on='src_hpo_id') 

final_drug_df = final_drug_df.fillna(0)
# -

# ### Now we can actually calculate the 'tangible success rate'

final_drug_df['drug_date_failure'] = \
round((final_drug_df['num_bad_records']) / final_drug_df['num_total_records'] * 100, 2)

# +
final_drug_df = final_drug_df.fillna(0)

final_drug_df = final_drug_df.sort_values(by=['drug_date_failure'], ascending = False)
# -

# ### Creating a shorter dataframe

# +
short_drug_df = final_drug_df.drop(columns=['num_total_records', 'num_bad_records'])

short_drug_df
# -

final_success_df = 0

final_success_df = pd.merge(short_drug_df, site_df, how='outer', on='src_hpo_id') 
final_success_df = final_success_df[['src_hpo_id', 'drug_date_failure']]  #rearrang columnds

# +
final_success_df = pd.merge(final_success_df, short_observation_df, how='outer', on='src_hpo_id') 
final_success_df = pd.merge(final_success_df, short_procedure_df, how='outer', on='src_hpo_id')
final_success_df = pd.merge(final_success_df, short_measurement_df, how='outer', on='src_hpo_id') 
final_success_df = pd.merge(final_success_df, short_condition_df, how='outer', on='src_hpo_id')

final_success_df
# -

final_success_df.to_csv("{cwd}/visit_date_disparity.csv".format(cwd = cwd))


