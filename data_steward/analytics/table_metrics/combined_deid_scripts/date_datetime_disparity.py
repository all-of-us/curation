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

# ### This notebook is intended to show the percentage of rows where there is a disparity between the 'date' and the 'datetime' listed for all of the rows in the 6 canonical tables. The 6 canonical tables are as follows:
# - Condition Occurrence
# - Procedure Occurrence
# - Visit Occurrence
# - Drug Exposure
# - Measurement
# - Observation

# ## NOTE: The queries used here detect a datetime_date difference of greater than 1 or less than 0. This leniency is because of a bug that has yet to be resolved (see DC-607). If this issue is resolved, the queries should be adjusted accordingly.

from google.cloud import bigquery
# %reload_ext google.cloud.bigquery
client = bigquery.Client()
# %load_ext google.cloud.bigquery

# +
from notebooks import parameters
DATASET = parameters.UNIONED_Q4_2018
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

site_df = full_names_df
# -

# # Condition Occurrence Table

condition_occurrence_query_str = """
SELECT
DISTINCT
total_rows.src_hpo_id,
-- IFNULL(bad_rows.bad_rows_cnt, 0) as bad_rows_cnt,
-- total_rows.total_rows,
ROUND(IFNULL(bad_rows.bad_rows_cnt, 0) / total_rows.total_rows * 100, 2) as condition_occurrence

FROM

  (SELECT
  DISTINCT
  mco.src_hpo_id, COUNT(*) as total_rows
  FROM
  `{DATASET}.condition_occurrence` co
  JOIN
  `{DATASET}._mapping_condition_occurrence` mco
  ON
  co.condition_occurrence_id = mco.condition_occurrence_id
  GROUP BY mco.src_hpo_id
  ORDER BY total_rows DESC) total_rows

LEFT JOIN

  (SELECT
  DISTINCT
  bad_rows_orig.src_hpo_id, SUM(bad_rows_orig.cnt) as bad_rows_cnt 
  FROM
    (SELECT
    DISTINCT
    mco.src_hpo_id,
    IFNULL(DATE_DIFF(CAST(co.condition_start_datetime AS DATE), co.condition_start_date, DAY), 0) as start_datetime_date_diff,
    IFNULL(DATE_DIFF(CAST(co.condition_end_datetime AS DATE), co.condition_end_date, DAY), 0) as end_datetime_date_diff,
    COUNT(*) as cnt

    FROM
    `{DATASET}.condition_occurrence` co
    JOIN
    `{DATASET}._mapping_condition_occurrence` mco
    ON
    co.condition_occurrence_id = mco.condition_occurrence_id
    WHERE

    -- adjusting for DC-607
    ((IFNULL(DATE_DIFF(CAST(co.condition_start_datetime AS DATE), co.condition_start_date, DAY), 0) > 1
    OR
    IFNULL(DATE_DIFF(CAST(co.condition_start_datetime AS DATE), co.condition_start_date, DAY), 0) < 0)

    OR

    (IFNULL(DATE_DIFF(CAST(co.condition_end_datetime AS DATE), co.condition_end_date, DAY), 0) > 1
    OR
    IFNULL(DATE_DIFF(CAST(co.condition_end_datetime AS DATE), co.condition_end_date, DAY), 0) < 0))

    GROUP BY 1, 2, 3
    ORDER BY cnt DESC) bad_rows_orig

  GROUP BY 1
  ORDER BY bad_rows_cnt DESC) bad_rows

ON

total_rows.src_hpo_id = bad_rows.src_hpo_id

WHERE
LOWER(total_rows.src_hpo_id) NOT LIKE '%rdr%'

GROUP BY 1, 2
ORDER BY condition_occurrence DESC
""".format(DATASET = DATASET)

condition_occurrence_df = pd.io.gbq.read_gbq(condition_occurrence_query_str, dialect ='standard')

condition_occurrence_df

# # Procedure Occurrence Table
# - NOTE: neither start nor end dates; simply date and datetime

procedure_occurrence_query_str = """
SELECT
DISTINCT
total_rows.src_hpo_id,
-- IFNULL(bad_rows.bad_rows_cnt, 0) as bad_rows_cnt,
-- total_rows.total_rows,
ROUND(IFNULL(bad_rows.bad_rows_cnt, 0) / total_rows.total_rows * 100, 2) as procedure_occurrence

FROM

  (SELECT
  DISTINCT
  mpo.src_hpo_id, COUNT(*) as total_rows
  FROM
  `{DATASET}.procedure_occurrence` po
  JOIN
  `{DATASET}._mapping_procedure_occurrence` mpo
  ON
  po.procedure_occurrence_id = mpo.procedure_occurrence_id
  GROUP BY mpo.src_hpo_id
  ORDER BY total_rows DESC) total_rows

LEFT JOIN

  (SELECT
  DISTINCT
  bad_rows_orig.src_hpo_id, SUM(bad_rows_orig.cnt) as bad_rows_cnt 
  FROM
    (SELECT
    DISTINCT
    mpo.src_hpo_id,
    IFNULL(DATE_DIFF(CAST(po.procedure_datetime AS DATE), po.procedure_date, DAY), 0) as start_datetime_date_diff,
    COUNT(*) as cnt

    FROM
    `{DATASET}.procedure_occurrence` po
    JOIN
    `{DATASET}._mapping_procedure_occurrence` mpo
    ON
    po.procedure_occurrence_id = mpo.procedure_occurrence_id

    WHERE

    -- adjusting for DC-607
    IFNULL(DATE_DIFF(CAST(po.procedure_datetime AS DATE), po.procedure_date, DAY), 0) > 1
    OR
    IFNULL(DATE_DIFF(CAST(po.procedure_datetime AS DATE), po.procedure_date, DAY), 0) < 0

    GROUP BY 1, 2
    ORDER BY cnt DESC) bad_rows_orig

  GROUP BY 1
  ORDER BY bad_rows_cnt DESC) bad_rows

ON

total_rows.src_hpo_id = bad_rows.src_hpo_id

WHERE
LOWER(total_rows.src_hpo_id) NOT LIKE '%rdr%'

GROUP BY 1, 2
ORDER BY procedure_occurrence DESC
""".format(DATASET = DATASET)

procedure_occurrence_df = pd.io.gbq.read_gbq(procedure_occurrence_query_str, dialect ='standard')

procedure_occurrence_df

# # Visit Occurrence Table

visit_occurrence_query_str = """
SELECT
DISTINCT
total_rows.src_hpo_id,
-- IFNULL(bad_rows.bad_rows_cnt, 0) as bad_rows_cnt,
-- total_rows.total_rows,
ROUND(IFNULL(bad_rows.bad_rows_cnt, 0) / total_rows.total_rows * 100, 2) as visit_occurrence

FROM

  (SELECT
  DISTINCT
  mvo.src_hpo_id, COUNT(*) as total_rows
  FROM
  `{DATASET}.visit_occurrence` vo
  JOIN
  `{DATASET}._mapping_visit_occurrence` mvo
  ON
  vo.visit_occurrence_id = mvo.visit_occurrence_id
  GROUP BY mvo.src_hpo_id
  ORDER BY total_rows DESC) total_rows

LEFT JOIN

  (SELECT
  DISTINCT
  bad_rows_orig.src_hpo_id, SUM(bad_rows_orig.cnt) as bad_rows_cnt 
  FROM
    (SELECT
    DISTINCT
    mvo.src_hpo_id,
    IFNULL(DATE_DIFF(CAST(vo.visit_start_datetime AS DATE), vo.visit_start_date, DAY), 0) as start_datetime_date_diff,
    IFNULL(DATE_DIFF(CAST(vo.visit_end_datetime AS DATE), vo.visit_end_date, DAY), 0) as end_datetime_date_diff,
    COUNT(*) as cnt

    FROM
    `{DATASET}.visit_occurrence` vo
    JOIN
    `{DATASET}._mapping_visit_occurrence` mvo
    ON
    vo.visit_occurrence_id = mvo.visit_occurrence_id

    WHERE

    -- adjusting for DC-607
    ((IFNULL(DATE_DIFF(CAST(vo.visit_start_datetime AS DATE), vo.visit_start_date, DAY), 0) > 1
    OR
    IFNULL(DATE_DIFF(CAST(vo.visit_start_datetime AS DATE), vo.visit_start_date, DAY), 0) < 0)

    OR

    (IFNULL(DATE_DIFF(CAST(vo.visit_end_datetime AS DATE), vo.visit_end_date, DAY), 0) > 1
    OR
    IFNULL(DATE_DIFF(CAST(vo.visit_end_datetime AS DATE), vo.visit_end_date, DAY), 0) < 0))

    GROUP BY 1, 2, 3
    ORDER BY cnt DESC) bad_rows_orig

  GROUP BY 1
  ORDER BY bad_rows_cnt DESC) bad_rows

ON

total_rows.src_hpo_id = bad_rows.src_hpo_id

WHERE
LOWER(total_rows.src_hpo_id) NOT LIKE '%rdr%'

GROUP BY 1, 2 --, 3, 4
ORDER BY visit_occurrence DESC
""".format(DATASET = DATASET)

visit_occurrence_df = pd.io.gbq.read_gbq(visit_occurrence_query_str, dialect ='standard')

visit_occurrence_df

# # Drug Exposure

drug_exposure_query_str = """
SELECT
DISTINCT
total_rows.src_hpo_id,
-- IFNULL(bad_rows.bad_rows_cnt, 0) as bad_rows_cnt,
-- total_rows.total_rows,
ROUND(IFNULL(bad_rows.bad_rows_cnt, 0) / total_rows.total_rows * 100, 2) as drug_exposure

FROM

  (SELECT
  DISTINCT
  mde.src_hpo_id, COUNT(*) as total_rows
  FROM
  `{DATASET}.drug_exposure` de
  JOIN
  `{DATASET}._mapping_drug_exposure` mde
  ON
  de.drug_exposure_id = mde.drug_exposure_id
  GROUP BY mde.src_hpo_id
  ORDER BY total_rows DESC) total_rows

LEFT JOIN

  (SELECT
  DISTINCT
  bad_rows_orig.src_hpo_id, SUM(bad_rows_orig.cnt) as bad_rows_cnt 
  FROM
    (SELECT
    DISTINCT
    mde.src_hpo_id,
    IFNULL(DATE_DIFF(CAST(de.drug_exposure_start_datetime AS DATE), de.drug_exposure_start_date, DAY), 0) as start_datetime_date_diff,
    IFNULL(DATE_DIFF(CAST(de.drug_exposure_end_datetime AS DATE), de.drug_exposure_end_date, DAY), 0) as end_datetime_date_diff,
    COUNT(*) as cnt

    FROM
    `{DATASET}.drug_exposure` de
    JOIN
    `{DATASET}._mapping_drug_exposure` mde
    ON
    de.drug_exposure_id = mde.drug_exposure_id
    WHERE

    -- adjusting for DC-607
    (IFNULL(DATE_DIFF(CAST(de.drug_exposure_start_datetime AS DATE), de.drug_exposure_start_date, DAY), 0) > 1
    OR
    IFNULL(DATE_DIFF(CAST(de.drug_exposure_start_datetime AS DATE), de.drug_exposure_start_date, DAY), 0) < 0)

    OR

    (
    IFNULL(DATE_DIFF(CAST(de.drug_exposure_end_datetime AS DATE), de.drug_exposure_end_date, DAY), 0) > 1
    OR
    IFNULL(DATE_DIFF(CAST(de.drug_exposure_end_datetime AS DATE), de.drug_exposure_end_date, DAY), 0) < 0
    )

    GROUP BY 1, 2, 3
    ORDER BY cnt DESC) bad_rows_orig

  GROUP BY 1
  ORDER BY bad_rows_cnt DESC) bad_rows

ON

total_rows.src_hpo_id = bad_rows.src_hpo_id

WHERE
LOWER(total_rows.src_hpo_id) NOT LIKE '%rdr%'

GROUP BY 1, 2
ORDER BY drug_exposure DESC
""".format(DATASET = DATASET)

drug_exposure_df = pd.io.gbq.read_gbq(drug_exposure_query_str, dialect ='standard')

drug_exposure_df

# # Measurement Table
# - NOTE: neither start nor end dates; simply date and datetime

measurement_query_str = """
SELECT
DISTINCT
total_rows.src_hpo_id,
-- IFNULL(bad_rows.bad_rows_cnt, 0) as bad_rows_cnt,
-- total_rows.total_rows,
ROUND(IFNULL(bad_rows.bad_rows_cnt, 0) / total_rows.total_rows * 100, 2) as measurement

FROM

  (SELECT
  DISTINCT
  mm.src_hpo_id, COUNT(*) as total_rows
  FROM
  `{DATASET}.measurement` m
  JOIN
  `{DATASET}._mapping_measurement` mm
  ON
  m.measurement_id = mm.measurement_id
  GROUP BY mm.src_hpo_id
  ORDER BY total_rows DESC) total_rows

LEFT JOIN

  (SELECT
  DISTINCT
  bad_rows_orig.src_hpo_id, SUM(bad_rows_orig.cnt) as bad_rows_cnt 
  FROM
    (SELECT
    DISTINCT
    mm.src_hpo_id,
    IFNULL(DATE_DIFF(CAST(m.measurement_datetime AS DATE), m.measurement_date, DAY), 0) as start_datetime_date_diff,
    COUNT(*) as cnt

    FROM
    `{DATASET}.measurement` m
    JOIN
    `{DATASET}._mapping_measurement` mm
    ON
    m.measurement_id = mm.measurement_id

    WHERE

    -- adjusting for DC-607
    IFNULL(DATE_DIFF(CAST(m.measurement_datetime AS DATE), m.measurement_date, DAY), 0) > 1
    OR
    IFNULL(DATE_DIFF(CAST(m.measurement_datetime AS DATE), m.measurement_date, DAY), 0) < 0

    GROUP BY 1, 2
    ORDER BY cnt DESC) bad_rows_orig

  GROUP BY 1
  ORDER BY bad_rows_cnt DESC) bad_rows

ON

total_rows.src_hpo_id = bad_rows.src_hpo_id

WHERE
LOWER(total_rows.src_hpo_id) NOT LIKE '%rdr%'

GROUP BY 1, 2
ORDER BY measurement DESC
""".format(DATASET = DATASET)

measurement_df = pd.io.gbq.read_gbq(measurement_query_str, dialect ='standard')

measurement_df

# # Observation Table
# - NOTE: neither start nor end dates; simply date and datetime

observation_query_str = """
SELECT
DISTINCT
total_rows.src_hpo_id,
-- IFNULL(bad_rows.bad_rows_cnt, 0) as bad_rows_cnt,
-- total_rows.total_rows,
ROUND(IFNULL(bad_rows.bad_rows_cnt, 0) / total_rows.total_rows * 100, 2) as observation

FROM

  (SELECT
  DISTINCT
  mo.src_hpo_id, COUNT(*) as total_rows
  FROM
  `{DATASET}.observation` o
  JOIN
  `{DATASET}._mapping_observation` mo
  ON
  o.observation_id = mo.observation_id
  GROUP BY mo.src_hpo_id
  ORDER BY total_rows DESC) total_rows

LEFT JOIN

  (SELECT
  DISTINCT
  bad_rows_orig.src_hpo_id, SUM(bad_rows_orig.cnt) as bad_rows_cnt 
  FROM
    (SELECT
    DISTINCT
    mo.src_hpo_id,
    IFNULL(DATE_DIFF(CAST(o.observation_datetime AS DATE), o.observation_date, DAY), 0) as start_datetime_date_diff,
    COUNT(*) as cnt

    FROM
    `{DATASET}.observation` o
    JOIN
    `{DATASET}._mapping_observation` mo
    ON
    o.observation_id = mo.observation_id

    WHERE

    -- adjusting for DC-607
    IFNULL(DATE_DIFF(CAST(o.observation_datetime AS DATE), o.observation_date, DAY), 0) > 1
    OR
    IFNULL(DATE_DIFF(CAST(o.observation_datetime AS DATE), o.observation_date, DAY), 0) < 0

    GROUP BY 1, 2
    ORDER BY cnt DESC) bad_rows_orig

  GROUP BY 1
  ORDER BY bad_rows_cnt DESC) bad_rows

ON

total_rows.src_hpo_id = bad_rows.src_hpo_id

WHERE
LOWER(total_rows.src_hpo_id) NOT LIKE '%rdr%'

GROUP BY 1, 2
ORDER BY observation DESC
""".format(DATASET = DATASET)

observation_df = pd.io.gbq.read_gbq(observation_query_str, dialect ='standard')

observation_df

# ## Now to merge all the dataframes

# +
date_datetime_df = pd.merge(
    site_df, observation_df, how='outer', on='src_hpo_id')

date_datetime_df = pd.merge(
    date_datetime_df, measurement_df, how='outer', on='src_hpo_id')

date_datetime_df = pd.merge(
    date_datetime_df, visit_occurrence_df, how='outer', on='src_hpo_id')

date_datetime_df = pd.merge(
    date_datetime_df, procedure_occurrence_df, how='outer', on='src_hpo_id')

date_datetime_df = pd.merge(
    date_datetime_df, drug_exposure_df, how='outer', on='src_hpo_id')

date_datetime_df = pd.merge(
    date_datetime_df, condition_occurrence_df, how='outer', on='src_hpo_id')
# -

date_datetime_df = date_datetime_df.fillna(0)

date_datetime_df

date_datetime_df.to_csv("{cwd}/date_datetime_disparity.csv".format(cwd = cwd))
